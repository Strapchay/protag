package locking

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// ErrDomainDivergence is returned when an agent attempts to lock a file outside its assigned paths.
var ErrDomainDivergence = errors.New("lock: domain divergence - file is outside assigned paths")

// LockEntry represents a held file lock.
type LockEntry struct {
	FilePath   string    `json:"file_path"`
	AgentID    string    `json:"agent_id"`
	AcquiredAt time.Time `json:"acquired_at"`
}

// Manager manages file-level locks for concurrent agent access.
// Only one agent can hold a lock on a specific file at a time.
// Shared boundary files cannot be locked directly — agents must
// use the Utility Agent for modifications.
type Manager struct {
	mu          sync.RWMutex
	locks       map[string]LockEntry // file path → lock entry
	sharedFiles map[string]bool      // files that cannot be directly locked
}

// NewManager creates a lock manager with the given set of shared files.
func NewManager(sharedFiles []string) *Manager {
	sf := make(map[string]bool, len(sharedFiles))
	for _, f := range sharedFiles {
		sf[f] = true
	}
	return &Manager{
		locks:       make(map[string]LockEntry),
		sharedFiles: sf,
	}
}

// Acquire attempts to lock a file for an agent.
// Returns an error if:
//   - The file is outside the agent's assigned domain paths
//   - The file is a shared boundary file (must use Utility Agent)
//   - The file is already locked by a different agent
func (m *Manager) Acquire(filePath, agentID string, assignedPaths []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.sharedFiles[filePath] {
		return fmt.Errorf("lock: file '%s' is a shared boundary file — use Utility Agent", filePath)
	}

	// Boundary check
	allowed := false
	for _, p := range assignedPaths {
		if strings.HasPrefix(filePath, p) {
			allowed = true
			break
		}
	}
	if !allowed && len(assignedPaths) > 0 {
		return fmt.Errorf("%w: requested '%s', assigned %v", ErrDomainDivergence, filePath, assignedPaths)
	}

	if existing, ok := m.locks[filePath]; ok {
		if existing.AgentID == agentID {
			// Same agent re-acquiring — idempotent
			return nil
		}
		return fmt.Errorf("lock: file '%s' already locked by agent '%s'", filePath, existing.AgentID)
	}

	m.locks[filePath] = LockEntry{
		FilePath:   filePath,
		AgentID:    agentID,
		AcquiredAt: time.Now(),
	}
	return nil
}

// Release releases a lock held by the specified agent.
// Returns an error if the agent is not the lock owner.
func (m *Manager) Release(filePath, agentID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.locks[filePath]
	if !ok {
		return nil // not locked — release is idempotent
	}

	if existing.AgentID != agentID {
		return fmt.Errorf("lock: file '%s' is locked by agent '%s', not '%s'", filePath, existing.AgentID, agentID)
	}

	delete(m.locks, filePath)
	return nil
}

// IsLocked returns whether a file is locked and the owner agent ID.
func (m *Manager) IsLocked(filePath string) (bool, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if entry, ok := m.locks[filePath]; ok {
		return true, entry.AgentID
	}
	return false, ""
}

// ListLocks returns all currently held locks.
func (m *Manager) ListLocks() []LockEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries := make([]LockEntry, 0, len(m.locks))
	for _, e := range m.locks {
		entries = append(entries, e)
	}
	return entries
}

// ReleaseAll releases all locks held by a specific agent.
// Used during crash recovery to clean up orphaned locks.
func (m *Manager) ReleaseAll(agentID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	released := 0
	for path, entry := range m.locks {
		if entry.AgentID == agentID {
			delete(m.locks, path)
			released++
		}
	}
	return released
}

// ActiveLockCount returns the total number of held locks.
func (m *Manager) ActiveLockCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.locks)
}

// IsSharedFile returns whether a file is in the shared boundary list.
func (m *Manager) IsSharedFile(filePath string) bool {
	return m.sharedFiles[filePath]
}
