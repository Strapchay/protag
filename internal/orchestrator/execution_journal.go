package orchestrator

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type ExecutionJournalEvent struct {
	EventID   string    `json:"event_id"`
	RunID     string    `json:"run_id,omitempty"`
	AttemptID string    `json:"attempt_id,omitempty"`
	Kind      string    `json:"kind"`
	Severity  string    `json:"severity"`
	AgentID   string    `json:"agent_id,omitempty"`
	DomainID  string    `json:"domain_id,omitempty"`
	NodeID    string    `json:"node_id,omitempty"`
	Summary   string    `json:"summary"`
	Timestamp time.Time `json:"timestamp"`
}

type RecoveryRecord struct {
	FailureID        string    `json:"failure_id"`
	RunID            string    `json:"run_id,omitempty"`
	AttemptID        string    `json:"attempt_id,omitempty"`
	Kind             string    `json:"kind"`
	Severity         string    `json:"severity"`
	Status           string    `json:"status"`
	AgentID          string    `json:"agent_id,omitempty"`
	DomainID         string    `json:"domain_id,omitempty"`
	NodeID           string    `json:"node_id,omitempty"`
	Summary          string    `json:"summary"`
	LastError        string    `json:"last_error,omitempty"`
	SuggestedCommand string    `json:"suggested_command,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func executionJournalPath(runRoot string) string {
	return filepath.Join(runRoot, "execution_journal.jsonl")
}

func recoveryRegistryPath(runRoot string) string {
	return filepath.Join(runRoot, "recovery_registry.json")
}

func appendExecutionJournalEvent(runRoot string, event ExecutionJournalEvent) error {
	if strings.TrimSpace(runRoot) == "" || strings.TrimSpace(event.Kind) == "" {
		return nil
	}
	if event.EventID == "" {
		event.EventID = fmt.Sprintf("evt-%d", time.Now().UnixNano())
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return appendTextFile(executionJournalPath(runRoot), string(data)+"\n")
}

func loadRecentExecutionJournalEvents(runRoot string, limit int) ([]ExecutionJournalEvent, error) {
	if limit <= 0 {
		limit = 20
	}
	f, err := os.Open(executionJournalPath(runRoot))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var events []ExecutionJournalEvent
	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		var event ExecutionJournalEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err == nil {
			events = append(events, event)
		}
	}
	if len(events) > limit {
		events = events[len(events)-limit:]
	}
	return events, scanner.Err()
}

func loadRecoveryRegistry(runRoot string) ([]RecoveryRecord, error) {
	data, err := os.ReadFile(recoveryRegistryPath(runRoot))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var records []RecoveryRecord
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, err
	}
	return records, nil
}

func saveRecoveryRegistry(runRoot string, records []RecoveryRecord) error {
	sort.SliceStable(records, func(i, j int) bool {
		return records[i].FailureID < records[j].FailureID
	})
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(recoveryRegistryPath(runRoot), append(data, '\n'))
}

func upsertRecoveryRecord(runRoot string, record RecoveryRecord) error {
	if strings.TrimSpace(runRoot) == "" || strings.TrimSpace(record.Kind) == "" {
		return nil
	}
	now := time.Now().UTC()
	if record.FailureID == "" {
		record.FailureID = recoveryID(record.Kind, record.NodeID, record.AgentID)
	}
	if record.Status == "" {
		record.Status = "open"
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.UpdatedAt = now

	records, err := loadRecoveryRegistry(runRoot)
	if err != nil {
		return err
	}
	found := false
	for i := range records {
		if records[i].FailureID == record.FailureID {
			record.CreatedAt = records[i].CreatedAt
			records[i] = record
			found = true
			break
		}
	}
	if !found {
		records = append(records, record)
	}
	return saveRecoveryRegistry(runRoot, records)
}

func markRecoveryResolved(runRoot, kind, nodeID, agentID string) error {
	records, err := loadRecoveryRegistry(runRoot)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}
	now := time.Now().UTC()
	changed := false
	for i := range records {
		if records[i].Status != "open" {
			continue
		}
		if kind != "" && records[i].Kind != kind {
			continue
		}
		if nodeID != "" && records[i].NodeID != nodeID {
			continue
		}
		if agentID != "" && records[i].AgentID != agentID {
			continue
		}
		records[i].Status = "recovered"
		records[i].UpdatedAt = now
		changed = true
	}
	if !changed {
		return nil
	}
	return saveRecoveryRegistry(runRoot, records)
}

func recoveryID(kind, nodeID, agentID string) string {
	parts := []string{kind}
	if strings.TrimSpace(nodeID) != "" {
		parts = append(parts, "node", nodeID)
	}
	if strings.TrimSpace(agentID) != "" {
		parts = append(parts, "agent", agentID)
	}
	return strings.NewReplacer(" ", "-", "/", "-", ":", "-").Replace(strings.Join(parts, "-"))
}
