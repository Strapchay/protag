package orchestrator

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
)

const agentCapabilityBytes = 32

type agentCapabilityRegistry struct {
	mu      sync.RWMutex
	byToken map[[sha256.Size]byte]string
	byAgent map[string][sha256.Size]byte
}

func newAgentCapabilityRegistry() *agentCapabilityRegistry {
	return &agentCapabilityRegistry{
		byToken: make(map[[sha256.Size]byte]string),
		byAgent: make(map[string][sha256.Size]byte),
	}
}

func (r *agentCapabilityRegistry) issue(agentID string) (string, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return "", fmt.Errorf("agent capability: agent ID is required")
	}

	raw := make([]byte, agentCapabilityBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("agent capability: generate token: %w", err)
	}
	token := "ac1_" + base64.RawURLEncoding.EncodeToString(raw)
	hash := sha256.Sum256([]byte(token))

	r.mu.Lock()
	defer r.mu.Unlock()
	if previous, ok := r.byAgent[agentID]; ok {
		delete(r.byToken, previous)
	}
	r.byToken[hash] = agentID
	r.byAgent[agentID] = hash
	return token, nil
}

func (r *agentCapabilityRegistry) resolve(token string) (string, bool) {
	token = strings.TrimSpace(token)
	if token == "" {
		return "", false
	}
	hash := sha256.Sum256([]byte(token))
	r.mu.RLock()
	agentID, ok := r.byToken[hash]
	r.mu.RUnlock()
	return agentID, ok
}

func (r *agentCapabilityRegistry) clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byToken = make(map[[sha256.Size]byte]string)
	r.byAgent = make(map[string][sha256.Size]byte)
}
