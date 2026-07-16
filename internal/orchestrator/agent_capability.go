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
	byToken map[[sha256.Size]byte]AgentCapabilityPolicy
	byAgent map[string][sha256.Size]byte
}

// AgentCapabilityPolicy is the server-owned identity and inference policy for
// one supervised agent generation. Callers receive only the opaque token.
type AgentCapabilityPolicy struct {
	AgentID    string
	DomainID   string
	Profile    string
	Provider   string
	Model      string
	Generation uint64
}

func newAgentCapabilityRegistry() *agentCapabilityRegistry {
	return &agentCapabilityRegistry{
		byToken: make(map[[sha256.Size]byte]AgentCapabilityPolicy),
		byAgent: make(map[string][sha256.Size]byte),
	}
}

func (r *agentCapabilityRegistry) issue(agentID string) (string, error) {
	return r.issuePolicy(AgentCapabilityPolicy{AgentID: agentID})
}

func (r *agentCapabilityRegistry) issuePolicy(policy AgentCapabilityPolicy) (string, error) {
	policy.AgentID = strings.TrimSpace(policy.AgentID)
	policy.DomainID = strings.TrimSpace(policy.DomainID)
	policy.Profile = strings.TrimSpace(policy.Profile)
	policy.Provider = strings.TrimSpace(policy.Provider)
	policy.Model = strings.TrimSpace(policy.Model)
	if policy.AgentID == "" {
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
	if previous, ok := r.byAgent[policy.AgentID]; ok {
		delete(r.byToken, previous)
	}
	r.byToken[hash] = policy
	r.byAgent[policy.AgentID] = hash
	return token, nil
}

func (r *agentCapabilityRegistry) resolve(token string) (string, bool) {
	policy, ok := r.resolvePolicy(token)
	return policy.AgentID, ok
}

func (r *agentCapabilityRegistry) resolvePolicy(token string) (AgentCapabilityPolicy, bool) {
	token = strings.TrimSpace(token)
	if token == "" {
		return AgentCapabilityPolicy{}, false
	}
	hash := sha256.Sum256([]byte(token))
	r.mu.RLock()
	policy, ok := r.byToken[hash]
	r.mu.RUnlock()
	return policy, ok
}

func (r *agentCapabilityRegistry) clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byToken = make(map[[sha256.Size]byte]AgentCapabilityPolicy)
	r.byAgent = make(map[string][sha256.Size]byte)
}
