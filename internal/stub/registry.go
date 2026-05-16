package stub

import (
	"fmt"
	"sync"
	"time"
)

// Status represents the lifecycle state of a stub contract.
type Status string

const (
	StatusPending   Status = "Pending"
	StatusFulfilled Status = "Fulfilled"
	StatusRejected  Status = "Rejected"
)

// ContractDetails describes the expected interface for a stub.
type ContractDetails struct {
	Name     string   `json:"name"`
	Kind     string   `json:"kind"`    // "function", "type", "interface"
	Inputs   []string `json:"inputs"`  // parameter types
	Outputs  []string `json:"outputs"` // return types
	FilePath string   `json:"file_path"`
	Language string   `json:"language,omitempty"`
}

// Contract represents a stub contract between a producer and consumer agent.
type Contract struct {
	ID          string          `json:"id"`
	ProducerID  string          `json:"producer_id"` // agent that must fulfill
	ConsumerID  string          `json:"consumer_id"` // agent waiting on fulfillment
	Details     ContractDetails `json:"contract"`
	Status      Status          `json:"status"`
	CreatedAt   time.Time       `json:"created_at"`
	FulfilledAt *time.Time      `json:"fulfilled_at,omitempty"`
}

// Registry manages active stub contracts.
type Registry struct {
	mu    sync.RWMutex
	stubs map[string]*Contract // stub ID → contract
}

// NewRegistry creates an empty stub registry.
func NewRegistry() *Registry {
	return &Registry{
		stubs: make(map[string]*Contract),
	}
}

// Register adds a new stub contract to the registry.
func (r *Registry) Register(contract Contract) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.stubs[contract.ID]; exists {
		return fmt.Errorf("stub: contract '%s' already exists", contract.ID)
	}

	if contract.Status == "" {
		contract.Status = StatusPending
	}
	if contract.CreatedAt.IsZero() {
		contract.CreatedAt = time.Now()
	}

	r.stubs[contract.ID] = &contract
	return nil
}

// Fulfill marks a stub contract as fulfilled. Returns the contract for routing.
func (r *Registry) Fulfill(stubID string) (*Contract, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	stub, ok := r.stubs[stubID]
	if !ok {
		return nil, fmt.Errorf("stub: contract '%s' not found", stubID)
	}

	if stub.Status != StatusPending {
		return nil, fmt.Errorf("stub: contract '%s' is already %s", stubID, stub.Status)
	}

	now := time.Now()
	stub.Status = StatusFulfilled
	stub.FulfilledAt = &now
	return stub, nil
}

// Reject marks a stub contract as rejected.
func (r *Registry) Reject(stubID, reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	stub, ok := r.stubs[stubID]
	if !ok {
		return fmt.Errorf("stub: contract '%s' not found", stubID)
	}

	stub.Status = StatusRejected
	return nil
}

// GetByID returns a stub contract by ID.
func (r *Registry) GetByID(stubID string) (*Contract, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stub, ok := r.stubs[stubID]
	if !ok {
		return nil, fmt.Errorf("stub: contract '%s' not found", stubID)
	}
	return stub, nil
}

// GetPending returns all pending stubs where the given agent is the producer.
func (r *Registry) GetPending(producerID string) []Contract {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []Contract
	for _, s := range r.stubs {
		if s.ProducerID == producerID && s.Status == StatusPending {
			result = append(result, *s)
		}
	}
	return result
}

// GetWaiting returns all pending stubs where the given agent is the consumer.
func (r *Registry) GetWaiting(consumerID string) []Contract {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []Contract
	for _, s := range r.stubs {
		if s.ConsumerID == consumerID && s.Status == StatusPending {
			result = append(result, *s)
		}
	}
	return result
}

// All returns all contracts.
func (r *Registry) All() []Contract {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]Contract, 0, len(r.stubs))
	for _, s := range r.stubs {
		result = append(result, *s)
	}
	return result
}
