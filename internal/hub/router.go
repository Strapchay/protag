package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"aion-kernel/internal/memory"
)

// ContextDeliverer is the interface needed to deliver context messages to agents.
type ContextDeliverer interface {
	DeliverContext(msg Message) error
	AgentID() string
}

// Router routes typed context messages to the correct agent supervisor,
// which then pushes them as follow_up to the Pi Agent.
type Router struct {
	mu         sync.RWMutex
	agents     map[string]ContextDeliverer // agentID → deliverer
	deadLetter map[string][]Message        // agentID → buffered messages
	retries    map[string]int              // msgID → retry count
	failedLog  *os.File                    // sink for unrouteable/dead messages
	memory     memory.Store
}

// NewRouter creates a context hub router.
func NewRouter(logDir string) *Router {
	if logDir == "" {
		logDir = filepath.Join(".aion", "logs")
	}
	_ = os.MkdirAll(logDir, 0755)

	f, err := os.OpenFile(filepath.Join(logDir, "failed_requests.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("hub: warning: could not open failed_requests.log: %v", err)
	}

	return &Router{
		agents:     make(map[string]ContextDeliverer),
		deadLetter: make(map[string][]Message),
		retries:    make(map[string]int),
		failedLog:  f,
	}
}

// Close gracefully closes router resources
func (r *Router) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failedLog != nil {
		r.failedLog.Close()
	}
}

// RegisterAgent registers an agent supervisor for message delivery.
func (r *Router) SetMemoryStore(store memory.Store) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.memory = store
}

// RegisterAgent registers an agent supervisor for message delivery.
func (r *Router) RegisterAgent(agentID string, deliverer ContextDeliverer) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.agents[agentID] = deliverer

	// Drain any dead letters for this agent
	if msgs, ok := r.deadLetter[agentID]; ok {
		log.Printf("hub: draining %d dead letters for agent %s", len(msgs), agentID)
		for _, msg := range msgs {
			go func(m Message) {
				if err := deliverer.DeliverContext(m); err != nil {
					log.Printf("hub: failed to deliver dead letter to %s: %v", agentID, err)
				}
			}(msg)
		}
		delete(r.deadLetter, agentID)
	}
}

// UnregisterAgent removes an agent from the routing table.
func (r *Router) UnregisterAgent(agentID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.agents, agentID)
}

// Route delivers a message to the appropriate agent(s).
func (r *Router) Route(msg Message) error {
	switch msg.Type {
	case MsgStubFulfilled, MsgContextShare, MsgCorrectionRequest:
		if msg.Type == MsgCorrectionRequest {
			r.recordCorrectionRequest(msg)
		}
		return r.routeToAgent(msg)
	case MsgDependencyDiscovered, MsgProgressUpdate, MsgSystemStatus:
		// Internal/TUI messages — no agent delivery needed
		return nil
	default:
		return fmt.Errorf("hub: unknown message type: %s", msg.Type)
	}
}

func (r *Router) recordCorrectionRequest(msg Message) {
	r.mu.RLock()
	mem := r.memory
	r.mu.RUnlock()

	if mem == nil {
		return
	}

	var payload CorrectionRequestPayload
	if err := json.Unmarshal(msg.Payload, &payload); err == nil {
		entry := memory.MemoryEntry{
			ID:        msg.ID,
			Text:      fmt.Sprintf("Correction Request for Stub %s: %s (Expected: %s)", payload.StubID, payload.ErrorDescription, payload.ExpectedContract),
			AgentID:   msg.FromAgent,
			TaskID:    "correction",
			ProjectID: "default",
			Timestamp: time.Now().Unix(),
		}
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := mem.Write(ctx, entry); err != nil {
				log.Printf("hub: failed to write correction memory: %v", err)
			}
		}()
	}
}

func (r *Router) routeToAgent(msg Message) error {
	if msg.ToAgent == "" {
		return fmt.Errorf("hub: message type %s requires a target agent", msg.Type)
	}

	r.mu.RLock()
	deliverer, ok := r.agents[msg.ToAgent]
	r.mu.RUnlock()

	if !ok {
		// Agent not available — buffer as dead letter
		r.mu.Lock()
		r.deadLetter[msg.ToAgent] = append(r.deadLetter[msg.ToAgent], msg)
		r.mu.Unlock()
		log.Printf("hub: agent %s not available, buffered as dead letter", msg.ToAgent)
		return nil
	}

	err := deliverer.DeliverContext(msg)
	if err != nil {
		if msg.Type == MsgCorrectionRequest {
			return r.handleDeliveryFailure(msg, err)
		}
		return err
	}
	return nil
}

func (r *Router) handleDeliveryFailure(msg Message, deliverErr error) error {
	r.mu.Lock()
	count := r.retries[msg.ID]
	r.retries[msg.ID] = count + 1
	r.mu.Unlock()

	maxRetries := 3
	if count >= maxRetries {
		log.Printf("hub: dropping msg %s after %d retries. Err: %v", msg.ID, maxRetries, deliverErr)
		r.logFailedRequest(msg, deliverErr)

		r.mu.Lock()
		delete(r.retries, msg.ID)
		r.mu.Unlock()
		return fmt.Errorf("delivery failed after %d retries: %w", maxRetries, deliverErr)
	}

	backoff := time.Duration(1<<count) * time.Second
	log.Printf("hub: delivery failed for %s to %s, retrying %d/%d in %v...", msg.ID, msg.ToAgent, count+1, maxRetries, backoff)

	// Async retry
	go func() {
		time.Sleep(backoff)
		if err := r.routeToAgent(msg); err != nil {
			log.Printf("hub: background retry failed for %s: %v", msg.ID, err)
		}
	}()

	return nil
}

func (r *Router) logFailedRequest(msg Message, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.failedLog == nil {
		return
	}

	record := map[string]interface{}{
		"timestamp": time.Now().Format(time.RFC3339),
		"message":   msg,
		"error":     err.Error(),
	}

	b, _ := json.Marshal(record)
	r.failedLog.Write(append(b, '\n'))
}

// DrainDeadLetters returns and clears all buffered messages for an agent.
func (r *Router) DrainDeadLetters(agentID string) []Message {
	r.mu.Lock()
	defer r.mu.Unlock()

	msgs := r.deadLetter[agentID]
	delete(r.deadLetter, agentID)
	return msgs
}

// DeadLetterCount returns the number of dead-lettered messages for an agent.
func (r *Router) DeadLetterCount(agentID string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.deadLetter[agentID])
}

// RegisteredAgentCount returns the number of registered agents.
func (r *Router) RegisteredAgentCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.agents)
}
