package hub

import (
	"encoding/json"
	"fmt"
	"time"
)

// MessageType identifies the type of context message.
type MessageType string

const (
	// MsgStubFulfilled signals that a stub contract has been fulfilled.
	MsgStubFulfilled MessageType = "StubFulfilled"
	// MsgContextShare delivers context from one agent to another.
	MsgContextShare MessageType = "ContextShare"
	// MsgCorrectionRequest asks a producer to fix a broken stub implementation.
	MsgCorrectionRequest MessageType = "CorrectionRequest"
	// MsgDependencyDiscovered reports a new dependency edge (internal to orchestrator).
	MsgDependencyDiscovered MessageType = "DependencyDiscovered"
	// MsgProgressUpdate reports node status change (internal to orchestrator).
	MsgProgressUpdate MessageType = "ProgressUpdate"
	// MsgSystemStatus broadcasts orchestrator control-plane status to the TUI dashboard.
	MsgSystemStatus MessageType = "SystemStatus"
)

// Message is a typed context message routed through the Context Hub.
type Message struct {
	ID        string          `json:"id"`
	Type      MessageType     `json:"type"`
	FromAgent string          `json:"from_agent"`
	ToAgent   string          `json:"to_agent,omitempty"` // empty for orchestrator-internal
	Payload   json.RawMessage `json:"payload"`
	Timestamp time.Time       `json:"timestamp"`
}

// NewMessage creates a new Message with a generated ID and current timestamp.
func NewMessage(msgType MessageType, fromAgent, toAgent string, payload interface{}) (*Message, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &Message{
		ID:        fmt.Sprintf("msg-%d", time.Now().UnixNano()),
		Type:      msgType,
		FromAgent: fromAgent,
		ToAgent:   toAgent,
		Payload:   data,
		Timestamp: time.Now(),
	}, nil
}

// StubFulfilledPayload is sent when a stub contract has been fulfilled.
type StubFulfilledPayload struct {
	StubID   string `json:"stub_id"`
	Contract string `json:"contract"`
	FilePath string `json:"file_path"`
}

// ContextSharePayload delivers context information between agents.
type ContextSharePayload struct {
	Summary        string   `json:"summary"`
	FileReferences []string `json:"file_references,omitempty"`
}

// CorrectionRequestPayload asks a producer to fix a broken stub.
type CorrectionRequestPayload struct {
	StubID           string `json:"stub_id"`
	ErrorDescription string `json:"error_description"`
	ExpectedContract string `json:"expected_contract"`
}

// DependencyDiscoveredPayload reports a newly discovered dependency.
type DependencyDiscoveredPayload struct {
	FromNode string `json:"from_node"`
	ToNode   string `json:"to_node"`
	Reason   string `json:"reason"`
}

// ProgressUpdatePayload reports a node status change.
type ProgressUpdatePayload struct {
	NodeID    string `json:"node_id"`
	OldStatus string `json:"old_status"`
	NewStatus string `json:"new_status"`
}

// SystemStatusPayload carries a human-readable status string for the TUI status bar.
type SystemStatusPayload struct {
	Text  string `json:"text"`
	Level string `json:"level"` // "info", "warn", "error", "ok"
}
