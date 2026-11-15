package hub

import (
	"encoding/json"
	"fmt"
	"time"
)

// MessageType identifies the type of context message.
type MessageType string

const (
	// MsgContextShare delivers context from one agent to another.
	MsgContextShare MessageType = "ContextShare"
	// MsgCorrectionRequest asks a producer to publish a corrected artifact revision.
	MsgCorrectionRequest MessageType = "CorrectionRequest"
	MsgContractRequested MessageType = "ContractRequested"
	MsgContractProduced  MessageType = "ContractProduced"
	MsgContractFulfilled MessageType = "ContractFulfilled"
	MsgContractWait      MessageType = "ContractWait"
	MsgContractTerminal  MessageType = "ContractTerminal"
	MsgContractCycle     MessageType = "ContractCycle"
	MsgTermsProposed     MessageType = "ContractTermsProposed"
	MsgTermsAgreed       MessageType = "ContractTermsAgreed"
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

// ContextSharePayload delivers context information between agents.
type ContextSharePayload struct {
	Summary        string   `json:"summary"`
	FileReferences []string `json:"file_references,omitempty"`
}

// CorrectionRequestPayload is the subset of a durable contract needed for
// correction telemetry. The full contract remains in Message.Payload.
type CorrectionRequestPayload struct {
	ID               string `json:"id"`
	Revision         int64  `json:"revision"`
	CorrectionReason string `json:"correction_reason"`
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
