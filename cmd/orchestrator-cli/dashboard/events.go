package dashboard

import (
	"encoding/json"
	"fmt"
	"strings"

	"aion-kernel/internal/hub"
)

const (
	tuiAudienceChat   = "chat"
	tuiAudienceLogs   = "logs"
	tuiAudienceStatus = "status"

	tuiKindText       = "text"
	tuiKindThinking   = "thinking"
	tuiKindToolStart  = "tool_start"
	tuiKindToolResult = "tool_result"
	tuiKindRaw        = "raw"
	tuiKindStatus     = "status"

	tuiStreamSnapshot = "snapshot"
	tuiStreamDelta    = "delta"
)

// tuiEventMsg is the dashboard's normalized display contract. Raw hub messages
// are converted once at the stream boundary so panes do not need their own
// payload-specific heuristics.
type tuiEventMsg struct {
	Audience string
	Kind     string
	AgentID  string
	Author   string
	Role     string
	Content  string
	Summary  string
	Tool     string
	Input    string
	Level    string
	Stream   string
	Block    string
	Raw      string
}

type tuiEventNormalizer struct{}

func (n *tuiEventNormalizer) Normalize(msg hub.Message) []tuiEventMsg {
	if msg.Type == hub.MsgSystemStatus {
		var payload struct {
			Text    string `json:"text"`
			Content string `json:"content"`
			Level   string `json:"level"`
		}
		_ = json.Unmarshal(msg.Payload, &payload)
		text := payload.Text
		if text == "" {
			text = payload.Content
		}
		events := []tuiEventMsg{{
			Audience: tuiAudienceStatus,
			Kind:     tuiKindStatus,
			AgentID:  coalesceAgentID(msg),
			Content:  text,
			Level:    payload.Level,
			Raw:      string(msg.Payload),
		}}
		if agentID := strings.TrimSpace(msg.FromAgent); agentID != "" && agentID != "orchestrator" {
			events = append(events, tuiEventMsg{
				Audience: tuiAudienceLogs,
				Kind:     tuiKindStatus,
				AgentID:  agentID,
				Author:   authorFor(agentID, ""),
				Content:  text,
				Level:    payload.Level,
				Raw:      string(msg.Payload),
			})
		}
		return events
	}
	if content, ok := summarizeContractEvent(msg); ok {
		target := strings.TrimSpace(msg.ToAgent)
		if target == "" {
			target = "orchestrator"
		}
		logEvent := tuiEventMsg{
			Audience: tuiAudienceLogs,
			Kind:     tuiKindStatus,
			AgentID:  target,
			Author:   "Orchestrator",
			Content:  content,
			Raw:      string(msg.Payload),
		}
		chatEvent := logEvent
		chatEvent.Audience = tuiAudienceChat
		chatEvent.Kind = tuiKindText
		chatEvent.AgentID = "orchestrator"
		return []tuiEventMsg{logEvent, chatEvent}
	}

	agentID := n.normalizeAgentID(msg)
	payload := parseTUIPayload(msg.Payload)
	kind := payload.Type
	if payload.Role == "toolResult" || payload.Role == "tool" {
		kind = tuiKindToolResult
	}
	if kind == "" {
		kind = tuiKindRaw
	}

	content := payload.Content
	if content == "" {
		content = string(msg.Payload)
	}
	if agentID == "context_prompt" && kind == tuiKindThinking {
		kind = tuiKindText
	}

	event := tuiEventMsg{
		Audience: tuiAudienceLogs,
		Kind:     kind,
		AgentID:  agentID,
		Author:   authorFor(agentID, payload.Role),
		Role:     payload.Role,
		Content:  content,
		Summary:  payload.Summary,
		Tool:     payload.Tool,
		Input:    payload.Input,
		Stream:   normalizeStream(payload.Stream),
		Block:    payload.Block,
		Raw:      string(msg.Payload),
	}

	events := []tuiEventMsg{event}
	if isChatRelevant(msg, agentID, payload.Role, kind) {
		chatEvent := event
		chatEvent.Audience = tuiAudienceChat
		events = append(events, chatEvent)
	}
	return events
}

func summarizeContractEvent(msg hub.Message) (string, bool) {
	type contractPayload struct {
		ID               string `json:"id"`
		ProducerAgentID  string `json:"producer_agent_id"`
		ProducerNodeID   string `json:"producer_node_id"`
		ConsumerAgentID  string `json:"consumer_agent_id"`
		ConsumerNodeID   string `json:"consumer_node_id"`
		Status           string `json:"status"`
		Revision         int64  `json:"revision"`
		CorrectionReason string `json:"correction_reason"`
		TerminalReason   string `json:"terminal_reason"`
		Details          struct {
			Name string `json:"name"`
		} `json:"details"`
	}
	var payload contractPayload
	switch msg.Type {
	case hub.MsgContractRequested, hub.MsgContractProduced, hub.MsgContractFulfilled,
		hub.MsgCorrectionRequest, hub.MsgContractTerminal:
		if err := json.Unmarshal(msg.Payload, &payload); err != nil {
			return "", false
		}
	case hub.MsgContractCycle:
		var cycle struct {
			ID          string   `json:"id"`
			NodeIDs     []string `json:"node_ids"`
			ContractIDs []string `json:"contract_ids"`
		}
		if err := json.Unmarshal(msg.Payload, &cycle); err != nil {
			return "", false
		}
		return fmt.Sprintf("Coordination cycle %s blocked nodes %s (contracts: %s).", cycle.ID, strings.Join(cycle.NodeIDs, ", "), strings.Join(cycle.ContractIDs, ", ")), true
	case hub.MsgTermsProposed, hub.MsgTermsAgreed:
		var terms struct {
			ContractID      string `json:"contract_id"`
			Revision        int64  `json:"revision"`
			ProposerAgentID string `json:"proposer_agent_id"`
		}
		if err := json.Unmarshal(msg.Payload, &terms); err != nil {
			return "", false
		}
		if msg.Type == hub.MsgTermsProposed {
			return fmt.Sprintf("Contract %s terms revision %d was proposed by %s.", terms.ContractID, terms.Revision, terms.ProposerAgentID), true
		}
		return fmt.Sprintf("Contract %s terms revision %d was accepted by both participants.", terms.ContractID, terms.Revision), true
	default:
		return "", false
	}

	label := payload.ID
	if payload.Details.Name != "" {
		label += " (" + payload.Details.Name + ")"
	}
	switch msg.Type {
	case hub.MsgContractRequested:
		return fmt.Sprintf("Contract %s requested by %s for %s.", label, payload.ConsumerNodeID, payload.ProducerNodeID), true
	case hub.MsgContractProduced:
		return fmt.Sprintf("Contract %s revision %d was published for validation by %s.", label, payload.Revision, payload.ConsumerAgentID), true
	case hub.MsgContractFulfilled:
		return fmt.Sprintf("Contract %s revision %d was accepted; dependent work may continue.", label, payload.Revision), true
	case hub.MsgCorrectionRequest:
		return fmt.Sprintf("Contract %s revision %d needs correction: %s", label, payload.Revision, payload.CorrectionReason), true
	case hub.MsgContractTerminal:
		reason := payload.TerminalReason
		if reason == "" {
			reason = "no reason supplied"
		}
		return fmt.Sprintf("Contract %s ended as %s: %s", label, payload.Status, reason), true
	default:
		return "", false
	}
}

type tuiPayload struct {
	Type    string `json:"type"`
	Content string `json:"content"`
	Summary string `json:"summary"`
	Tool    string `json:"tool"`
	Input   string `json:"input"`
	Role    string `json:"role"`
	Stream  string `json:"stream"`
	Block   string `json:"block"`
}

func normalizeStream(stream string) string {
	if stream == tuiStreamDelta {
		return tuiStreamDelta
	}
	return tuiStreamSnapshot
}

func parseTUIPayload(raw json.RawMessage) tuiPayload {
	var payload tuiPayload
	if err := json.Unmarshal(raw, &payload); err == nil {
		return payload
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		payload.Type = tuiKindText
		payload.Content = text
		return payload
	}
	payload.Type = tuiKindRaw
	payload.Content = string(raw)
	return payload
}

func (n *tuiEventNormalizer) normalizeAgentID(msg hub.Message) string {
	agentID := strings.TrimSpace(msg.FromAgent)
	if agentID == "" {
		return "system"
	}
	return agentID
}

func coalesceAgentID(msg hub.Message) string {
	if msg.FromAgent != "" {
		return msg.FromAgent
	}
	return msg.ToAgent
}

func isChatRelevant(msg hub.Message, agentID, role, kind string) bool {
	if kind == tuiKindStatus || kind == tuiKindToolResult {
		return false
	}
	if agentID == "context_prompt" || agentID == "user" {
		return true
	}
	// Domain agents stream into the Agents pane. Architect Chat stays focused
	// on the user-facing planning conversation.
	return agentID == "orchestrator" || agentID == "architect" || agentID == "coordinator"
}

func authorFor(agentID, role string) string {
	if agentID == "context_prompt" || agentID == "user" || role == "user" {
		return "User"
	}
	if agentID == "orchestrator" || agentID == "architect" {
		return "Architect"
	}
	if agentID == "coordinator" {
		return "Coordinator"
	}
	if strings.TrimSpace(agentID) == "" {
		return "System"
	}
	return agentID
}
