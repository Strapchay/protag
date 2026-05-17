package dashboard

import (
	"encoding/json"
	"strings"

	"aion-kernel/internal/hub"
)

const (
	tuiAudienceChat   = "chat"
	tuiAudienceLogs   = "logs"
	tuiAudienceStatus = "status"

	tuiKindText      = "text"
	tuiKindThinking  = "thinking"
	tuiKindToolStart = "tool_start"
	tuiKindRaw       = "raw"
	tuiKindStatus    = "status"
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
	Raw      string
}

type tuiEventNormalizer struct {
	foundNamedAgent bool
}

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

	agentID := n.normalizeAgentID(msg)
	payload := parseTUIPayload(msg.Payload)
	kind := payload.Type
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

type tuiPayload struct {
	Type    string `json:"type"`
	Content string `json:"content"`
	Summary string `json:"summary"`
	Tool    string `json:"tool"`
	Input   string `json:"input"`
	Role    string `json:"role"`
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
	agentID := coalesceAgentID(msg)
	if agentID == "" && !n.foundNamedAgent {
		return "context_prompt"
	}
	if agentID == "" {
		return "system"
	}
	if agentID != "context_prompt" && agentID != "system" {
		n.foundNamedAgent = true
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
	if kind == tuiKindStatus {
		return false
	}
	if agentID == "context_prompt" || agentID == "user" || role == "user" {
		return true
	}
	if msg.ToAgent == "tui" {
		return true
	}
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
