package orchestrator

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
)

const maxGatewayUsageCapture = 16 << 20

// gatewayTokenUsage is the token usage observed for one agent.
type gatewayTokenUsage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
}

// gatewayUsageCapture retains a bounded sample of an upstream response so the
// final usage object can be parsed without buffering unbounded streams.
type gatewayUsageCapture struct {
	buf bytes.Buffer
}

func (c *gatewayUsageCapture) Write(p []byte) {
	if c == nil || len(p) == 0 {
		return
	}
	if c.buf.Len()+len(p) <= maxGatewayUsageCapture {
		c.buf.Write(p)
		return
	}
	combined := append(c.buf.Bytes(), p...)
	c.buf.Reset()
	c.buf.Write(combined[len(combined)-maxGatewayUsageCapture:])
}

// Usage extracts the largest usage values observed in the captured payload.
// It supports plain JSON responses, OpenAI-compatible SSE chunks, and
// Anthropic message_start/message_delta events.
func (c *gatewayUsageCapture) Usage() (gatewayTokenUsage, bool) {
	if c == nil || c.buf.Len() == 0 {
		return gatewayTokenUsage{}, false
	}
	data := c.buf.Bytes()
	var usage gatewayTokenUsage
	found := false

	if trimmed := bytes.TrimSpace(data); len(trimmed) > 0 && trimmed[0] == '{' {
		var value any
		if json.Unmarshal(trimmed, &value) == nil {
			collectGatewayUsage(value, &usage, &found)
			if found {
				return usage, true
			}
		}
	}

	for _, line := range bytes.Split(data, []byte{'\n'}) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		if bytes.HasPrefix(line, []byte("data:")) {
			line = bytes.TrimSpace(line[len("data:"):])
		}
		if len(line) == 0 || bytes.Equal(line, []byte("[DONE]")) {
			continue
		}
		var value any
		if json.Unmarshal(line, &value) != nil {
			continue
		}
		collectGatewayUsage(value, &usage, &found)
	}
	return usage, found
}

func collectGatewayUsage(value any, usage *gatewayTokenUsage, found *bool) {
	switch typed := value.(type) {
	case map[string]any:
		if raw, ok := typed["usage"]; ok {
			if fields, ok := raw.(map[string]any); ok {
				mergeGatewayUsageFields(fields, usage, found)
			}
		}
		for key, child := range typed {
			if key == "usage" {
				continue
			}
			collectGatewayUsage(child, usage, found)
		}
	case []any:
		for _, child := range typed {
			collectGatewayUsage(child, usage, found)
		}
	}
}

func mergeGatewayUsageFields(fields map[string]any, usage *gatewayTokenUsage, found *bool) {
	prompt, promptOK := firstGatewayInt(fields, "prompt_tokens", "input_tokens", "promptTokens", "inputTokens")
	completion, completionOK := firstGatewayInt(fields, "completion_tokens", "output_tokens", "completionTokens", "outputTokens")
	if !promptOK && !completionOK {
		return
	}
	if promptOK && prompt > usage.PromptTokens {
		usage.PromptTokens = prompt
	}
	if completionOK && completion > usage.CompletionTokens {
		usage.CompletionTokens = completion
	}
	*found = true
}

func firstGatewayInt(fields map[string]any, keys ...string) (int64, bool) {
	for _, key := range keys {
		value, ok := fields[key]
		if !ok {
			continue
		}
		switch typed := value.(type) {
		case float64:
			return int64(typed), true
		case int64:
			return typed, true
		case int:
			return int64(typed), true
		case string:
			parsed, err := strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
			if err == nil {
				return parsed, true
			}
		}
	}
	return 0, false
}

// recordTokenUsage adds one request's usage to the agent's cumulative total.
func (g *InferenceGateway) recordTokenUsage(agentID string, usage gatewayTokenUsage) {
	agentID = strings.TrimSpace(agentID)
	if g == nil || agentID == "" || (usage.PromptTokens <= 0 && usage.CompletionTokens <= 0) {
		return
	}
	g.usageMu.Lock()
	defer g.usageMu.Unlock()
	if g.usage == nil {
		g.usage = make(map[string]gatewayTokenUsage)
	}
	current := g.usage[agentID]
	current.PromptTokens += usage.PromptTokens
	current.CompletionTokens += usage.CompletionTokens
	g.usage[agentID] = current
}

// AgentTokenUsage returns the cumulative usage observed for an agent.
func (g *InferenceGateway) AgentTokenUsage(agentID string) (int64, int64) {
	if g == nil {
		return 0, 0
	}
	g.usageMu.Lock()
	defer g.usageMu.Unlock()
	usage := g.usage[strings.TrimSpace(agentID)]
	return usage.PromptTokens, usage.CompletionTokens
}

func (g *InferenceGateway) agentUsageSnapshot() map[string]GatewayTokenUsage {
	if g == nil {
		return nil
	}
	g.usageMu.Lock()
	defer g.usageMu.Unlock()
	if len(g.usage) == 0 {
		return nil
	}
	snapshot := make(map[string]GatewayTokenUsage, len(g.usage))
	for agentID, usage := range g.usage {
		snapshot[agentID] = GatewayTokenUsage{
			PromptTokens:     usage.PromptTokens,
			CompletionTokens: usage.CompletionTokens,
		}
	}
	return snapshot
}

type GatewayTokenUsage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
}

// SetTokenUsageCallback installs the per-agent cumulative token source used by
// node completion attribution.
func (s *Server) SetTokenUsageCallback(cb func(string) (int64, int64)) {
	s.tokenUsageMu.Lock()
	s.tokenUsageCb = cb
	s.tokenUsageMu.Unlock()
}

// consumeAgentTokenUsage returns usage accumulated since the agent's previous
// completed node and advances the agent baseline.
func (s *Server) consumeAgentTokenUsage(agentID string) (int64, int64) {
	s.tokenUsageMu.Lock()
	defer s.tokenUsageMu.Unlock()
	if s.tokenUsageCb == nil || strings.TrimSpace(agentID) == "" {
		return 0, 0
	}
	prompt, completion := s.tokenUsageCb(agentID)
	if s.tokenUsageBaseline == nil {
		s.tokenUsageBaseline = make(map[string]gatewayTokenUsage)
	}
	previous := s.tokenUsageBaseline[agentID]
	s.tokenUsageBaseline[agentID] = gatewayTokenUsage{PromptTokens: prompt, CompletionTokens: completion}
	prompt -= previous.PromptTokens
	completion -= previous.CompletionTokens
	if prompt < 0 {
		prompt = 0
	}
	if completion < 0 {
		completion = 0
	}
	return prompt, completion
}

func clampTokenCount(value int64) int32 {
	if value <= 0 {
		return 0
	}
	if value > int64(^uint32(0)>>1) {
		return int32(^uint32(0) >> 1)
	}
	return int32(value)
}
