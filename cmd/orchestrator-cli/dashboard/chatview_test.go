package dashboard

import (
	"encoding/json"
	"strings"
	"testing"

	"aion-kernel/internal/hub"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

func TestArchitectCommandHelpIncludesResilienceCommands(t *testing.T) {
	help := architectCommandHelp()
	for _, want := range []string{"/resume", "/retry", "/continue", "/continue-agents", "/stop-agents", "/gateway-capacity", "/show-spec", "/show-plan", "/show-build-spec-trace", "/coordinator-status", "/progress", "/integration-status", "/integration-run", "/clear", "/reset-session", "/reset-agent"} {
		if !strings.Contains(help, want) {
			t.Fatalf("help missing %s:\n%s", want, help)
		}
	}
}

func TestChatEscapePreservesBlurUntilNextInput(t *testing.T) {
	m := NewChatModel("127.0.0.1:0")
	updated, _ := m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = updated.(*ChatModel)
	if !m.inputBlurred || m.input.Focused() {
		t.Fatalf("first escape should blur composer: blurred=%v focused=%v", m.inputBlurred, m.input.Focused())
	}
	root := NewModel("127.0.0.1:0")
	root.ChatInput = m
	updatedRoot, _ := root.Update(tea.KeyMsg{Type: tea.KeyPgUp})
	root = updatedRoot.(*Model)
	if root.ChatInput.input.Focused() {
		t.Fatal("root model unexpectedly refocused an explicitly blurred composer")
	}
}

func TestChatSecondEscapeRemainsBlurredForStopDispatch(t *testing.T) {
	root := NewModel("127.0.0.1:0")
	root.ChatInput.input.Blur()
	root.ChatInput.inputBlurred = true
	called := false
	root.stopAgentsFn = func() { called = true }

	updated, _ := root.Update(tea.KeyMsg{Type: tea.KeyEsc})
	root = updated.(*Model)
	if !called {
		t.Fatal("second escape did not dispatch stop-agents")
	}
	if !root.ChatInput.inputBlurred || root.ChatInput.input.Focused() {
		t.Fatalf("second escape should retain the stop-dispatch state: blurred=%v focused=%v", root.ChatInput.inputBlurred, root.ChatInput.input.Focused())
	}
}

func TestCommandSuggestionMatches(t *testing.T) {
	m := NewChatModel("127.0.0.1:0")
	m.input.SetValue("/r")
	matches := m.commandSuggestionMatches()
	var names []string
	for _, match := range matches {
		names = append(names, match.Name)
	}
	got := strings.Join(names, ",")
	for _, want := range []string{"/resume", "/retry", "/replan", "/revive"} {
		if !strings.Contains(got, want) {
			t.Fatalf("suggestions for /r missing %s: %s", want, got)
		}
	}
}

func TestUnknownSlashCommandDoesNotForward(t *testing.T) {
	m := NewChatModel("127.0.0.1:0")
	m.executeCommand("/bogus")
	if len(m.history) != 1 {
		t.Fatalf("expected local error message, got %d history entries", len(m.history))
	}
	if !strings.Contains(m.history[0].Text, "Unknown command") {
		t.Fatalf("unexpected message: %q", m.history[0].Text)
	}
}

func TestChatMessageWrapsWithinViewport(t *testing.T) {
	m := NewChatModel("127.0.0.1:0")
	m.Update(tea.WindowSizeMsg{Width: 38, Height: 12})
	m.AddMessage("orchestrator", "Architect", "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz", lipgloss.NewStyle(), tuiKindText)
	if len(m.history) != 1 {
		t.Fatalf("expected one history item")
	}
	for _, line := range strings.Split(m.history[0].CachedRender, "\n") {
		if got := lipgloss.Width(line); got > 34 {
			t.Fatalf("line width %d exceeds viewport budget: %q", got, line)
		}
	}
}

func TestThinkingEventKeepsDomainAgentAttribution(t *testing.T) {
	m := NewChatModel("127.0.0.1:0")
	m.addTUIEvent(tuiEventMsg{AgentID: "agent-intake", Author: "agent-intake", Kind: tuiKindThinking, Content: "checking integration"})
	if len(m.history) != 1 || m.history[0].Author != "agent-intake Thinking" {
		t.Fatalf("domain thinking attribution = %#v", m.history)
	}
}

func TestChatHydratesFromHubSnapshot(t *testing.T) {
	m := NewChatModel("127.0.0.1:0")
	m.Update(tea.WindowSizeMsg{Width: 80, Height: 20})

	userPayload, _ := json.Marshal(map[string]string{
		"type":    tuiKindText,
		"role":    "user",
		"content": "build me an app",
	})
	architectPayload, _ := json.Marshal(map[string]string{
		"type":    tuiKindText,
		"role":    "assistant",
		"content": "I will refine the spec.",
	})

	updated, _ := m.Update(agentHistorySnapshotMsg{Messages: []hub.Message{
		{FromAgent: "user", ToAgent: "orchestrator", Payload: userPayload},
		{FromAgent: "orchestrator", ToAgent: "tui", Payload: architectPayload},
	}})
	m = updated.(*ChatModel)

	if len(m.history) != 2 {
		t.Fatalf("expected hydrated chat history, got %d", len(m.history))
	}
	if m.history[0].Author != "User" || !strings.Contains(m.history[0].Text, "build me") {
		t.Fatalf("unexpected first history item: %#v", m.history[0])
	}
	if m.history[1].Author != "Architect" || !strings.Contains(m.history[1].Text, "refine") {
		t.Fatalf("unexpected second history item: %#v", m.history[1])
	}
}

func TestAnonymousTranscriptEventDoesNotInheritDestinationIdentity(t *testing.T) {
	payload, _ := json.Marshal(map[string]string{
		"type":    tuiKindText,
		"content": "bootstrap payload",
	})
	events := (&tuiEventNormalizer{}).Normalize(hub.Message{
		ToAgent: "orchestrator",
		Payload: payload,
	})
	if len(events) != 1 {
		t.Fatalf("anonymous event should remain log-only, got %#v", events)
	}
	if events[0].AgentID != "system" || events[0].Author == "Architect" || events[0].Author == "User" {
		t.Fatalf("anonymous event inherited a conversational identity: %#v", events[0])
	}
}

func TestContractEventsRenderConciseMainAndAgentContext(t *testing.T) {
	payload := json.RawMessage(`{"id":"contract-api","producer_agent_id":"agent-api","producer_node_id":"api-node","consumer_agent_id":"agent-web","consumer_node_id":"web-node","status":"Produced","revision":2,"details":{"name":"PublicAPI"}}`)
	events := (&tuiEventNormalizer{}).Normalize(hub.Message{
		Type:      hub.MsgContractProduced,
		FromAgent: "orchestrator",
		ToAgent:   "agent-web",
		Payload:   payload,
	})
	if len(events) != 2 {
		t.Fatalf("expected agent and main events, got %#v", events)
	}
	if events[0].Audience != tuiAudienceLogs || events[0].AgentID != "agent-web" || !strings.Contains(events[0].Content, "revision 2") {
		t.Fatalf("agent contract event = %#v", events[0])
	}
	if events[1].Audience != tuiAudienceChat || events[1].Author != "Orchestrator" || strings.Contains(events[1].Content, `producer_agent_id`) {
		t.Fatalf("main contract event = %#v", events[1])
	}
}

func TestContractCycleRendersAffectedNodes(t *testing.T) {
	events := (&tuiEventNormalizer{}).Normalize(hub.Message{
		Type:      hub.MsgContractCycle,
		FromAgent: "orchestrator",
		Payload:   json.RawMessage(`{"id":"cycle-1","node_ids":["node-a","node-b"],"contract_ids":["contract-a","contract-b"]}`),
	})
	if len(events) != 2 || !strings.Contains(events[1].Content, "node-a, node-b") {
		t.Fatalf("cycle events = %#v", events)
	}
}

func TestResponseTextFormatsBuildProgress(t *testing.T) {
	raw := json.RawMessage(`{"client_summary":"Overall build progress is 50%.","overall_percent":50,"milestones":[{"title":"API","status":"active","percent":50,"client_summary":"API is active."}],"warnings":["fallback used"],"open_recoveries":[{"summary":"API task failed.","suggested_command":"/continue-agents"}],"recent_events":[{"summary":"Task API started."}]}`)
	text := responseText(raw)
	for _, want := range []string{"Overall build progress", "API: 50% active", "fallback used", "API task failed", "Task API started"} {
		if !strings.Contains(text, want) {
			t.Fatalf("formatted progress missing %q:\n%s", want, text)
		}
	}
}

func TestResponseTextFormatsIntegrationStatus(t *testing.T) {
	raw := json.RawMessage(`{"enabled":true,"status":"failed","directive_count":2,"contract_count":1,"error":"acceptance failed","results":[{"directive_id":"api-smoke","status":"failed","error":"exit status 1"}]}`)
	text := responseText(raw)
	for _, want := range []string{"Integration validation: failed", "2 directive(s)", "1 contract(s)", "acceptance failed", "api-smoke"} {
		if !strings.Contains(text, want) {
			t.Fatalf("formatted integration status missing %q:\n%s", want, text)
		}
	}
}

func TestChatCoalescesSnapshotsByStreamBlock(t *testing.T) {
	m := NewChatModel("127.0.0.1:0")
	m.Update(tea.WindowSizeMsg{Width: 80, Height: 20})

	m.addTUIEvent(tuiEventMsg{
		AgentID: "orchestrator", Author: "Architect", Kind: tuiKindThinking,
		Content: "first", Stream: tuiStreamSnapshot, Block: "orchestrator-1",
	})
	m.addTUIEvent(tuiEventMsg{
		AgentID: "orchestrator", Author: "Architect", Kind: tuiKindThinking,
		Content: "first thought", Stream: tuiStreamSnapshot, Block: "orchestrator-1",
	})

	if len(m.history) != 1 {
		t.Fatalf("expected one coalesced thinking block, got %d", len(m.history))
	}
	if m.history[0].Text != "first thought" {
		t.Fatalf("text=%q", m.history[0].Text)
	}

	m.addTUIEvent(tuiEventMsg{
		AgentID: "orchestrator", Author: "Architect", Kind: tuiKindThinking,
		Content: "second block", Stream: tuiStreamSnapshot, Block: "orchestrator-2",
	})
	if len(m.history) != 2 {
		t.Fatalf("expected a new block for a new stream identity, got %d", len(m.history))
	}
}

func TestChatAppendsDeltaWithinSameBlock(t *testing.T) {
	m := NewChatModel("127.0.0.1:0")
	m.Update(tea.WindowSizeMsg{Width: 80, Height: 20})

	m.addTUIEvent(tuiEventMsg{
		AgentID: "orchestrator", Author: "Architect", Kind: tuiKindText,
		Content: "hello", Stream: tuiStreamSnapshot, Block: "orchestrator-1",
	})
	m.addTUIEvent(tuiEventMsg{
		AgentID: "orchestrator", Author: "Architect", Kind: tuiKindText,
		Content: " world", Stream: tuiStreamDelta, Block: "orchestrator-1",
	})

	if len(m.history) != 1 || m.history[0].Text != "hello world" {
		t.Fatalf("delta merge = %#v", m.history)
	}
}

func TestChatNormalizerCarriesStreamMetadata(t *testing.T) {
	msg, err := hub.NewMessage(hub.MsgContextShare, "orchestrator", "tui", map[string]string{
		"type":    tuiKindThinking,
		"content": "snapshot",
		"role":    "assistant",
		"stream":  tuiStreamDelta,
		"block":   "orchestrator-7",
	})
	if err != nil {
		t.Fatal(err)
	}

	events := (&tuiEventNormalizer{}).Normalize(*msg)
	for _, event := range events {
		if event.Audience == tuiAudienceChat {
			if event.Stream != tuiStreamDelta || event.Block != "orchestrator-7" {
				t.Fatalf("stream metadata = %#v", event)
			}
			return
		}
	}
	t.Fatal("missing chat event")
}
