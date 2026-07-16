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
	for _, want := range []string{"/resume", "/retry", "/continue", "/continue-agents", "/stop-agents", "/gateway-capacity", "/show-spec", "/show-plan", "/show-build-spec-trace", "/coordinator-status", "/progress", "/clear", "/reset-session"} {
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

func TestResponseTextFormatsBuildProgress(t *testing.T) {
	raw := json.RawMessage(`{"client_summary":"Overall build progress is 50%.","overall_percent":50,"milestones":[{"title":"API","status":"active","percent":50,"client_summary":"API is active."}],"warnings":["fallback used"],"open_recoveries":[{"summary":"API task failed.","suggested_command":"/continue-agents"}],"recent_events":[{"summary":"Task API started."}]}`)
	text := responseText(raw)
	for _, want := range []string{"Overall build progress", "API: 50% active", "fallback used", "API task failed", "Task API started"} {
		if !strings.Contains(text, want) {
			t.Fatalf("formatted progress missing %q:\n%s", want, text)
		}
	}
}
