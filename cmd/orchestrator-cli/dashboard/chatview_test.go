package dashboard

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

func TestArchitectCommandHelpIncludesResilienceCommands(t *testing.T) {
	help := architectCommandHelp()
	for _, want := range []string{"/resume", "/retry", "/continue", "/show-spec", "/clear", "/reset-session"} {
		if !strings.Contains(help, want) {
			t.Fatalf("help missing %s:\n%s", want, help)
		}
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
