package dashboard

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

func TestPaneNavigationIsChronologicalWithBacktab(t *testing.T) {
	m := NewModel("127.0.0.1:0")
	m.Update(tea.WindowSizeMsg{Width: 100, Height: 30})

	updated, _ := m.Update(tea.KeyMsg{Type: tea.KeyTab})
	m = updated.(*Model)
	if m.SelectedPane != "chat" {
		t.Fatalf("plain tab should not switch root panes, got %q", m.SelectedPane)
	}

	for i, want := range m.paneOrder() {
		if m.SelectedPane != want {
			t.Fatalf("step %d: got pane %q want %q", i, m.SelectedPane, want)
		}
		updated, _ = m.Update(tea.KeyMsg{Type: tea.KeyShiftTab})
		m = updated.(*Model)
	}
	if m.SelectedPane != "chat" {
		t.Fatalf("expected chronological navigation to wrap to chat, got %q", m.SelectedPane)
	}

	updated, _ = m.Update(tea.KeyMsg{Type: tea.KeyShiftTab})
	m = updated.(*Model)
	if m.SelectedPane != "ops" {
		t.Fatalf("shift+tab should move forward to ops, got %q", m.SelectedPane)
	}

	updated, _ = m.Update(tea.KeyMsg{Type: tea.KeyCtrlK})
	m = updated.(*Model)
	if m.SelectedPane != "chat" {
		t.Fatalf("ctrl+k should move backward to chat, got %q", m.SelectedPane)
	}
}

func TestDashboardViewKeepsTabsVisibleAcrossPanes(t *testing.T) {
	m := NewModel("127.0.0.1:0")
	m.Update(tea.WindowSizeMsg{Width: 80, Height: 24})

	for _, pane := range m.paneOrder() {
		m.SelectedPane = pane
		m.resizePanes()
		view := m.View()
		if !strings.Contains(view, "AION KERNEL DASHBOARD") {
			t.Fatalf("%s pane lost dashboard title", pane)
		}
		if !strings.Contains(view, "Architect Chat") || !strings.Contains(view, "Agents") {
			t.Fatalf("%s pane lost tab row:\n%s", pane, view)
		}
		if got := lipgloss.Height(view); got > m.Height {
			t.Fatalf("%s pane rendered %d rows into %d-row dashboard", pane, got, m.Height)
		}
	}
}
