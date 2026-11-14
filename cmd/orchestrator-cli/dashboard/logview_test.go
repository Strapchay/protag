package dashboard

import (
	"errors"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

func TestAgentsPaneRetriesSnapshotAndStreamFailures(t *testing.T) {
	m := NewMultiLogModelWithOptions("127.0.0.1:0", Options{Verbose: true})

	updated, cmd := m.Update(agentHistorySnapshotMsg{Err: errors.New("not ready")})
	m = updated.(*MultiLogModel)
	if cmd == nil || m.reconnectAttempt != 1 {
		t.Fatalf("snapshot failure did not schedule reconnect: attempt=%d cmd=%v", m.reconnectAttempt, cmd)
	}

	updated, cmd = m.Update(agentStreamClosedMsg{Err: errors.New("eof")})
	m = updated.(*MultiLogModel)
	if cmd == nil || m.reconnectAttempt != 2 {
		t.Fatalf("stream failure did not schedule reconnect: attempt=%d cmd=%v", m.reconnectAttempt, cmd)
	}

	updated, _ = m.Update(agentHistorySnapshotMsg{})
	m = updated.(*MultiLogModel)
	if m.reconnectAttempt != 0 {
		t.Fatalf("successful snapshot did not reset reconnect attempt: %d", m.reconnectAttempt)
	}
}

func TestAgentsPaneFiltersNonAgentParticipants(t *testing.T) {
	m := NewMultiLogModel("127.0.0.1:0")
	m.Update(tea.WindowSizeMsg{Width: 90, Height: 24})

	for _, event := range []tuiEventMsg{
		{Audience: tuiAudienceLogs, AgentID: "orchestrator", Kind: tuiKindStatus, Content: "orchestrator status"},
		{Audience: tuiAudienceLogs, AgentID: "user", Kind: tuiKindText, Content: "hello"},
		{Audience: tuiAudienceLogs, AgentID: "coordinator", Kind: tuiKindStatus, Content: "planning"},
		{Audience: tuiAudienceLogs, AgentID: "agent-api", Kind: tuiKindText, Content: "working"},
	} {
		updated, _ := m.Update(event)
		m = updated.(*MultiLogModel)
	}

	if len(m.agents) != 2 {
		t.Fatalf("expected coordinator and one domain agent, got %v", m.agents)
	}
	if m.agents[0] != "coordinator" || m.agents[1] != "agent-api" {
		t.Fatalf("unexpected agent tabs: %v", m.agents)
	}
}

func TestAgentsPaneSeedsFromAgentListSnapshot(t *testing.T) {
	m := NewMultiLogModel("127.0.0.1:0")
	m.Update(tea.WindowSizeMsg{Width: 90, Height: 24})

	updated, _ := m.Update(agentListMsg{Agents: []agentListItem{
		{AgentID: "agent-worker", DomainID: "worker", State: "Running"},
		{AgentID: "coordinator", DomainID: "coordinator", State: "Available"},
	}})
	m = updated.(*MultiLogModel)

	if len(m.agents) != 2 {
		t.Fatalf("expected snapshot agents, got %v", m.agents)
	}
	if m.agents[0] != "coordinator" || m.agents[1] != "agent-worker" {
		t.Fatalf("unexpected snapshot order: %v", m.agents)
	}
	if got := m.agentMeta["agent-worker"].LastStatus; got != "Running" {
		t.Fatalf("snapshot status not recorded, got %q", got)
	}
}

func TestAgentsPaneArrowNavigationWorksWhileFocused(t *testing.T) {
	m := NewMultiLogModel("127.0.0.1:0")
	m.Update(tea.WindowSizeMsg{Width: 90, Height: 24})
	updated, _ := m.Update(agentListMsg{Agents: []agentListItem{
		{AgentID: "coordinator", DomainID: "coordinator", State: "Available"},
		{AgentID: "agent-a", DomainID: "a", State: "Running"},
	}})
	m = updated.(*MultiLogModel)

	if m.activeAgentID() != "coordinator" {
		t.Fatalf("expected coordinator first, got %q", m.activeAgentID())
	}

	updated, _ = m.Update(tea.KeyMsg{Type: tea.KeyRight})
	m = updated.(*MultiLogModel)
	if m.activeAgentID() != "agent-a" {
		t.Fatalf("right arrow should move to next agent, got %q", m.activeAgentID())
	}

	updated, _ = m.Update(tea.KeyMsg{Type: tea.KeyLeft})
	m = updated.(*MultiLogModel)
	if m.activeAgentID() != "coordinator" {
		t.Fatalf("left arrow should move back to coordinator, got %q", m.activeAgentID())
	}
}

func TestAgentsPaneKeepsActiveTabVisibleWhenOverflowing(t *testing.T) {
	m := NewMultiLogModel("127.0.0.1:0")
	m.Update(tea.WindowSizeMsg{Width: 40, Height: 24})
	agents := []agentListItem{
		{AgentID: "coordinator", DomainID: "coordinator", State: "Available"},
		{AgentID: "agent-a", DomainID: "a", State: "Running"},
		{AgentID: "agent-b", DomainID: "b", State: "Running"},
		{AgentID: "agent-c", DomainID: "c", State: "Running"},
		{AgentID: "agent-d", DomainID: "d", State: "Running"},
		{AgentID: "agent-e", DomainID: "e", State: "Running"},
	}
	updated, _ := m.Update(agentListMsg{Agents: agents})
	m = updated.(*MultiLogModel)
	m.activeIdx = len(m.agents) - 1

	view := m.View()
	lines := strings.Split(view, "\n")
	if len(lines) == 0 {
		t.Fatal("empty view")
	}
	if got := lipgloss.Width(lines[0]); got > 40 {
		t.Fatalf("tab line exceeds width: %d > 40\n%s", got, lines[0])
	}
	if !strings.Contains(lines[0], "agent-e") {
		t.Fatalf("active agent should remain visible: %s", lines[0])
	}
}

func TestToolResultRenderingIsBounded(t *testing.T) {
	rendered := formatLogEvent(tuiEventMsg{
		Kind:    tuiKindToolResult,
		Content: "first line\nsecond line that should not render",
	})
	if !strings.Contains(rendered, "Result: first line") {
		t.Fatalf("unexpected tool result rendering: %q", rendered)
	}
	if strings.Contains(rendered, "second line") {
		t.Fatalf("tool result preview was not bounded: %q", rendered)
	}
}

func TestAgentLogCoalescesSnapshotsByBlock(t *testing.T) {
	m := NewMultiLogModel("127.0.0.1:0")

	m.appendAgentEvent("agent-report", tuiEventMsg{
		AgentID: "agent-report", Kind: tuiKindThinking, Content: "first",
		Stream: tuiStreamSnapshot, Block: "agent-report-1",
	})
	m.appendAgentEvent("agent-report", tuiEventMsg{
		AgentID: "agent-report", Kind: tuiKindThinking, Content: "first thought",
		Stream: tuiStreamSnapshot, Block: "agent-report-1",
	})
	if got := len(m.agentOutput["agent-report"]); got != 1 {
		t.Fatalf("expected one coalesced log line, got %d", got)
	}

	m.appendAgentEvent("agent-report", tuiEventMsg{
		AgentID: "agent-report", Kind: tuiKindThinking, Content: "new block",
		Stream: tuiStreamSnapshot, Block: "agent-report-2",
	})
	if got := len(m.agentOutput["agent-report"]); got != 2 {
		t.Fatalf("expected a new line for a new block, got %d", got)
	}
}
