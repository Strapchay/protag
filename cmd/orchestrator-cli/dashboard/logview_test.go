package dashboard

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

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
