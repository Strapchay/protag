package dashboard

import (
	"encoding/json"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

func TestDecodeDAGSnapshotFromServerShape(t *testing.T) {
	raw := json.RawMessage(`{
		"header":{"schema_version":1,"active_node_count":1,"max_nodes":200},
		"nodes":[{"id":"task-a","domain_id":"api","status":1,"assigned_agent":"agent-api"}],
		"edges":[{"from_node":"task-a","to_node":"task-b","edge_type":0}]
	}`)

	nodes, edges, err := decodeDAGSnapshot(raw)
	if err != nil {
		t.Fatalf("decodeDAGSnapshot: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected one node, got %d", len(nodes))
	}
	if nodes[0].Status != "InProgress" {
		t.Fatalf("expected numeric status to normalize, got %q", nodes[0].Status)
	}
	if nodes[0].DomainID != "api" || nodes[0].AssignedAgent != "agent-api" {
		t.Fatalf("node metadata not preserved: %#v", nodes[0])
	}
	if len(edges) != 1 || edges[0].From != "task-a" || edges[0].To != "task-b" {
		t.Fatalf("edge endpoints not decoded: %#v", edges)
	}
}

func TestDecodeDAGSnapshotFromLegacyNestedShape(t *testing.T) {
	raw := json.RawMessage(`{
		"dag":{
			"nodes":[{"id":"task-a","domain_id":"api","status":"Pending"}],
			"edges":[{"from":"task-a","to":"task-b"}]
		}
	}`)

	nodes, edges, err := decodeDAGSnapshot(raw)
	if err != nil {
		t.Fatalf("decodeDAGSnapshot: %v", err)
	}
	if len(nodes) != 1 || nodes[0].Status != "Pending" {
		t.Fatalf("legacy node decode failed: %#v", nodes)
	}
	if len(edges) != 1 || edges[0].From != "task-a" || edges[0].To != "task-b" {
		t.Fatalf("legacy edge decode failed: %#v", edges)
	}
}

func TestDagViewFallsBackToDomainAgentName(t *testing.T) {
	m := NewDagModel("127.0.0.1:0")
	m.Update(tea.WindowSizeMsg{Width: 100, Height: 24})
	m.nodes = []NodeData{{ID: "task-a", DomainID: "api", Status: "Pending"}}
	m.dirty = true

	view := m.View()
	if !strings.Contains(view, "agent-api") {
		t.Fatalf("expected fallback agent label in view:\n%s", view)
	}
}
