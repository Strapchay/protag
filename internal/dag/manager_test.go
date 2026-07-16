package dag

import (
	"path/filepath"
	"testing"
	"time"
)

func tempManagerConfig(t *testing.T) ManagerConfig {
	t.Helper()
	dir := t.TempDir()
	return ManagerConfig{
		StoreFilePath: filepath.Join(dir, "dag.bin"),
		WalFilePath:   filepath.Join(dir, "dag.wal"),
		StoreSize:     DefaultStoreSize,
		MaxNodes:      200,
		FlushDeadline: 50 * time.Millisecond,
	}
}

func TestManagerAddAndGetNode(t *testing.T) {
	mgr, err := NewManager(tempManagerConfig(t))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	node := DagNode{
		ID:          "test-001",
		DomainID:    "auth",
		TaskSpec:    "Implement JWT",
		Status:      StatusPending,
		TargetFiles: []string{"auth/jwt.go"},
		Priority:    1,
	}

	if err := mgr.AddNode(node); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	got, err := mgr.GetNode("test-001")
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.ID != "test-001" {
		t.Fatalf("expected ID 'test-001', got '%s'", got.ID)
	}
	if got.TaskSpec != "Implement JWT" {
		t.Fatalf("expected task spec 'Implement JWT', got '%s'", got.TaskSpec)
	}
}

func TestManagerUpdateNode(t *testing.T) {
	mgr, err := NewManager(tempManagerConfig(t))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	mgr.AddNode(DagNode{ID: "n1", DomainID: "auth", TaskSpec: "Task 1"})

	if err := mgr.UpdateNode("n1", StatusDone); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	got, _ := mgr.GetNode("n1")
	if got.Status != StatusDone {
		t.Fatalf("expected StatusDone, got %s", got.Status)
	}
	// TaskSpec should be nulled on Done (growth control)
	if got.TaskSpec != "" {
		t.Fatalf("expected empty TaskSpec after Done, got '%s'", got.TaskSpec)
	}
}

func TestManagerUpdateNodeNotFound(t *testing.T) {
	mgr, err := NewManager(tempManagerConfig(t))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	if err := mgr.UpdateNode("nonexistent", StatusDone); err == nil {
		t.Fatal("expected error for nonexistent node")
	}
}

func TestManagerUpdateNodeForAgentEnforcesAssignment(t *testing.T) {
	mgr, err := NewManager(tempManagerConfig(t))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	if err := mgr.AddNode(DagNode{ID: "owned-node", DomainID: "auth", TaskSpec: "Task 1"}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := mgr.AssignNode("owned-node", "agent-auth"); err != nil {
		t.Fatalf("AssignNode: %v", err)
	}

	if err := mgr.UpdateNodeForAgent("owned-node", "agent-data", StatusDone, nil); err == nil {
		t.Fatal("expected assignment mismatch error")
	}
	got, err := mgr.GetNode("owned-node")
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.Status != StatusPending || got.TaskSpec == "" {
		t.Fatalf("rejected update mutated node: %#v", got)
	}

	if err := mgr.UpdateNodeForAgent("owned-node", "agent-auth", StatusDone, nil); err != nil {
		t.Fatalf("owner update: %v", err)
	}
	got, err = mgr.GetNode("owned-node")
	if err != nil {
		t.Fatalf("GetNode after update: %v", err)
	}
	if got.Status != StatusDone {
		t.Fatalf("expected StatusDone, got %s", got.Status)
	}
}

func TestManagerAddEdge(t *testing.T) {
	mgr, err := NewManager(tempManagerConfig(t))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	mgr.AddNode(DagNode{ID: "a", DomainID: "auth", TargetFiles: []string{"a.go"}})
	mgr.AddNode(DagNode{ID: "b", DomainID: "data", TargetFiles: []string{"b.go"}})

	if err := mgr.AddEdge(DagEdge{FromNode: "a", ToNode: "b", Type: EdgeDependency}); err != nil {
		t.Fatalf("AddEdge: %v", err)
	}

	snap := mgr.Snapshot()
	if len(snap.Edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(snap.Edges))
	}
}

func TestManagerAddEdgeCycleRejected(t *testing.T) {
	mgr, err := NewManager(tempManagerConfig(t))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	mgr.AddNode(DagNode{ID: "a", DomainID: "auth", TargetFiles: []string{"a.go"}})
	mgr.AddNode(DagNode{ID: "b", DomainID: "data", TargetFiles: []string{"b.go"}})

	mgr.AddEdge(DagEdge{FromNode: "a", ToNode: "b"})

	// This would create cycle: a→b→a
	if err := mgr.AddEdge(DagEdge{FromNode: "b", ToNode: "a"}); err == nil {
		t.Fatal("expected cycle rejection")
	}
}

func TestManagerSplitNode(t *testing.T) {
	config := tempManagerConfig(t)
	config.MaxNodes = 100
	mgr, err := NewManager(config)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	mgr.AddNode(DagNode{ID: "parent", DomainID: "auth", TaskSpec: "Big task", TargetFiles: []string{"auth.go"}})

	subNodes := []DagNode{
		{ID: "sub-1", DomainID: "auth", TaskSpec: "Part 1", TargetFiles: []string{"auth.go"}},
		{ID: "sub-2", DomainID: "auth", TaskSpec: "Part 2", TargetFiles: []string{"auth.go"}},
	}

	if err := mgr.SplitNode("parent", subNodes); err != nil {
		t.Fatalf("SplitNode: %v", err)
	}

	// Parent should be Done
	parent, _ := mgr.GetNode("parent")
	if parent.Status != StatusDone {
		t.Fatalf("expected parent to be Done, got %s", parent.Status)
	}

	// Sub-nodes should exist
	sub1, err := mgr.GetNode("sub-1")
	if err != nil {
		t.Fatalf("sub-1 not found: %v", err)
	}
	if sub1.TaskSpec != "Part 1" {
		t.Fatalf("expected sub-1 task 'Part 1', got '%s'", sub1.TaskSpec)
	}
}

func TestManagerNodeCeilingEnforced(t *testing.T) {
	config := tempManagerConfig(t)
	config.MaxNodes = 2
	mgr, err := NewManager(config)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	mgr.AddNode(DagNode{ID: "n1", DomainID: "d1", TargetFiles: []string{"a.go"}})
	mgr.AddNode(DagNode{ID: "n2", DomainID: "d2", TargetFiles: []string{"b.go"}})

	// Third node should be rejected (ceiling = 2)
	err = mgr.AddNode(DagNode{ID: "n3", DomainID: "d3", TargetFiles: []string{"c.go"}})
	if err == nil {
		t.Fatal("expected ceiling rejection")
	}
}

func TestManagerGetReadyNodes(t *testing.T) {
	mgr, err := NewManager(tempManagerConfig(t))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	mgr.AddNode(DagNode{ID: "a", DomainID: "d1", TargetFiles: []string{"a.go"}})
	mgr.AddNode(DagNode{ID: "b", DomainID: "d2", TargetFiles: []string{"b.go"}})
	mgr.AddNode(DagNode{ID: "c", DomainID: "d3", TargetFiles: []string{"c.go"}})

	// b depends on a
	mgr.AddEdge(DagEdge{FromNode: "a", ToNode: "b"})

	// Ready: a, c (no deps). Not ready: b (depends on a)
	ready := mgr.GetReadyNodes()
	if len(ready) != 2 {
		t.Fatalf("expected 2 ready nodes, got %d", len(ready))
	}

	readyIDs := make(map[string]bool)
	for _, n := range ready {
		readyIDs[n.ID] = true
	}
	if !readyIDs["a"] || !readyIDs["c"] {
		t.Fatalf("expected a and c to be ready, got %v", readyIDs)
	}

	// Mark a as Done → b should become ready
	mgr.UpdateNode("a", StatusDone)
	ready2 := mgr.GetReadyNodes()

	readyIDs2 := make(map[string]bool)
	for _, n := range ready2 {
		readyIDs2[n.ID] = true
	}
	if !readyIDs2["b"] {
		t.Fatal("expected b to be ready after a is Done")
	}
}

func TestManagerGetNodesByDomain(t *testing.T) {
	mgr, err := NewManager(tempManagerConfig(t))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	mgr.AddNode(DagNode{ID: "a1", DomainID: "auth", TargetFiles: []string{"auth1.go"}})
	mgr.AddNode(DagNode{ID: "a2", DomainID: "auth", TargetFiles: []string{"auth2.go"}})
	mgr.AddNode(DagNode{ID: "d1", DomainID: "data", TargetFiles: []string{"db.go"}})

	authNodes := mgr.GetNodesByDomain("auth")
	if len(authNodes) != 2 {
		t.Fatalf("expected 2 auth nodes, got %d", len(authNodes))
	}

	dataNodes := mgr.GetNodesByDomain("data")
	if len(dataNodes) != 1 {
		t.Fatalf("expected 1 data node, got %d", len(dataNodes))
	}
}

func TestManagerBulkLoad(t *testing.T) {
	mgr, err := NewManager(tempManagerConfig(t))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	dag := &DagData{
		Header: DagHeader{MaxNodes: 200},
		Nodes: []DagNode{
			{ID: "bulk-1", DomainID: "auth", TargetFiles: []string{"a.go"}, Status: StatusPending},
			{ID: "bulk-2", DomainID: "data", TargetFiles: []string{"b.go"}, Status: StatusPending},
		},
		Edges: []DagEdge{
			{FromNode: "bulk-1", ToNode: "bulk-2", Type: EdgeDependency},
		},
	}

	if err := mgr.BulkLoad(dag); err != nil {
		t.Fatalf("BulkLoad: %v", err)
	}

	snap := mgr.Snapshot()
	if len(snap.Nodes) != 2 {
		t.Fatalf("expected 2 nodes after bulk load, got %d", len(snap.Nodes))
	}
}

func TestManagerWALReplay(t *testing.T) {
	config := tempManagerConfig(t)

	// Create manager, add data, close
	mgr1, err := NewManager(config)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	mgr1.AddNode(DagNode{ID: "replay-1", DomainID: "auth", TaskSpec: "Test", TargetFiles: []string{"r.go"}})
	mgr1.UpdateNode("replay-1", StatusInProgress)
	mgr1.Close()

	// Reopen — should replay WAL
	mgr2, err := NewManager(config)
	if err != nil {
		t.Fatalf("NewManager reopen: %v", err)
	}
	defer mgr2.Close()

	got, err := mgr2.GetNode("replay-1")
	if err != nil {
		t.Fatalf("GetNode after replay: %v", err)
	}
	if got.Status != StatusInProgress {
		t.Fatalf("expected StatusInProgress after replay, got %s", got.Status)
	}
}

func TestManagerSnapshot(t *testing.T) {
	mgr, err := NewManager(tempManagerConfig(t))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	mgr.AddNode(DagNode{ID: "snap-1", DomainID: "auth", TargetFiles: []string{"s.go"}})

	snap := mgr.Snapshot()
	if len(snap.Nodes) != 1 {
		t.Fatalf("snapshot should have 1 node")
	}

	// Snapshot should be independent of manager state
	snap.Nodes[0].ID = "modified"
	got, _ := mgr.GetNode("snap-1")
	if got.ID != "snap-1" {
		t.Fatal("snapshot mutation leaked into manager")
	}
}
