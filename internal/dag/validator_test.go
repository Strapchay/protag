package dag

import "testing"

func TestValidateCyclesNoCycle(t *testing.T) {
	nodes := []DagNode{
		{ID: "a"}, {ID: "b"}, {ID: "c"},
	}
	edges := []DagEdge{
		{FromNode: "a", ToNode: "b"},
		{FromNode: "b", ToNode: "c"},
	}
	if err := ValidateCycles(nodes, edges); err != nil {
		t.Fatalf("expected no cycle: %v", err)
	}
}

func TestValidateCyclesWithCycle(t *testing.T) {
	nodes := []DagNode{
		{ID: "a"}, {ID: "b"}, {ID: "c"},
	}
	edges := []DagEdge{
		{FromNode: "a", ToNode: "b"},
		{FromNode: "b", ToNode: "c"},
		{FromNode: "c", ToNode: "a"}, // cycle
	}
	if err := ValidateCycles(nodes, edges); err == nil {
		t.Fatal("expected cycle detection error")
	}
}

func TestValidateCyclesSelfLoop(t *testing.T) {
	nodes := []DagNode{{ID: "a"}}
	edges := []DagEdge{{FromNode: "a", ToNode: "a"}}
	if err := ValidateCycles(nodes, edges); err == nil {
		t.Fatal("expected self-loop detection")
	}
}

func TestValidateCyclesEmpty(t *testing.T) {
	if err := ValidateCycles(nil, nil); err != nil {
		t.Fatalf("empty graph should be valid: %v", err)
	}
}

func TestValidateCyclesParallel(t *testing.T) {
	// Diamond: a→b, a→c, b→d, c→d (no cycle)
	nodes := []DagNode{
		{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "d"},
	}
	edges := []DagEdge{
		{FromNode: "a", ToNode: "b"},
		{FromNode: "a", ToNode: "c"},
		{FromNode: "b", ToNode: "d"},
		{FromNode: "c", ToNode: "d"},
	}
	if err := ValidateCycles(nodes, edges); err != nil {
		t.Fatalf("diamond should be valid: %v", err)
	}
}

func TestValidateAssignmentsValid(t *testing.T) {
	nodes := []DagNode{
		{ID: "n1", DomainID: "auth", TargetFiles: []string{"auth.go"}},
		{ID: "n2", DomainID: "data", TargetFiles: []string{"db.go"}},
	}
	if err := ValidateAssignments(nodes); err != nil {
		t.Fatalf("expected valid assignments: %v", err)
	}
}

func TestValidateAssignmentsSameDomainOk(t *testing.T) {
	// Same file in multiple nodes of the SAME domain is fine
	nodes := []DagNode{
		{ID: "n1", DomainID: "auth", TargetFiles: []string{"auth.go"}},
		{ID: "n2", DomainID: "auth", TargetFiles: []string{"auth.go"}},
	}
	if err := ValidateAssignments(nodes); err != nil {
		t.Fatalf("same domain same file should be valid: %v", err)
	}
}

func TestValidateAssignmentsConflict(t *testing.T) {
	nodes := []DagNode{
		{ID: "n1", DomainID: "auth", TargetFiles: []string{"shared.go"}},
		{ID: "n2", DomainID: "data", TargetFiles: []string{"shared.go"}},
	}
	if err := ValidateAssignments(nodes); err == nil {
		t.Fatal("expected assignment conflict error")
	}
}

func TestValidateNodeCeilingOk(t *testing.T) {
	nodes := []DagNode{
		{ID: "n1", Status: StatusPending},
		{ID: "n2", Status: StatusDone}, // terminal, doesn't count
	}
	if err := ValidateNodeCeiling(nodes, 5); err != nil {
		t.Fatalf("should be under ceiling: %v", err)
	}
}

func TestValidateNodeCeilingExceeded(t *testing.T) {
	nodes := []DagNode{
		{ID: "n1", Status: StatusPending},
		{ID: "n2", Status: StatusInProgress},
		{ID: "n3", Status: StatusBlocked},
	}
	if err := ValidateNodeCeiling(nodes, 2); err == nil {
		t.Fatal("expected ceiling exceeded error")
	}
}

func TestValidateStubEdgesValid(t *testing.T) {
	nodes := []DagNode{{ID: "a"}, {ID: "b"}}
	edges := []DagEdge{
		{FromNode: "a", ToNode: "b", Type: EdgeStubContract, StubID: "stub-1"},
	}
	if err := ValidateStubEdges(nodes, edges); err != nil {
		t.Fatalf("valid stub edge: %v", err)
	}
}

func TestValidateStubEdgesInvalid(t *testing.T) {
	nodes := []DagNode{{ID: "a"}}
	edges := []DagEdge{
		{FromNode: "a", ToNode: "missing", Type: EdgeStubContract},
	}
	if err := ValidateStubEdges(nodes, edges); err == nil {
		t.Fatal("expected error for missing node")
	}
}

func TestValidateAllValid(t *testing.T) {
	dag := &DagData{
		Header: DagHeader{MaxNodes: 100},
		Nodes: []DagNode{
			{ID: "n1", DomainID: "auth", TargetFiles: []string{"a.go"}, Status: StatusPending},
			{ID: "n2", DomainID: "data", TargetFiles: []string{"b.go"}, Status: StatusPending},
		},
		Edges: []DagEdge{
			{FromNode: "n1", ToNode: "n2", Type: EdgeDependency},
		},
	}
	if err := ValidateAll(dag); err != nil {
		t.Fatalf("expected valid DAG: %v", err)
	}
}

func TestValidateAllCatchesCycle(t *testing.T) {
	dag := &DagData{
		Header: DagHeader{MaxNodes: 100},
		Nodes: []DagNode{
			{ID: "a", Status: StatusPending},
			{ID: "b", Status: StatusPending},
		},
		Edges: []DagEdge{
			{FromNode: "a", ToNode: "b"},
			{FromNode: "b", ToNode: "a"},
		},
	}
	if err := ValidateAll(dag); err == nil {
		t.Fatal("ValidateAll should catch cycle")
	}
}
