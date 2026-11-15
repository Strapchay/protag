package orchestrator

import (
	"testing"
	"time"

	"aion-kernel/internal/dag"
)

func TestAuditorSuppressesPausedBuildSpecStaleNode(t *testing.T) {
	mgr := newTestDagManager(t)
	if err := mgr.AddNode(dag.DagNode{
		ID:        "server-main",
		DomainID:  "server",
		Status:    dag.StatusInProgress,
		CreatedAt: time.Now().Add(-10 * time.Minute).UnixMilli(),
	}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	called := false
	auditor := NewAuditor(mgr, nil, nil, time.Second, time.Hour)
	auditor.SetSuppressStaleNodeFunc(func(node dag.DagNode) bool {
		return node.ID == "server-main"
	})
	auditor.SetStaleNodeFunc(func(dag.DagNode, time.Duration) {
		called = true
	})

	auditor.scan()

	if called {
		t.Fatal("expected suppressed stale node not to invoke stale callback")
	}
}

func TestAuditorReportsUnsuppressedStaleNode(t *testing.T) {
	mgr := newTestDagManager(t)
	if err := mgr.AddNode(dag.DagNode{
		ID:            "server-main",
		DomainID:      "server",
		AssignedAgent: "agent-server",
		Status:        dag.StatusInProgress,
		CreatedAt:     time.Now().Add(-10 * time.Minute).UnixMilli(),
	}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	called := false
	auditor := NewAuditor(mgr, nil, nil, time.Second, time.Hour)
	auditor.SetStaleNodeFunc(func(node dag.DagNode, _ time.Duration) {
		called = node.ID == "server-main"
	})

	auditor.scan()

	if !called {
		t.Fatal("expected stale node callback")
	}
}

func TestAuditorInfersAssignedAgentFromDomain(t *testing.T) {
	mgr := newTestDagManager(t)
	if err := mgr.AddNode(dag.DagNode{
		ID:        "server-main",
		DomainID:  "server",
		Status:    dag.StatusInProgress,
		CreatedAt: time.Now().Add(-10 * time.Minute).UnixMilli(),
	}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	var assigned string
	auditor := NewAuditor(mgr, nil, nil, time.Second, time.Hour)
	auditor.SetStaleNodeFunc(func(node dag.DagNode, _ time.Duration) {
		assigned = node.AssignedAgent
	})
	auditor.scan()
	if assigned != "agent-server" {
		t.Fatalf("assigned agent = %q", assigned)
	}
}

func TestDaemonSuppressesStaleBuildSpecNodeBeforeContinueAgents(t *testing.T) {
	daemon := testBuildSpecDaemon(t)
	attempt := newBuildSpecAttempt(daemon.runState.RunID, "docs/build_spec.md", []byte("hello"))
	attempt.Status = BuildSpecAttemptActive
	attempt.CreatedNodeIDs = []string{"server-main"}
	if err := saveBuildSpecAttempt(daemon.runState.Root, attempt); err != nil {
		t.Fatalf("saveBuildSpecAttempt: %v", err)
	}

	node := dag.DagNode{
		ID:       "server-main",
		DomainID: "server",
		Status:   dag.StatusInProgress,
	}

	if !daemon.shouldSuppressStaleNodeAudit(node) {
		t.Fatal("expected paused persisted build-spec node to be suppressed")
	}
}
