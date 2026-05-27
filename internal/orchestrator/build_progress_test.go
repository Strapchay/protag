package orchestrator

import (
	"os"
	"strings"
	"testing"

	"aion-kernel/internal/coordinator"
	"aion-kernel/internal/dag"
	"aion-kernel/internal/supervisor"
)

func TestBuildProgressUsesCoordinatorMilestonesAndFallbacks(t *testing.T) {
	attempt := newBuildSpecAttempt("run_1", "docs/build_spec.md", []byte("spec"))
	attempt.Plan = &coordinator.PlanResponse{
		Milestones: []coordinator.Milestone{
			{ID: "auth", Title: "Authentication", NodeIDs: []string{"auth-api"}},
		},
	}
	snapshot := &dag.DagData{
		Nodes: []dag.DagNode{
			{ID: "auth-api", DomainID: "api", TaskSpec: "Build auth API", Status: dag.StatusDone},
			{ID: "ui-test", DomainID: "ui", TaskSpec: "Validate UI tests", Status: dag.StatusFailed},
			{ID: "ui-shell", DomainID: "ui", TaskSpec: "Build UI shell", Status: dag.StatusInProgress},
		},
	}

	progress := computeBuildProgress(attempt, snapshot)
	if progress.OverallPercent != 33 {
		t.Fatalf("overall percent = %d, want 33", progress.OverallPercent)
	}
	if len(progress.Milestones) != 3 {
		t.Fatalf("milestones = %d, want 3: %#v", len(progress.Milestones), progress.Milestones)
	}
	byID := map[string]MilestoneProgress{}
	for _, milestone := range progress.Milestones {
		byID[milestone.MilestoneID] = milestone
	}
	if byID["auth"].Status != "complete" || byID["auth"].ClassificationSource != "coordinator" {
		t.Fatalf("unexpected auth milestone: %#v", byID["auth"])
	}
	if byID["validation"].Status != "failed" || byID["validation"].ClassificationSource != "fallback" {
		t.Fatalf("unexpected validation fallback: %#v", byID["validation"])
	}
	if byID["domain:ui"].Status != "active" {
		t.Fatalf("unexpected ui fallback: %#v", byID["domain:ui"])
	}
	if !strings.Contains(progress.ClientSummary, "Overall build progress") {
		t.Fatalf("missing client summary: %q", progress.ClientSummary)
	}
}

func TestBuildProgressSnapshotPersistsRunScoped(t *testing.T) {
	root := t.TempDir()
	snapshot := &BuildProgressSnapshot{
		AttemptID:      "attempt-1",
		Status:         "active",
		OverallPercent: 50,
		DoneNodes:      1,
		TotalNodes:     2,
		ClientSummary:  "Overall build progress is 50%.",
	}
	if err := saveBuildProgressSnapshot(root, snapshot); err != nil {
		t.Fatalf("saveBuildProgressSnapshot: %v", err)
	}
	if _, err := os.Stat(buildProgressSnapshotPath(root)); err != nil {
		t.Fatalf("snapshot file missing: %v", err)
	}
	loaded, err := loadBuildProgressSnapshot(root)
	if err != nil {
		t.Fatalf("loadBuildProgressSnapshot: %v", err)
	}
	if loaded.AttemptID != "attempt-1" || loaded.OverallPercent != 50 {
		t.Fatalf("unexpected loaded snapshot: %#v", loaded)
	}
}

func TestProgressEventSignatureUsesMeaningfulDeltas(t *testing.T) {
	snapshot := &BuildProgressSnapshot{
		Status:         "active",
		OverallPercent: 41,
		DoneNodes:      4,
		TotalNodes:     10,
		Milestones: []MilestoneProgress{{
			MilestoneID: "api",
			Status:      "active",
			Percent:     41,
		}},
	}
	sigA := progressEventSignature(snapshot)
	snapshot.OverallPercent = 44
	snapshot.Milestones[0].Percent = 44
	sigB := progressEventSignature(snapshot)
	if sigA != sigB {
		t.Fatalf("signature changed within same progress bucket: %q != %q", sigA, sigB)
	}
	snapshot.Milestones[0].Status = "blocked"
	sigC := progressEventSignature(snapshot)
	if sigC == sigB {
		t.Fatalf("signature did not change for milestone status change")
	}
}

func TestBuildProgressAppliesOpenRecoveryFacts(t *testing.T) {
	progress := &BuildProgressSnapshot{
		TotalNodes: 1,
		Milestones: []MilestoneProgress{{
			MilestoneID: "api",
			Title:       "API",
			Status:      "active",
			NodeIDs:     []string{"api-node"},
			TotalNodes:  1,
		}},
		OpenRecoveries: []RecoveryRecord{{
			Kind:    "task_blocked",
			Status:  "open",
			NodeID:  "api-node",
			Summary: "API task is waiting on a contract.",
		}},
	}
	applyRecoveryFacts(progress)
	got := progress.Milestones[0]
	if got.Status != "blocked" {
		t.Fatalf("milestone status = %s, want blocked", got.Status)
	}
	if len(got.BlockedNodes) != 1 || got.BlockedNodes[0] != "api-node" {
		t.Fatalf("blocked nodes not applied: %#v", got.BlockedNodes)
	}
	if !strings.Contains(got.ClientSummary, "Recovery") {
		t.Fatalf("recovery summary missing: %q", got.ClientSummary)
	}
}

func TestClassifyLifecycleFailure(t *testing.T) {
	tests := []struct {
		name string
		kind string
		err  string
		want string
	}{
		{name: "auth", kind: "provider_auth_error", err: "401 unauthorized", want: "provider_auth_error"},
		{name: "rate", kind: "tool_error", err: "429 rate limit", want: "provider_rate_limited"},
		{name: "timeout", kind: "network_timeout", err: "request timeout", want: "network_timeout"},
		{name: "context", kind: "agent_error", err: "context window too long", want: "context_window_error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, _, _ := classifyLifecycleFailure("agent-api", supervisor.AgentLifecycleEvent{
				Kind:    tt.kind,
				IsError: true,
				Error:   tt.err,
			})
			if got != tt.want {
				t.Fatalf("kind = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildProgressHandlesMissingMilestones(t *testing.T) {
	attempt := newBuildSpecAttempt("run_1", "docs/build_spec.md", []byte("spec"))
	attempt.Plan = &coordinator.PlanResponse{}
	snapshot := &dag.DagData{Nodes: []dag.DagNode{
		{ID: "api-router", DomainID: "api", Status: dag.StatusDone},
		{ID: "api-db", DomainID: "api", Status: dag.StatusPending},
	}}

	progress := computeBuildProgress(attempt, snapshot)
	if len(progress.Milestones) != 1 {
		t.Fatalf("milestones = %d, want 1", len(progress.Milestones))
	}
	got := progress.Milestones[0]
	if got.MilestoneID != "domain:api" || got.Percent != 50 || got.Status != "active" {
		t.Fatalf("unexpected fallback milestone: %#v", got)
	}
	if len(progress.Warnings) == 0 {
		t.Fatalf("expected fallback warning")
	}
}
