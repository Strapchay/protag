package orchestrator

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"aion-kernel/internal/dag"
)

func TestPrepareIntegrationFailureRecoveryReopensOnlyFailedOwner(t *testing.T) {
	daemon := testBuildSpecDaemon(t)
	daemon.ctx = context.Background()
	t.Cleanup(func() { _ = daemon.dagManager.Close() })
	for _, node := range []dag.DagNode{
		{ID: "ledger-node", DomainID: "ledger", Status: dag.StatusDone, AssignedAgent: "agent-ledger"},
		{ID: "intake-node", DomainID: "intake", Status: dag.StatusDone, AssignedAgent: "agent-intake"},
	} {
		if err := daemon.dagManager.AddNode(node); err != nil {
			t.Fatal(err)
		}
	}

	manifest := IntegrationManifest{Directives: []IntegrationDirectiveArtifact{{
		DomainID: "intake",
		Directive: IntegrationDirective{
			ID: "intake-ledger", OwnerNodeIDs: []string{"intake-node"},
		},
	}}}
	manifestPath := filepath.Join(daemon.runState.Root, "integration", "manifest.json")
	data, _ := json.Marshal(manifest)
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	status := IntegrationRunStatus{
		Status:       IntegrationFailed,
		Error:        "integration directive intake-ledger failed",
		ManifestPath: manifestPath,
		Results: []IntegrationDirectiveResult{{
			DirectiveID: "intake-ledger",
			DomainID:    "intake",
			Status:      "failed",
			ExitCode:    1,
			Stderr:      "ModuleNotFoundError: intake.intake",
		}},
	}

	owners, err := daemon.prepareIntegrationFailureRecovery(status)
	if err != nil {
		t.Fatal(err)
	}
	if len(owners) != 1 || owners[0] != "intake-node" {
		t.Fatalf("recovery owners = %v", owners)
	}
	ledger, _ := daemon.dagManager.GetNode("ledger-node")
	intake, _ := daemon.dagManager.GetNode("intake-node")
	if ledger.Status != dag.StatusDone {
		t.Fatalf("unrelated ledger node reopened to %s", ledger.Status)
	}
	if intake.Status != dag.StatusInProgress || intake.AssignedAgent != "agent-intake" {
		t.Fatalf("failed owner was not reopened with ownership intact: %+v", intake)
	}
	recovery := daemon.allocator.nodeRecoveryContextFor("intake-node")
	if !strings.Contains(recovery, "ModuleNotFoundError") || !strings.Contains(recovery, "intake-ledger") {
		t.Fatalf("recovery context lacks runner evidence:\n%s", recovery)
	}
}

func TestReconcilePersistedRecoveriesClosesStaleBlockedRecord(t *testing.T) {
	daemon := testBuildSpecDaemon(t)
	t.Cleanup(func() { _ = daemon.dagManager.Close() })
	if err := daemon.dagManager.AddNode(dag.DagNode{
		ID: "api-node", DomainID: "api", Status: dag.StatusInProgress, AssignedAgent: "agent-api",
	}); err != nil {
		t.Fatal(err)
	}
	daemon.recordRecovery(RecoveryRecord{
		Kind: "task_blocked", Status: "open", NodeID: "api-node", AgentID: "agent-api",
	})

	daemon.reconcilePersistedRecoveries()
	records, err := loadRecoveryRegistry(daemon.runState.Root)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Status != "recovered" {
		t.Fatalf("stale blocked recovery was not reconciled: %+v", records)
	}
}
