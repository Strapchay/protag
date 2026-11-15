package orchestrator

import (
	"testing"
	"time"

	"aion-kernel/internal/dag"
)

func TestRecoveryArbiterStateTransitions(t *testing.T) {
	daemon := testBuildSpecDaemon(t)
	daemon.observeAgentRecovery("agent-alpha", "alpha", "Crashed", "process exited")
	state := daemon.recoverySnapshot("agent-alpha")
	if state.Phase != "process_recovering" || state.Owner != "supervisor" || state.Attempts != 1 {
		t.Fatalf("crashed state = %#v", state)
	}
	daemon.observeAgentRecovery("agent-alpha", "alpha", "Running", "")
	state = daemon.recoverySnapshot("agent-alpha")
	if state.Phase != "idle" || state.Owner != "" {
		t.Fatalf("running state = %#v", state)
	}
}

func TestRecoveryArbiterRecoversInterruptedPhase(t *testing.T) {
	daemon := testBuildSpecDaemon(t)
	attempt := newBuildSpecAttempt(daemon.runState.RunID, "docs/build_spec.md", []byte("spec"))
	attempt.AgentStates = map[string]BuildSpecAgentState{
		"agent-alpha": {
			AgentID: "agent-alpha", DomainID: "alpha", State: "Crashed",
			RecoveryEpoch: 2, RecoveryPhase: "process_recovering", RecoveryOwner: "supervisor",
			RecoveryAttempts: 1, RecoveryBudget: 3,
		},
	}
	if err := saveBuildSpecAttempt(daemon.runState.Root, attempt); err != nil {
		t.Fatal(err)
	}
	daemon.initRecoveryArbiter()
	state := daemon.recoverySnapshot("agent-alpha")
	if state.Phase != "work_recovery_pending" || state.Owner != "arbiter" || state.Epoch != 3 {
		t.Fatalf("interrupted recovery = %#v", state)
	}
}

func TestResumePreflightBlocksSupervisorRecovery(t *testing.T) {
	daemon, attempt := acceptanceFixture(t)
	daemon.recoveryStates = map[string]AgentRecoveryState{
		"agent-alpha": {AgentID: "agent-alpha", Phase: "process_recovering", Owner: "supervisor", UpdatedAt: time.Now()},
	}
	preflight := daemon.buildResumePreflight()
	if preflight.CanResume {
		t.Fatalf("preflight allowed resume during supervisor recovery: %#v", preflight)
	}
	if len(preflight.BlockingReasons) == 0 {
		t.Fatal("preflight did not explain the block")
	}
	if preflight.AttemptID != attempt.AttemptID || len(preflight.ResumePlan) == 0 {
		t.Fatalf("preflight missing state: %#v", preflight)
	}
}

func TestIntegrationFailureDisposition(t *testing.T) {
	if integrationFailureDisposition("directive_execution_failed") != "retry" {
		t.Fatal("directive execution failure should be retryable")
	}
	if integrationFailureDisposition("directive_timeout") != "retry" {
		t.Fatal("directive timeout should be retryable")
	}
	for _, class := range []string{"prepare_failed", "workspace_prepare_failed", "manifest_invalid", ""} {
		if integrationFailureDisposition(class) != "attention" {
			t.Fatalf("%q should require attention", class)
		}
	}
}

func TestResumePreflightAndRetryFailedNode(t *testing.T) {
	daemon, _ := acceptanceFixture(t)
	if err := daemon.dagManager.UpdateNode("alpha-core", dag.StatusFailed); err != nil {
		t.Fatal(err)
	}
	preflight := daemon.buildResumePreflight()
	if !preflight.CanResume || len(preflight.Warnings) == 0 {
		t.Fatalf("failed-node preflight = %#v", preflight)
	}
	if err := daemon.reopenFailedNodesForResume(); err != nil {
		t.Fatal(err)
	}
	node, err := daemon.dagManager.GetNode("alpha-core")
	if err != nil || node.Status != dag.StatusPending {
		t.Fatalf("reopened node = %#v, err=%v", node, err)
	}
}

func TestRecoveryPolicyActionUsesConfig(t *testing.T) {
	daemon := testBuildSpecDaemon(t)
	daemon.config.Coordination.Recovery.Actions = map[string]string{"agent.transient_network": "attention"}
	daemon.config.Coordination.Recovery.MaxAttempts = map[string]int{"agent.transient_network": 4}
	action, attempts := daemon.recoveryPolicyAction("agent.transient_network")
	if action != "attention" || attempts != 4 {
		t.Fatalf("policy = %s/%d", action, attempts)
	}
	action, attempts = daemon.recoveryPolicyAction("unknown.code")
	if action != "attention" || attempts != 2 {
		t.Fatalf("unknown policy = %s/%d", action, attempts)
	}
}

func TestRecoveryQueueDeduplicates(t *testing.T) {
	daemon := testBuildSpecDaemon(t)
	daemon.recoveryMu.Lock()
	daemon.recoveryProcessing = true
	daemon.recoveryMu.Unlock()
	daemon.enqueueRecovery(RecoveryRequest{AgentID: "agent-alpha", Action: "revive", Cause: "test"})
	daemon.enqueueRecovery(RecoveryRequest{AgentID: "agent-alpha", Action: "revive", Cause: "test"})
	daemon.recoveryMu.Lock()
	queued := len(daemon.recoveryQueue)
	daemon.recoveryProcessing = false
	daemon.recoveryMu.Unlock()
	if queued != 1 {
		t.Fatalf("queue length = %d, want 1", queued)
	}
}
