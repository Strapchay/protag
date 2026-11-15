package orchestrator

import (
	"testing"
	"time"
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
