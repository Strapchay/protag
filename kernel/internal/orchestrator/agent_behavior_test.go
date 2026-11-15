package orchestrator

import (
	"testing"

	"aion-kernel/internal/supervisor"
)

func TestAgentBehaviorDetectsToolLoop(t *testing.T) {
	daemon := testBuildSpecDaemon(t)
	for i := 0; i < behaviorLoopThreshold; i++ {
		daemon.observeAgentLifecycleBehavior("agent-api", "api", supervisor.AgentLifecycleEvent{
			Kind:      "tool_start",
			ToolName:  "read_file",
			ToolInput: "api.go",
		})
	}
	records, err := loadRecoveryRegistry(daemon.runState.Root)
	if err != nil {
		t.Fatalf("loadRecoveryRegistry: %v", err)
	}
	if !hasOpenRecovery(records, "agent_looping") {
		t.Fatalf("expected agent_looping recovery, got %#v", records)
	}
	store, err := loadAgentBehaviorStore(daemon.runState.Root)
	if err != nil {
		t.Fatalf("loadAgentBehaviorStore: %v", err)
	}
	if store["agent-api"].ConsecutiveToolRepeats != behaviorLoopThreshold {
		t.Fatalf("repeat count = %d", store["agent-api"].ConsecutiveToolRepeats)
	}
}

func TestAgentBehaviorDetectsResumeConfusion(t *testing.T) {
	daemon := testBuildSpecDaemon(t)
	for i := 0; i < behaviorResumeConfuseThreshold; i++ {
		daemon.observeAgentLifecycleBehavior("agent-api", "api", supervisor.AgentLifecycleEvent{
			Kind:    "text",
			Content: "What should I do next? I need more context.",
		})
	}
	records, err := loadRecoveryRegistry(daemon.runState.Root)
	if err != nil {
		t.Fatalf("loadRecoveryRegistry: %v", err)
	}
	if !hasOpenRecovery(records, "resume_context_drift") {
		t.Fatalf("expected resume_context_drift recovery, got %#v", records)
	}
}

func TestAgentBehaviorDetectsCoordinationMisuse(t *testing.T) {
	daemon := testBuildSpecDaemon(t)
	for i := 0; i < 3; i++ {
		daemon.observeAgentCoordinationBehavior("agent-api", "api", "lock_denied", "outside.go")
	}
	records, err := loadRecoveryRegistry(daemon.runState.Root)
	if err != nil {
		t.Fatalf("loadRecoveryRegistry: %v", err)
	}
	if !hasOpenRecovery(records, "coordination_misuse") {
		t.Fatalf("expected coordination_misuse recovery, got %#v", records)
	}
}

func hasOpenRecovery(records []RecoveryRecord, kind string) bool {
	for _, record := range records {
		if record.Kind == kind && record.Status == "open" {
			return true
		}
	}
	return false
}
