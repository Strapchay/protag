package coordination

import (
	"context"
	"testing"
)

func TestHardWaitCycleDetectionAndResolution(t *testing.T) {
	manager := testManager(t)
	ctx := context.Background()
	agentA := principal("agent-a", "node-a")
	agentB := principal("agent-b", "node-b")

	contractAB := contractFixture("contract-a-needs-b", agentB.AgentID, "node-b", agentA.AgentID, "node-a")
	contractAB.HardWait = true
	if _, err := manager.Request(ctx, agentA, RequestCommand{Contract: contractAB}); err != nil {
		t.Fatalf("Request A->B: %v", err)
	}
	snapshot, err := manager.Snapshot(ctx)
	if err != nil || len(snapshot.Cycles) != 0 {
		t.Fatalf("unexpected early cycle: %+v err=%v", snapshot.Cycles, err)
	}

	contractBA := contractFixture("contract-b-needs-a", agentA.AgentID, "node-a", agentB.AgentID, "node-b")
	contractBA.HardWait = true
	if _, err := manager.Request(ctx, agentB, RequestCommand{Contract: contractBA}); err != nil {
		t.Fatalf("Request B->A: %v", err)
	}
	snapshot, err = manager.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Cycles) != 1 || len(snapshot.Cycles[0].NodeIDs) != 2 || len(snapshot.Cycles[0].ContractIDs) != 2 {
		t.Fatalf("expected one two-node cycle, got %+v", snapshot.Cycles)
	}
	foundCycleEffect := false
	for _, effect := range snapshot.PendingEffects {
		if effect.Type == EffectCycleDetected {
			foundCycleEffect = true
		}
	}
	if !foundCycleEffect {
		t.Fatal("cycle did not produce a durable recovery effect")
	}

	if _, err := manager.SetWait(ctx, agentB, WaitCommand{ContractID: contractBA.ID, Hard: false}); err != nil {
		t.Fatalf("clear hard wait: %v", err)
	}
	snapshot, err = manager.Snapshot(ctx)
	if err != nil || len(snapshot.Cycles) != 0 {
		t.Fatalf("cycle did not resolve: %+v err=%v", snapshot.Cycles, err)
	}
}

func TestSelfWaitIsRejectedAsContractIdentity(t *testing.T) {
	manager := testManager(t)
	agent := principal("agent-a", "node-a")
	contract := contractFixture("self", agent.AgentID, "node-a", agent.AgentID, "node-a")
	if _, err := manager.Request(context.Background(), agent, RequestCommand{Contract: contract}); err == nil {
		t.Fatal("same-node contract was accepted")
	}
}
