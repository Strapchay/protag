package orchestrator

import (
	"context"
	"strings"
	"testing"
	"time"

	coordination "aion-coordination"
)

func TestDeadlockScanEscalatesIdleContractOnce(t *testing.T) {
	fixture := newCoordinationKernelFixture(t,
		coordinationNode("producer-node", "producer", "agent-producer"),
		coordinationNode("consumer-node", "consumer", "agent-consumer"),
	)
	ctx := context.Background()
	admin := coordination.Principal{Role: coordination.RoleAdmin}
	contract := coordinationContract("contract-deadlock", "agent-producer", "producer-node", "agent-consumer", "consumer-node")
	contract.Details.Name = "DeadlockBoundary"
	if _, err := fixture.manager.Request(ctx, admin, coordination.RequestCommand{Contract: contract}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.manager.Publish(ctx, admin, coordination.PublishCommand{
		ContractID: contract.ID, ExpectedRevision: 0, Content: []byte("deadlocked artifact"),
	}); err != nil {
		t.Fatal(err)
	}
	fixture.daemon.config = &Config{Coordination: CoordinationConfig{DeadlockTimeoutSec: 10}}

	now := time.Now().Add(15 * time.Second)
	if created := fixture.daemon.scanCoordinationDeadlocks(now); created != 1 {
		t.Fatalf("created %d resolutions, want 1", created)
	}
	resolutions, err := fixture.manager.ListResolutions(ctx)
	if err != nil || len(resolutions) != 1 {
		t.Fatalf("resolutions = %#v, err=%v", resolutions, err)
	}
	if !strings.HasPrefix(resolutions[0].Reason, "automatic deadlock escalation:") {
		t.Fatalf("unexpected reason: %s", resolutions[0].Reason)
	}
	if created := fixture.daemon.scanCoordinationDeadlocks(now); created != 0 {
		t.Fatalf("created %d duplicate resolutions, want 0", created)
	}
	resolutions, err = fixture.manager.ListResolutions(ctx)
	if err != nil || len(resolutions) != 1 {
		t.Fatalf("duplicate resolutions = %#v, err=%v", resolutions, err)
	}
}

func TestDeadlockScanSkipsActiveAndUnstartedWork(t *testing.T) {
	fixture := newCoordinationKernelFixture(t,
		coordinationNode("producer-node", "producer", "agent-producer"),
		coordinationNode("consumer-node", "consumer", "agent-consumer"),
	)
	ctx := context.Background()
	admin := coordination.Principal{Role: coordination.RoleAdmin}
	contract := coordinationContract("contract-active", "agent-producer", "producer-node", "agent-consumer", "consumer-node")
	if _, err := fixture.manager.Request(ctx, admin, coordination.RequestCommand{Contract: contract}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.manager.Publish(ctx, admin, coordination.PublishCommand{
		ContractID: contract.ID, ExpectedRevision: 0, Content: []byte("active artifact"),
	}); err != nil {
		t.Fatal(err)
	}
	contracts, err := fixture.manager.List(ctx, admin)
	if err != nil || len(contracts) != 1 {
		t.Fatalf("contracts = %#v, err=%v", contracts, err)
	}
	now := time.Now().Add(15 * time.Second)
	timeout := 10 * time.Second
	if _, ok := fixture.daemon.coordinationDeadlock(contracts[0], map[string]AgentInfo{
		"agent-producer": {AgentID: "agent-producer", InTurn: true},
	}, now, timeout); ok {
		t.Fatal("active producer was treated as a deadlock")
	}
	if _, ok := fixture.daemon.coordinationDeadlock(contracts[0], map[string]AgentInfo{
		"agent-consumer": {AgentID: "agent-consumer", ToolActive: true},
	}, now, timeout); ok {
		t.Fatal("active consumer tool was treated as a deadlock")
	}
	if _, ok := fixture.daemon.coordinationDeadlock(contracts[0], nil, time.Now().Add(5*time.Second), timeout); ok {
		t.Fatal("recent activity was treated as a deadlock")
	}
}

func TestResolutionOperatorHandlersListShowAndRetry(t *testing.T) {
	fixture := newCoordinationKernelFixture(t,
		coordinationNode("producer-node", "producer", "agent-producer"),
		coordinationNode("consumer-node", "consumer", "agent-consumer"),
	)
	ctx := context.Background()
	admin := coordination.Principal{Role: coordination.RoleAdmin}
	contract := coordinationContract("contract-retry", "agent-producer", "producer-node", "agent-consumer", "consumer-node")
	if _, err := fixture.manager.Request(ctx, admin, coordination.RequestCommand{Contract: contract}); err != nil {
		t.Fatal(err)
	}
	resolution, created, err := fixture.manager.EscalateDeadlock(ctx, contract.ID, "operator retry test", nil, time.Time{})
	if err != nil || !created {
		t.Fatalf("escalate = %#v, created=%v err=%v", resolution, created, err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		claimed, err := fixture.manager.ClaimNextResolution(ctx, time.Minute)
		if err != nil || claimed == nil {
			t.Fatalf("claim %d = %#v, err=%v", attempt, claimed, err)
		}
		if _, err := fixture.manager.FailResolutionAttempt(ctx, claimed.ID, claimed.LeaseToken, "simulated failure"); err != nil {
			t.Fatal(err)
		}
	}
	awaiting, err := fixture.manager.GetResolution(ctx, resolution.ID)
	if err != nil || awaiting.Status != coordination.ResolutionAwaitingUser {
		t.Fatalf("awaiting = %#v, err=%v", awaiting, err)
	}

	rejected := callCoordinationServer(t, fixture.server, "resolution-list", fixture.capabilities["agent-consumer"], map[string]any{})
	if rejected.Error == "" || !strings.Contains(rejected.Error, "operator-only") {
		t.Fatalf("agent resolution-list was accepted: %+v", rejected)
	}
	list := decodeCoordinationResult[struct {
		Resolutions []coordination.Resolution `json:"resolutions"`
	}](t, callCoordinationServer(t, fixture.server, "resolution-list", "", map[string]any{}))
	if len(list.Resolutions) != 1 || list.Resolutions[0].ID != resolution.ID {
		t.Fatalf("resolution-list = %#v", list)
	}
	shown := decodeCoordinationResult[coordination.Resolution](t, callCoordinationServer(t, fixture.server, "resolution-show", "", map[string]any{"id": resolution.ID}))
	if shown.Status != coordination.ResolutionAwaitingUser {
		t.Fatalf("resolution-show = %#v", shown)
	}
	retried := decodeCoordinationResult[coordination.Resolution](t, callCoordinationServer(t, fixture.server, "resolution-retry", "", map[string]any{"id": resolution.ID}))
	if retried.Status != coordination.ResolutionPending || retried.ProcessAttempts != 0 || retried.RepairAttempts != 0 {
		t.Fatalf("resolution-retry = %#v", retried)
	}
	reloaded, err := fixture.manager.GetResolution(ctx, resolution.ID)
	if err != nil || reloaded.Status != coordination.ResolutionPending {
		t.Fatalf("reloaded = %#v, err=%v", reloaded, err)
	}
}
