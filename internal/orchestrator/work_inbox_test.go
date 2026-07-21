package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	coordination "aion-coordination"
	"aion-kernel/internal/dag"
	"aion-kernel/internal/hub"
)

func TestWorkInboxRanksContractValidationBeforeReadyNode(t *testing.T) {
	root := t.TempDir()
	dagManager, err := dag.NewManager(dag.ManagerConfig{
		StoreFilePath: filepath.Join(root, "dag.bin"),
		WalFilePath:   filepath.Join(root, "dag.wal"),
		MaxNodes:      20,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dagManager.Close() })
	producer := dag.DagNode{ID: "provider", DomainID: "provider", AssignedAgent: "agent-provider", Status: dag.StatusDone, Priority: 1}
	consumer := dag.DagNode{ID: "consumer", DomainID: "consumer", AssignedAgent: "agent-consumer", Status: dag.StatusPending, Priority: 2, TaskSpec: "consume provider boundary"}
	for _, node := range []dag.DagNode{producer, consumer} {
		if err := dagManager.AddNode(node); err != nil {
			t.Fatal(err)
		}
	}
	manager, err := coordination.Open(coordination.Options{StateDir: filepath.Join(root, "coordination")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	consumerPrincipal := coordination.Principal{Role: coordination.RoleAgent, AgentID: "agent-consumer", NodeIDs: []string{"consumer"}}
	producerPrincipal := coordination.Principal{Role: coordination.RoleAgent, AgentID: "agent-provider", NodeIDs: []string{"provider"}}
	contract, err := manager.Request(context.Background(), consumerPrincipal, coordination.RequestCommand{Contract: coordination.Contract{
		ID: "contract-boundary", ProducerAgentID: "agent-provider", ProducerNodeID: "provider",
		ConsumerAgentID: "agent-consumer", ConsumerNodeID: "consumer",
		Details: coordination.ContractDetails{Name: "Boundary", Kind: "interface", Acceptance: []string{"shape is usable"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Publish(context.Background(), producerPrincipal, coordination.PublishCommand{
		ContractID: contract.ID, ExpectedRevision: 0, Content: []byte(`{"entrypoint":"Boundary"}`), MediaType: "application/json",
	}); err != nil {
		t.Fatal(err)
	}

	service := NewWorkInboxService(dagManager, manager, filepath.Join(root, "checkpoints.json"))
	inbox, err := service.Inbox(context.Background(), consumerPrincipal)
	if err != nil {
		t.Fatal(err)
	}
	if len(inbox.Ready) != 2 {
		t.Fatalf("ready items = %#v", inbox.Ready)
	}
	if inbox.Ready[0].Kind != WorkItemContractValidate || inbox.Ready[1].Kind != WorkItemNode {
		t.Fatalf("unexpected ready order: %#v", inbox.Ready)
	}
	position, err := service.QueuePosition(context.Background(), consumerPrincipal, inbox.Ready[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if position.State != WorkItemReady || position.ReadyAhead != 0 {
		t.Fatalf("position = %#v", position)
	}
}

func TestWorkInboxCheckpointPersistsAcrossServiceRestart(t *testing.T) {
	root := t.TempDir()
	dagManager, err := dag.NewManager(dag.ManagerConfig{
		StoreFilePath: filepath.Join(root, "dag.bin"), WalFilePath: filepath.Join(root, "dag.wal"), MaxNodes: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dagManager.Close() })
	if err := dagManager.AddNode(dag.DagNode{ID: "node-a", DomainID: "a", AssignedAgent: "agent-a", Status: dag.StatusInProgress, TaskSpec: "build a"}); err != nil {
		t.Fatal(err)
	}
	manager, err := coordination.Open(coordination.Options{StateDir: filepath.Join(root, "coordination")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	principal := coordination.Principal{Role: coordination.RoleAgent, AgentID: "agent-a", NodeIDs: []string{"node-a"}}
	path := filepath.Join(root, "checkpoints.json")
	service := NewWorkInboxService(dagManager, manager, path)
	checkpoint, err := service.Checkpoint(context.Background(), principal, "node-a", "implementation started", "run focused tests")
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.AgentID != "agent-a" {
		t.Fatalf("checkpoint = %#v", checkpoint)
	}
	reloaded := NewWorkInboxService(dagManager, manager, path)
	inbox, err := reloaded.Inbox(context.Background(), principal)
	if err != nil {
		t.Fatal(err)
	}
	if inbox.Active == nil || inbox.Active.Checkpoint == nil || inbox.Active.Checkpoint.Next != "run focused tests" {
		t.Fatalf("reloaded inbox = %#v", inbox)
	}
	foreign := coordination.Principal{Role: coordination.RoleAgent, AgentID: "agent-b", NodeIDs: []string{"node-b"}}
	if _, err := reloaded.Checkpoint(context.Background(), foreign, "node-a", "overwrite", "continue"); err == nil || !strings.Contains(err.Error(), "not visible") {
		t.Fatalf("foreign checkpoint error = %v", err)
	}
}

func TestWorkInboxRanksAllContractActionsBeforeReadyNode(t *testing.T) {
	root := t.TempDir()
	dagManager, err := dag.NewManager(dag.ManagerConfig{
		StoreFilePath: filepath.Join(root, "dag.bin"), WalFilePath: filepath.Join(root, "dag.wal"), MaxNodes: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dagManager.Close() })
	nodes := []dag.DagNode{
		{ID: "correct-provider", DomainID: "shared", AssignedAgent: "agent-shared", Status: dag.StatusDone, Priority: 3},
		{ID: "validate-consumer", DomainID: "shared", AssignedAgent: "agent-shared", Status: dag.StatusDone, Priority: 3},
		{ID: "publish-provider", DomainID: "shared", AssignedAgent: "agent-shared", Status: dag.StatusDone, Priority: 3},
		{ID: "ready-node", DomainID: "shared", AssignedAgent: "agent-shared", Status: dag.StatusPending, Priority: 1},
		{ID: "correct-consumer", DomainID: "peer", AssignedAgent: "agent-peer", Status: dag.StatusPending},
		{ID: "validate-provider", DomainID: "peer", AssignedAgent: "agent-peer", Status: dag.StatusDone},
		{ID: "publish-consumer", DomainID: "peer", AssignedAgent: "agent-peer", Status: dag.StatusPending},
	}
	for _, node := range nodes {
		if err := dagManager.AddNode(node); err != nil {
			t.Fatal(err)
		}
	}
	manager, err := coordination.Open(coordination.Options{StateDir: filepath.Join(root, "coordination")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	shared := coordination.Principal{Role: coordination.RoleAgent, AgentID: "agent-shared", NodeIDs: []string{"correct-provider", "validate-consumer", "publish-provider", "ready-node"}}
	peer := coordination.Principal{Role: coordination.RoleAgent, AgentID: "agent-peer", NodeIDs: []string{"correct-consumer", "validate-provider", "publish-consumer"}}

	corrected, err := manager.Request(context.Background(), peer, coordination.RequestCommand{Contract: coordinationContract(
		"contract-correct", "agent-shared", "correct-provider", "agent-peer", "correct-consumer",
	)})
	if err != nil {
		t.Fatal(err)
	}
	corrected, err = manager.Publish(context.Background(), shared, coordination.PublishCommand{ContractID: corrected.ID, Content: []byte("v1")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Correct(context.Background(), peer, coordination.CorrectCommand{ContractID: corrected.ID, Revision: corrected.Revision, Reason: "shape mismatch"}); err != nil {
		t.Fatal(err)
	}

	produced, err := manager.Request(context.Background(), shared, coordination.RequestCommand{Contract: coordinationContract(
		"contract-validate", "agent-peer", "validate-provider", "agent-shared", "validate-consumer",
	)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Publish(context.Background(), peer, coordination.PublishCommand{ContractID: produced.ID, Content: []byte("boundary")}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Request(context.Background(), peer, coordination.RequestCommand{Contract: coordinationContract(
		"contract-publish", "agent-shared", "publish-provider", "agent-peer", "publish-consumer",
	)}); err != nil {
		t.Fatal(err)
	}

	service := NewWorkInboxService(dagManager, manager, "")
	inbox, err := service.Inbox(context.Background(), shared)
	if err != nil {
		t.Fatal(err)
	}
	if len(inbox.Ready) != 4 {
		t.Fatalf("ready items = %#v", inbox.Ready)
	}
	want := []WorkItemKind{WorkItemContractCorrection, WorkItemContractValidate, WorkItemContractPublish, WorkItemNode}
	for i, kind := range want {
		if inbox.Ready[i].Kind != kind || inbox.Ready[i].ReadyRank != i+1 {
			t.Fatalf("ready[%d] = %#v, want kind %s rank %d", i, inbox.Ready[i], kind, i+1)
		}
	}
}

func TestWorkInboxKeepsDependencyBlockedNodeWaiting(t *testing.T) {
	root := t.TempDir()
	dagManager, err := dag.NewManager(dag.ManagerConfig{
		StoreFilePath: filepath.Join(root, "dag.bin"), WalFilePath: filepath.Join(root, "dag.wal"), MaxNodes: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dagManager.Close() })
	for _, node := range []dag.DagNode{
		{ID: "prerequisite", DomainID: "a", AssignedAgent: "agent-a", Status: dag.StatusPending},
		{ID: "dependent", DomainID: "b", AssignedAgent: "agent-b", Status: dag.StatusPending},
	} {
		if err := dagManager.AddNode(node); err != nil {
			t.Fatal(err)
		}
	}
	if err := dagManager.AddEdge(dag.DagEdge{FromNode: "prerequisite", ToNode: "dependent", Type: dag.EdgeDependency}); err != nil {
		t.Fatal(err)
	}
	manager, err := coordination.Open(coordination.Options{StateDir: filepath.Join(root, "coordination")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	service := NewWorkInboxService(dagManager, manager, "")
	inbox, err := service.Inbox(context.Background(), coordination.Principal{Role: coordination.RoleAgent, AgentID: "agent-b", NodeIDs: []string{"dependent"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(inbox.Ready) != 0 || len(inbox.Waiting) != 1 || !strings.Contains(inbox.Waiting[0].WaitingReason, "prerequisite") {
		t.Fatalf("inbox = %#v", inbox)
	}
}

func TestClearBuildSpecArtifactsRemovesWorkCheckpoints(t *testing.T) {
	root := t.TempDir()
	runState := &RunState{
		Root:    root,
		DagFile: filepath.Join(root, "dag.bin"),
		WalFile: filepath.Join(root, "dag.wal"),
	}
	paths := []string{runState.DagFile, runState.WalFile, filepath.Join(root, "work_checkpoints.json")}
	for _, path := range paths {
		if err := os.WriteFile(path, []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := clearBuildSpecArtifacts(runState); err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("artifact %s still exists: %v", path, err)
		}
	}
}

func TestAllocatorConvergenceWaitsForOpenContract(t *testing.T) {
	fixture := newCoordinationKernelFixture(t,
		dag.DagNode{ID: "provider", DomainID: "provider", AssignedAgent: "agent-provider", Status: dag.StatusDone},
		dag.DagNode{ID: "consumer", DomainID: "consumer", AssignedAgent: "agent-consumer", Status: dag.StatusDone},
	)
	principal := fixture.server.coordinationPrincipal("agent-consumer")
	if _, err := fixture.manager.Request(context.Background(), principal, coordination.RequestCommand{Contract: coordinationContract(
		"contract-open", "agent-provider", "provider", "agent-consumer", "consumer",
	)}); err != nil {
		t.Fatal(err)
	}
	service := NewWorkInboxService(fixture.dag, fixture.manager, "")
	allocator := NewAllocator(&Config{}, t.TempDir(), fixture.router, fixture.dag)
	allocator.SetWorkInboxService(service)
	converged, err := allocator.checkConvergence()
	if err != nil {
		t.Fatal(err)
	}
	if converged {
		t.Fatal("DAG converged while a contract remained open")
	}
}

func TestOnlyActionableCoordinationEffectsStartAgentTurns(t *testing.T) {
	for _, effectType := range []coordination.EffectType{
		coordination.EffectContractRequested,
		coordination.EffectContractProduced,
		coordination.EffectCorrectionRequested,
		coordination.EffectTermsProposed,
		coordination.EffectTermsAgreed,
	} {
		if !coordinationEffectRequiresAgentTurn(coordination.Effect{Type: effectType}) {
			t.Fatalf("effect %s should start an agent turn", effectType)
		}
	}
	for _, effectType := range []coordination.EffectType{
		coordination.EffectContractFulfilled,
		coordination.EffectContractTerminal,
		coordination.EffectWaitDeclared,
		coordination.EffectWaitCleared,
	} {
		if coordinationEffectRequiresAgentTurn(coordination.Effect{Type: effectType}) {
			t.Fatalf("status-only effect %s should not start an agent turn", effectType)
		}
	}
}

func TestAllocatorConvergesAfterContractFulfillment(t *testing.T) {
	fixture := newCoordinationKernelFixture(t,
		dag.DagNode{ID: "provider", DomainID: "provider", AssignedAgent: "agent-provider", Status: dag.StatusDone},
		dag.DagNode{ID: "consumer", DomainID: "consumer", AssignedAgent: "agent-consumer", Status: dag.StatusDone},
	)
	consumer := fixture.server.coordinationPrincipal("agent-consumer")
	producer := fixture.server.coordinationPrincipal("agent-provider")
	contract, err := fixture.manager.Request(context.Background(), consumer, coordination.RequestCommand{Contract: coordinationContract(
		"contract-complete", "agent-provider", "provider", "agent-consumer", "consumer",
	)})
	if err != nil {
		t.Fatal(err)
	}
	contract, err = fixture.manager.Publish(context.Background(), producer, coordination.PublishCommand{ContractID: contract.ID, Content: []byte("boundary")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.manager.Validate(context.Background(), consumer, coordination.ValidateCommand{ContractID: contract.ID, Revision: contract.Revision}); err != nil {
		t.Fatal(err)
	}
	service := NewWorkInboxService(fixture.dag, fixture.manager, "")
	if _, err := service.Checkpoint(context.Background(), consumer, contractWorkItemID(contract.ID, WorkItemContractValidate), "stale update", "none"); err == nil || !strings.Contains(err.Error(), "not visible") {
		t.Fatalf("completed work checkpoint error = %v", err)
	}
	allocator := NewAllocator(&Config{}, t.TempDir(), fixture.router, fixture.dag)
	allocator.SetWorkInboxService(service)
	converged, err := allocator.checkConvergence()
	if err != nil {
		t.Fatal(err)
	}
	if !converged {
		t.Fatal("terminal DAG with fulfilled contracts did not converge")
	}
}

func TestContractEnsureIsIdempotentAndReportsProducerQueue(t *testing.T) {
	fixture := newCoordinationKernelFixture(t,
		dag.DagNode{ID: "provider", DomainID: "provider", AssignedAgent: "agent-provider", Status: dag.StatusDone, Priority: 1},
		dag.DagNode{ID: "consumer", DomainID: "consumer", AssignedAgent: "agent-consumer", Status: dag.StatusInProgress, Priority: 2},
	)
	fixture.server.SetWorkInboxService(NewWorkInboxService(fixture.dag, fixture.manager, ""))
	params := map[string]interface{}{
		"producer_node_id": "provider", "consumer_node_id": "consumer", "resource": "public-boundary",
		"details": map[string]interface{}{"name": "PublicBoundary", "kind": "interface", "acceptance": []string{"consumer can parse it"}},
	}
	first := callCoordinationServer(t, fixture.server, "contract-ensure", fixture.capabilities["agent-consumer"], params)
	if first.Error != "" {
		t.Fatal(first.Error)
	}
	second := callCoordinationServer(t, fixture.server, "contract-ensure", fixture.capabilities["agent-consumer"], params)
	if second.Error != "" {
		t.Fatal(second.Error)
	}
	contracts, err := fixture.manager.List(context.Background(), coordination.Principal{Role: coordination.RoleAdmin})
	if err != nil {
		t.Fatal(err)
	}
	if len(contracts) != 1 {
		t.Fatalf("contracts = %#v", contracts)
	}
	if !fixture.agents["agent-provider"].hasMessage(hub.MsgContractRequested) {
		t.Fatal("producer did not receive ensured contract request")
	}
	result, ok := first.Result.(map[string]interface{})
	if !ok || result["producer_work"] == nil {
		t.Fatalf("ensure result = %#v", first.Result)
	}
}

func TestContractEnsureRejectsAgentThatDoesNotOwnConsumerNode(t *testing.T) {
	fixture := newCoordinationKernelFixture(t,
		dag.DagNode{ID: "provider", DomainID: "provider", AssignedAgent: "agent-provider", Status: dag.StatusDone},
		dag.DagNode{ID: "consumer", DomainID: "consumer", AssignedAgent: "agent-consumer", Status: dag.StatusInProgress},
		dag.DagNode{ID: "foreign", DomainID: "foreign", AssignedAgent: "agent-foreign", Status: dag.StatusInProgress},
	)
	response := callCoordinationServer(t, fixture.server, "contract-ensure", fixture.capabilities["agent-foreign"], map[string]interface{}{
		"producer_node_id": "provider", "consumer_node_id": "consumer", "resource": "boundary",
	})
	if response.Error == "" || !strings.Contains(response.Error, "does not own consumer node") {
		t.Fatalf("response = %#v", response)
	}
	contracts, err := fixture.manager.List(context.Background(), coordination.Principal{Role: coordination.RoleAdmin})
	if err != nil {
		t.Fatal(err)
	}
	if len(contracts) != 0 {
		t.Fatalf("unauthorized ensure created contracts: %#v", contracts)
	}
}
