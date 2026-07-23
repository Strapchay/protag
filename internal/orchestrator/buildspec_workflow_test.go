package orchestrator

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	coordination "aion-coordination"
	"aion-kernel/internal/coordinator"
	"aion-kernel/internal/dag"
	"aion-kernel/internal/hub"
)

func TestBuildSpecAttemptSaveLoad(t *testing.T) {
	root := t.TempDir()
	attempt := newBuildSpecAttempt("run_1", "docs/build_spec.md", []byte("hello world"))
	attempt.Status = BuildSpecAttemptPlanning
	attempt.RecordAgentState("agent-a", "domain-a", "Running", "")
	if err := saveBuildSpecAttempt(root, attempt); err != nil {
		t.Fatalf("saveBuildSpecAttempt: %v", err)
	}
	loaded, err := loadBuildSpecAttempt(root)
	if err != nil {
		t.Fatalf("loadBuildSpecAttempt: %v", err)
	}
	if loaded == nil || loaded.SpecHash != attempt.SpecHash || loaded.Status != BuildSpecAttemptPlanning {
		t.Fatalf("unexpected loaded attempt: %#v", loaded)
	}
	if loaded.AgentStates["agent-a"].State != "Running" {
		t.Fatalf("expected persisted agent state, got %#v", loaded.AgentStates)
	}
}

func TestFinalizeBuildSpecAttemptConvergencePersistsCompletedState(t *testing.T) {
	daemon := testBuildSpecDaemon(t)
	attempt := newBuildSpecAttempt(daemon.runState.RunID, "docs/build_spec.md", []byte("hello world"))
	attempt.Status = BuildSpecAttemptActive
	previousCompletion := time.Now().Add(-time.Hour)
	attempt.CompletedAt = &previousCompletion
	attempt.Plan = &coordinator.PlanResponse{
		Domains: []coordinator.Domain{{DomainID: "api"}},
	}
	attempt.RecordAgentState("agent-api", "api", "Running", "")
	if err := saveBuildSpecAttempt(daemon.runState.Root, attempt); err != nil {
		t.Fatalf("save active attempt: %v", err)
	}

	if err := daemon.finalizeBuildSpecAttemptConvergence(); err != nil {
		t.Fatalf("finalize convergence: %v", err)
	}
	completed, err := loadBuildSpecAttempt(daemon.runState.Root)
	if err != nil {
		t.Fatalf("load completed attempt: %v", err)
	}
	if completed == nil || completed.Status != BuildSpecAttemptCompleted || completed.CompletedAt == nil {
		t.Fatalf("expected completed attempt, got %#v", completed)
	}
	state := completed.AgentStates["agent-api"]
	if state.State != "Stopped" || state.Reason != "execution converged" {
		t.Fatalf("expected terminal agent state, got %#v", state)
	}
}

func TestInitializePlannedContractsMaterializesDurableWork(t *testing.T) {
	daemon := testBuildSpecDaemon(t)
	manager, err := coordination.Open(coordinationOptions(daemon.config, daemon.runState.CoordinationDir))
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	daemon.coordinationManager = manager
	daemon.ctx = context.Background()
	attempt := newBuildSpecAttempt(daemon.runState.RunID, "docs/build_spec.md", []byte("spec"))
	plan := &coordinator.PlanResponse{
		Domains: []coordinator.Domain{{DomainID: "producer"}, {DomainID: "consumer"}},
		Nodes: []coordinator.TaskNode{
			{ID: "producer-node", DomainID: "producer"},
			{ID: "consumer-node", DomainID: "consumer"},
		},
		Edges: []coordinator.TaskEdge{{FromNode: "producer-node", ToNode: "consumer-node"}},
		Contracts: []coordinator.PlannedContract{{
			Resource: "public-api", ProducerNode: "producer-node", ConsumerNode: "consumer-node",
			Kind: "interface", PublicEntrypoint: "producer.api", Acceptance: []string{"call succeeds"}, HardWait: true,
		}},
	}
	if err := daemon.initializePlannedContracts(context.Background(), plan, attempt); err != nil {
		t.Fatal(err)
	}
	contracts, err := manager.List(context.Background(), coordination.Principal{Role: coordination.RoleAdmin})
	if err != nil {
		t.Fatal(err)
	}
	if len(contracts) != 1 || len(attempt.CreatedContractIDs) != 1 {
		t.Fatalf("contracts = %+v, attempt = %+v", contracts, attempt.CreatedContractIDs)
	}
	contract := contracts[0]
	if contract.ID != attempt.CreatedContractIDs[0] || contract.ProducerAgentID != "agent-producer" || contract.ConsumerAgentID != "agent-consumer" {
		t.Fatalf("unexpected contract identity: %+v", contract)
	}
	if contract.Details.PublicEntrypoint != "producer.api" || !contract.HardWait {
		t.Fatalf("unexpected contract details: %+v", contract)
	}
}

func TestBuildSpecAttemptHubMessagesIncludeActivePlanContext(t *testing.T) {
	root := t.TempDir()
	attempt := newBuildSpecAttempt("run_1", "docs/build_spec.md", []byte("hello world"))
	attempt.Status = BuildSpecAttemptActive
	attempt.CreatedNodeIDs = []string{"api-task"}
	attempt.CreatedEdgeIDs = []string{"api-task->ui-task"}
	attempt.AllocatedDomainIDs = []string{"api", "ui"}
	attempt.Plan = &coordinator.PlanResponse{
		Domains: []coordinator.Domain{
			{DomainID: "api", Description: "API work"},
		},
		Nodes: []coordinator.TaskNode{
			{ID: "api-task", DomainID: "api", TaskSpec: "Build the API"},
		},
		Edges: []coordinator.TaskEdge{
			{FromNode: "api-task", ToNode: "ui-task"},
		},
	}
	if err := os.WriteFile(buildSpecTracePath(root), []byte("coordinator plan received\n"), 0o644); err != nil {
		t.Fatalf("write trace: %v", err)
	}

	messages := buildSpecAttemptHubMessages(root, attempt)
	if len(messages) < 3 {
		t.Fatalf("expected replay messages, got %d", len(messages))
	}
	combined := ""
	for _, msg := range messages {
		if msg.FromAgent != "coordinator" || msg.ToAgent != "tui" {
			t.Fatalf("unexpected replay routing: %#v", msg)
		}
		combined += string(msg.Payload) + "\n"
	}
	for _, want := range []string{"Loaded persisted build-spec attempt", "Persisted Coordinator plan response", "coordinator plan received"} {
		if !strings.Contains(combined, want) {
			t.Fatalf("replay payload missing %q:\n%s", want, combined)
		}
	}
}

func TestAppendBuildSpecTraceWritesProjectDebugLog(t *testing.T) {
	projectRoot := t.TempDir()
	runRoot := filepath.Join(projectRoot, ".aion", "runs", "run_1")
	if err := os.MkdirAll(runRoot, 0o755); err != nil {
		t.Fatalf("mkdir run root: %v", err)
	}

	if err := appendBuildSpecTrace(runRoot, "trace line"); err != nil {
		t.Fatalf("appendBuildSpecTrace: %v", err)
	}

	for _, path := range []string{
		filepath.Join(runRoot, "build_spec_trace.log"),
		filepath.Join(projectRoot, "build_spec_debug.log"),
	} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if !strings.Contains(string(data), "trace line") {
			t.Fatalf("expected trace line in %s, got %q", path, string(data))
		}
	}
}

func TestPrepareBuildSpecAttemptRefusesActiveAttempt(t *testing.T) {
	daemon := testBuildSpecDaemon(t)
	attempt := newBuildSpecAttempt(daemon.runState.RunID, "docs/build_spec.md", []byte("hello"))
	attempt.Status = BuildSpecAttemptActive
	if err := saveBuildSpecAttempt(daemon.runState.Root, attempt); err != nil {
		t.Fatalf("saveBuildSpecAttempt: %v", err)
	}
	if _, err := daemon.prepareBuildSpecAttempt("docs/build_spec.md", []byte("hello")); err == nil {
		t.Fatal("expected active attempt refusal")
	}
}

func TestPrepareBuildSpecAttemptRetriesStaleAttempt(t *testing.T) {
	daemon := testBuildSpecDaemon(t)
	old := newBuildSpecAttempt(daemon.runState.RunID, "docs/build_spec.md", []byte("hello"))
	old.Status = BuildSpecAttemptPlanning
	old.UpdatedAt = time.Now().Add(-buildSpecStaleAfter - time.Minute)
	data, err := json.MarshalIndent(old, "", "  ")
	if err != nil {
		t.Fatalf("marshal stale attempt: %v", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(buildSpecAttemptPath(daemon.runState.Root), data, 0o644); err != nil {
		t.Fatalf("write stale attempt: %v", err)
	}
	if err := os.WriteFile(daemon.runState.DagFile, []byte("dag"), 0o644); err != nil {
		t.Fatalf("write dag: %v", err)
	}
	if err := os.WriteFile(daemon.runState.WalFile, []byte("wal"), 0o644); err != nil {
		t.Fatalf("write wal: %v", err)
	}
	attempt, err := daemon.prepareBuildSpecAttempt("docs/build_spec.md", []byte("hello"))
	if err != nil {
		t.Fatalf("prepareBuildSpecAttempt: %v", err)
	}
	if attempt == nil || attempt.Status != BuildSpecAttemptPlanning {
		t.Fatalf("expected fresh planning attempt, got %#v", attempt)
	}
	if snapshot := daemon.dagManager.Snapshot(); len(snapshot.Nodes) != 0 || len(snapshot.Edges) != 0 {
		t.Fatalf("expected cleared dag snapshot, got %#v", snapshot)
	}
}

func TestReconcileInterruptedBuildSpecPlanningAllowsImmediateRetry(t *testing.T) {
	daemon := testBuildSpecDaemon(t)
	attempt := newBuildSpecAttempt(daemon.runState.RunID, "docs/build_spec.md", []byte("hello"))
	attempt.Status = BuildSpecAttemptPlanning
	if err := saveBuildSpecAttempt(daemon.runState.Root, attempt); err != nil {
		t.Fatalf("saveBuildSpecAttempt: %v", err)
	}

	if err := daemon.reconcileInterruptedBuildSpecAttempt(); err != nil {
		t.Fatalf("reconcileInterruptedBuildSpecAttempt: %v", err)
	}
	reconciled, err := loadBuildSpecAttempt(daemon.runState.Root)
	if err != nil {
		t.Fatalf("loadBuildSpecAttempt: %v", err)
	}
	if reconciled == nil || reconciled.Status != BuildSpecAttemptCanceled || reconciled.CompletedAt == nil {
		t.Fatalf("expected canceled interrupted attempt, got %#v", reconciled)
	}
	if !strings.Contains(reconciled.FailureReason, "daemon restarted") {
		t.Fatalf("unexpected reconciliation reason: %q", reconciled.FailureReason)
	}

	fresh, err := daemon.prepareBuildSpecAttempt("docs/build_spec.md", []byte("hello"))
	if err != nil {
		t.Fatalf("prepareBuildSpecAttempt after reconciliation: %v", err)
	}
	if fresh == nil || fresh.AttemptID == attempt.AttemptID {
		t.Fatalf("expected a fresh attempt, got %#v", fresh)
	}
}

func testBuildSpecDaemon(t *testing.T) *Daemon {
	t.Helper()
	root := t.TempDir()
	cfg := &Config{}
	applyDefaults(cfg)
	cfg.Orchestrator.ListenAddr = "127.0.0.1:0"
	run, err := CreateNewCurrentRun(root, cfg)
	if err != nil {
		t.Fatalf("CreateNewCurrentRun: %v", err)
	}
	dagMgr, err := dag.NewManager(dag.ManagerConfig{
		StoreFilePath: run.DagFile,
		WalFilePath:   run.WalFile,
		MaxNodes:      cfg.Orchestrator.MaxActiveNodes,
		FlushDeadline: cfg.FlushDeadline(),
	})
	if err != nil {
		t.Fatalf("dag.NewManager: %v", err)
	}
	router := hub.NewRouter(run.LogsDir)
	alloc := NewAllocator(cfg, root, router, dagMgr)
	server := NewServer(dagMgr, nil, nil, nil)
	d := &Daemon{
		config:      cfg,
		dagManager:  dagMgr,
		allocator:   alloc,
		server:      server,
		hubRouter:   router,
		projectRoot: root,
		runState:    run,
	}
	return d
}

func TestPrepareBuildSpecAttemptRefusesDagWithoutMetadata(t *testing.T) {
	daemon := testBuildSpecDaemon(t)
	if err := daemon.dagManager.AddNode(dag.DagNode{ID: "node-1", DomainID: "domain-1"}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if _, err := daemon.prepareBuildSpecAttempt("docs/build_spec.md", []byte("hello")); err == nil {
		t.Fatal("expected refusal when DAG exists without attempt metadata")
	}
}

func TestBuildSpecFailedAgentIDs(t *testing.T) {
	daemon := testBuildSpecDaemon(t)
	attempt := newBuildSpecAttempt(daemon.runState.RunID, "docs/build_spec.md", []byte("hello"))
	attempt.AgentStates = map[string]BuildSpecAgentState{
		"agent-a": {AgentID: "agent-a", DomainID: "domain-a", State: "Running"},
		"agent-b": {AgentID: "agent-b", DomainID: "domain-b", State: "Crashed"},
	}

	ids := daemon.buildSpecFailedAgentIDs(attempt, []AgentInfo{
		{AgentID: "agent-a", DomainID: "domain-a", State: "Running"},
		{AgentID: "agent-b", DomainID: "domain-b", State: "Stopped"},
		{AgentID: "agent-c", DomainID: "domain-c", State: "Running"},
	})

	if len(ids) != 1 || ids[0] != "agent-b" {
		t.Fatalf("unexpected failed agent ids: %#v", ids)
	}
}

func TestContinueBuildSpecAgentsRequiresExplicitArm(t *testing.T) {
	daemon := testBuildSpecDaemon(t)
	if err := daemon.continueBuildSpecAgents(); err == nil {
		t.Fatal("expected explicit arm requirement")
	}
}

func TestBuildSpecExecutionMonitorStartsOnceAndStops(t *testing.T) {
	daemon := testBuildSpecDaemon(t)
	t.Cleanup(daemon.stopBuildSpecExecutionMonitor)

	daemon.startBuildSpecExecutionMonitorWithInterval(time.Hour)
	daemon.executionMonitorMu.Lock()
	firstRun := daemon.executionMonitorRun
	active := daemon.executionMonitorActive
	hasCancel := daemon.executionMonitorCancel != nil
	daemon.executionMonitorMu.Unlock()
	if !active || !hasCancel {
		t.Fatal("expected execution monitor to be active")
	}

	daemon.startBuildSpecExecutionMonitorWithInterval(time.Hour)
	daemon.executionMonitorMu.Lock()
	secondRun := daemon.executionMonitorRun
	daemon.executionMonitorMu.Unlock()
	if secondRun != firstRun {
		t.Fatalf("duplicate monitor start changed generation: first=%d second=%d", firstRun, secondRun)
	}

	daemon.stopBuildSpecExecutionMonitor()
	daemon.executionMonitorMu.Lock()
	active = daemon.executionMonitorActive
	hasCancel = daemon.executionMonitorCancel != nil
	daemon.executionMonitorMu.Unlock()
	if active || hasCancel {
		t.Fatal("expected execution monitor to be stopped")
	}
}
