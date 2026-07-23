package orchestrator

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	coordination "aion-coordination"
	isolation "aion-isolation"
	"aion-kernel/internal/coordinator"
	"aion-kernel/internal/dag"
)

type recordingIsolationEngine struct {
	mu       sync.Mutex
	policies []isolation.Policy
}

func (e *recordingIsolationEngine) Prepare(_ context.Context, policy isolation.Policy) (isolation.Workspace, error) {
	e.mu.Lock()
	e.policies = append(e.policies, policy)
	e.mu.Unlock()
	return recordingIsolationWorkspace{}, nil
}

func (e *recordingIsolationEngine) snapshot() []isolation.Policy {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]isolation.Policy(nil), e.policies...)
}

type recordingIsolationWorkspace struct{}

func (recordingIsolationWorkspace) Snapshot() isolation.Snapshot {
	return isolation.Snapshot{Backend: "test"}
}
func (recordingIsolationWorkspace) Command(ctx context.Context, spec isolation.CommandSpec) (*exec.Cmd, error) {
	return exec.CommandContext(ctx, spec.Path, spec.Args...), nil
}
func (recordingIsolationWorkspace) Close() error { return nil }

func TestIntegrationRunnerAssemblesContractsAndPersistsSuccess(t *testing.T) {
	projectRoot := t.TempDir()
	runRoot := filepath.Join(projectRoot, ".aion", "runs", "run-1")
	if err := os.MkdirAll(runRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	writeIntegrationFixtureFile(t, filepath.Join(projectRoot, "alpha", "value.txt"), "alpha")
	writeIntegrationFixtureFile(t, filepath.Join(projectRoot, "beta", "value.txt"), "beta")
	writeIntegrationFixtureFile(t, filepath.Join(projectRoot, "shared.lock"), "shared")

	directive := IntegrationDirective{
		SchemaVersion:     1,
		ID:                "cross-domain-smoke",
		Command:           []string{"/bin/true"},
		RequiredContracts: []string{"contract-public"},
		OwnerNodeIDs:      []string{"alpha-node"},
	}
	writeIntegrationDirective(t, projectRoot, "alpha", directive)

	manager, err := coordination.Open(coordination.Options{StateDir: filepath.Join(runRoot, "coordination")})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	createFulfilledIntegrationContract(t, manager, "contract-public")

	attempt := integrationAttempt(runRoot)
	if err := saveBuildSpecAttempt(runRoot, attempt); err != nil {
		t.Fatal(err)
	}
	snapshot := integrationDAGSnapshot()
	engine := &recordingIsolationEngine{}
	config := integrationTestConfig()
	config.Agents.SharedFiles = []string{"shared.lock", "missing.optional"}
	runState := &RunState{RunID: "run-1", Root: runRoot}
	runner := NewIntegrationRunner(config, projectRoot, runState, manager, engine, context.Background())

	converged, err := runner.Gate(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("Gate: %v", err)
	}
	if converged {
		t.Fatal("integration gate converged before the directive completed")
	}
	status := waitForIntegrationStatus(t, runner, IntegrationPassed)
	if status.DirectiveCount != 1 || status.ContractCount != 1 || status.SharedFileCount != 1 {
		t.Fatalf("unexpected integration counts: %+v", status)
	}
	if len(status.Results) != 1 || status.Results[0].Status != "passed" {
		t.Fatalf("unexpected results: %+v", status.Results)
	}
	if converged, err = runner.Gate(context.Background(), snapshot); err != nil || !converged {
		t.Fatalf("persisted successful gate = %v, %v", converged, err)
	}

	manifestData, err := os.ReadFile(status.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifest IntegrationManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.InputDigest == "" || len(manifest.Contracts) != 1 || manifest.Contracts[0].Digest == "" {
		t.Fatalf("shared artifact manifest incomplete: %+v", manifest)
	}
	contractPath := filepath.Join(filepath.Dir(status.ManifestPath), "contracts", safeArtifactKey("contract-public"), "artifact")
	if content, err := os.ReadFile(contractPath); err != nil || string(content) != "public artifact" {
		t.Fatalf("materialized contract = %q, %v", content, err)
	}

	policies := engine.snapshot()
	if len(policies) != 1 {
		t.Fatalf("isolation policies = %d, want 1", len(policies))
	}
	policy := policies[0]
	if policy.Network != isolation.NetworkIsolated || len(policy.Writable) != 1 || policy.Writable[0].Target != "/state/integration" {
		t.Fatalf("runner policy is not isolated/read-only: %+v", policy)
	}
	for _, mount := range policy.ReadOnly {
		if mount.Target == "/workspace" || mount.Target == "/workspace/.aion" {
			t.Fatalf("runner received broad/private workspace mount: %+v", mount)
		}
	}

	restarted := NewIntegrationRunner(config, projectRoot, runState, manager, engine, context.Background())
	if converged, err := restarted.Gate(context.Background(), snapshot); err != nil || !converged {
		t.Fatalf("restarted runner did not reuse durable pass: %v, %v", converged, err)
	}
	if len(engine.snapshot()) != 1 {
		t.Fatal("durable pass reran the integration command after restart")
	}
	writeIntegrationFixtureFile(t, filepath.Join(projectRoot, "alpha", "value.txt"), "alpha changed")
	if converged, err := restarted.Gate(context.Background(), snapshot); err != nil || converged {
		t.Fatalf("changed inputs reused stale integration pass: %v, %v", converged, err)
	}
	changed := waitForIntegrationStatus(t, restarted, IntegrationPassed)
	if changed.InputDigest == status.InputDigest || len(engine.snapshot()) != 2 {
		t.Fatal("changed assembled inputs did not produce a fresh validation")
	}
}

func TestIntegrationRunnerFailureIsRetryable(t *testing.T) {
	projectRoot := t.TempDir()
	runRoot := filepath.Join(projectRoot, ".aion", "runs", "run-1")
	if err := os.MkdirAll(runRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	writeIntegrationFixtureFile(t, filepath.Join(projectRoot, "alpha", "value.txt"), "alpha")
	writeIntegrationFixtureFile(t, filepath.Join(projectRoot, "beta", "value.txt"), "beta")
	directive := IntegrationDirective{SchemaVersion: 1, ID: "retryable", Command: []string{"/bin/false"}, OwnerNodeIDs: []string{"alpha-node"}}
	writeIntegrationDirective(t, projectRoot, "alpha", directive)
	if err := saveBuildSpecAttempt(runRoot, integrationAttempt(runRoot)); err != nil {
		t.Fatal(err)
	}
	config := integrationTestConfig()
	engine := &recordingIsolationEngine{}
	runner := NewIntegrationRunner(config, projectRoot, &RunState{RunID: "run-1", Root: runRoot}, nil, engine, context.Background())
	if converged, err := runner.Gate(context.Background(), integrationDAGSnapshot()); err != nil || converged {
		t.Fatalf("initial gate = %v, %v", converged, err)
	}
	failed := waitForIntegrationStatus(t, runner, IntegrationFailed)
	if failed.Error == "" || failed.Results[0].ExitCode == 0 {
		t.Fatalf("failure evidence missing: %+v", failed)
	}

	directive.Command = []string{"/bin/true"}
	writeIntegrationDirective(t, projectRoot, "alpha", directive)
	if err := runner.Retry(); err != nil {
		t.Fatal(err)
	}
	if converged, err := runner.Gate(context.Background(), integrationDAGSnapshot()); err != nil || converged {
		t.Fatalf("retry gate = %v, %v", converged, err)
	}
	passed := waitForIntegrationStatus(t, runner, IntegrationPassed)
	if passed.InputDigest == failed.InputDigest {
		t.Fatal("retry did not bind to the changed directive/input digest")
	}
}

func TestIntegrationRunnerFailsWhenPlannedDirectiveWasNotPublished(t *testing.T) {
	projectRoot := t.TempDir()
	runRoot := filepath.Join(projectRoot, ".aion", "runs", "run-1")
	if err := os.MkdirAll(runRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	writeIntegrationFixtureFile(t, filepath.Join(projectRoot, "alpha", "value.txt"), "alpha")
	writeIntegrationFixtureFile(t, filepath.Join(projectRoot, "beta", "value.txt"), "beta")
	attempt := integrationAttempt(runRoot)
	attempt.Plan.Nodes = []coordinator.TaskNode{{
		ID: "alpha-node", DomainID: "alpha",
		TargetFiles: []string{"alpha/integration/directives/cross-domain.json"},
	}}
	if err := saveBuildSpecAttempt(runRoot, attempt); err != nil {
		t.Fatal(err)
	}
	runner := NewIntegrationRunner(integrationTestConfig(), projectRoot, &RunState{RunID: "run-1", Root: runRoot}, nil, &recordingIsolationEngine{}, context.Background())
	if converged, err := runner.Gate(context.Background(), integrationDAGSnapshot()); err != nil || converged {
		t.Fatalf("gate = %v, %v", converged, err)
	}
	status := runner.Status()
	if status.Status != IntegrationFailed || !strings.Contains(status.Error, "did not publish planned directive") {
		t.Fatalf("status = %+v", status)
	}
}

func TestIntegrationRunnerRejectsForeignNodesAndUnfulfilledContracts(t *testing.T) {
	tests := []struct {
		name      string
		directive IntegrationDirective
		want      string
	}{
		{
			name:      "foreign node",
			directive: IntegrationDirective{SchemaVersion: 1, ID: "foreign", Command: []string{"/bin/true"}, OwnerNodeIDs: []string{"beta-node"}},
			want:      "cannot claim node beta-node",
		},
		{
			name:      "unfulfilled contract",
			directive: IntegrationDirective{SchemaVersion: 1, ID: "missing-contract", Command: []string{"/bin/true"}, OwnerNodeIDs: []string{"alpha-node"}, RequiredContracts: []string{"missing"}},
			want:      "requires unfulfilled contract missing",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			projectRoot := t.TempDir()
			runRoot := filepath.Join(projectRoot, ".aion", "runs", "run-1")
			if err := os.MkdirAll(runRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			writeIntegrationFixtureFile(t, filepath.Join(projectRoot, "alpha", "value.txt"), "alpha")
			writeIntegrationFixtureFile(t, filepath.Join(projectRoot, "beta", "value.txt"), "beta")
			writeIntegrationDirective(t, projectRoot, "alpha", tt.directive)
			if err := saveBuildSpecAttempt(runRoot, integrationAttempt(runRoot)); err != nil {
				t.Fatal(err)
			}
			runner := NewIntegrationRunner(integrationTestConfig(), projectRoot, &RunState{RunID: "run-1", Root: runRoot}, nil, &recordingIsolationEngine{}, context.Background())
			if converged, err := runner.Gate(context.Background(), integrationDAGSnapshot()); err != nil || converged {
				t.Fatalf("Gate = %v, %v", converged, err)
			}
			status := waitForIntegrationStatus(t, runner, IntegrationFailed)
			if !strings.Contains(status.Error, tt.want) {
				t.Fatalf("error = %q, want %q", status.Error, tt.want)
			}
		})
	}
}

func TestValidatePlannedNodeDirectivesRejectsMalformedArtifactBeforeCompletion(t *testing.T) {
	root := t.TempDir()
	target := "api/integration/directives/api.json"
	writeIntegrationFixtureFile(t, filepath.Join(root, target), `{"input_source":"stdin"}`)
	node := &dag.DagNode{ID: "api-node", DomainID: "api", TargetFiles: []string{target}}

	err := validatePlannedNodeDirectives(root, "integration/directives", node)
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("validation error = %v, want strict schema rejection", err)
	}

	writeIntegrationFixtureFile(t, filepath.Join(root, target), `{
  "schema_version": 1,
  "id": "api",
  "command": ["/bin/true"],
  "owner_node_ids": ["api-node"],
  "timeout_sec": 5
}`)
	if err := validatePlannedNodeDirectives(root, "integration/directives", node); err != nil {
		t.Fatalf("valid planned directive rejected: %v", err)
	}
}

func TestValidatePlannedNodeDirectivesRejectsCrossDomainWorkingDirectory(t *testing.T) {
	root := t.TempDir()
	target := "api/integration/directives/api.json"
	writeIntegrationFixtureFile(t, filepath.Join(root, target), `{
  "schema_version": 1,
  "id": "api",
  "command": ["/bin/true"],
  "working_directory": "api",
  "required_contracts": ["contract-public"],
  "owner_node_ids": ["api-node"]
}`)
	node := &dag.DagNode{ID: "api-node", DomainID: "api", TargetFiles: []string{target}}

	err := validatePlannedNodeDirectives(root, "integration/directives", node)
	if err == nil || !strings.Contains(err.Error(), "must be omitted for a cross-domain directive") {
		t.Fatalf("validation error = %v", err)
	}
}

func TestAllocatorConvergenceWaitsForIntegrationGate(t *testing.T) {
	root := t.TempDir()
	manager, err := dag.NewManager(dag.ManagerConfig{StoreFilePath: filepath.Join(root, "dag.bin"), WalFilePath: filepath.Join(root, "dag.wal"), MaxNodes: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if err := manager.AddNode(dag.DagNode{ID: "done", DomainID: "alpha", Status: dag.StatusDone}); err != nil {
		t.Fatal(err)
	}
	allocator := NewAllocator(&Config{}, root, nil, manager)
	ready := false
	allocator.SetIntegrationGateFunc(func(context.Context, *dag.DagData) (bool, error) { return ready, nil })
	if converged, err := allocator.checkConvergence(); err != nil || converged {
		t.Fatalf("convergence passed before integration: %v, %v", converged, err)
	}
	ready = true
	if converged, err := allocator.checkConvergence(); err != nil || !converged {
		t.Fatalf("convergence remained blocked after integration: %v, %v", converged, err)
	}
}

func TestIntegrationRunnerBubblewrapLiveBoundary(t *testing.T) {
	projectRoot := t.TempDir()
	runRoot := filepath.Join(projectRoot, ".aion", "runs", "run-1")
	if err := os.MkdirAll(runRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	writeIntegrationFixtureFile(t, filepath.Join(projectRoot, "alpha", "value.txt"), "alpha")
	writeIntegrationFixtureFile(t, filepath.Join(projectRoot, "beta", "value.txt"), "beta")
	writeIntegrationFixtureFile(t, filepath.Join(projectRoot, ".aion", "private.txt"), "private")
	directive := IntegrationDirective{
		SchemaVersion:     1,
		ID:                "live-boundary",
		OwnerNodeIDs:      []string{"alpha-node"},
		RequiredContracts: []string{"live-contract"},
		Command: []string{"/bin/sh", "-c",
			"test -r /workspace/alpha/value.txt && test -r /workspace/beta/value.txt && grep -q 'public artifact' /contracts/artifacts/" + safeArtifactKey("live-contract") + "/artifact && ! touch /workspace/alpha/forbidden && test ! -e /workspace/.aion && touch /state/integration/evidence"},
	}
	writeIntegrationDirective(t, projectRoot, "alpha", directive)
	if err := saveBuildSpecAttempt(runRoot, integrationAttempt(runRoot)); err != nil {
		t.Fatal(err)
	}
	manager, err := coordination.Open(coordination.Options{StateDir: filepath.Join(runRoot, "coordination")})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	createFulfilledIntegrationContract(t, manager, "live-contract")
	engine, err := isolation.NewBubblewrapEngine(isolation.BubblewrapConfig{RuntimeBase: filepath.Join(t.TempDir(), "runtime")})
	if err != nil {
		t.Skipf("bubblewrap unavailable: %v", err)
	}
	runner := NewIntegrationRunner(integrationTestConfig(), projectRoot, &RunState{RunID: "run-1", Root: runRoot}, manager, engine, context.Background())
	if converged, err := runner.Gate(context.Background(), integrationDAGSnapshot()); err != nil || converged {
		t.Fatalf("initial gate = %v, %v", converged, err)
	}
	status := waitForIntegrationTerminal(t, runner)
	if status.Status == IntegrationFailed && (strings.Contains(status.Error, "bubblewrap is unavailable") || strings.Contains(strings.ToLower(status.Error), "operation not permitted")) {
		t.Skipf("rootless bubblewrap unavailable in test environment: %s", status.Error)
	}
	if status.Status != IntegrationPassed {
		t.Fatalf("live integration failed: %+v", status)
	}
	if _, err := os.Stat(filepath.Join(runRoot, "integration", "output", status.InputDigest, "evidence")); err != nil {
		t.Fatalf("writable evidence missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(projectRoot, "alpha", "forbidden")); !os.IsNotExist(err) {
		t.Fatalf("read-only domain output was modified: %v", err)
	}
}

func integrationTestConfig() *Config {
	return &Config{Integration: IntegrationConfig{Enabled: true, DirectiveDir: "integration/directives", TimeoutSec: 5, MaxOutputBytes: 4096, MaxDirectives: 8}}
}

func integrationAttempt(runRoot string) *BuildSpecAttempt {
	return &BuildSpecAttempt{
		AttemptID: "attempt-1", RunID: "run-1", Status: BuildSpecAttemptActive,
		StartedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
		Plan: &coordinator.PlanResponse{Domains: []coordinator.Domain{
			{DomainID: "alpha", AssignedPaths: []string{"alpha"}, AgentType: "domain"},
			{DomainID: "beta", AssignedPaths: []string{"beta"}, AgentType: "domain"},
		}},
	}
}

func integrationDAGSnapshot() *dag.DagData {
	return &dag.DagData{Nodes: []dag.DagNode{
		{ID: "alpha-node", DomainID: "alpha", Status: dag.StatusDone},
		{ID: "beta-node", DomainID: "beta", Status: dag.StatusDone},
	}}
}

func createFulfilledIntegrationContract(t *testing.T, manager *coordination.Manager, id string) {
	t.Helper()
	ctx := context.Background()
	producer := coordination.Principal{Role: coordination.RoleAgent, AgentID: "agent-alpha", NodeIDs: []string{"alpha-node"}}
	consumer := coordination.Principal{Role: coordination.RoleAgent, AgentID: "agent-beta", NodeIDs: []string{"beta-node"}}
	_, err := manager.Request(ctx, consumer, coordination.RequestCommand{CommandID: "request", Contract: coordination.Contract{
		ID: id, ProducerAgentID: producer.AgentID, ProducerNodeID: "alpha-node",
		ConsumerAgentID: consumer.AgentID, ConsumerNodeID: "beta-node",
		Details: coordination.ContractDetails{Name: "PublicArtifact", Kind: "fixture"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	produced, err := manager.Publish(ctx, producer, coordination.PublishCommand{CommandID: "publish", ContractID: id, Content: []byte("public artifact"), MediaType: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Validate(ctx, consumer, coordination.ValidateCommand{CommandID: "validate", ContractID: id, Revision: produced.Revision}); err != nil {
		t.Fatal(err)
	}
}

func writeIntegrationDirective(t *testing.T, projectRoot, domain string, directive IntegrationDirective) {
	t.Helper()
	data, err := json.MarshalIndent(directive, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	writeIntegrationFixtureFile(t, filepath.Join(projectRoot, domain, "integration", "directives", directive.ID+".json"), string(data))
}

func writeIntegrationFixtureFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func waitForIntegrationStatus(t *testing.T, runner *IntegrationRunner, want IntegrationPhaseStatus) IntegrationRunStatus {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status := runner.Status()
		if status.Status == want {
			return status
		}
		time.Sleep(10 * time.Millisecond)
	}
	status := runner.Status()
	resultError := ""
	if len(status.Results) > 0 {
		resultError = status.Results[0].Error
	}
	t.Fatalf("integration status = %s, want %s; error=%s results=%s", status.Status, want, status.Error, strings.TrimSpace(resultError))
	return status
}

func waitForIntegrationTerminal(t *testing.T, runner *IntegrationRunner) IntegrationRunStatus {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		status := runner.Status()
		if status.Status == IntegrationPassed || status.Status == IntegrationFailed || status.Status == IntegrationSkipped {
			return status
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("integration runner did not reach a terminal state: %+v", runner.Status())
	return IntegrationRunStatus{}
}
