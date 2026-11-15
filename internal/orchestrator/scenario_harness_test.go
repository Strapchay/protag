//go:build scenario

package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

// scenarioCoordinator is the scripted model boundary for deterministic
// scenarios. It never calls a provider and never inspects the host.
type scenarioCoordinator struct {
	plan       *coordinator.PlanResponse
	reviews    []scenarioReview
	reviewCall int
	resolve    *coordinator.ResolutionResult
}

type scenarioReview struct {
	output json.RawMessage
	err    error
}

type scenarioFixture struct {
	BuildSpec string                    `json:"build_spec"`
	Plan      *coordinator.PlanResponse `json:"plan,omitempty"`
	Reviews   []json.RawMessage         `json:"reviews,omitempty"`
}

func (c *scenarioCoordinator) Plan(context.Context, coordinator.PlanRequest) (*coordinator.PlanResponse, error) {
	return c.plan, nil
}

func (c *scenarioCoordinator) Replan(context.Context, coordinator.ReplanRequest) (*coordinator.ReplanResponse, error) {
	return &coordinator.ReplanResponse{}, nil
}

func (c *scenarioCoordinator) Refine(context.Context, string) error { return nil }

func (c *scenarioCoordinator) GetArchitectAgent() interface{} { return nil }

func (c *scenarioCoordinator) ResolveContract(context.Context, coordinator.ResolutionRequest) (*coordinator.ResolutionResult, error) {
	return c.resolve, nil
}

func (c *scenarioCoordinator) ReviewBuild(_ context.Context, req coordinator.BuildReviewRequest) (*coordinator.BuildReviewResult, error) {
	if c.reviewCall >= len(c.reviews) {
		return nil, fmt.Errorf("no scripted review for call %d", c.reviewCall)
	}
	review := c.reviews[c.reviewCall]
	c.reviewCall++
	if review.err != nil {
		return nil, review.err
	}
	output := []byte(strings.ReplaceAll(string(review.output), "__ATTEMPT_ID__", req.AttemptID))
	path := filepath.Join(req.ArtifactDir, "acceptance_output.json")
	if err := os.WriteFile(path, output, 0o644); err != nil {
		return nil, err
	}
	return &coordinator.BuildReviewResult{OutputPath: path}, nil
}

func scenarioCoordinatorFromFixture(fixture scenarioFixture) *scenarioCoordinator {
	reviews := make([]scenarioReview, 0, len(fixture.Reviews))
	for _, output := range fixture.Reviews {
		reviews = append(reviews, scenarioReview{output: output})
	}
	return &scenarioCoordinator{plan: fixture.Plan, reviews: reviews}
}

type scenarioResult struct {
	Name       string    `json:"name"`
	Status     string    `json:"status"`
	StartedAt  time.Time `json:"started_at"`
	DurationMS int64     `json:"duration_ms"`
	Error      string    `json:"error,omitempty"`
	Artifacts  string    `json:"artifacts,omitempty"`
}

// scenarioConfig gives each scenario short, deterministic bounds while keeping
// the production validation rules intact.
func scenarioConfig() *Config {
	cfg := &Config{}
	applyDefaults(cfg)
	cfg.Orchestrator.ListenAddr = "127.0.0.1:0"
	cfg.Coordination.LeaseTimeoutSec = 30
	cfg.Coordination.ArtifactTimeoutSec = 30
	cfg.Coordination.DeadlockTimeoutSec = 10
	cfg.Coordination.MaxSuccessorDepth = 2
	cfg.Coordination.MaxIntegrationRecoveries = 2
	cfg.Coordination.AcceptanceReviewTimeoutSec = 120
	cfg.Health.HeartbeatTimeoutSec = 10
	cfg.Health.ProgressTimeoutSec = 20
	cfg.Agents.IncompleteTurnRetries = 1
	return cfg
}

// scenarioHarness owns one isolated in-process kernel scenario.
type scenarioHarness struct {
	t      *testing.T
	daemon *Daemon
	cfg    *Config
	result scenarioResult
	closed bool
}

func newScenarioHarness(t *testing.T, coordinatorImpl coordinator.Coordinator) *scenarioHarness {
	t.Helper()
	cfg := scenarioConfig()
	root := t.TempDir()
	run, err := CreateNewCurrentRun(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	dagManager, err := dag.NewManager(dag.ManagerConfig{
		StoreFilePath: run.DagFile,
		WalFilePath:   run.WalFile,
		MaxNodes:      cfg.Orchestrator.MaxActiveNodes,
		FlushDeadline: cfg.FlushDeadline(),
	})
	if err != nil {
		t.Fatal(err)
	}
	router := hub.NewRouter(run.LogsDir)
	manager, err := coordination.Open(coordination.Options{StateDir: filepath.Join(root, "coordination")})
	if err != nil {
		t.Fatal(err)
	}
	allocator := NewAllocator(cfg, root, router, dagManager)
	server := NewServer(dagManager, nil, manager, nil)
	daemon := &Daemon{
		config: cfg, dagManager: dagManager, allocator: allocator, server: server,
		coordinationManager: manager, hubRouter: router, projectRoot: root, runState: run, ctx: context.Background(),
	}
	if coordinatorImpl != nil {
		daemon.coordinator = coordinatorImpl
	}
	if err := os.MkdirAll(filepath.Join(root, "docs"), 0o755); err != nil {
		t.Fatal(err)
	}
	harness := &scenarioHarness{
		t: t, daemon: daemon, cfg: cfg,
		result: scenarioResult{Name: t.Name(), Status: "running", StartedAt: time.Now().UTC()},
	}
	go func() {
		_ = server.ListenAndServe("127.0.0.1:0")
	}()
	eventually(t, 3*time.Second, 10*time.Millisecond, func() bool {
		return server.Addr() != ""
	})
	return harness
}

func (h *scenarioHarness) ProjectRoot() string { return h.daemon.projectRoot }

func (h *scenarioHarness) WriteBuildSpec(text string) {
	h.t.Helper()
	if err := os.WriteFile(filepath.Join(h.daemon.projectRoot, "docs", "build_spec.md"), []byte(text), 0o644); err != nil {
		h.t.Fatal(err)
	}
}

func (h *scenarioHarness) Close() {
	if h == nil || h.closed {
		return
	}
	h.closed = true
	h.daemon.stopBuildSpecExecutionMonitor()
	_ = h.daemon.server.Stop()
	if h.daemon.dagManager != nil {
		_ = h.daemon.dagManager.Close()
	}
	if h.daemon.coordinationManager != nil {
		_ = h.daemon.coordinationManager.Close()
	}
	if h.daemon.hubRouter != nil {
		h.daemon.hubRouter.Close()
	}
	h.result.DurationMS = time.Since(h.result.StartedAt).Milliseconds()
	if h.t.Failed() {
		h.result.Status = "failed"
	} else {
		h.result.Status = "passed"
	}
	h.result.Artifacts = h.daemon.runState.Root
	h.writeReport()
	if h.t.Failed() || strings.TrimSpace(os.Getenv("AION_SCENARIO_ARTIFACTS")) != "" {
		root := os.Getenv("AION_SCENARIO_ARTIFACTS")
		if root == "" {
			root = filepath.Join("testdata", "scenario-artifacts")
		}
		target := filepath.Join(root, sanitizeScenarioName(h.result.Name)+"-"+time.Now().UTC().Format("20060102T150405"))
		if err := copyScenarioDir(h.daemon.runState.Root, target); err == nil {
			_ = copyScenarioDir(filepath.Join(h.daemon.projectRoot, "docs", "aion"), filepath.Join(target, "docs-aion"))
			h.result.Artifacts = target
			h.writeReportAt(target)
		}
	}
}

func (h *scenarioHarness) writeReport() {
	h.writeReportAt(h.daemon.runState.Root)
}

func (h *scenarioHarness) writeReportAt(dir string) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	data, err := json.MarshalIndent(h.result, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(dir, "scenario_result.json"), append(data, '\n'), 0o644)
}

func sanitizeScenarioName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, name)
	if name == "" {
		return "scenario"
	}
	return name
}

func copyScenarioDir(source, target string) error {
	return filepath.WalkDir(source, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil || strings.HasPrefix(relative, "..") {
			return nil
		}
		destination := filepath.Join(target, relative)
		if entry.IsDir() {
			return os.MkdirAll(destination, 0o755)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(destination)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(out, in)
		closeErr := out.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
}

func loadScenarioFixture(t *testing.T, name string) scenarioFixture {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "scenarios", name+".json"))
	if err != nil {
		t.Fatalf("load scenario fixture %s: %v", name, err)
	}
	var fixture scenarioFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatalf("decode scenario fixture %s: %v", name, err)
	}
	return fixture
}

func seedAcceptanceScenario(h *scenarioHarness, fixture scenarioFixture) *BuildSpecAttempt {
	h.t.Helper()
	h.WriteBuildSpec(fixture.BuildSpec)
	attempt := newBuildSpecAttempt(h.daemon.runState.RunID, "docs/build_spec.md", []byte(fixture.BuildSpec))
	attempt.Status = BuildSpecAttemptActive
	attempt.Plan = fixture.Plan
	for _, node := range fixture.Plan.Nodes {
		h.t.Helper()
		if err := h.daemon.dagManager.AddNode(dag.DagNode{
			ID: node.ID, DomainID: node.DomainID, TaskSpec: node.TaskSpec, Status: dag.StatusDone,
			TargetFiles: node.TargetFiles, Priority: node.Priority,
		}); err != nil {
			h.t.Fatal(err)
		}
	}
	if err := saveBuildSpecAttempt(h.daemon.runState.Root, attempt); err != nil {
		h.t.Fatal(err)
	}
	return attempt
}

func eventually(t *testing.T, timeout, interval time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(interval)
	}
	t.Fatalf("condition not satisfied within %s", timeout)
}

type scenarioCase struct {
	Name string
	Run  func(*scenarioHarness)
}

func runScenario(t *testing.T, tc scenarioCase, coordinatorImpl coordinator.Coordinator) {
	t.Helper()
	h := newScenarioHarness(t, coordinatorImpl)
	t.Cleanup(h.Close)
	tc.Run(h)
	if strings.TrimSpace(h.daemon.server.Addr()) == "" {
		t.Fatal("scenario server did not start")
	}
}

func scenarioRegistry() []scenarioCase {
	return []scenarioCase{
		{
			Name: "smoke",
			Run: func(h *scenarioHarness) {
				h.WriteBuildSpec("# Smoke\nScenario harness is alive.\n")
				if _, err := os.Stat(filepath.Join(h.ProjectRoot(), "docs", "build_spec.md")); err != nil {
					h.t.Fatal(err)
				}
			},
		},
		{
			Name: "fixture-smoke",
			Run: func(h *scenarioHarness) {
				fixture := loadScenarioFixture(h.t, "smoke")
				h.WriteBuildSpec(fixture.BuildSpec)
				if fixture.Plan == nil || len(fixture.Plan.Domains) == 0 {
					h.t.Fatal("fixture plan was not loaded")
				}
			},
		},
		{
			Name: "acceptance-complete",
			Run: func(h *scenarioHarness) {
				fixture := loadScenarioFixture(h.t, "acceptance-complete")
				attempt := seedAcceptanceScenario(h, fixture)
				complete, err := h.daemon.reviewBuildAcceptance(attempt)
				if err != nil || !complete {
					h.t.Fatalf("acceptance complete=%v err=%v", complete, err)
				}
				loaded, err := loadBuildSpecAttempt(h.daemon.runState.Root)
				if err != nil {
					h.t.Fatal(err)
				}
				if loaded.AcceptanceStatus != "passed" {
					h.t.Fatalf("acceptance status = %s", loaded.AcceptanceStatus)
				}
				if _, err := os.Stat(loaded.AcceptanceReviewPath); err != nil {
					h.t.Fatalf("acceptance review artifact: %v", err)
				}
			},
		},
		{
			Name: "acceptance-retry-cycle",
			Run: func(h *scenarioHarness) {
				fixture := loadScenarioFixture(h.t, "acceptance-retry-cycle")
				attempt := seedAcceptanceScenario(h, fixture)
				h.daemon.workResumeFn = func(*BuildSpecAttempt) error {
					return h.daemon.dagManager.UpdateNode("alpha-node", dag.StatusDone)
				}
				complete, err := h.daemon.reviewBuildAcceptance(attempt)
				if err != nil || complete {
					h.t.Fatalf("first retry review complete=%v err=%v", complete, err)
				}
				if attempt.AcceptanceStatus != "retry" || attempt.AcceptanceRounds != 1 {
					h.t.Fatalf("retry state = %s/%d", attempt.AcceptanceStatus, attempt.AcceptanceRounds)
				}
				complete, err = h.daemon.reviewBuildAcceptance(attempt)
				if err != nil || !complete {
					h.t.Fatalf("second review complete=%v err=%v", complete, err)
				}
				if attempt.AcceptanceStatus != "passed" {
					h.t.Fatalf("final acceptance = %s", attempt.AcceptanceStatus)
				}
				coordinator, ok := h.daemon.coordinator.(*scenarioCoordinator)
				if !ok || coordinator.reviewCall != 2 {
					h.t.Fatalf("review calls = %#v", coordinator)
				}
			},
		},
		{
			Name: "acceptance-successor-contract",
			Run: func(h *scenarioHarness) {
				fixture := loadScenarioFixture(h.t, "acceptance-complete")
				attempt := seedAcceptanceScenario(h, fixture)
				attempt.Plan.Domains = append(attempt.Plan.Domains, coordinator.Domain{DomainID: "beta", AssignedPaths: []string{"beta/"}})
				if err := saveBuildSpecAttempt(h.daemon.runState.Root, attempt); err != nil {
					h.t.Fatal(err)
				}
				candidate := buildAcceptanceCandidate{
					Type: "aion_build_acceptance", AttemptID: attempt.AttemptID, Verdict: "successor",
					Summary:      "Add the follow-on result contract.",
					Requirements: []buildAcceptanceRequirement{{ID: "R3", Status: "missing", Evidence: "A follow-on contract is required."}},
					AddNodes: []coordinator.TaskNode{
						{ID: "alpha-result", DomainID: "alpha", TaskSpec: "Publish result", TargetFiles: []string{"alpha/result.py"}, OriginNodeID: "alpha-node", Priority: 1},
						{ID: "beta-result", DomainID: "beta", TaskSpec: "Consume result", TargetFiles: []string{"beta/result.py"}, OriginNodeID: "alpha-node", Priority: 1},
					},
					AddContracts: []coordinator.PlannedContract{{
						Resource: "alpha-result-v2", ProducerNode: "alpha-result", ConsumerNode: "beta-result",
						Kind: "interface", Language: "python", PublicEntrypoint: "alpha.result",
						Acceptance: []string{"consumer imports the result"}, SupersedesContractID: "contract-alpha-old",
					}},
				}
				data, _ := json.Marshal(candidate)
				patch, err := h.daemon.validateAcceptanceCandidate(attempt, data)
				if err != nil {
					h.t.Fatal(err)
				}
				if err := h.daemon.applyAcceptancePatch(attempt, patch); err != nil {
					h.t.Fatal(err)
				}
				contracts, err := h.daemon.coordinationManager.List(context.Background(), coordination.Principal{Role: coordination.RoleAdmin})
				if err != nil || len(contracts) != 1 {
					h.t.Fatalf("contracts = %#v err=%v", contracts, err)
				}
				if contracts[0].Details.SupersedesContractID != "contract-alpha-old" {
					h.t.Fatalf("supersedes = %#v", contracts[0].Details)
				}
			},
		},
		{
			Name: "acceptance-surface-retry-rejected",
			Run: func(h *scenarioHarness) {
				fixture := loadScenarioFixture(h.t, "acceptance-complete")
				attempt := seedAcceptanceScenario(h, fixture)
				candidate := buildAcceptanceCandidate{
					Type: "aion_build_acceptance", AttemptID: attempt.AttemptID, Verdict: "retry",
					ContractImpact: "surface", Summary: "Surface change cannot use retry.",
					Requirements: []buildAcceptanceRequirement{{ID: "R3", Status: "missing", Evidence: "public surface."}},
					ReopenNodes:  []buildAcceptanceRework{{NodeID: "alpha-node", Instruction: "Change the public surface."}},
				}
				data, _ := json.Marshal(candidate)
				if _, err := h.daemon.validateAcceptanceCandidate(attempt, data); err == nil {
					h.t.Fatal("surface retry was accepted")
				}
			},
		},
		{
			Name: "acceptance-complete-failed-node",
			Run: func(h *scenarioHarness) {
				fixture := loadScenarioFixture(h.t, "acceptance-complete")
				attempt := seedAcceptanceScenario(h, fixture)
				if err := h.daemon.dagManager.UpdateNode("alpha-node", dag.StatusFailed); err != nil {
					h.t.Fatal(err)
				}
				complete, err := h.daemon.reviewBuildAcceptance(attempt)
				if err == nil || complete {
					h.t.Fatalf("failed-node acceptance complete=%v err=%v", complete, err)
				}
				loaded, loadErr := loadBuildSpecAttempt(h.daemon.runState.Root)
				if loadErr != nil || loaded.AcceptanceStatus != "attention" {
					h.t.Fatalf("acceptance = %#v err=%v", loaded, loadErr)
				}
			},
		},
		{
			Name: "acceptance-attention",
			Run: func(h *scenarioHarness) {
				fixture := loadScenarioFixture(h.t, "acceptance-complete")
				attempt := seedAcceptanceScenario(h, fixture)
				h.daemon.coordinator = &scenarioCoordinator{reviews: []scenarioReview{{err: fmt.Errorf("resolution artifact was not written within 2m")}}}
				complete, err := h.daemon.reviewBuildAcceptance(attempt)
				if err == nil || complete {
					h.t.Fatalf("attention complete=%v err=%v", complete, err)
				}
				loaded, loadErr := loadBuildSpecAttempt(h.daemon.runState.Root)
				if loadErr != nil {
					h.t.Fatal(loadErr)
				}
				if loaded.AcceptanceStatus != "attention" || loaded.AcceptanceFailureCode != "acceptance.timeout" {
					h.t.Fatalf("acceptance state = %s/%s", loaded.AcceptanceStatus, loaded.AcceptanceFailureCode)
				}
				preflight := h.daemon.buildResumePreflight()
				if !preflight.CanResume || preflight.AcceptanceStatus != "attention" {
					h.t.Fatalf("preflight = %#v", preflight)
				}
			},
		},
		{
			Name: "acceptance-retry-apply",
			Run: func(h *scenarioHarness) {
				fixture := loadScenarioFixture(h.t, "acceptance-complete")
				attempt := seedAcceptanceScenario(h, fixture)
				candidate := buildAcceptanceCandidate{
					Type: "aion_build_acceptance", AttemptID: attempt.AttemptID, Verdict: "retry",
					ContractImpact: "none", Summary: "The fixture node needs a missing release document.",
					Requirements: []buildAcceptanceRequirement{{ID: "R2", Status: "missing", Evidence: "fixture release file is absent."}},
					ReopenNodes:  []buildAcceptanceRework{{NodeID: "alpha-node", Instruction: "Create the missing fixture release document."}},
				}
				data, _ := json.Marshal(candidate)
				patch, err := h.daemon.validateAcceptanceCandidate(attempt, data)
				if err != nil {
					h.t.Fatal(err)
				}
				if err := h.daemon.applyAcceptancePatch(attempt, patch); err != nil {
					h.t.Fatal(err)
				}
				node, err := h.daemon.dagManager.GetNode("alpha-node")
				if err != nil || node.Status != dag.StatusPending || !strings.Contains(node.TaskSpec, "ACCEPTANCE RETRY") {
					h.t.Fatalf("retry node = %#v err=%v", node, err)
				}
			},
		},
		{
			Name: "acceptance-repair-fails",
			Run: func(h *scenarioHarness) {
				fixture := loadScenarioFixture(h.t, "acceptance-complete")
				attempt := seedAcceptanceScenario(h, fixture)
				h.daemon.coordinator = &scenarioCoordinator{reviews: []scenarioReview{
					{output: []byte(`{"bad":`)},
					{output: []byte(`{"still":"bad"}`)},
				}}
				complete, err := h.daemon.reviewBuildAcceptance(attempt)
				if err == nil || complete {
					h.t.Fatalf("repair-fails complete=%v err=%v", complete, err)
				}
				loaded, loadErr := loadBuildSpecAttempt(h.daemon.runState.Root)
				if loadErr != nil || loaded.AcceptanceStatus != "attention" || loaded.AcceptanceFailureCode != "acceptance.invalid_report" {
					h.t.Fatalf("acceptance = %#v err=%v", loaded, loadErr)
				}
			},
		},
		{
			Name: "acceptance-successor-depth-rejected",
			Run: func(h *scenarioHarness) {
				fixture := loadScenarioFixture(h.t, "acceptance-complete")
				attempt := seedAcceptanceScenario(h, fixture)
				attempt.Plan.Nodes = append(attempt.Plan.Nodes,
					coordinator.TaskNode{ID: "alpha-second", DomainID: "alpha", TaskSpec: "second", TargetFiles: []string{"alpha/second.py"}, OriginNodeID: "alpha-node"},
					coordinator.TaskNode{ID: "alpha-third", DomainID: "alpha", TaskSpec: "third", TargetFiles: []string{"alpha/third.py"}, OriginNodeID: "alpha-second"},
				)
				for _, id := range []string{"alpha-second", "alpha-third"} {
					if err := h.daemon.dagManager.AddNode(dag.DagNode{ID: id, DomainID: "alpha", TaskSpec: id, Status: dag.StatusDone, TargetFiles: []string{"alpha/" + id + ".py"}}); err != nil {
						h.t.Fatal(err)
					}
				}
				candidate := buildAcceptanceCandidate{
					Type: "aion_build_acceptance", AttemptID: attempt.AttemptID, Verdict: "successor",
					Summary: "Too deep successor.", Requirements: []buildAcceptanceRequirement{{ID: "R4", Status: "missing", Evidence: "deep."}},
					AddNodes: []coordinator.TaskNode{{ID: "alpha-fourth", DomainID: "alpha", TaskSpec: "fourth", TargetFiles: []string{"alpha/fourth.py"}, OriginNodeID: "alpha-third"}},
				}
				data, _ := json.Marshal(candidate)
				if _, err := h.daemon.validateAcceptanceCandidate(attempt, data); err == nil {
					h.t.Fatal("successor depth was not enforced")
				}
			},
		},
		{
			Name: "integration-directive-recovery",
			Run: func(h *scenarioHarness) {
				fixture := loadScenarioFixture(h.t, "acceptance-complete")
				seedAcceptanceScenario(h, fixture)
				runner := NewIntegrationRunner(h.daemon.config, h.daemon.projectRoot, h.daemon.runState, h.daemon.coordinationManager, nil, context.Background())
				defer runner.Close()
				h.daemon.integrationRunner = runner
				h.daemon.handleIntegrationStatusChange(IntegrationRunStatus{
					Status: IntegrationFailed, FailureClass: "directive_execution_failed",
					Error: "directive failed", Results: []IntegrationDirectiveResult{{
						DirectiveID: "fixture-directive", OwnerNodeIDs: []string{"alpha-node"}, Status: "failed", ExitCode: 1, FailureClass: "directive_execution_failed",
					}},
				})
				loaded, err := loadBuildSpecAttempt(h.daemon.runState.Root)
				if err != nil {
					h.t.Fatal(err)
				}
				if loaded.IntegrationRecoveryRounds != 1 {
					h.t.Fatalf("recovery rounds = %d", loaded.IntegrationRecoveryRounds)
				}
				node, err := h.daemon.dagManager.GetNode("alpha-node")
				if err != nil || node.Status != dag.StatusInProgress {
					h.t.Fatalf("node = %#v err=%v", node, err)
				}
				if runner.Status().Status != IntegrationPending {
					h.t.Fatalf("runner status = %s", runner.Status().Status)
				}
			},
		},
		{
			Name: "integration-recovery-budget",
			Run: func(h *scenarioHarness) {
				fixture := loadScenarioFixture(h.t, "acceptance-complete")
				seedAcceptanceScenario(h, fixture)
				runner := NewIntegrationRunner(h.daemon.config, h.daemon.projectRoot, h.daemon.runState, h.daemon.coordinationManager, nil, context.Background())
				defer runner.Close()
				h.daemon.integrationRunner = runner
				status := IntegrationRunStatus{
					Status: IntegrationFailed, FailureClass: "directive_execution_failed", Error: "directive failed",
					Results: []IntegrationDirectiveResult{{DirectiveID: "fixture-directive", OwnerNodeIDs: []string{"alpha-node"}, Status: "failed", ExitCode: 1, FailureClass: "directive_execution_failed"}},
				}
				for i := 0; i < 3; i++ {
					h.daemon.handleIntegrationStatusChange(status)
				}
				loaded, err := loadBuildSpecAttempt(h.daemon.runState.Root)
				if err != nil {
					h.t.Fatal(err)
				}
				if loaded.Status != BuildSpecAttemptPaused || !strings.Contains(loaded.FailureReason, "integration requires operator attention") {
					h.t.Fatalf("attempt = %s/%s", loaded.Status, loaded.FailureReason)
				}
			},
		},
		{
			Name: "integration-prepare-attention",
			Run: func(h *scenarioHarness) {
				fixture := loadScenarioFixture(h.t, "acceptance-complete")
				seedAcceptanceScenario(h, fixture)
				h.daemon.handleIntegrationStatusChange(IntegrationRunStatus{
					Status: IntegrationFailed, FailureClass: "prepare_failed", Error: "invalid manifest",
				})
				loaded, err := loadBuildSpecAttempt(h.daemon.runState.Root)
				if err != nil {
					h.t.Fatal(err)
				}
				if loaded.Status != BuildSpecAttemptPaused || !strings.Contains(loaded.FailureReason, "prepare_failed") {
					h.t.Fatalf("attempt = %s/%s", loaded.Status, loaded.FailureReason)
				}
			},
		},
		{
			Name: "recovery-crash-budget",
			Run: func(h *scenarioHarness) {
				for i := 0; i < 3; i++ {
					h.daemon.observeAgentRecovery("agent-alpha", "alpha", "Crashed", "process exited")
				}
				h.daemon.observeAgentRecovery("agent-alpha", "alpha", "Stopped", "supervisor exhausted retries")
				state := h.daemon.recoverySnapshot("agent-alpha")
				if state.Phase != "awaiting_operator" || state.FailureCode != "supervisor.restart_budget_exhausted" {
					h.t.Fatalf("recovery state = %#v", state)
				}
			},
		},
		{
			Name: "recovery-supervisor-wait",
			Run: func(h *scenarioHarness) {
				h.daemon.observeAgentRecovery("agent-alpha", "alpha", "Crashed", "process exited")
				h.daemon.recoveryMu.Lock()
				h.daemon.recoveryProcessing = true
				h.daemon.recoveryMu.Unlock()
				h.daemon.enqueueRecovery(RecoveryRequest{AgentID: "agent-alpha", Action: "revive", Cause: "test"})
				h.daemon.recoveryMu.Lock()
				h.daemon.recoveryProcessing = false
				queued := len(h.daemon.recoveryQueue)
				h.daemon.recoveryMu.Unlock()
				if queued != 1 {
					h.t.Fatalf("queue = %d", queued)
				}
			},
		},
		{
			Name: "recovery-queue-dedup",
			Run: func(h *scenarioHarness) {
				h.daemon.recoveryMu.Lock()
				h.daemon.recoveryProcessing = true
				h.daemon.recoveryMu.Unlock()
				h.daemon.enqueueRecovery(RecoveryRequest{AgentID: "agent-alpha", Action: "revive", Cause: "duplicate"})
				h.daemon.enqueueRecovery(RecoveryRequest{AgentID: "agent-alpha", Action: "revive", Cause: "duplicate"})
				h.daemon.recoveryMu.Lock()
				queued := len(h.daemon.recoveryQueue)
				h.daemon.recoveryProcessing = false
				h.daemon.recoveryMu.Unlock()
				if queued != 1 {
					h.t.Fatalf("queue = %d", queued)
				}
			},
		},
		{
			Name: "recovery-restart-reconstruct",
			Run: func(h *scenarioHarness) {
				fixture := loadScenarioFixture(h.t, "acceptance-complete")
				attempt := seedAcceptanceScenario(h, fixture)
				attempt.AgentStates = map[string]BuildSpecAgentState{
					"agent-alpha": {AgentID: "agent-alpha", DomainID: "alpha", State: "Crashed", RecoveryEpoch: 2, RecoveryPhase: "process_recovering", RecoveryOwner: "supervisor", RecoveryAttempts: 1, RecoveryBudget: 3},
				}
				if err := saveBuildSpecAttempt(h.daemon.runState.Root, attempt); err != nil {
					h.t.Fatal(err)
				}
				h.daemon.initRecoveryArbiter()
				state := h.daemon.recoverySnapshot("agent-alpha")
				if state.Phase != "work_recovery_pending" || state.Epoch != 3 {
					h.t.Fatalf("reconstructed state = %#v", state)
				}
			},
		},
		{
			Name: "recovery-failed-node-operator-retry",
			Run: func(h *scenarioHarness) {
				fixture := loadScenarioFixture(h.t, "acceptance-complete")
				attempt := seedAcceptanceScenario(h, fixture)
				if err := h.daemon.dagManager.UpdateNode("alpha-node", dag.StatusFailed); err != nil {
					h.t.Fatal(err)
				}
				preflight := h.daemon.buildResumePreflight()
				if !preflight.CanResume {
					h.t.Fatalf("preflight = %#v", preflight)
				}
				if err := h.daemon.reopenFailedNodesForResume(); err != nil {
					h.t.Fatal(err)
				}
				node, err := h.daemon.dagManager.GetNode("alpha-node")
				if err != nil || node.Status != dag.StatusPending {
					h.t.Fatalf("node = %#v err=%v", node, err)
				}
				_ = attempt
			},
		},
		{
			Name: "acceptance-repair",
			Run: func(h *scenarioHarness) {
				fixture := loadScenarioFixture(h.t, "acceptance-complete")
				attempt := seedAcceptanceScenario(h, fixture)
				valid := fixture.Reviews[0]
				h.daemon.coordinator = &scenarioCoordinator{reviews: []scenarioReview{
					{output: []byte(`{"type":"aion_build_acceptance"`)},
					{output: valid},
				}}
				complete, err := h.daemon.reviewBuildAcceptance(attempt)
				if err != nil || !complete {
					h.t.Fatalf("repair complete=%v err=%v", complete, err)
				}
				coordinator, ok := h.daemon.coordinator.(*scenarioCoordinator)
				if !ok || coordinator.reviewCall != 2 {
					h.t.Fatalf("review calls = %#v", coordinator)
				}
				loaded, loadErr := loadBuildSpecAttempt(h.daemon.runState.Root)
				if loadErr != nil || loaded.AcceptanceStatus != "passed" {
					h.t.Fatalf("acceptance state = %#v err=%v", loaded, loadErr)
				}
			},
		},
	}
}

func TestScenarioRegistry(t *testing.T) {
	filter := strings.TrimSpace(os.Getenv("AION_SCENARIO"))
	for _, tc := range scenarioRegistry() {
		tc := tc
		if filter != "" && !strings.Contains(tc.Name, filter) {
			continue
		}
		t.Run(tc.Name, func(t *testing.T) {
			switch tc.Name {
			case "fixture-smoke":
				loaded := loadScenarioFixture(t, "smoke")
				runScenario(t, tc, scenarioCoordinatorFromFixture(loaded))
			case "acceptance-complete":
				loaded := loadScenarioFixture(t, "acceptance-complete")
				runScenario(t, tc, scenarioCoordinatorFromFixture(loaded))
			case "acceptance-attention", "acceptance-retry-apply", "acceptance-successor-contract",
				"acceptance-surface-retry-rejected", "acceptance-complete-failed-node",
				"acceptance-repair-fails", "acceptance-successor-depth-rejected",
				"recovery-crash-budget", "recovery-supervisor-wait", "recovery-queue-dedup",
				"recovery-restart-reconstruct", "recovery-failed-node-operator-retry",
				"integration-directive-recovery", "integration-recovery-budget", "integration-prepare-attention":
				runScenario(t, tc, &scenarioCoordinator{})
			case "acceptance-retry-cycle":
				loaded := loadScenarioFixture(t, "acceptance-retry-cycle")
				runScenario(t, tc, scenarioCoordinatorFromFixture(loaded))
			case "acceptance-repair":
				runScenario(t, tc, &scenarioCoordinator{})
			default:
				runScenario(t, tc, &scenarioCoordinator{plan: &coordinator.PlanResponse{}})
			}
		})
	}
}
