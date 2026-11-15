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
	allocator := NewAllocator(cfg, root, router, dagManager)
	server := NewServer(dagManager, nil, nil, nil)
	daemon := &Daemon{
		config: cfg, dagManager: dagManager, allocator: allocator, server: server,
		hubRouter: router, projectRoot: root, runState: run, ctx: context.Background(),
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
	_ = h.daemon.server.Stop()
	if h.daemon.dagManager != nil {
		_ = h.daemon.dagManager.Close()
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
			default:
				runScenario(t, tc, &scenarioCoordinator{plan: &coordinator.PlanResponse{}})
			}
		})
	}
}
