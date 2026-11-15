//go:build scenario

package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"aion-kernel/internal/coordinator"
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
	path := filepath.Join(req.ArtifactDir, "acceptance_output.json")
	if err := os.WriteFile(path, review.output, 0o644); err != nil {
		return nil, err
	}
	return &coordinator.BuildReviewResult{OutputPath: path}, nil
}

type scenarioResult struct {
	Name       string    `json:"name"`
	Status     string    `json:"status"`
	StartedAt  time.Time `json:"started_at"`
	DurationMS int64     `json:"duration_ms"`
	Error      string    `json:"error,omitempty"`
	Artifacts  string    `json:"artifacts,omitempty"`
}

// scenarioHarness owns one isolated in-process kernel scenario.
type scenarioHarness struct {
	t      *testing.T
	daemon *Daemon
	result scenarioResult
	closed bool
}

func newScenarioHarness(t *testing.T, coordinatorImpl coordinator.Coordinator) *scenarioHarness {
	t.Helper()
	daemon := testBuildSpecDaemon(t)
	daemon.ctx = context.Background()
	if coordinatorImpl != nil {
		daemon.coordinator = coordinatorImpl
	}
	if err := os.MkdirAll(filepath.Join(daemon.projectRoot, "docs"), 0o755); err != nil {
		t.Fatal(err)
	}
	harness := &scenarioHarness{
		t: t, daemon: daemon,
		result: scenarioResult{Name: t.Name(), Status: "running", StartedAt: time.Now().UTC()},
	}
	go func() {
		_ = daemon.server.ListenAndServe("127.0.0.1:0")
	}()
	eventually(t, 3*time.Second, 10*time.Millisecond, func() bool {
		return daemon.server.Addr() != ""
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
}

func (h *scenarioHarness) writeReport() {
	path := filepath.Join(h.daemon.runState.Root, "scenario_result.json")
	data, err := json.MarshalIndent(h.result, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(path, append(data, '\n'), 0o644)
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

func TestScenarioSmokeHarness(t *testing.T) {
	runScenario(t, scenarioCase{
		Name: "smoke",
		Run: func(h *scenarioHarness) {
			h.WriteBuildSpec("# Smoke\nScenario harness is alive.\n")
			if _, err := os.Stat(filepath.Join(h.ProjectRoot(), "docs", "build_spec.md")); err != nil {
				t.Fatal(err)
			}
		},
	}, &scenarioCoordinator{})
}
