package coordinator

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPiCoordinator_PlanRequiresPlannerBinary(t *testing.T) {
	tmpDir := t.TempDir()
	docsDir := filepath.Join(tmpDir, "docs")
	if err := os.MkdirAll(docsDir, 0755); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(docsDir, "build_spec.md"), []byte("## Domains\n- Backend: API (Paths: internal/api)\n## Tasks\n- Init [Backend]: bootstrap"), 0644); err != nil {
		t.Fatalf("write spec: %v", err)
	}

	coord := NewPiCoordinator(tmpDir, PiCoordinatorConfig{})
	_, err := coord.Plan(context.Background(), PlanRequest{ProjectScan: &ProjectScan{EntryPoints: []string{"cmd/main.go"}}})
	if err == nil {
		t.Fatal("expected planner binary error")
	}
	if got := err.Error(); !strings.Contains(got, "planner binary is not configured") {
		t.Fatalf("unexpected error: %s", got)
	}
}

func TestPiCoordinator_PlanDoesNotParseStructuredSpecWithoutPlanner(t *testing.T) {
	tmpDir := t.TempDir()
	docsDir := filepath.Join(tmpDir, "docs")
	if err := os.MkdirAll(docsDir, 0755); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	spec := "## Domains\n- Backend: API (Paths: internal/api)\n## Tasks\n- Init [Backend]: bootstrap"
	if err := os.WriteFile(filepath.Join(docsDir, "build_spec.md"), []byte(spec), 0644); err != nil {
		t.Fatalf("write spec: %v", err)
	}

	coord := NewPiCoordinator(tmpDir, PiCoordinatorConfig{})
	_, err := coord.Plan(context.Background(), PlanRequest{ProjectScan: &ProjectScan{EntryPoints: []string{"cmd/main.go"}}})
	if err == nil {
		t.Fatal("expected planner binary error")
	}
	if got := err.Error(); !strings.Contains(got, "planner binary is not configured") {
		t.Fatalf("unexpected error: %s", got)
	}
}

func TestPiCoordinator_PlannerStartsWithoutBootstrapPrompt(t *testing.T) {
	coord := NewPiCoordinator(t.TempDir(), PiCoordinatorConfig{Binary: "pi"})
	planner := coord.plannerSupervisorConfig(filepath.Join(t.TempDir(), "coordinator"))
	if planner.InitialPrompt != "" {
		t.Fatalf("planner should not send a bootstrap turn, got %q", planner.InitialPrompt)
	}
	if planner.AgentID != "coordinator" || planner.DomainID != "coordinator" {
		t.Fatalf("unexpected planner identity: %#v", planner)
	}
}

func TestPiCoordinator_PreparePlanningArtifacts(t *testing.T) {
	tmpDir := t.TempDir()
	coord := NewPiCoordinator(tmpDir, PiCoordinatorConfig{Binary: "pi"})
	paths, err := coord.preparePlanningArtifacts("Build the app", PlanRequest{
		ProjectRoot: tmpDir,
		ProjectScan: &ProjectScan{EntryPoints: []string{"cmd/main.go"}},
	})
	if err != nil {
		t.Fatalf("preparePlanningArtifacts: %v", err)
	}
	if paths.InputPath != filepath.Join(tmpDir, "docs", "aion", "planning_input.json") {
		t.Fatalf("unexpected input path: %s", paths.InputPath)
	}
	if paths.OutputPath != filepath.Join(tmpDir, "docs", "aion", "plan_response.json") {
		t.Fatalf("unexpected output path: %s", paths.OutputPath)
	}

	data, err := os.ReadFile(paths.InputPath)
	if err != nil {
		t.Fatalf("read planning input: %v", err)
	}
	var input planningInputArtifact
	if err := json.Unmarshal(data, &input); err != nil {
		t.Fatalf("unmarshal planning input: %v", err)
	}
	if input.BuildSpec != "Build the app" || input.OutputPath != paths.OutputPath {
		t.Fatalf("unexpected planning input: %#v", input)
	}
}

func TestReadPlanArtifact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plan_response.json")
	raw := `{"type":"plan_response","domains":[{"domain_id":"api","description":"API","assigned_paths":["cmd/"],"agent_type":"domain"}],"nodes":[{"id":"api-task","domain_id":"api","task_spec":"build api","target_files":["cmd/main.go"],"priority":1}],"edges":[]}`
	if err := os.WriteFile(path, []byte(raw), 0644); err != nil {
		t.Fatalf("write plan artifact: %v", err)
	}

	plan, err := readPlanArtifact(path)
	if err != nil {
		t.Fatalf("readPlanArtifact: %v", err)
	}
	if len(plan.Domains) != 1 || len(plan.Nodes) != 1 {
		t.Fatalf("unexpected plan: %#v", plan)
	}
}

func TestPiCoordinator_PlanMissingBuildSpec(t *testing.T) {
	tmpDir := t.TempDir()
	coord := NewPiCoordinator(tmpDir, PiCoordinatorConfig{})

	_, err := coord.Plan(context.Background(), PlanRequest{})
	if err == nil {
		t.Fatal("expected missing build spec error")
	}
	if got := err.Error(); !strings.Contains(got, "docs/build_spec.md is missing") {
		t.Fatalf("unexpected error: %s", got)
	}
}

func TestPiCoordinator_PlanEmptyBuildSpec(t *testing.T) {
	tmpDir := t.TempDir()
	docsDir := filepath.Join(tmpDir, "docs")
	if err := os.MkdirAll(docsDir, 0755); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(docsDir, "build_spec.md"), []byte(" \n\t"), 0644); err != nil {
		t.Fatalf("write empty spec: %v", err)
	}
	coord := NewPiCoordinator(tmpDir, PiCoordinatorConfig{})

	_, err := coord.Plan(context.Background(), PlanRequest{})
	if err == nil {
		t.Fatal("expected empty build spec error")
	}
	if got := err.Error(); !strings.Contains(got, "docs/build_spec.md is empty") {
		t.Fatalf("unexpected error: %s", got)
	}
}

func TestPiCoordinator_Refine_NotStarted(t *testing.T) {
	coord := NewPiCoordinator(".", PiCoordinatorConfig{})
	err := coord.Refine(context.Background(), "hello")
	if err == nil {
		t.Error("expected error when refining without starting architect")
	}
}
