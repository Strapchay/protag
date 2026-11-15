package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	isolation "aion-isolation"
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
		AttemptID:   "buildspec-test-1",
		ProjectRoot: tmpDir,
		ProjectScan: &ProjectScan{EntryPoints: []string{"cmd/main.go"}},
	})
	if err != nil {
		t.Fatalf("preparePlanningArtifacts: %v", err)
	}
	if paths.InputPath != filepath.Join(tmpDir, "docs", "aion", "planning", "buildspec-test-1", "planning_input.json") {
		t.Fatalf("unexpected input path: %s", paths.InputPath)
	}
	if paths.OutputPath != filepath.Join(tmpDir, "docs", "aion", "planning", "buildspec-test-1", "plan_response.json") {
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
	if input.AttemptHint != "buildspec-test-1" || input.BuildSpec != "Build the app" || input.OutputPath != paths.OutputPath {
		t.Fatalf("unexpected planning input: %#v", input)
	}
	if len(input.ExcludedPaths) == 0 || !IsProjectScanExcluded(".aion/runs/current", input.ExcludedPaths) {
		t.Fatalf("planning input should include generated scan exclusions: %#v", input.ExcludedPaths)
	}
}

func TestPlannerAttemptIDIsPathSafe(t *testing.T) {
	got := plannerAttemptID("../../build spec:one")
	if got != "build-spec-one" {
		t.Fatalf("plannerAttemptID() = %q", got)
	}
}

func TestPiCoordinator_PlannerGatewayActivityHandshake(t *testing.T) {
	coord := NewPiCoordinator(t.TempDir(), PiCoordinatorConfig{
		PlannerFirstRequestTimeout: time.Second,
	})
	ch := coord.beginPlannerGatewayWatch()
	go coord.RecordCoordinatorGatewayActivity("coordinator", "coordinator", "forwarding")

	if err := coord.waitForPlannerGatewayActivity(context.Background(), ch, filepath.Join(t.TempDir(), "plan_response.json")); err != nil {
		t.Fatalf("waitForPlannerGatewayActivity: %v", err)
	}
}

func TestPiCoordinator_ResolutionGatewayActivityRenewsOnlyResolution(t *testing.T) {
	coord := NewPiCoordinator(t.TempDir(), PiCoordinatorConfig{})
	progress := 0
	coord.setResolutionProgress(func() { progress++ })
	planner := coord.beginPlannerGatewayWatch()

	coord.RecordCoordinatorGatewayActivity("coordinator-resolution", "coordinator", "active")
	if progress != 1 {
		t.Fatalf("resolution progress calls = %d, want 1", progress)
	}
	select {
	case <-planner:
		t.Fatal("resolution activity incorrectly satisfied the planner handshake")
	default:
	}

	coord.RecordCoordinatorGatewayActivity("coordinator", "coordinator", "forwarding")
	select {
	case <-planner:
	default:
		t.Fatal("planner activity did not satisfy the planner handshake")
	}
}

func TestPiCoordinator_ResolutionProcessEnvUsesCredentialIssuer(t *testing.T) {
	coord := NewPiCoordinator(t.TempDir(), PiCoordinatorConfig{ResolutionEnv: []string{"BASE=value"}})
	coord.SetResolutionEnvFunc(func() ([]string, error) {
		return []string{"AION_AGENT_CAPABILITY=opaque"}, nil
	})
	env, err := coord.resolutionProcessEnv()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(env, "\n") != "BASE=value\nAION_AGENT_CAPABILITY=opaque" {
		t.Fatalf("resolution env = %#v", env)
	}
}

func TestPiCoordinator_PlannerGatewayActivityTimeoutIncludesRawTail(t *testing.T) {
	tmpDir := t.TempDir()
	sessionDir := filepath.Join(".aion", "runs", "run-test", "pi_sessions")
	rawDir := filepath.Join(tmpDir, sessionDir, "coordinator")
	if err := os.MkdirAll(rawDir, 0755); err != nil {
		t.Fatalf("mkdir raw dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(rawDir, "pi_raw.log"), []byte("line one\nline two\n"), 0644); err != nil {
		t.Fatalf("write raw log: %v", err)
	}

	coord := NewPiCoordinator(tmpDir, PiCoordinatorConfig{
		SessionDir:                 sessionDir,
		PlannerFirstRequestTimeout: 10 * time.Millisecond,
	})
	ch := coord.beginPlannerGatewayWatch()

	err := coord.waitForPlannerGatewayActivity(context.Background(), ch, filepath.Join(tmpDir, "missing.json"))
	if err == nil {
		t.Fatal("expected timeout")
	}
	if got := err.Error(); !strings.Contains(got, "no coordinator inference request observed") || !strings.Contains(got, "line two") {
		t.Fatalf("unexpected timeout error: %s", got)
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

func TestPiCoordinator_WaitForPlanArtifactReturnsAfterCompletedTurnWithoutArtifact(t *testing.T) {
	tmpDir := t.TempDir()
	coord := NewPiCoordinator(tmpDir, PiCoordinatorConfig{
		PlannerArtifactTimeout: 10 * time.Second,
	})
	capture := newPlanCapture()
	coord.plannerCapture = capture
	coord.plannerTurnErrors = make(chan error, 1)

	go func() {
		time.Sleep(10 * time.Millisecond)
		capture.setText(`{"domains":[],"nodes":[]}`)
		capture.close()
	}()

	started := time.Now()
	_, err := coord.waitForPlanArtifact(context.Background(), filepath.Join(tmpDir, "missing-plan.json"))
	if err == nil {
		t.Fatal("expected missing artifact error")
	}
	if !isPlannerArtifactTurnError(err) {
		t.Fatalf("expected plannerArtifactTurnError, got %T: %v", err, err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("completed turn should fail promptly, took %s", elapsed)
	}
	if got := err.Error(); !strings.Contains(got, "chat response preview") || !strings.Contains(got, `"domains"`) {
		t.Fatalf("missing response context: %s", got)
	}
}

func TestPiCoordinator_WaitForPlanArtifactUsesGatewayQuiescenceWhenLifecycleStops(t *testing.T) {
	tmpDir := t.TempDir()
	coord := NewPiCoordinator(tmpDir, PiCoordinatorConfig{
		PlannerArtifactTimeout:   10 * time.Second,
		PlannerTurnSettleTimeout: 20 * time.Millisecond,
	})
	coord.plannerCapture = newPlanCapture()
	coord.plannerTurnErrors = make(chan error, 1)
	coord.beginPlannerGatewayWatch()
	coord.RecordCoordinatorGatewayActivity("coordinator", "coordinator", "completed")

	started := time.Now()
	_, err := coord.waitForPlanArtifact(context.Background(), filepath.Join(tmpDir, "missing-plan.json"))
	if err == nil || !isPlannerArtifactTurnError(err) {
		t.Fatalf("expected plannerArtifactTurnError, got %T: %v", err, err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("gateway quiescence should fail promptly, took %s", elapsed)
	}
}

func TestCoordinatorArtifactCorrectionPromptRequiresWriteAndExactSchema(t *testing.T) {
	paths := planningArtifactPaths{
		InputPath:  "/project/docs/aion/planning/attempt/planning_input.json",
		OutputPath: "/project/docs/aion/planning/attempt/plan_response.json",
	}
	prompt := coordinatorArtifactCorrectionPrompt(paths, fmt.Errorf("missing artifact"))
	for _, required := range []string{
		"Use the write tool now",
		paths.InputPath,
		paths.OutputPath,
		`"domain_id"`,
		`"target_files"`,
		`"from_node"`,
		`"to_node"`,
		`"public_entrypoint"`,
		`Do not return or restate the plan in chat`,
	} {
		if !strings.Contains(prompt, required) {
			t.Fatalf("correction prompt missing %q:\n%s", required, prompt)
		}
	}
}

func TestTransientPlannerTurnErrorClassification(t *testing.T) {
	for _, message := range []string{
		"planner inference turn failed: ResourceExhausted: request limit reached",
		"too many requests (429)",
		"temporarily unavailable",
	} {
		if !transientPlannerTurnError(fmt.Errorf("%s", message)) {
			t.Fatalf("expected transient classification for %q", message)
		}
	}
	if transientPlannerTurnError(fmt.Errorf("invalid API key")) {
		t.Fatal("authentication error must not be retried as transient")
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

func TestResolutionPromptUsesSealedTemplate(t *testing.T) {
	prompt := resolutionPrompt(ResolutionRequest{})
	for _, want := range []string{
		"/workspace/artifacts/resolution_input.json",
		"/workspace/artifacts/resolution_template.json",
		"/workspace/artifacts/resolution_output.json",
		"Keep the exact type, resolution_id, and terms.details",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("resolution prompt missing %q:\n%s", want, prompt)
		}
	}
	repair := resolutionPrompt(ResolutionRequest{Repair: true, ValidationError: "bad candidate"})
	if !strings.Contains(repair, "bad candidate") || !strings.Contains(repair, "resolution_template.json") {
		t.Fatalf("repair prompt = %s", repair)
	}
}

func TestReviewPromptUsesTemplateAndRejectsSourceWrites(t *testing.T) {
	prompt := reviewPrompt(BuildReviewRequest{AttemptID: "buildspec_1"}, "/workspace/docs/aion/acceptance/buildspec_1")
	for _, want := range []string{
		"/workspace/docs/aion/acceptance/buildspec_1/acceptance_input.json",
		"/workspace/docs/aion/acceptance/buildspec_1/acceptance_template.json",
		"/workspace/docs/aion/acceptance/buildspec_1/acceptance_output.json",
		"Do not modify source files",
		"verdict complete",
		"verdict rework",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("review prompt missing %q:\n%s", want, prompt)
		}
	}
	repair := reviewPrompt(BuildReviewRequest{AttemptID: "buildspec_1", Repair: true, ValidationError: "bad verdict"}, "/workspace/docs/aion/acceptance/buildspec_1")
	if !strings.Contains(repair, "bad verdict") || !strings.Contains(repair, "acceptance_output.json") {
		t.Fatalf("repair prompt = %s", repair)
	}
}

func TestReviewPolicyMountHelpers(t *testing.T) {
	mounts := []isolation.Mount{{Source: "/old", Target: "/state/pi"}, {Source: "/docs", Target: "/workspace/docs"}, {Source: "/bin", Target: "/runtime/bin/orchestrator-cli"}}
	replaced := replaceMountSource(mounts, "/state/pi", "/new")
	if replaced[0].Source != "/new" || replaced[1].Source != "/docs" {
		t.Fatalf("replaceMountSource = %#v", replaced)
	}
	replaced = replaceMount(replaced, "/workspace/docs", "/audit", "/workspace/docs/aion/acceptance/a1")
	if replaced[1].Source != "/audit" || replaced[1].Target != "/workspace/docs/aion/acceptance/a1" {
		t.Fatalf("replaceMount = %#v", replaced)
	}
	dropped := dropMountTarget(replaced, "/runtime/bin/orchestrator-cli")
	if len(dropped) != 2 || dropped[1].Target != "/workspace/docs/aion/acceptance/a1" {
		t.Fatalf("dropMountTarget = %#v", dropped)
	}
}
