package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	coordination "aion-coordination"
	"aion-kernel/internal/coordinator"
	"aion-kernel/internal/dag"
)

type fakeBuildReviewer struct {
	output buildAcceptanceCandidate
	err    error
}

func (f *fakeBuildReviewer) Plan(context.Context, coordinator.PlanRequest) (*coordinator.PlanResponse, error) {
	return nil, nil
}
func (f *fakeBuildReviewer) Replan(context.Context, coordinator.ReplanRequest) (*coordinator.ReplanResponse, error) {
	return nil, nil
}
func (f *fakeBuildReviewer) Refine(context.Context, string) error { return nil }
func (f *fakeBuildReviewer) GetArchitectAgent() interface{}       { return nil }
func (f *fakeBuildReviewer) ReviewBuild(_ context.Context, req coordinator.BuildReviewRequest) (*coordinator.BuildReviewResult, error) {
	if f.err != nil {
		return nil, f.err
	}
	data, _ := json.Marshal(f.output)
	path := filepath.Join(req.ArtifactDir, "acceptance_output.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return nil, err
	}
	return &coordinator.BuildReviewResult{OutputPath: path}, nil
}

func acceptanceFixture(t *testing.T) (*Daemon, *BuildSpecAttempt) {
	t.Helper()
	daemon := testBuildSpecDaemon(t)
	if err := os.MkdirAll(filepath.Join(daemon.projectRoot, "docs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(daemon.projectRoot, "docs", "build_spec.md"), []byte("# Spec\nImplement alpha."), 0o644); err != nil {
		t.Fatal(err)
	}
	attempt := newBuildSpecAttempt(daemon.runState.RunID, "docs/build_spec.md", []byte("# Spec\nImplement alpha."))
	attempt.Status = BuildSpecAttemptActive
	attempt.Plan = &coordinator.PlanResponse{
		Domains: []coordinator.Domain{{DomainID: "alpha", AssignedPaths: []string{"alpha/"}}},
		Nodes: []coordinator.TaskNode{{
			ID: "alpha-core", DomainID: "alpha", TaskSpec: "Implement alpha.", TargetFiles: []string{"alpha/alpha.py"},
		}},
	}
	if err := daemon.dagManager.AddNode(dag.DagNode{
		ID: "alpha-core", DomainID: "alpha", TaskSpec: "Implement alpha.", Status: dag.StatusDone,
		TargetFiles: []string{"alpha/alpha.py"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := saveBuildSpecAttempt(daemon.runState.Root, attempt); err != nil {
		t.Fatal(err)
	}
	return daemon, attempt
}

func TestAcceptanceCandidateCompleteAndRework(t *testing.T) {
	daemon, attempt := acceptanceFixture(t)
	complete := buildAcceptanceCandidate{
		Type: "aion_build_acceptance", AttemptID: attempt.AttemptID, Verdict: "complete", Summary: "All requirements are implemented.",
		Requirements: []buildAcceptanceRequirement{{ID: "R1", Status: "met", Evidence: "alpha/alpha.py and integration passed."}},
	}
	data, _ := json.Marshal(complete)
	patch, err := daemon.validateAcceptanceCandidate(attempt, data)
	if err != nil || patch.candidate.Verdict != "complete" {
		t.Fatalf("complete patch = %#v, err=%v", patch, err)
	}

	rework := buildAcceptanceCandidate{
		Type: "aion_build_acceptance", AttemptID: attempt.AttemptID, Verdict: "retry", Summary: "The required error type is missing.",
		Requirements: []buildAcceptanceRequirement{{ID: "R1", Status: "missing", Evidence: "alpha/alpha.py has no validation error type."}},
		ReopenNodes: []buildAcceptanceRework{{
			NodeID: "alpha-core", Instruction: "Add the missing validation error type and tests.",
		}},
	}
	data, _ = json.Marshal(rework)
	patch, err = daemon.validateAcceptanceCandidate(attempt, data)
	if err != nil {
		t.Fatalf("rework patch: %v", err)
	}
	if err := daemon.applyAcceptancePatch(attempt, patch); err != nil {
		t.Fatalf("apply rework: %v", err)
	}
	node, err := daemon.dagManager.GetNode("alpha-core")
	if err != nil || node.Status != dag.StatusPending {
		t.Fatalf("reopened node = %#v, err=%v", node, err)
	}
	if !strings.Contains(node.TaskSpec, "ACCEPTANCE RETRY") || !strings.Contains(node.TaskSpec, "validation error type") {
		t.Fatalf("rework task spec = %q", node.TaskSpec)
	}
}

func TestAcceptanceCandidateRejectsOwnershipEscape(t *testing.T) {
	daemon, attempt := acceptanceFixture(t)
	rework := buildAcceptanceCandidate{
		Type: "aion_build_acceptance", AttemptID: attempt.AttemptID, Verdict: "retry", Summary: "Needs work.",
		Requirements: []buildAcceptanceRequirement{{ID: "R1", Status: "missing", Evidence: "missing."}},
		AddNodes: []coordinator.TaskNode{{
			ID: "escape", DomainID: "alpha", TaskSpec: "write elsewhere", TargetFiles: []string{"beta/escape.py"},
		}},
	}
	data, _ := json.Marshal(rework)
	if _, err := daemon.validateAcceptanceCandidate(attempt, data); err == nil {
		t.Fatal("ownership escape was accepted")
	}
}

func TestReviewBuildAcceptancePassesAndStoresVerdict(t *testing.T) {
	daemon, attempt := acceptanceFixture(t)
	daemon.coordinator = &fakeBuildReviewer{output: buildAcceptanceCandidate{
		Type: "aion_build_acceptance", AttemptID: attempt.AttemptID, Verdict: "complete", Summary: "All requirements are implemented.",
		Requirements: []buildAcceptanceRequirement{{ID: "R1", Status: "met", Evidence: "alpha/alpha.py and integration passed."}},
	}}
	complete, err := daemon.reviewBuildAcceptance(attempt)
	if err != nil || !complete {
		t.Fatalf("review complete=%v err=%v", complete, err)
	}
	loaded, err := loadBuildSpecAttempt(daemon.runState.Root)
	if err != nil || loaded.AcceptanceStatus != "passed" {
		t.Fatalf("stored attempt = %#v, err=%v", loaded, err)
	}
	if !strings.Contains(loaded.AcceptanceSummary, "implemented") {
		t.Fatalf("summary = %q", loaded.AcceptanceSummary)
	}
	if loaded.AcceptanceReviews != 1 {
		t.Fatalf("acceptance reviews = %d, want 1", loaded.AcceptanceReviews)
	}
}

func TestAcceptanceInputIncludesExecutionEvidence(t *testing.T) {
	daemon, attempt := acceptanceFixture(t)
	daemon.coordinator = &fakeBuildReviewer{output: buildAcceptanceCandidate{
		Type: "aion_build_acceptance", AttemptID: attempt.AttemptID, Verdict: "complete", Summary: "All requirements are implemented.",
		Requirements: []buildAcceptanceRequirement{{ID: "R1", Status: "met", Evidence: "source and integration."}},
	}}
	if _, err := daemon.reviewBuildAcceptance(attempt); err != nil {
		t.Fatal(err)
	}
	inputPath := filepath.Join(daemon.projectRoot, "docs", "aion", "acceptance", attempt.AttemptID, "acceptance_input.json")
	data, err := os.ReadFile(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	var input buildAcceptanceInput
	if err := json.Unmarshal(data, &input); err != nil {
		t.Fatal(err)
	}
	if input.BuildSpec == "" || input.Plan == nil || len(input.DAG.Nodes) != 1 || len(input.ValidationRules) == 0 {
		t.Fatalf("acceptance input = %#v", input)
	}
}

func TestAcceptanceSuccessorRequiresDoneOriginAndDepth(t *testing.T) {
	daemon, attempt := acceptanceFixture(t)
	daemon.config.Coordination.MaxSuccessorDepth = 2
	successor := buildAcceptanceCandidate{
		Type: "aion_build_acceptance", AttemptID: attempt.AttemptID, Verdict: "successor", Summary: "Add the follow-on normalization behavior.",
		Requirements: []buildAcceptanceRequirement{{ID: "R2", Status: "missing", Evidence: "The completed alpha package lacks normalization."}},
		AddNodes: []coordinator.TaskNode{{
			ID: "alpha-normalize", DomainID: "alpha", TaskSpec: "Add normalization as a successor to alpha-core.",
			TargetFiles: []string{"alpha/normalize.py"}, OriginNodeID: "alpha-core",
		}},
	}
	data, _ := json.Marshal(successor)
	patch, err := daemon.validateAcceptanceCandidate(attempt, data)
	if err != nil {
		t.Fatalf("successor patch: %v", err)
	}
	if err := daemon.applyAcceptancePatch(attempt, patch); err != nil {
		t.Fatal(err)
	}
	node, err := daemon.dagManager.GetNode("alpha-normalize")
	if err != nil || node.OriginNodeID != "alpha-core" || node.Status != dag.StatusPending {
		t.Fatalf("successor node = %#v, err=%v", node, err)
	}
	foundEdge := false
	for _, edge := range daemon.dagManager.Snapshot().Edges {
		if edge.FromNode == "alpha-core" && edge.ToNode == "alpha-normalize" {
			foundEdge = true
		}
	}
	if !foundEdge {
		t.Fatal("successor origin edge was not added")
	}
	deep := buildAcceptanceCandidate{
		Type: "aion_build_acceptance", AttemptID: attempt.AttemptID, Verdict: "successor", Summary: "Too deep.",
		Requirements: []buildAcceptanceRequirement{{ID: "R3", Status: "missing", Evidence: "deep."}},
		AddNodes: []coordinator.TaskNode{{
			ID: "alpha-too-deep", DomainID: "alpha", TaskSpec: "too deep", TargetFiles: []string{"alpha/deep.py"}, OriginNodeID: "alpha-normalize",
		}},
	}
	data, _ = json.Marshal(deep)
	if _, err := daemon.validateAcceptanceCandidate(attempt, data); err == nil {
		t.Fatal("successor depth limit was not enforced")
	}
}

func TestAcceptanceCompleteRejectsFailedNode(t *testing.T) {
	daemon, attempt := acceptanceFixture(t)
	if err := daemon.dagManager.UpdateNode("alpha-core", dag.StatusFailed); err != nil {
		t.Fatal(err)
	}
	complete := buildAcceptanceCandidate{
		Type: "aion_build_acceptance", AttemptID: attempt.AttemptID, Verdict: "complete", Summary: "Everything is complete.",
		Requirements: []buildAcceptanceRequirement{{ID: "R1", Status: "met", Evidence: "claimed."}},
	}
	data, _ := json.Marshal(complete)
	if _, err := daemon.validateAcceptanceCandidate(attempt, data); err == nil {
		t.Fatal("complete with a failed node was accepted")
	}
}

func TestAcceptanceSuccessorCreatesContract(t *testing.T) {
	daemon, attempt := acceptanceFixture(t)
	manager, err := coordination.Open(coordinationOptions(daemon.config, daemon.runState.CoordinationDir))
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	daemon.coordinationManager = manager
	attempt.Plan.Domains = append(attempt.Plan.Domains, coordinator.Domain{DomainID: "beta", AssignedPaths: []string{"beta/"}})
	successor := buildAcceptanceCandidate{
		Type: "aion_build_acceptance", AttemptID: attempt.AttemptID, Verdict: "successor", Summary: "Add a public follow-on contract.",
		Requirements: []buildAcceptanceRequirement{{ID: "R2", Status: "missing", Evidence: "A new public result contract is required."}},
		AddNodes: []coordinator.TaskNode{
			{ID: "alpha-result", DomainID: "alpha", TaskSpec: "Publish the follow-on result.", TargetFiles: []string{"alpha/result.py"}, OriginNodeID: "alpha-core"},
			{ID: "beta-result", DomainID: "beta", TaskSpec: "Consume the follow-on result.", TargetFiles: []string{"beta/result.py"}, OriginNodeID: "alpha-core"},
		},
		AddContracts: []coordinator.PlannedContract{{
			Resource: "alpha-result-v2", ProducerNode: "alpha-result", ConsumerNode: "beta-result",
			Kind: "interface", Language: "python", PublicEntrypoint: "alpha.result", Acceptance: []string{"consumer imports the follow-on result"},
			SupersedesContractID: "contract-alpha-core-old",
		}},
	}
	data, _ := json.Marshal(successor)
	patch, err := daemon.validateAcceptanceCandidate(attempt, data)
	if err != nil {
		t.Fatal(err)
	}
	if err := daemon.applyAcceptancePatch(attempt, patch); err != nil {
		t.Fatal(err)
	}
	contracts, err := manager.List(context.Background(), coordination.Principal{Role: coordination.RoleAdmin})
	if err != nil || len(contracts) != 1 {
		t.Fatalf("contracts = %#v, err=%v", contracts, err)
	}
	if contracts[0].Details.SupersedesContractID != "contract-alpha-core-old" {
		t.Fatalf("contract supersession = %#v", contracts[0].Details)
	}
}

func TestAcceptanceBriefClarifiesRuntimeAndHardWait(t *testing.T) {
	daemon, attempt := acceptanceFixture(t)
	attempt.Plan.Contracts = []coordinator.PlannedContract{{
		Resource: "alpha-public-api", ProducerNode: "alpha-core", ConsumerNode: "alpha-core", HardWait: true,
	}}
	contracts := []coordination.Contract{{
		ID: "contract-alpha", Status: coordination.StatusFulfilled, Revision: 1,
		Details: coordination.ContractDetails{Name: "alpha-public-api"}, HardWait: false,
	}}
	data, err := daemon.buildAcceptanceBriefData(attempt, contracts)
	if err != nil {
		t.Fatal(err)
	}
	var brief buildAcceptanceBrief
	if err := json.Unmarshal(data, &brief); err != nil {
		t.Fatal(err)
	}
	if len(brief.Contracts) != 1 || !brief.Contracts[0].HardWaitDeclared || brief.Contracts[0].HardWaitCurrent {
		t.Fatalf("contract brief = %#v", brief.Contracts)
	}
	if !strings.Contains(brief.TargetLanguageNote, "runtime may differ") || !strings.Contains(brief.HardWaitNote, "cleared") {
		t.Fatalf("brief notes = %#v", brief)
	}
	if len(brief.Requirements) == 0 || len(brief.OutputSchema) == 0 {
		t.Fatalf("brief content = %#v", brief)
	}
}

func TestAcceptanceFailureCodes(t *testing.T) {
	if acceptanceFailureCode(fmt.Errorf("resolution artifact was not written within 5m0s")) != "acceptance.timeout" {
		t.Fatal("timeout code not classified")
	}
	if acceptanceFailureCode(fmt.Errorf("acceptance review modified the build spec")) != "acceptance.spec_modified" {
		t.Fatal("spec mutation code not classified")
	}
}

func TestAcceptanceSchemaDefinesReworkShapes(t *testing.T) {
	var schema map[string]interface{}
	if err := json.Unmarshal(buildAcceptanceSchema("buildspec_1"), &schema); err != nil {
		t.Fatal(err)
	}
	reopen := schema["reopen_nodes"].([]interface{})[0].(map[string]interface{})
	if reopen["node_id"] == nil || reopen["instruction"] == nil || reopen["target_files"] == nil {
		t.Fatalf("reopen schema = %#v", reopen)
	}
	addNodes := schema["add_nodes"].([]interface{})[0].(map[string]interface{})
	if addNodes["origin_node_id"] == nil || addNodes["target_files"] == nil {
		t.Fatalf("add_nodes schema = %#v", addNodes)
	}
	addContracts := schema["add_contracts"].([]interface{})[0].(map[string]interface{})
	if addContracts["supersedes_contract_id"] == nil || addContracts["resource"] == nil {
		t.Fatalf("add_contracts schema = %#v", addContracts)
	}
}
