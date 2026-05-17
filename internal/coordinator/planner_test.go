package coordinator

import (
	"strings"
	"testing"
)

func TestPlanFromPromptAndScanNarrativeSpec(t *testing.T) {
	spec := `# Sample Spec

## Project Overview
Build a sample service.

## Core Functional Requirements
- HTTP API
- Persistence layer

## Deployment & Operations
- Linux deployment
`
	scan := &ProjectScan{
		EntryPoints:     []string{"cmd/server/main.go"},
		DependencyFiles: []string{"go.mod"},
	}

	plan, err := PlanFromPromptAndScan(spec, scan)
	if err != nil {
		t.Fatalf("PlanFromPromptAndScan: %v", err)
	}
	if len(plan.Domains) == 0 || len(plan.Nodes) == 0 {
		t.Fatalf("expected non-empty plan: %#v", plan)
	}
	if err := ValidatePlanResponse(plan); err != nil {
		t.Fatalf("ValidatePlanResponse: %v", err)
	}
}

func TestValidatePlanResponseRejectsInvalid(t *testing.T) {
	plan := &PlanResponse{
		Domains: []Domain{{DomainID: "...", AssignedPaths: []string{"."}}},
		Nodes:   []TaskNode{{ID: "...", DomainID: "..."}},
	}
	if err := ValidatePlanResponse(plan); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestParsePlanResponseText(t *testing.T) {
	raw := "```json\n{\"type\":\"plan_response\",\"domains\":[{\"domain_id\":\"api\",\"description\":\"API\",\"assigned_paths\":[\"cmd/\"],\"agent_type\":\"domain\"}],\"nodes\":[{\"id\":\"api-task\",\"domain_id\":\"api\",\"task_spec\":\"build api\",\"target_files\":[\"cmd/server/main.go\"],\"priority\":1}],\"edges\":[]}\n```"

	plan, err := ParsePlanResponseText(raw)
	if err != nil {
		t.Fatalf("ParsePlanResponseText: %v", err)
	}
	if len(plan.Domains) != 1 || len(plan.Nodes) != 1 {
		t.Fatalf("unexpected plan: %#v", plan)
	}
}

func TestParsePlanResponseTextIgnoresMarkdownAroundJSON(t *testing.T) {
	raw := "# Coordinator Plan\n\nHere is the final plan:\n\n{\"type\":\"plan_response\",\"domains\":[{\"domain_id\":\"api\",\"description\":\"API\",\"assigned_paths\":[\"cmd/\"],\"agent_type\":\"domain\"}],\"nodes\":[{\"id\":\"api-task\",\"domain_id\":\"api\",\"task_spec\":\"build api\",\"target_files\":[\"cmd/server/main.go\"],\"priority\":1}],\"edges\":[]}\n"

	plan, err := ParsePlanResponseText(raw)
	if err != nil {
		t.Fatalf("ParsePlanResponseText: %v", err)
	}
	if len(plan.Domains) != 1 || len(plan.Nodes) != 1 {
		t.Fatalf("unexpected plan: %#v", plan)
	}
}

func TestParsePlanResponseTextExtractsMarkedJSON(t *testing.T) {
	raw := "AION_PLAN_RESPONSE_JSON_BEGIN\n{\"type\":\"plan_response\",\"domains\":[{\"domain_id\":\"api\",\"description\":\"API\",\"assigned_paths\":[\"cmd/\"],\"agent_type\":\"domain\"}],\"nodes\":[{\"id\":\"api-task\",\"domain_id\":\"api\",\"task_spec\":\"build api\",\"target_files\":[\"cmd/server/main.go\"],\"priority\":1}],\"edges\":[]}\nAION_PLAN_RESPONSE_JSON_END"

	plan, err := ParsePlanResponseText(raw)
	if err != nil {
		t.Fatalf("ParsePlanResponseText: %v", err)
	}
	if len(plan.Domains) != 1 || len(plan.Nodes) != 1 {
		t.Fatalf("unexpected plan: %#v", plan)
	}
}

func TestParsePlanResponseTextRejectsMarkdownOnly(t *testing.T) {
	raw := "# Coordinator Plan\n\nNo JSON here."
	if _, err := ParsePlanResponseText(raw); err == nil {
		t.Fatal("expected parse error")
	} else if !strings.Contains(err.Error(), "preview:") {
		t.Fatalf("expected preview in error, got %v", err)
	}
}

func TestGenerateCoordinatorInstruction(t *testing.T) {
	prompt := GenerateCoordinatorInstruction("workspace context")
	if prompt == "" {
		t.Fatal("expected prompt content")
	}
}
