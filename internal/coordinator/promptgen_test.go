package coordinator

import (
	"strings"
	"testing"
)

func TestGenerateSystemInstructionsIncludesBuildSpecAndCoordinationRules(t *testing.T) {
	plan := &PlanResponse{
		Domains: []Domain{{
			DomainID:      "api",
			Description:   "API domain",
			AssignedPaths: []string{"internal/api"},
			AgentType:     "domain",
		}},
	}

	prompts, err := GenerateSystemInstructions(plan, "project context", "build spec body")
	if err != nil {
		t.Fatalf("GenerateSystemInstructions: %v", err)
	}
	prompt := prompts["api"]
	for _, want := range []string{
		"## Mission",
		"Remain idle until the orchestrator dispatches work",
		"orchestrator-cli work-inbox --current",
		"execute only the authoritative item it returns",
		"A DAG node is actionable only when its state is `active`",
		"## Build Spec",
		"build spec body",
		"isolated view containing only your domain's writable ownership mounts",
		"Every persistent implementation, fixture, and test file must be created beneath one of your assigned paths",
		"/workspace/<domain>/tests/",
		"do not create shared root directories such as `/workspace/tests`",
		"The loaded orchestrator-cli skill is authoritative for commands and workflow.",
		"integration/directives/*.json",
		"reserved kernel execution manifests",
		"overrides any contradictory Build Spec wording",
		"application input, configuration",
		"strictly rejects unknown fields",
		"never set it to `.`",
		"read-only, network-isolated integration workspace",
		"do not create a local peer implementation",
		"test-local module/import mocks",
		"Execute only the item returned by `work-inbox --current`",
		"pass its exact raw `node_id` to `update-node`",
		"use `contract-ensure` with the producer node, consumer node, and stable resource name",
		"Use `--wait` for a hard dependency",
		"Contract publication, validation, and correction arrive as separate ready inbox items",
		"record a concise `work-checkpoint`",
		"Use durable artifact contracts or orchestrator messages for cross-domain work",
		"Report blocked work instead of inventing missing dependencies or ownership.",
		"project context",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestGenerateCoordinatorPlanningInstructionUsesArtifactOnly(t *testing.T) {
	prompt := GenerateCoordinatorPlanningInstruction("FULL BUILD SPEC SHOULD NOT BE INLINE", &ProjectScan{
		RootPath:    "/tmp/project",
		Languages:   []string{"Python"},
		FileCount:   12,
		ModuleCount: 3,
		EntryPoints: []string{"pyrdbms/__main__.py"},
	}, "/tmp/project/docs/aion/planning_input.json", "/tmp/project/docs/aion/plan_response.json")

	for _, want := range []string{
		"Planning input artifact: /tmp/project/docs/aion/planning_input.json",
		"Output artifact: /tmp/project/docs/aion/plan_response.json",
		"write the output artifact",
		"complete, parseable JSON",
		"\"from_node\":\"source-task-id\"",
		"exact `from_node` and `to_node` field names",
		"`from_node` is the prerequisite/producer",
		"one cohesive implementation-and-test node per explicit domain",
		"enumerate the concrete test files in that node's target_files",
		"\"contracts\":[",
		"\"producer_node\":\"source-task-id\"",
		"\"public_entrypoint\":\"exact consumer-visible import, package, route, or resource locator\"",
		"never make an isolated consumer guess an import path",
		"Every target under `integration/directives/` must be a concrete file ending in `.json`",
		"reserved for schema-v1 kernel execution manifests",
		"never as application input or configuration",
		"Honor any explicit domain count or ownership split",
		"never assign it to a domain or target it from a node",
		"The build spec is inside the planning input artifact",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
	for _, forbidden := range []string{
		"FULL BUILD SPEC SHOULD NOT BE INLINE",
		"BUILD SPEC\n",
	} {
		if strings.Contains(prompt, forbidden) {
			t.Fatalf("prompt should not contain %q:\n%s", forbidden, prompt)
		}
	}
}

func TestGenerateArchitectInstructionDefinesReservedIntegrationDirectiveSemantics(t *testing.T) {
	prompt := GenerateArchitectInstruction("")
	for _, want := range []string{
		"reserved kernel execution-manifest namespace",
		"schema-v1 executable acceptance manifest",
		"Never describe it as application input/configuration",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("architect prompt missing %q:\n%s", want, prompt)
		}
	}
}
