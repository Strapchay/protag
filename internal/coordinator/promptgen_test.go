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
		"This bootstrap turn is orientation-only",
		"do not inspect project files, run implementation tools, or modify files until the orchestrator sends a `task_dispatch` message",
		"## Build Spec",
		"build spec body",
		"## Excluded Paths",
		"`.aion/`",
		"Project-specific exclusions may also be listed in `.aionignore`",
		"The loaded orchestrator-cli skill is authoritative for commands and workflow.",
		"Execute only dispatched nodes assigned to this agent",
		"Use stub contracts or orchestrator messages for cross-domain work",
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
		"prefer 3-8 domains and 1-3 nodes per domain",
		"Use the `excluded_paths` array",
		"Do not inspect `.aionignore`",
		"The build spec is inside the planning input artifact",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
	for _, forbidden := range []string{
		"FULL BUILD SPEC SHOULD NOT BE INLINE",
		"BUILD SPEC\n",
		"paths listed in `.aionignore`",
	} {
		if strings.Contains(prompt, forbidden) {
			t.Fatalf("prompt should not contain %q:\n%s", forbidden, prompt)
		}
	}
}
