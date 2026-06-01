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
		"## Build Spec",
		"build spec body",
		"## Excluded Paths",
		"`.aion/`",
		"Project-specific exclusions may also be listed in `.aionignore`",
		"Use the loaded skills and workspace conventions for operational commands and coordination details; do not restate command syntax here.",
		"Communicate with the orchestrator when you need clarification, status updates, or a scope decision.",
		"Coordinate with other domain agents only when the task truly crosses ownership boundaries.",
		"project context",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
}
