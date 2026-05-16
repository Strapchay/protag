package coordinator

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
)

// PromptData contains all the data needed to render an AGENTS.md template.
type PromptData struct {
	DomainID          string
	AgentID           string
	DomainDescription string
	AssignedPaths     []string
}

// GenerateSystemInstructions creates per-agent system prompts from a plan response.
func GenerateSystemInstructions(plan *PlanResponse, projectContext string) (map[string]string, error) {
	prompts := make(map[string]string)

	for _, domain := range plan.Domains {
		prompt := buildDomainPrompt(domain, projectContext)
		prompts[domain.DomainID] = prompt
	}

	return prompts, nil
}

func buildDomainPrompt(domain Domain, projectContext string) string {
	var b strings.Builder

	b.WriteString(fmt.Sprintf("# Domain Agent: %s\n\n", domain.DomainID))
	b.WriteString(fmt.Sprintf("## Description\n%s\n\n", domain.Description))

	b.WriteString("## Assigned Paths\n")
	for _, p := range domain.AssignedPaths {
		b.WriteString(fmt.Sprintf("- `%s`\n", p))
	}
	b.WriteString("\n")

	b.WriteString("## Protocol\n")
	b.WriteString("You are participating in a multi-agent orchestrated process.\n")
	b.WriteString("Tasks will no longer be listed upfront. Instead, the Aion-Kernel Orchestrator will DISPATCH tasks to you dynamically over your JSON-RPC connection.\n")
	b.WriteString("When you receive a `task_dispatch` follow-up, orient yourself, execute it, and then mark the target node as `Done` using the orchestrator-cli.\n")

	if projectContext != "" {
		b.WriteString("\n## Project Context\n")
		b.WriteString(projectContext)
		b.WriteString("\n\n")
	}

	return b.String()
}

// RenderAgentsTemplate renders the AGENTS.md.tmpl with the given data.
func RenderAgentsTemplate(tmplContent string, data PromptData) (string, error) {
	tmpl, err := template.New("agents").Parse(tmplContent)
	if err != nil {
		return "", fmt.Errorf("promptgen: parse template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("promptgen: execute template: %w", err)
	}

	return buf.String(), nil
}

// GenerateArchitectInstruction builds the system prompt for the solution architect mode.
func GenerateArchitectInstruction(projectContext string) string {
	var b strings.Builder
	b.WriteString("# SOLUTION ARCHITECT MODE\n\n")
	b.WriteString("You are the Aion-Kernel Solution Architect. Your goal is to work with the user to build a precise technical specification for their project.\n\n")
	b.WriteString("## GUIDELINES\n")
	b.WriteString("- Analyze the current project structure and files.\n")
	b.WriteString("- Ask clarifying questions about technical stack, architecture, and requirements.\n")
	b.WriteString("- Refine the user's high-level goal into a structured spec.\n")
	b.WriteString("- DO NOT start building or modifying files yourself.\n")
	b.WriteString("- When the spec is finalized, write it to `docs/build_spec.md` using your file-writing skills.\n")
	b.WriteString("- If the `docs/` directory does not exist in the working directory, create it before writing `docs/build_spec.md`.\n")
	b.WriteString("- Do not ask the user to create this file manually; creating the directory and file is part of your spec finalization responsibility.\n")
	b.WriteString("- Once the file is written, tell the user they can initiate the engineering swarm by issuing the command: `/build-spec`.\n\n")

	if projectContext != "" {
		b.WriteString("## CURRENT PROJECT CONTEXT\n")
		b.WriteString(projectContext)
		b.WriteString("\n\n")
	}

	return b.String()
}
