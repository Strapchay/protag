package coordinator

import (
	"bytes"
	"fmt"
	"sort"
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
func GenerateSystemInstructions(plan *PlanResponse, projectContext, buildSpec string) (map[string]string, error) {
	prompts := make(map[string]string)

	for _, domain := range plan.Domains {
		prompt := buildDomainPrompt(domain, projectContext, buildSpec)
		prompts[domain.DomainID] = prompt
	}

	return prompts, nil
}

func buildDomainPrompt(domain Domain, projectContext, buildSpec string) string {
	var b strings.Builder

	b.WriteString(fmt.Sprintf("# Domain Agent: %s\n\n", domain.DomainID))
	b.WriteString(fmt.Sprintf("## Description\n%s\n\n", domain.Description))
	b.WriteString("## Mission\n")
	b.WriteString("Prepare to implement only work dispatched to your domain. Keep all later changes inside the assigned paths and the dispatched node/task scope.\n")
	b.WriteString("This bootstrap turn is orientation-only: do not inspect project files, run implementation tools, or modify files until the orchestrator sends a `task_dispatch` message. Acknowledge readiness briefly, then wait.\n\n")
	b.WriteString("Your current working directory is a filtered source workspace for your assigned domain. Treat it as the project source view; do not cd outside it or inspect parent/runtime directories.\n\n")

	if strings.TrimSpace(buildSpec) != "" {
		b.WriteString("## Build Spec\n")
		b.WriteString(buildSpec)
		b.WriteString("\n\n")
	}

	b.WriteString("## Assigned Paths\n")
	for _, p := range domain.AssignedPaths {
		b.WriteString(fmt.Sprintf("- `%s`\n", p))
	}
	b.WriteString("\n")

	b.WriteString("## Excluded Paths\n")
	b.WriteString("Do not inspect, read, search, summarize, modify, or reason from these runtime/generated paths unless the orchestrator explicitly instructs you to debug Aion runtime state:\n")
	for _, p := range DefaultAgentExcludePaths() {
		b.WriteString(fmt.Sprintf("- `%s`\n", p))
	}
	b.WriteString("Project-specific exclusions may also be listed in `.aionignore`; obey them as part of your domain boundary.\n\n")

	b.WriteString("## Protocol\n")
	b.WriteString("You are participating in a multi-agent orchestrated process.\n")
	b.WriteString("The orchestrator owns task allocation and coordination. Your job is to execute the domain scope, not to broaden it.\n")
	b.WriteString("Use the loaded skills and workspace conventions for operational commands and coordination details; do not restate command syntax here.\n")
	b.WriteString("- Report node progress through the coordination workflow defined by your skills.\n")
	b.WriteString("- Communicate with the orchestrator when you need clarification, status updates, or a scope decision.\n")
	b.WriteString("- Coordinate with other domain agents only when the task truly crosses ownership boundaries.\n")
	b.WriteString("- If your work requires another domain to change files outside your scope, ask for a stub or a handoff instead of widening your own scope.\n")
	b.WriteString("- Do not assume you have every Pi capability available. Stay within the supplied skills, your assigned domain scope, and the build spec.\n")
	b.WriteString("Only when you receive a `task_dispatch` follow-up should you orient to that node, execute it, and complete it using the coordination flow in your skills.\n")

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
	b.WriteString("- Do not inspect, summarize, or modify `.aion/`, `.git/`, `.agents/`, `.codex/`, dependency caches, build outputs, or paths listed in `.aionignore`; these are runtime/generated areas, not project source.\n")
	b.WriteString("- Do not ask the user to create this file manually; creating the directory and file is part of your spec finalization responsibility.\n")
	b.WriteString("- Once the file is written, tell the user they can initiate the engineering swarm by issuing the command: `/build-spec`.\n\n")

	if projectContext != "" {
		b.WriteString("## CURRENT PROJECT CONTEXT\n")
		b.WriteString(projectContext)
		b.WriteString("\n\n")
	}

	return b.String()
}

// GenerateCoordinatorInstruction builds the system prompt for the persistent
// coordinator planner agent that reasons over build specs.
func GenerateCoordinatorInstruction(projectContext string) string {
	var b strings.Builder
	b.WriteString("# COORDINATOR PLANNER MODE\n\n")
	b.WriteString("You are the Aion-Kernel Coordinator Planner. Your job is to reason over a finalized build spec and the project scan, then emit a valid plan_response JSON object.\n\n")
	b.WriteString("## GUIDELINES\n")
	b.WriteString("- Analyze the spec and project scan before deciding on domains, nodes, and edges.\n")
	b.WriteString("- Do not assume a fixed number of agents or tasks.\n")
	b.WriteString("- Prefer explicit dependencies only when the work truly requires ordering.\n")
	b.WriteString("- Output only JSON content that matches the plan_response schema.\n")
	b.WriteString("- Do not assign runtime/generated paths such as `.aion/`, `.git/`, `.agents/`, `.codex/`, dependency caches, build outputs, or `.aionignore` entries to any domain or node.\n")
	b.WriteString("- Keep the response structured and machine-readable.\n\n")

	if projectContext != "" {
		b.WriteString("## CURRENT PROJECT CONTEXT\n")
		b.WriteString(projectContext)
		b.WriteString("\n\n")
	}

	return b.String()
}

// GenerateCoordinatorPlanningInstruction builds the prompt for the Coordinator
// Pi agent to transform a build spec into a structured plan.
func GenerateCoordinatorPlanningInstruction(_ string, scan *ProjectScan, inputPath, outputPath string) string {
	var b strings.Builder
	b.WriteString("Read the Coordinator planning input artifact and write the plan artifact.\n")
	b.WriteString("Do not return the final plan in chat. The chat stream is only for short status updates and observability.\n")
	b.WriteString("Planning input artifact: ")
	b.WriteString(inputPath)
	b.WriteString("\n")
	b.WriteString("Output artifact: ")
	b.WriteString(outputPath)
	b.WriteString("\n\n")
	b.WriteString("Write a JSON object directly to the output artifact with this shape:\n")
	b.WriteString("{\"type\":\"plan_response\",\"domains\":[{\"domain_id\":\"short-stable-domain-id\",\"description\":\"owned implementation area\",\"assigned_paths\":[\"relative/path\"],\"agent_type\":\"domain\"}],\"nodes\":[{\"id\":\"short-stable-task-id\",\"domain_id\":\"short-stable-domain-id\",\"task_spec\":\"specific implementation task\",\"target_files\":[\"relative/path/file.ext\"],\"priority\":1}],\"edges\":[]}\n\n")
	b.WriteString("The artifact must contain real, non-empty `domains` and `nodes` arrays. Do not copy placeholder IDs or empty arrays.\n")
	b.WriteString("Keep the initial plan coarse enough for reliable artifact writing: prefer 3-8 domains and 1-3 nodes per domain. Do not exceed 18 nodes unless the spec truly cannot be represented otherwise.\n")
	b.WriteString("Use the `excluded_paths` array from the planning input artifact. Do not inspect `.aionignore`; the daemon already resolved it for you.\n")
	b.WriteString("Do not run exploratory shell commands or inspect runtime/generated folders. Read the input artifact, plan from it, and write the output artifact.\n")
	b.WriteString("The daemon will wait until the output becomes complete, parseable JSON and then validate its plan contract.\n")
	b.WriteString("After writing the file, you may briefly state that the artifact was written.\n\n")

	if scan != nil {
		b.WriteString("PROJECT SCAN SUMMARY\n")
		if scan.RootPath != "" {
			b.WriteString("Root: ")
			b.WriteString(scan.RootPath)
			b.WriteString("\n")
		}
		if len(scan.Languages) > 0 {
			b.WriteString("Languages: ")
			b.WriteString(strings.Join(scan.Languages, ", "))
			b.WriteString("\n")
		}
		if len(scan.EntryPoints) > 0 {
			entryPoints := append([]string(nil), scan.EntryPoints...)
			sort.Strings(entryPoints)
			b.WriteString("Entry points:\n")
			for _, ep := range entryPoints {
				b.WriteString("- ")
				b.WriteString(ep)
				b.WriteString("\n")
			}
		}
		if len(scan.DependencyFiles) > 0 {
			deps := append([]string(nil), scan.DependencyFiles...)
			sort.Strings(deps)
			b.WriteString("Dependency files:\n")
			for _, dep := range deps {
				b.WriteString("- ")
				b.WriteString(dep)
				b.WriteString("\n")
			}
		}
		b.WriteString("File count: ")
		b.WriteString(fmt.Sprintf("%d", scan.FileCount))
		b.WriteString("\nModule count: ")
		b.WriteString(fmt.Sprintf("%d", scan.ModuleCount))
		b.WriteString("\n\n")
	}

	b.WriteString("The build spec is inside the planning input artifact. Write the output artifact now.\n")

	return b.String()
}
