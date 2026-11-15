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
	b.WriteString("Remain idle until the orchestrator dispatches work. At the start of every dispatched turn, run `orchestrator-cli work-inbox --current` and execute only the authoritative item it returns. A DAG node is actionable only when its state is `active`; a ready contract item is actionable when explicitly dispatched. If no current item is returned, end the turn and wait. Report the item's terminal state before ending the turn.\n\n")
	b.WriteString("Your current working directory is an isolated view containing only your domain's writable ownership mounts. Other domains and kernel runtime state are absent.\n\n")
	b.WriteString("Every persistent implementation, fixture, and test file must be created beneath one of your assigned paths. Keep focused tests in an owned test directory such as `/workspace/<domain>/tests/`. The sandbox root outside ownership mounts is ephemeral; do not create shared root directories such as `/workspace/tests` unless that path is explicitly assigned to your domain.\n\n")
	b.WriteString("Paths beneath `<assigned-path>/integration/directives/*.json` are reserved kernel execution manifests. Create one only when its exact path appears in the active node's target files. Cross-domain directives are consumer-owned; producer nodes publish public contract descriptors and focused unit tests instead of inventing directive paths. This kernel policy is authoritative and overrides any contradictory Build Spec wording that calls such a file application input, configuration, or something the domain implementation should load. Use this exact field shape: `{\"schema_version\":1,\"id\":\"stable-id\",\"command\":[\"/usr/bin/program\",\"arg\"],\"required_contracts\":[],\"owner_node_ids\":[\"your-node-id\"],\"timeout_sec\":30}`. `working_directory` is optional; omit it for the assembled workspace root, and never set it to `.`. The command must execute the consumer's acceptance behavior against the assembled workspace. The kernel strictly rejects unknown fields and assembles directives with fulfilled public contract artifacts in a read-only, network-isolated integration workspace.\n\n")
	b.WriteString("When your implementation imports a peer domain that is intentionally absent from your isolated workspace, do not create a local peer implementation. Build focused unit tests with test-local module/import mocks that match the accepted contract, and use the integration directive to exercise the real producer and consumer artifacts after kernel assembly.\n\n")

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

	b.WriteString("## Coordination\n")
	b.WriteString("The loaded orchestrator-cli skill is authoritative for commands and workflow.\n")
	b.WriteString("- Execute only the item returned by `work-inbox --current`; never claim a DAG node unless it is `active` and assigned to you, and do not reorder the kernel-owned inbox.\n")
	b.WriteString("- For a `dag_node` item, pass its exact raw `node_id` to `update-node`; its inbox `id` is the same value. For contract work, pass `contract_id` to lifecycle commands and `id` to `work-checkpoint`.\n")
	b.WriteString("- When a dispatched node needs a cross-domain resource, use `contract-ensure` with the producer node, consumer node, and stable resource name. Use `--wait` for a hard dependency, then end the turn instead of polling or inventing the artifact.\n")
	b.WriteString("- Contract publication, validation, and correction arrive as separate ready inbox items. Process them before the kernel dispatches another DAG node.\n")
	b.WriteString("- Before yielding incomplete work, record a concise `work-checkpoint` with the current item ID, completed result, and next concrete action.\n")
	b.WriteString("- Use durable artifact contracts or orchestrator messages for cross-domain work; do not widen your own file scope.\n")
	b.WriteString("- Report blocked work instead of inventing missing dependencies or ownership.\n")

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
	b.WriteString("- Your filesystem view is restricted to `/workspace` (the project) and `/runtime` (skills and extensions). Kernel source, kernel binaries, `/home`, and `/tmp` are not mounted. Never attempt to read them.\n")
	b.WriteString("- Do not use `strings`, `grep`, `sed`, or `find` against kernel/runtime installation paths to discover schemas. Use only the loaded orchestrator-cli skill.\n")
	b.WriteString("- Contracts are referenced by stable resource names. Never emit kernel-generated contract IDs; the kernel creates them and supplies exact IDs in dispatch.\n")
	b.WriteString("- If a schema or identifier is not documented in the skill, state your assumption in the spec instead of inspecting kernel internals.\n")
	b.WriteString("- Write only `docs/build_spec.md` and supporting files beneath `docs/`.\n")
	b.WriteString("- Analyze the current project structure and files.\n")
	b.WriteString("- Ask clarifying questions about technical stack, architecture, and requirements.\n")
	b.WriteString("- Refine the user's high-level goal into a structured spec.\n")
	b.WriteString("- State every required cross-domain public artifact as a producer node, consumer node, stable resource name, artifact kind, and acceptance criteria so the planner can create a durable contract.\n")
	b.WriteString("- Every cross-domain contract must name a consumer-owned integration directive under the consumer's assigned `integration/directives/` directory. Producers publish public descriptors and unit tests; consumers own assembled acceptance directives.\n")
	b.WriteString("- Any integration directive file named in the spec must end in `.json`.\n")
	b.WriteString("- `integration/directives/*.json` is a reserved kernel execution-manifest namespace. Describe each such file as a schema-v1 executable acceptance manifest with `schema_version`, `id`, an absolute command array, required contract IDs, owner node IDs, and a timeout. Never describe it as application input/configuration or instruct product code to load it.\n")
	b.WriteString("- DO NOT start building or modifying files yourself.\n")
	b.WriteString("- When the spec is finalized, write it to `docs/build_spec.md` using your file-writing skills.\n")
	b.WriteString("- If the `docs/` directory does not exist in the working directory, create it before writing `docs/build_spec.md`.\n")
	b.WriteString("- Treat generated/runtime directories as implementation noise unless they are directly relevant to the specification.\n")
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
	b.WriteString("- A dependency edge is prerequisite -> dependent: from_node must finish before to_node can start.\n")
	b.WriteString("- Keep implementation and its tests in one domain node unless the spec explicitly requires independently schedulable stages.\n")
	b.WriteString("- Use only project-relative, non-overlapping `assigned_paths`, and keep every node target beneath its domain's assigned paths.\n")
	b.WriteString("- When a node requires tests, include every planned test file in `target_files` beneath that domain's `assigned_paths`. Do not rely on an unassigned shared test root.\n")
	b.WriteString("- Convert every explicit cross-domain public artifact into one `contracts` entry with exact producer and consumer node IDs and a matching producer-to-consumer edge.\n")
	b.WriteString("- Every cross-domain contract's consumer node must own a concrete `integration/directives/*.json` target. Its task must identify that target as a schema-v1 assembled acceptance manifest. Do not assign the contract's directive to its producer.\n")
	b.WriteString("- Output only JSON content that matches the plan_response schema.\n")
	b.WriteString("- Do not assign paths listed in the planning artifact's `excluded_paths` to any domain or node.\n")
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
	b.WriteString("{\"type\":\"plan_response\",\"domains\":[{\"domain_id\":\"short-stable-domain-id\",\"description\":\"owned implementation area\",\"assigned_paths\":[\"relative/path\"],\"agent_type\":\"domain\"}],\"nodes\":[{\"id\":\"short-stable-task-id\",\"domain_id\":\"short-stable-domain-id\",\"task_spec\":\"specific implementation task\",\"target_files\":[\"relative/path/file.ext\"],\"priority\":1}],\"edges\":[{\"from_node\":\"source-task-id\",\"to_node\":\"dependent-task-id\"}],\"contracts\":[{\"resource\":\"stable-public-resource\",\"producer_node\":\"source-task-id\",\"consumer_node\":\"dependent-task-id\",\"kind\":\"interface\",\"language\":\"implementation-language\",\"public_entrypoint\":\"exact consumer-visible import, package, route, or resource locator\",\"acceptance\":[\"machine-checkable condition\"],\"hard_wait\":true,\"handshake_required\":false}]}\n\n")
	b.WriteString("Every edge must use the exact `from_node` and `to_node` field names shown above. Use an empty `edges` array when there are no dependencies.\n")
	b.WriteString("Edge direction is strict: `from_node` is the prerequisite/producer that must finish first; `to_node` is the blocked dependent/consumer. Put provider implementations before consumers and implementations before separately modeled validation/test nodes.\n")
	b.WriteString("Use an empty `contracts` array when no cross-domain artifact is required. For every explicit cross-domain API, schema, fixture, protocol, or public interface in the build spec, add one contract using exact node IDs and a matching producer-to-consumer edge. Every interface contract must name the implementation language and exact consumer-visible public_entrypoint; never make an isolated consumer guess an import path, package, route, symbol, or resource locator. Never leave such a dependency for agents to discover after execution begins.\n")
	b.WriteString("Set `handshake_required` to true only when the build spec requires the consumer to send an acknowledged peer request (for example a consumer plan) to the producer before it may validate the contract; otherwise omit it or set it to false. Do not infer a handshake from unrelated task wording.\n")
	b.WriteString("For every cross-domain contract, include at least one concrete `integration/directives/*.json` target in the consumer node. The consumer directive must execute assembled acceptance behavior against the real producer artifact; producer nodes publish the public descriptor and focused unit tests instead of owning that cross-domain directive.\n")
	b.WriteString("The artifact must contain real, non-empty `domains` and `nodes` arrays. Do not copy placeholder IDs or empty arrays.\n")
	b.WriteString("All assigned_paths must be project-relative and non-overlapping. Every target_file must be beneath its node domain's assigned_paths.\n")
	b.WriteString("Keep the initial plan coarse enough for reliable artifact writing: normally use one cohesive implementation-and-test node per explicit domain. Split a domain only when its work is independently schedulable or the build spec explicitly requires separate stages. Honor any explicit domain count or ownership split in the build spec. Do not invent documentation, runtime, test-only, or placeholder domains/nodes to reach a preferred count. Do not exceed 18 nodes unless the spec truly cannot be represented otherwise.\n")
	b.WriteString("If a task_spec requires tests, enumerate the concrete test files in that node's target_files and keep them under the same domain's assigned_paths. Never place tests in an unassigned shared root.\n")
	b.WriteString("Every target under `integration/directives/` must be a concrete file ending in `.json`. Treat that namespace as reserved for schema-v1 kernel execution manifests; describe it in the node task as an executable acceptance check and never as application input or configuration, even if the build spec uses conflicting terminology.\n")
	b.WriteString("Treat every entry in the planning input `excluded_paths` array as forbidden: never assign it to a domain or target it from a node.\n")
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
