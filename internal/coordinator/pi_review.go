package coordinator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	isolation "aion-isolation"
	"aion-kernel/internal/supervisor"
)

// ReviewBuild runs one source-readable coordinator acceptance turn. It reuses
// the Architect isolation policy (read-only project skeleton plus writable
// docs), but writes only a structured acceptance report under docs/aion.
func (c *PiCoordinator) ReviewBuild(ctx context.Context, req BuildReviewRequest) (*BuildReviewResult, error) {
	if c.config.ArchitectIsolationEngine == nil {
		return nil, fmt.Errorf("pi_coordinator: acceptance review isolation is unavailable")
	}
	artifactDir, err := filepath.Abs(strings.TrimSpace(req.ArtifactDir))
	if err != nil || artifactDir == "" {
		return nil, fmt.Errorf("pi_coordinator: invalid acceptance artifact directory")
	}
	sessionDir, err := filepath.Abs(strings.TrimSpace(req.SessionDir))
	if err != nil || sessionDir == "" {
		return nil, fmt.Errorf("pi_coordinator: invalid acceptance session directory")
	}
	sandboxDir := strings.TrimSpace(req.SandboxDir)
	if sandboxDir == "" {
		return nil, fmt.Errorf("pi_coordinator: acceptance sandbox directory is required")
	}
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		return nil, fmt.Errorf("pi_coordinator: create acceptance artifacts: %w", err)
	}
	if err := os.MkdirAll(sessionDir, 0o700); err != nil {
		return nil, fmt.Errorf("pi_coordinator: create acceptance session: %w", err)
	}
	outputPath := filepath.Join(artifactDir, "acceptance_output.json")
	if err := os.Remove(outputPath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	env, err := c.reviewProcessEnv()
	if err != nil {
		return nil, fmt.Errorf("pi_coordinator: issue acceptance runtime credential: %w", err)
	}

	policy := cloneIsolationPolicy(c.config.ArchitectPolicy)
	policy.ID = "review-" + sanitizeResolutionID(req.AttemptID)
	policy.WorkingDir = "/workspace"
	policy.Hostname = "coordinator-review"
	policy.Network = isolation.NetworkIsolated
	policy.Writable = replaceMountSource(policy.Writable, "/state/pi", sessionDir)
	policy.Writable = replaceMount(policy.Writable, "/workspace/docs", artifactDir, sandboxDir)
	policy.ReadOnly = dropMountTarget(policy.ReadOnly, "/runtime/bin/orchestrator-cli")

	agent := supervisor.NewAgentSupervisor(supervisor.AgentConfig{
		AgentID:          "coordinator-review",
		DomainID:         "coordinator",
		InitialPrompt:    "",
		HeartbeatTimeout: 30 * time.Minute,
		ProgressTimeout:  30 * time.Minute,
		MaxCrashRestarts: 1,
		IsolationEngine:  c.config.ArchitectIsolationEngine,
		IsolationPolicy:  policy,
		PiAgent: supervisor.PiAgentConfig{
			Binary: c.config.ArchitectBinary, SessionDir: "/state/pi", HostSessionDir: sessionDir,
			WorkingDir: "/workspace", Provider: "aion-gateway", Endpoint: "http://unix",
			Model: c.config.Model, ExtensionPaths: c.config.ResolutionExtensionPaths,
			Env: append(env, "AION_AGENT_ID=coordinator-review", "AION_DOMAIN_ID=coordinator"),
		},
	})
	if req.Progress != nil {
		agent.SetLifecycleFunc(func(supervisor.AgentLifecycleEvent) { req.Progress() })
	}
	if err := agent.Start(ctx); err != nil {
		return nil, err
	}
	defer agent.Stop()
	if err := agent.SendPrompt(reviewPrompt(req, sandboxDir)); err != nil {
		return nil, fmt.Errorf("pi_coordinator: send acceptance prompt: %w", err)
	}
	timeout := c.config.ReviewArtifactTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	if err := waitForStableResolutionArtifact(ctx, outputPath, timeout); err != nil {
		return nil, err
	}
	return &BuildReviewResult{OutputPath: outputPath}, nil
}

func reviewPrompt(req BuildReviewRequest, sandboxDir string) string {
	sandboxDir = strings.TrimRight(strings.TrimSpace(sandboxDir), "/")
	input := sandboxDir + "/acceptance_input.json"
	template := sandboxDir + "/acceptance_template.json"
	output := sandboxDir + "/acceptance_output.json"
	if req.Repair {
		return fmt.Sprintf(`The prior acceptance report failed deterministic validation:
%s

Read %s and %s. Inspect the read-only project under /workspace. Overwrite %s with one valid JSON object using the template. Keep the exact type and attempt_id. Report only real missing or unverified requirements; do not invent work when the implementation is complete. Do not modify source files. Do not use orchestrator-cli. Do not answer in chat.`, strings.TrimSpace(req.ValidationError), input, template, output)
	}
	return fmt.Sprintf(`You are the Aion Coordinator performing final acceptance review. Read %s and %s. Inspect the read-only implementation under /workspace and compare it with the build spec, accepted plan, contract evidence, DAG completion state, and integration results in the input. Write one acceptance report to %s using the template. Use verdict complete only when every requirement is implemented and evidenced. Use verdict retry only for unmet original requirements and populate reopen_nodes; retry cannot add successors or contracts. Use verdict successor only for new or expanded work on a completed implementation and populate add_nodes with origin_node_id; successor cannot reopen nodes. Use add_contracts with a new resource and supersedes_contract_id when a public contract surface changes; never reopen a fulfilled contract. Use verdict attention when the evidence is insufficient or the state is inconsistent. Use only existing domain ownership. Do not modify source files. Do not use orchestrator-cli. Do not answer in chat.`, input, template, output)
}

func replaceMount(mounts []isolation.Mount, oldTarget, source, target string) []isolation.Mount {
	out := append([]isolation.Mount(nil), mounts...)
	for i := range out {
		if out[i].Target == oldTarget {
			out[i].Source = source
			out[i].Target = target
		}
	}
	return out
}

func replaceMountSource(mounts []isolation.Mount, target, source string) []isolation.Mount {
	out := append([]isolation.Mount(nil), mounts...)
	for i := range out {
		if out[i].Target == target {
			out[i].Source = source
		}
	}
	return out
}

func dropMountTarget(mounts []isolation.Mount, target string) []isolation.Mount {
	out := make([]isolation.Mount, 0, len(mounts))
	for _, mount := range mounts {
		if mount.Target == target {
			continue
		}
		out = append(out, mount)
	}
	return out
}
