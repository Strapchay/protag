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

const (
	resolutionSandboxRoot      = "/workspace"
	resolutionSandboxArtifacts = "/workspace/artifacts"
	resolutionSandboxSession   = "/state/pi"
)

// ResolveContract runs one short-lived, source-blind coordinator turn. The
// only mutable inputs visible to it are the sealed artifact directory and its
// own Pi session directory.
func (c *PiCoordinator) ResolveContract(ctx context.Context, req ResolutionRequest) (*ResolutionResult, error) {
	if c.config.ResolutionIsolationEngine == nil {
		return nil, fmt.Errorf("pi_coordinator: resolution isolation is unavailable")
	}
	artifactDir, err := filepath.Abs(strings.TrimSpace(req.ArtifactDir))
	if err != nil || artifactDir == "" {
		return nil, fmt.Errorf("pi_coordinator: invalid resolution artifact directory")
	}
	sessionDir, err := filepath.Abs(strings.TrimSpace(req.SessionDir))
	if err != nil || sessionDir == "" {
		return nil, fmt.Errorf("pi_coordinator: invalid resolution session directory")
	}
	if err := os.MkdirAll(artifactDir, 0o700); err != nil {
		return nil, fmt.Errorf("pi_coordinator: create resolution artifacts: %w", err)
	}
	if err := os.MkdirAll(sessionDir, 0o700); err != nil {
		return nil, fmt.Errorf("pi_coordinator: create resolution session: %w", err)
	}
	outputPath := filepath.Join(artifactDir, "resolution_output.json")
	if err := os.Remove(outputPath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	processEnv, err := c.resolutionProcessEnv()
	if err != nil {
		return nil, fmt.Errorf("pi_coordinator: issue resolution runtime credential: %w", err)
	}

	policy := cloneIsolationPolicy(c.config.ResolutionPolicy)
	policy.ID = "resolution-" + sanitizeResolutionID(req.ResolutionID)
	policy.WorkingDir = resolutionSandboxArtifacts
	policy.Hostname = "coordinator-resolution"
	policy.Network = isolation.NetworkIsolated
	// Workspace mounts must be strict descendants of a source root; the
	// sandbox owns /workspace itself and exposes only the sealed artifact
	// directory beneath it.
	policy.SourceRoot = filepath.Dir(artifactDir)
	policy.Writable = append(policy.Writable,
		isolation.Mount{Source: artifactDir, Target: resolutionSandboxArtifacts, Kind: isolation.MountDirectory},
		isolation.Mount{Source: sessionDir, Target: resolutionSandboxSession, Kind: isolation.MountDirectory},
	)

	prompt := resolutionPrompt(req)
	agent := supervisor.NewAgentSupervisor(supervisor.AgentConfig{
		AgentID:          "coordinator-resolution",
		DomainID:         "coordinator",
		InitialPrompt:    "",
		HeartbeatTimeout: 30 * time.Minute,
		ProgressTimeout:  30 * time.Minute,
		// The reconciliation state machine owns its one bounded process retry.
		// The supervisor must not add an independent respawn loop.
		MaxCrashRestarts: 1,
		IsolationEngine:  c.config.ResolutionIsolationEngine,
		IsolationPolicy:  policy,
		PiAgent: supervisor.PiAgentConfig{
			Binary: c.config.ResolutionBinary, SessionDir: resolutionSandboxSession, HostSessionDir: sessionDir,
			WorkingDir: resolutionSandboxArtifacts, Provider: "aion-gateway", Endpoint: "http://unix",
			Model: c.config.Model, SkillPaths: c.config.ResolutionSkillPaths,
			ExtensionPaths: c.config.ResolutionExtensionPaths,
			Env: append(processEnv,
				"AION_AGENT_ID=coordinator-resolution", "AION_DOMAIN_ID=coordinator"),
		},
	})
	if req.Progress != nil {
		c.setResolutionProgress(req.Progress)
		defer c.setResolutionProgress(nil)
		agent.SetLifecycleFunc(func(supervisor.AgentLifecycleEvent) {
			req.Progress()
		})
	}
	if err := agent.Start(ctx); err != nil {
		return nil, err
	}
	defer agent.Stop()
	// A direct prompt is required for a newly started or idle Pi process. A
	// follow-up can remain queued indefinitely in that state.
	if err := agent.SendPrompt(prompt); err != nil {
		return nil, fmt.Errorf("pi_coordinator: send resolution prompt: %w", err)
	}
	if err := waitForStableResolutionArtifact(ctx, outputPath, c.config.ResolutionArtifactTimeout); err != nil {
		return nil, err
	}
	return &ResolutionResult{OutputPath: outputPath}, nil
}

func resolutionPrompt(req ResolutionRequest) string {
	if req.Repair {
		return fmt.Sprintf(`The prior resolution artifact failed deterministic validation:
%s

The artifact directory contains /workspace/artifacts/resolution_input.json and /workspace/artifacts/resolution_template.json. Read both, then overwrite /workspace/artifacts/resolution_output.json with the template copied and its placeholder fields replaced by concrete values. Keep the exact type, resolution_id, and terms.details from the template. Do not explore other paths. Do not answer in chat. Write the file immediately.`, strings.TrimSpace(req.ValidationError))
	}
	return `The artifact directory contains /workspace/artifacts/resolution_input.json and /workspace/artifacts/resolution_template.json. Read both with the read tool. Copy the template to /workspace/artifacts/resolution_output.json and replace its placeholder rationale, evidence, actions, producer/consumer obligations, tradeoffs, and risks with concrete values from the input. Keep the exact type, resolution_id, and terms.details from the template. Ownership is immutable. Do not explore other paths. Do not answer in chat. Write the file immediately.`
}

func waitForStableResolutionArtifact(ctx context.Context, path string, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 3 * time.Minute
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var priorSize int64 = -1
	stable := 0
	for {
		info, err := os.Stat(path)
		if err == nil && info.Size() > 0 {
			if info.Size() == priorSize {
				stable++
			} else {
				priorSize = info.Size()
				stable = 0
			}
			if stable >= 2 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("pi_coordinator: resolution artifact was not written within %s", timeout)
		case <-ticker.C:
		}
	}
}

func cloneIsolationPolicy(source isolation.Policy) isolation.Policy {
	clone := source
	clone.Writable = append([]isolation.Mount(nil), source.Writable...)
	clone.ReadOnly = append([]isolation.Mount(nil), source.ReadOnly...)
	clone.DeniedSources = append([]string(nil), source.DeniedSources...)
	clone.InheritEnvironment = append([]string(nil), source.InheritEnvironment...)
	clone.Environment = make(map[string]string, len(source.Environment))
	for key, value := range source.Environment {
		clone.Environment[key] = value
	}
	return clone
}

func sanitizeResolutionID(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '-'
	}, value)
	if value == "" {
		return "unknown"
	}
	return value
}
