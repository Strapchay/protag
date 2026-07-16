package orchestrator

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	isolation "aion-isolation"
	"aion-kernel/internal/coordinator"
)

const (
	agentSandboxRoot       = "/workspace"
	agentSandboxSessionDir = "/state/pi"
)

type isolatedAgentRuntime struct {
	policy         isolation.Policy
	binary         string
	skillPaths     []string
	extensionPaths []string
}

func newDomainIsolationEngine(config *Config) (isolation.Engine, error) {
	backend := "bubblewrap"
	runtimeBase := ""
	if config != nil {
		if strings.TrimSpace(config.Isolation.Backend) != "" {
			backend = strings.ToLower(strings.TrimSpace(config.Isolation.Backend))
		}
		runtimeBase = config.Isolation.RuntimeBase
	}
	if backend != "bubblewrap" {
		return nil, fmt.Errorf("allocator: unsupported isolation backend %q", backend)
	}
	return isolation.NewBubblewrapEngine(isolation.BubblewrapConfig{RuntimeBase: runtimeBase})
}

func (a *Allocator) SetIsolationEngine(engine isolation.Engine) {
	a.mu.Lock()
	a.isolationEngine = engine
	a.isolationErr = nil
	a.mu.Unlock()
}

func (a *Allocator) prepareIsolatedAgentRuntime(agentID string, domain coordinator.Domain, agentDir string) (isolatedAgentRuntime, error) {
	if a.isolationErr != nil {
		return isolatedAgentRuntime{}, a.isolationErr
	}
	if a.isolationEngine == nil {
		return isolatedAgentRuntime{}, fmt.Errorf("allocator: domain isolation engine is unavailable")
	}
	projectRoot, err := filepath.Abs(a.projectRoot)
	if err != nil {
		return isolatedAgentRuntime{}, fmt.Errorf("allocator: resolve project root: %w", err)
	}

	policy := isolation.Policy{
		ID:         agentID,
		WorkingDir: agentSandboxRoot,
		Hostname:   agentID,
		Network:    isolation.NetworkIsolated,
		SourceRoot: projectRoot,
		Environment: map[string]string{
			"PI_CODING_AGENT_DIR": "/state/pi/config",
			"PI_TELEMETRY":        "0",
		},
	}
	if strings.EqualFold(strings.TrimSpace(a.config.Isolation.Network), string(isolation.NetworkShared)) {
		policy.Network = isolation.NetworkShared
	}
	if policy.Network == isolation.NetworkIsolated && !a.config.GatewayEnabled() {
		return isolatedAgentRuntime{}, fmt.Errorf("allocator: isolated agent networking requires the inference gateway; enable gateway mode or explicitly select shared networking")
	}
	for _, name := range []string{".aion", ".git", ".agents", ".codex"} {
		policy.DeniedSources = append(policy.DeniedSources, filepath.Join(projectRoot, name))
	}

	if len(domain.AssignedPaths) == 0 {
		return isolatedAgentRuntime{}, fmt.Errorf("allocator: domain %s has no owned paths", domain.DomainID)
	}
	for _, assignedPath := range domain.AssignedPaths {
		kind := isolation.MountDirectory
		trimmed := strings.Trim(strings.TrimSpace(assignedPath), "`\"'")
		if !strings.HasSuffix(trimmed, "/") && filepath.Ext(trimmed) != "" {
			kind = isolation.MountFile
		}
		mount, err := isolation.NewWorkspaceMount(projectRoot, assignedPath, kind)
		if err != nil {
			return isolatedAgentRuntime{}, fmt.Errorf("allocator: domain %s ownership: %w", domain.DomainID, err)
		}
		policy.Writable = append(policy.Writable, mount)
	}
	policy.Writable = append(policy.Writable, isolation.Mount{
		Source: agentDir,
		Target: agentSandboxSessionDir,
		Kind:   isolation.MountDirectory,
	})
	if strings.TrimSpace(a.ipcDir) == "" {
		return isolatedAgentRuntime{}, fmt.Errorf("allocator: agent IPC directory is unavailable")
	}
	policy.ReadOnly = append(policy.ReadOnly, isolation.Mount{
		Source: a.ipcDir,
		Target: agentIPCMountPath,
		Kind:   isolation.MountDirectory,
	})

	binary, runtimeMounts, runtimePath, err := resolveAgentBinary(a.config.Agents.CommandPath)
	if err != nil {
		return isolatedAgentRuntime{}, err
	}
	policy.ReadOnly = append(policy.ReadOnly, runtimeMounts...)

	skills, skillMounts, err := resolveAgentResources(projectRoot, a.config.Agents.SkillPaths)
	if err != nil {
		return isolatedAgentRuntime{}, fmt.Errorf("allocator: resolve agent skills: %w", err)
	}
	extensions, extensionMounts, err := resolveAgentResources(projectRoot, a.config.Agents.ExtensionPaths)
	if err != nil {
		return isolatedAgentRuntime{}, fmt.Errorf("allocator: resolve agent extensions: %w", err)
	}
	policy.ReadOnly = append(policy.ReadOnly, skillMounts...)
	policy.ReadOnly = append(policy.ReadOnly, extensionMounts...)

	cliPath, err := exec.LookPath("orchestrator-cli")
	if err != nil {
		return isolatedAgentRuntime{}, fmt.Errorf("allocator: locate orchestrator-cli for isolated agent: %w", err)
	}
	cliPath, err = filepath.EvalSymlinks(cliPath)
	if err != nil {
		return isolatedAgentRuntime{}, fmt.Errorf("allocator: resolve orchestrator-cli: %w", err)
	}
	if !pathCoveredByMount(cliPath, policy.ReadOnly) && !pathWithinRoot(cliPath, "/usr") {
		policy.ReadOnly = append(policy.ReadOnly, isolation.Mount{
			Source: cliPath,
			Target: "/runtime/bin/orchestrator-cli",
			Kind:   isolation.MountFile,
		})
	}

	pathEntries := []string{"/runtime/bin"}
	if runtimePath != "" {
		pathEntries = append(pathEntries, runtimePath)
	}
	pathEntries = append(pathEntries, "/usr/local/bin", "/usr/bin", "/bin")
	policy.Environment["PATH"] = strings.Join(uniqueStrings(pathEntries), ":")

	return isolatedAgentRuntime{
		policy:         policy,
		binary:         binary,
		skillPaths:     skills,
		extensionPaths: extensions,
	}, nil
}

func resolveAgentBinary(configured string) (string, []isolation.Mount, string, error) {
	binary := strings.TrimSpace(configured)
	if binary == "" {
		binary = "pi"
	}
	resolved, err := exec.LookPath(binary)
	if err != nil {
		return "", nil, "", fmt.Errorf("allocator: locate Pi binary: %w", err)
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil {
		return "", nil, "", fmt.Errorf("allocator: resolve Pi binary: %w", err)
	}
	if pathWithinRoot(resolved, "/usr") {
		return resolved, nil, filepath.Dir(resolved), nil
	}

	clean := filepath.ToSlash(resolved)
	if index := strings.LastIndex(clean, "/bin/"); index > 0 {
		runtimeRoot := filepath.FromSlash(clean[:index])
		if info, statErr := os.Stat(runtimeRoot); statErr == nil && info.IsDir() {
			return resolved, []isolation.Mount{{
				Source: runtimeRoot,
				Target: filepath.ToSlash(runtimeRoot),
				Kind:   isolation.MountDirectory,
			}}, filepath.ToSlash(filepath.Join(runtimeRoot, "bin")), nil
		}
	}

	realPath, err := filepath.EvalSymlinks(resolved)
	if err != nil {
		return "", nil, "", fmt.Errorf("allocator: resolve Pi binary symlink: %w", err)
	}
	if pathWithinRoot(realPath, "/usr") {
		return realPath, nil, filepath.Dir(realPath), nil
	}
	return realPath, []isolation.Mount{{Source: realPath, Target: filepath.ToSlash(realPath), Kind: isolation.MountFile}}, "", nil
}

func resolveAgentResources(projectRoot string, configured []string) ([]string, []isolation.Mount, error) {
	paths := make([]string, 0, len(configured))
	mounts := make([]isolation.Mount, 0, len(configured))
	for _, raw := range configured {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		path, err := resolveAgentResourcePath(projectRoot, raw)
		if err != nil {
			return nil, nil, err
		}
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil {
			return nil, nil, fmt.Errorf("resolve %q: %w", raw, err)
		}
		info, err := os.Stat(resolved)
		if err != nil {
			return nil, nil, fmt.Errorf("stat %q: %w", resolved, err)
		}
		kind := isolation.MountFile
		if info.IsDir() {
			kind = isolation.MountDirectory
		}
		paths = append(paths, resolved)
		mounts = append(mounts, isolation.Mount{Source: resolved, Target: filepath.ToSlash(resolved), Kind: kind})
	}
	return paths, mounts, nil
}

func resolveAgentResourcePath(projectRoot, path string) (string, error) {
	if filepath.IsAbs(path) {
		return path, nil
	}
	candidates := []string{filepath.Join(projectRoot, path)}
	if cwd, err := os.Getwd(); err == nil {
		candidates = append(candidates, filepath.Join(cwd, path))
	}
	if executable, err := os.Executable(); err == nil {
		dir := filepath.Dir(executable)
		candidates = append(candidates, filepath.Join(dir, path), filepath.Join(dir, "..", path))
	}
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("resolve %q: no configured resource candidate exists", path)
}

func pathCoveredByMount(path string, mounts []isolation.Mount) bool {
	for _, mount := range mounts {
		if pathWithinRoot(path, mount.Source) {
			return true
		}
	}
	return false
}

func pathWithinRoot(path, root string) bool {
	path = filepath.Clean(path)
	root = filepath.Clean(root)
	if path == root {
		return true
	}
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func isolationGenerationStorePath(agentBaseDir, agentID string) (string, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" || filepath.Base(agentID) != agentID || agentID == "." {
		return "", fmt.Errorf("allocator: invalid agent ID for isolation state %q", agentID)
	}
	return filepath.Join(filepath.Dir(agentBaseDir), "isolation", agentID, "generation"), nil
}

func loadIsolationGeneration(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	generation, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", path, err)
	}
	return generation, nil
}

func saveIsolationGeneration(path string, generation uint64) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(dir, ".generation-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := fmt.Fprintf(temporary, "%d\n", generation); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	removeTemporary = false
	return syncDirectory(dir)
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
