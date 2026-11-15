package isolation

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

var (
	policyIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	envNamePattern  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

func normalizePolicy(policy Policy) (Policy, error) {
	policy.Writable = append([]Mount(nil), policy.Writable...)
	policy.ReadOnly = append([]Mount(nil), policy.ReadOnly...)
	policy.DeniedSources = append([]string(nil), policy.DeniedSources...)
	policy.InheritEnvironment = append([]string(nil), policy.InheritEnvironment...)
	if policy.Environment != nil {
		environment := make(map[string]string, len(policy.Environment))
		for name, value := range policy.Environment {
			environment[name] = value
		}
		policy.Environment = environment
	}

	policy.ID = strings.TrimSpace(policy.ID)
	if !policyIDPattern.MatchString(policy.ID) {
		return Policy{}, fmt.Errorf("isolation: invalid policy ID %q", policy.ID)
	}
	if policy.Generation == 0 {
		policy.Generation = 1
	}
	if policy.WorkingDir == "" {
		policy.WorkingDir = "/workspace"
	}
	workingDir, err := cleanSandboxPath(policy.WorkingDir)
	if err != nil {
		return Policy{}, fmt.Errorf("isolation: working directory: %w", err)
	}
	policy.WorkingDir = workingDir
	if policy.Network == "" {
		policy.Network = NetworkShared
	}
	if policy.Network != NetworkShared && policy.Network != NetworkIsolated {
		return Policy{}, fmt.Errorf("isolation: unsupported network mode %q", policy.Network)
	}
	if policy.Hostname == "" {
		policy.Hostname = "aion-agent"
	}
	if !policyIDPattern.MatchString(policy.Hostname) {
		return Policy{}, fmt.Errorf("isolation: invalid hostname %q", policy.Hostname)
	}
	if strings.TrimSpace(policy.SourceRoot) != "" {
		policy.SourceRoot, err = cleanHostPath(policy.SourceRoot)
		if err != nil {
			return Policy{}, fmt.Errorf("isolation: source root: %w", err)
		}
	}
	for i, denied := range policy.DeniedSources {
		policy.DeniedSources[i], err = cleanHostPath(denied)
		if err != nil {
			return Policy{}, fmt.Errorf("isolation: denied source %d: %w", i, err)
		}
	}
	sort.Strings(policy.DeniedSources)
	if err := rejectOverlaps("denied source", append([]string(nil), policy.DeniedSources...)); err != nil {
		return Policy{}, err
	}
	for name := range policy.Environment {
		if !envNamePattern.MatchString(name) {
			return Policy{}, fmt.Errorf("isolation: invalid environment name %q", name)
		}
	}
	seenEnvironment := make(map[string]struct{}, len(policy.InheritEnvironment))
	for i, name := range policy.InheritEnvironment {
		name = strings.TrimSpace(name)
		if !envNamePattern.MatchString(name) {
			return Policy{}, fmt.Errorf("isolation: invalid inherited environment name %q", name)
		}
		if _, exists := seenEnvironment[name]; exists {
			return Policy{}, fmt.Errorf("isolation: inherited environment name %q is duplicated", name)
		}
		seenEnvironment[name] = struct{}{}
		policy.InheritEnvironment[i] = name
	}
	sort.Strings(policy.InheritEnvironment)

	targets := make([]string, 0, len(policy.Writable)+len(policy.ReadOnly))
	sources := make([]string, 0, len(policy.Writable)+len(policy.ReadOnly))
	for i := range policy.Writable {
		mount, err := normalizeMount(policy.Writable[i], true)
		if err != nil {
			return Policy{}, fmt.Errorf("isolation: writable mount %d: %w", i, err)
		}
		policy.Writable[i] = mount
		if pathWithin(mount.Target, "/workspace") {
			if policy.SourceRoot == "" {
				return Policy{}, fmt.Errorf("isolation: source_root is required for workspace mount %q", mount.Source)
			}
			if mount.Source == policy.SourceRoot || !pathWithin(mount.Source, policy.SourceRoot) {
				return Policy{}, fmt.Errorf("isolation: workspace source %q must be a strict descendant of source root %q", mount.Source, policy.SourceRoot)
			}
			for _, denied := range policy.DeniedSources {
				if pathWithin(mount.Source, denied) || pathWithin(denied, mount.Source) {
					return Policy{}, fmt.Errorf("isolation: workspace source %q overlaps denied source %q", mount.Source, denied)
				}
			}
		}
		targets = append(targets, mount.Target)
		sources = append(sources, mount.Source)
	}
	for i := range policy.ReadOnly {
		mount, err := normalizeMount(policy.ReadOnly[i], false)
		if err != nil {
			return Policy{}, fmt.Errorf("isolation: read-only mount %d: %w", i, err)
		}
		policy.ReadOnly[i] = mount
		targets = append(targets, mount.Target)
		sources = append(sources, mount.Source)
	}
	if err := rejectOverlaps("mount target", targets); err != nil {
		return Policy{}, err
	}
	if err := rejectOverlaps("mount source", sources); err != nil {
		return Policy{}, err
	}
	return policy, nil
}

func newWorkspaceMount(sourceRoot, assignedPath string, kind MountKind) (Mount, error) {
	root, err := cleanHostPath(sourceRoot)
	if err != nil {
		return Mount{}, fmt.Errorf("isolation: source root: %w", err)
	}
	raw := strings.Trim(strings.TrimSpace(assignedPath), "`\"'")
	if raw == "" || filepath.IsAbs(raw) {
		return Mount{}, fmt.Errorf("isolation: assigned path %q must be project-relative", assignedPath)
	}
	rel := filepath.Clean(raw)
	if rel == "." || rel == string(os.PathSeparator) || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return Mount{}, fmt.Errorf("isolation: assigned path %q must identify a project subtree or file", assignedPath)
	}
	source := filepath.Join(root, rel)
	if !pathWithin(source, root) || source == root {
		return Mount{}, fmt.Errorf("isolation: assigned path %q escapes source root", assignedPath)
	}
	target := filepath.ToSlash(filepath.Join("/workspace", rel))
	return Mount{Source: source, Target: target, Kind: kind, Create: true}, nil
}

func cleanHostPath(path string) (string, error) {
	path = filepath.Clean(strings.TrimSpace(path))
	if !filepath.IsAbs(path) || path == string(os.PathSeparator) {
		return "", fmt.Errorf("path %q must be absolute and cannot be host root", path)
	}
	return path, nil
}

func normalizeMount(mount Mount, writable bool) (Mount, error) {
	mount.Source = filepath.Clean(strings.TrimSpace(mount.Source))
	if !filepath.IsAbs(mount.Source) {
		return Mount{}, fmt.Errorf("source %q must be absolute", mount.Source)
	}
	target, err := cleanSandboxPath(mount.Target)
	if err != nil {
		return Mount{}, fmt.Errorf("target: %w", err)
	}
	mount.Target = target
	if target == "/workspace" {
		return Mount{}, fmt.Errorf("target %q cannot replace the sandbox workspace root", target)
	}
	if isReservedTarget(target) {
		return Mount{}, fmt.Errorf("target %q is reserved by the sandbox runtime", target)
	}
	if writable && !pathWithin(target, "/workspace") && !pathWithin(target, "/state") {
		return Mount{}, fmt.Errorf("writable target %q must be under /workspace or /state", target)
	}
	if mount.Kind == "" {
		mount.Kind = MountAuto
	}
	if mount.Kind != MountAuto && mount.Kind != MountDirectory && mount.Kind != MountFile {
		return Mount{}, fmt.Errorf("invalid kind %q", mount.Kind)
	}
	if mount.Create && !writable {
		return Mount{}, fmt.Errorf("read-only mounts cannot materialize missing sources")
	}
	if mount.Create && mount.Kind == MountAuto {
		return Mount{}, fmt.Errorf("materialized mounts require an explicit kind")
	}
	return mount, nil
}

func cleanSandboxPath(path string) (string, error) {
	path = filepath.Clean(strings.TrimSpace(path))
	if !filepath.IsAbs(path) || path == "/" {
		return "", fmt.Errorf("path %q must be absolute and cannot be sandbox root", path)
	}
	return filepath.ToSlash(path), nil
}

func isReservedTarget(path string) bool {
	for _, reserved := range []string{"/proc", "/dev", "/tmp", "/usr", "/bin", "/sbin", "/lib", "/lib64", "/etc"} {
		if pathWithin(path, reserved) {
			return true
		}
	}
	return false
}

func rejectOverlaps(kind string, paths []string) error {
	sort.Strings(paths)
	for i := 1; i < len(paths); i++ {
		if paths[i] == paths[i-1] || pathWithin(paths[i], paths[i-1]) {
			return fmt.Errorf("isolation: overlapping %s paths %q and %q", kind, paths[i-1], paths[i])
		}
	}
	return nil
}

func pathWithin(path, root string) bool {
	path = filepath.Clean(path)
	root = filepath.Clean(root)
	if path == root {
		return true
	}
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}
