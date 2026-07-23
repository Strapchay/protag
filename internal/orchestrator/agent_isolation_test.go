package orchestrator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	isolation "aion-isolation"
)

func TestResolveAgentBinaryMountsOnlyNodeAndPiPackage(t *testing.T) {
	runtimeRoot := t.TempDir()
	binDir := filepath.Join(runtimeRoot, "bin")
	packageRoot := filepath.Join(runtimeRoot, "lib", "node_modules", "example-pi")
	cliPath := filepath.Join(packageRoot, "dist", "cli.js")
	for _, dir := range []string{binDir, filepath.Dir(cliPath)} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(packageRoot, "package.json"), []byte(`{"name":"example-pi"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cliPath, []byte("#!/usr/bin/env node\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	nodePath := filepath.Join(binDir, "node")
	if err := os.WriteFile(nodePath, []byte("node"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(cliPath, filepath.Join(binDir, "pi")); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)

	binary, mounts, runtimePath, err := resolveAgentBinary("pi")
	if err != nil {
		t.Fatal(err)
	}
	if binary != "/runtime/pi/dist/cli.js" || runtimePath != "/runtime/bin" {
		t.Fatalf("runtime = binary %q path %q", binary, runtimePath)
	}
	want := map[string]isolation.Mount{
		"/runtime/pi":       {Source: packageRoot, Target: "/runtime/pi", Kind: isolation.MountDirectory},
		"/runtime/bin/node": {Source: nodePath, Target: "/runtime/bin/node", Kind: isolation.MountFile},
	}
	if len(mounts) != len(want) {
		t.Fatalf("mounts = %+v", mounts)
	}
	for _, mount := range mounts {
		if mount.Source == runtimeRoot || strings.HasPrefix(mount.Target, runtimeRoot) {
			t.Fatalf("broad or host-shaped runtime mount: %+v", mount)
		}
		if expected, ok := want[mount.Target]; !ok || expected != mount {
			t.Fatalf("unexpected mount: %+v", mount)
		}
	}
}

func TestResolveAgentResourcesUsesOpaqueSandboxTargets(t *testing.T) {
	projectRoot := t.TempDir()
	skill := filepath.Join(projectRoot, "skills", "orchestrator-cli")
	if err := os.MkdirAll(skill, 0o755); err != nil {
		t.Fatal(err)
	}
	paths, mounts, err := resolveAgentResources(projectRoot, []string{"skills/orchestrator-cli"}, "/runtime/skills")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 || paths[0] != "/runtime/skills/00-orchestrator-cli" {
		t.Fatalf("paths = %v", paths)
	}
	if len(mounts) != 1 || mounts[0].Source != skill || mounts[0].Target != paths[0] {
		t.Fatalf("mounts = %+v", mounts)
	}
}

func TestIsolationGenerationStorePersistsMonotonically(t *testing.T) {
	agentBaseDir := filepath.Join(t.TempDir(), "run", "pi_sessions")
	path, err := isolationGenerationStorePath(agentBaseDir, "agent-domain-a")
	if err != nil {
		t.Fatal(err)
	}
	wantPath := filepath.Join(filepath.Dir(agentBaseDir), "isolation", "agent-domain-a", "generation")
	if path != wantPath {
		t.Fatalf("generation path = %q, want %q", path, wantPath)
	}
	if generation, err := loadIsolationGeneration(path); err != nil || generation != 0 {
		t.Fatalf("missing generation = %d, %v; want 0, nil", generation, err)
	}
	if err := saveIsolationGeneration(path, 7); err != nil {
		t.Fatal(err)
	}
	if generation, err := loadIsolationGeneration(path); err != nil || generation != 7 {
		t.Fatalf("stored generation = %d, %v; want 7, nil", generation, err)
	}
	if info, err := os.Stat(path); err != nil {
		t.Fatal(err)
	} else if info.Mode().Perm() != 0o600 {
		t.Fatalf("generation mode = %o, want 600", info.Mode().Perm())
	}
}

func TestIsolationGenerationStoreRejectsInvalidState(t *testing.T) {
	agentBaseDir := filepath.Join(t.TempDir(), "run", "pi_sessions")
	if _, err := isolationGenerationStorePath(agentBaseDir, "../escape"); err == nil {
		t.Fatal("expected invalid agent ID to be rejected")
	}
	path, err := isolationGenerationStorePath(agentBaseDir, "agent-domain-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("not-a-generation"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadIsolationGeneration(path); err == nil {
		t.Fatal("expected corrupt generation state to fail closed")
	}
}
