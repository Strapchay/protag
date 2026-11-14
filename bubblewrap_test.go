package isolation

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func requireBubblewrapEngine(t *testing.T) *BubblewrapEngine {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("bubblewrap backend requires Linux")
	}
	if _, err := exec.LookPath("bwrap"); err != nil {
		t.Skip("bubblewrap is not installed")
	}
	engine, err := NewBubblewrapEngine(BubblewrapConfig{RuntimeBase: filepath.Join(t.TempDir(), "runtime")})
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func TestBubblewrapWorkspaceEnforcesBoundary(t *testing.T) {
	engine := requireBubblewrapEngine(t)
	ctx := context.Background()
	root := t.TempDir()
	owned := filepath.Join(root, "owned")
	hidden := filepath.Join(root, "hidden")
	contextFile := filepath.Join(root, "build-spec.md")
	for _, path := range []string{owned, hidden} {
		if err := os.Mkdir(path, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(owned, "input.txt"), []byte("owned-input"), 0o644); err != nil {
		t.Fatal(err)
	}
	hiddenCanary := filepath.Join(hidden, "host-secret.txt")
	if err := os.WriteFile(hiddenCanary, []byte("hidden"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(contextFile, []byte("read-only-context"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AION_ISOLATION_ALLOWED", "allowlisted-value")
	t.Setenv("AION_ISOLATION_HOST_SECRET", "must-not-cross")

	workspace, err := engine.Prepare(ctx, Policy{
		ID:          "agent-boundary",
		Generation:  7,
		WorkingDir:  "/workspace",
		SourceRoot:  root,
		Writable:    []Mount{{Source: owned, Target: "/workspace/owned", Kind: MountDirectory}},
		ReadOnly:    []Mount{{Source: contextFile, Target: "/context/build-spec.md", Kind: MountFile}},
		Environment: map[string]string{"AION_ISOLATION_DIRECT": "direct-value"},
		InheritEnvironment: []string{
			"AION_ISOLATION_ALLOWED",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := workspace.Snapshot()
	manifest, err := os.ReadFile(filepath.Join(snapshot.RuntimeDir, "workspace.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"allowlisted-value", "direct-value", "must-not-cross"} {
		if strings.Contains(string(manifest), secret) {
			t.Fatalf("workspace manifest leaked environment value %q", secret)
		}
	}

	script := fmt.Sprintf(`
set -eu
test "$PWD" = /workspace
test "$(cat owned/input.txt)" = owned-input
test "$(cat /context/build-spec.md)" = read-only-context
test "$AION_ISOLATION_ALLOWED" = allowlisted-value
test "$AION_ISOLATION_DIRECT" = direct-value
test -z "${AION_ISOLATION_HOST_SECRET+x}"
test ! -e %q
test ! -e /workspace/other-domain
if printf forbidden > /workspace/unowned.txt 2>/dev/null; then exit 32; fi
test ! -e /workspace/unowned.txt
if printf forbidden > /context/build-spec.md 2>/dev/null; then exit 31; fi
test ! -e /proc/%d
printf isolated-write > owned/result.txt
printf private > /tmp/generation-canary
`, hiddenCanary, os.Getpid())
	command, err := workspace.Command(ctx, CommandSpec{Path: "/bin/sh", Args: []string{"-c", script}})
	if err != nil {
		t.Fatal(err)
	}
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("isolated command failed: %v\n%s", err, output)
	}
	result, err := os.ReadFile(filepath.Join(owned, "result.txt"))
	if err != nil || string(result) != "isolated-write" {
		t.Fatalf("writable mount result = %q, %v", result, err)
	}
	contextResult, err := os.ReadFile(contextFile)
	if err != nil || string(contextResult) != "read-only-context" {
		t.Fatalf("read-only context changed: %q, %v", contextResult, err)
	}
	if err := workspace.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(snapshot.RuntimeDir); !os.IsNotExist(err) {
		t.Fatalf("runtime directory still exists after close: %v", err)
	}
	if _, err := workspace.Command(ctx, CommandSpec{Path: "/bin/true"}); err == nil {
		t.Fatal("closed workspace accepted a command")
	}

	next, err := engine.Prepare(ctx, Policy{
		ID:         "agent-boundary",
		Generation: 8,
		SourceRoot: root,
		Writable:   []Mount{{Source: owned, Target: "/workspace/owned", Kind: MountDirectory}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	nextCommand, err := next.Command(ctx, CommandSpec{Path: "/bin/sh", Args: []string{"-c", "test ! -e /tmp/generation-canary"}})
	if err != nil {
		t.Fatal(err)
	}
	if output, err := nextCommand.CombinedOutput(); err != nil {
		t.Fatalf("new generation inherited private temporary state: %v\n%s", err, output)
	}
}

func TestBubblewrapWorkspaceFailsWhenInheritedEnvironmentIsMissing(t *testing.T) {
	engine := requireBubblewrapEngine(t)
	workspace, err := engine.Prepare(context.Background(), Policy{
		ID:                 "agent-env",
		InheritEnvironment: []string{"AION_ISOLATION_DEFINITELY_MISSING"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	if _, err := workspace.Command(context.Background(), CommandSpec{Path: "/bin/true"}); err == nil || !strings.Contains(err.Error(), "is not set") {
		t.Fatalf("Command() error = %v, want missing environment error", err)
	}
}

func TestBubblewrapWorkspacePassesSanitizedEnvironmentOutsideArguments(t *testing.T) {
	engine := requireBubblewrapEngine(t)
	workspace, err := engine.Prepare(context.Background(), Policy{
		ID:          "agent-env-transport",
		Environment: map[string]string{"AION_AGENT_CAPABILITY": "opaque-test-capability"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	command, err := workspace.Command(context.Background(), CommandSpec{Path: "/bin/sh", Args: []string{"-c", `test "$AION_AGENT_CAPABILITY" = opaque-test-capability`}})
	if err != nil {
		t.Fatal(err)
	}
	for _, argument := range command.Args[:len(command.Args)-3] {
		if strings.Contains(argument, "opaque-test-capability") {
			t.Fatalf("capability leaked into Bubblewrap arguments: %q", argument)
		}
	}
	found := false
	for _, entry := range command.Env {
		if entry == "AION_AGENT_CAPABILITY=opaque-test-capability" {
			found = true
		}
	}
	if !found {
		t.Fatalf("sanitized environment missing capability: %#v", command.Env)
	}
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("isolated command failed: %v\n%s", err, output)
	}
}
