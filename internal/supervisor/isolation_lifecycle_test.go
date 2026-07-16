package supervisor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	isolation "aion-isolation"
	"aion-isolation/isolationtest"
)

func TestSupervisorReplacesIsolationGenerationAfterCrash(t *testing.T) {
	root := t.TempDir()
	sessionDir := filepath.Join(root, "session")
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(root, "pi-rpc.sh")
	if err := os.WriteFile(script, []byte(`#!/usr/bin/env bash
set -eu
count_file="$AION_TEST_SESSION/launch-count"
count=0
if [[ -f "$count_file" ]]; then
  count="$(cat "$count_file")"
fi
count=$((count + 1))
printf '%s' "$count" > "$count_file"
printf '%s\n' "$AION_AGENT_CAPABILITY" >> "$AION_TEST_SESSION/capabilities"
while IFS= read -r line; do
  if [[ "$line" == *'"type":"abort"'* ]]; then
    exit 0
  fi
  if [[ "$line" == *'"type":"prompt"'* && "$count" == "1" ]]; then
    exit 7
  fi
done
`), 0o755); err != nil {
		t.Fatal(err)
	}

	engine := &isolationtest.FakeEngine{}
	var generationsMu sync.Mutex
	var issuedGenerations []uint64
	supervisor := NewAgentSupervisor(AgentConfig{
		AgentID:       "agent-domain-a",
		DomainID:      "domain-a",
		InitialPrompt: "begin assigned work",
		PiAgent: PiAgentConfig{
			Binary:         script,
			SessionDir:     "/state/pi",
			HostSessionDir: sessionDir,
			WorkingDir:     "/workspace",
			Env:            []string{"AION_TEST_SESSION=/state/pi"},
		},
		IsolationEngine: engine,
		PrepareGenerationEnv: func(generation uint64) ([]string, error) {
			generationsMu.Lock()
			issuedGenerations = append(issuedGenerations, generation)
			generationsMu.Unlock()
			return []string{fmt.Sprintf("AION_AGENT_CAPABILITY=generation-%d", generation)}, nil
		},
		IsolationPolicy: isolation.Policy{
			ID:         "agent-domain-a",
			WorkingDir: "/workspace",
			SourceRoot: root,
			Writable: []isolation.Mount{
				{Source: root, Target: "/workspace", Kind: isolation.MountDirectory},
				{Source: sessionDir, Target: "/state/pi", Kind: isolation.MountDirectory},
			},
		},
		HeartbeatTimeout: time.Hour,
		ProgressTimeout:  time.Hour,
		MaxCrashRestarts: 3,
	})

	if err := supervisor.Start(context.Background()); err != nil {
		t.Fatalf("start supervisor: %v", err)
	}
	t.Cleanup(func() { _ = supervisor.Stop() })

	waitForSupervisor(t, 5*time.Second, func() bool {
		snapshot := supervisor.RuntimeSnapshot()
		return snapshot.State == StateRunning.String() &&
			snapshot.Workspace != nil &&
			snapshot.Workspace.Generation == 2
	})

	if !engine.Closed(1) {
		t.Fatal("crashed isolation generation was not closed")
	}
	if engine.Closed(2) {
		t.Fatal("replacement isolation generation closed while agent is running")
	}
	waitForSupervisor(t, 5*time.Second, func() bool {
		got, err := os.ReadFile(filepath.Join(sessionDir, "launch-count"))
		return err == nil && string(got) == "2"
	})
	if got, err := os.ReadFile(filepath.Join(sessionDir, "launch-count")); err != nil {
		t.Fatalf("read persisted launch count: %v", err)
	} else if string(got) != "2" {
		t.Fatalf("expected resumed process to share persisted session state, got launch count %q", got)
	}
	if got, err := os.ReadFile(filepath.Join(sessionDir, "capabilities")); err != nil {
		t.Fatalf("read generation capabilities: %v", err)
	} else if string(got) != "generation-1\ngeneration-2\n" {
		t.Fatalf("generation capabilities were not rotated: %q", got)
	}
	generationsMu.Lock()
	defer generationsMu.Unlock()
	if len(issuedGenerations) != 2 || issuedGenerations[0] != 1 || issuedGenerations[1] != 2 {
		t.Fatalf("issued generations = %#v", issuedGenerations)
	}

	if err := supervisor.Stop(); err != nil {
		t.Fatalf("stop supervisor: %v", err)
	}
	if !engine.Closed(2) {
		t.Fatal("active isolation generation was not closed on stop")
	}
	if snapshot := supervisor.RuntimeSnapshot(); snapshot.Workspace != nil {
		t.Fatalf("stopped supervisor retained workspace snapshot: %#v", snapshot.Workspace)
	}
}

func waitForSupervisor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for supervisor state")
}
