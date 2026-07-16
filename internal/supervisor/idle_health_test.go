package supervisor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSupervisorDoesNotCrashIdleRPCProcess(t *testing.T) {
	root := t.TempDir()
	script := filepath.Join(root, "idle-pi.sh")
	if err := os.WriteFile(script, []byte(`#!/usr/bin/env bash
while IFS= read -r line; do
  if [[ "$line" == *'"type":"prompt"'* ]]; then
    echo '{"type":"turn_start"}'
    echo '{"type":"turn_end"}'
    echo '{"type":"agent_end"}'
  fi
  if [[ "$line" == *'"type":"abort"'* ]]; then
    exit 0
  fi
done
`), 0o755); err != nil {
		t.Fatal(err)
	}
	supervisor := NewAgentSupervisor(AgentConfig{
		AgentID:       "agent-idle",
		DomainID:      "idle",
		InitialPrompt: "complete one turn",
		PiAgent: PiAgentConfig{
			Binary:     script,
			SessionDir: filepath.Join(root, "session"),
			WorkingDir: root,
		},
		HeartbeatTimeout: 75 * time.Millisecond,
		ProgressTimeout:  75 * time.Millisecond,
		MaxCrashRestarts: 3,
	})
	if err := supervisor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = supervisor.Stop() })
	time.Sleep(1200 * time.Millisecond)
	if state := supervisor.State(); state != StateRunning {
		t.Fatalf("idle RPC process state = %s", state)
	}
	if supervisor.crashCount != 0 {
		t.Fatalf("idle RPC process crash count = %d", supervisor.crashCount)
	}
}
