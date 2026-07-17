package supervisor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"aion-kernel/internal/hub"
)

func TestDeliverContextAcknowledgesPiSubmission(t *testing.T) {
	root := t.TempDir()
	received := filepath.Join(root, "received.jsonl")
	script := filepath.Join(root, "pi-rpc.sh")
	if err := os.WriteFile(script, []byte(`#!/usr/bin/env bash
set -eu
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$AION_TEST_RECEIVED"
  if [[ "$line" == *'"type":"prompt"'* ]]; then
    echo '{"type":"turn_start"}'
    echo '{"type":"turn_end"}'
  fi
  if [[ "$line" == *'"type":"abort"'* ]]; then
    exit 0
  fi
done
`), 0o755); err != nil {
		t.Fatal(err)
	}
	supervisor := NewAgentSupervisor(AgentConfig{
		AgentID:  "agent-provider",
		DomainID: "provider",
		PiAgent: PiAgentConfig{
			Binary:     script,
			SessionDir: filepath.Join(root, "session"),
			WorkingDir: root,
			Env:        []string{"AION_TEST_RECEIVED=" + received},
		},
		HeartbeatTimeout: time.Hour,
		ProgressTimeout:  time.Hour,
		MaxCrashRestarts: 1,
	})
	if err := supervisor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = supervisor.Stop() })

	payload, _ := json.Marshal(map[string]any{"id": "contract-1", "revision": 1})
	if err := supervisor.DeliverContext(hub.Message{
		ID:        "effect-1",
		Type:      hub.MsgContractProduced,
		FromAgent: "orchestrator",
		ToAgent:   "agent-provider",
		Payload:   payload,
	}); err != nil {
		t.Fatalf("deliver context: %v", err)
	}

	var data []byte
	waitForSupervisor(t, 2*time.Second, func() bool {
		var err error
		data, err = os.ReadFile(received)
		return err == nil && strings.Contains(string(data), "ContractProduced")
	})
	if !strings.Contains(string(data), "ContractProduced") || !strings.Contains(string(data), "effect-1") {
		t.Fatalf("Pi did not receive durable contract event: %s", data)
	}
}
