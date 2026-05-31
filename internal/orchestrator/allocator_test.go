package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"aion-kernel/internal/coordinator"
	"aion-kernel/internal/hub"
)

func TestAllocateResumeUsesConcisePromptForExistingPiSession(t *testing.T) {
	root := t.TempDir()
	script := filepath.Join(root, "dummy-pi.sh")
	if err := os.WriteFile(script, []byte(`#!/usr/bin/env bash
session=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --session-dir)
      session="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
mkdir -p "$session"
while IFS= read -r line; do
  echo "$line" >> "$session/received.jsonl"
  echo '{"type":"turn_start","message":"ready"}'
done
`), 0o755); err != nil {
		t.Fatalf("write dummy pi: %v", err)
	}

	config := &Config{}
	config.Agents.SessionDir = filepath.Join(root, "sessions")
	config.Agents.CommandPath = script
	config.Health.HeartbeatTimeoutSec = 60
	config.Health.ProgressTimeoutSec = 60
	config.Cgroups.Mode = "disabled"
	config.Cgroups.Enabled = false

	agentDir := filepath.Join(config.Agents.SessionDir, "agent-api")
	if err := os.MkdirAll(agentDir, 0o755); err != nil {
		t.Fatalf("mkdir agent dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(agentDir, "session_state.json"), []byte("{}"), 0o644); err != nil {
		t.Fatalf("seed session state: %v", err)
	}

	router := hub.NewRouter(filepath.Join(root, "logs"))
	defer router.Close()
	allocator := NewAllocator(config, root, router, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer allocator.StopAll()

	err := allocator.AllocateWithOptions(ctx,
		[]coordinator.Domain{{DomainID: "api"}},
		map[string]string{"api": "FULL INITIAL PROMPT"},
		AllocationOptions{Mode: AllocationModeResume, ResumeMessage: "resume existing work"},
	)
	if err != nil {
		t.Fatalf("AllocateWithOptions: %v", err)
	}

	receivedPath := filepath.Join(agentDir, "received.jsonl")
	received := waitForFileContains(t, receivedPath, "resume existing work")
	if !strings.Contains(received, `"type":"prompt"`) {
		t.Fatalf("expected prompt resume message, got:\n%s", received)
	}
	if strings.Contains(received, `"type":"prompt","message":"FULL INITIAL PROMPT"`) {
		t.Fatalf("resume path sent fresh initial prompt:\n%s", received)
	}
}

func TestAllocateGatewayUsesAionProviderAndTargetEnv(t *testing.T) {
	root := t.TempDir()
	script := filepath.Join(root, "dummy-pi.sh")
	if err := os.WriteFile(script, []byte(`#!/usr/bin/env bash
session=""
provider=""
model=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --session-dir)
      session="$2"
      shift 2
      ;;
    --provider)
      provider="$2"
      shift 2
      ;;
    --model)
      model="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
mkdir -p "$session"
{
  echo "provider=$provider"
  echo "model=$model"
  echo "target_provider=$AION_TARGET_PROVIDER"
  echo "target_model=$AION_TARGET_MODEL"
  echo "target_profile=$AION_TARGET_PROFILE"
  echo "target_api=$AION_TARGET_API"
  echo "gateway_url=$AION_INFERENCE_GATEWAY_URL"
} > "$session/launch.env"
while IFS= read -r line; do
  echo "$line" >> "$session/received.jsonl"
  echo '{"type":"turn_start","message":"ready"}'
done
`), 0o755); err != nil {
		t.Fatalf("write dummy pi: %v", err)
	}

	config := &Config{}
	config.Agents.SessionDir = filepath.Join(root, "sessions")
	config.Agents.CommandPath = script
	config.Health.HeartbeatTimeoutSec = 60
	config.Health.ProgressTimeoutSec = 60
	config.Cgroups.Mode = "disabled"
	config.Cgroups.Enabled = false
	config.Execution.Mode = "gateway"
	config.InferenceGateway.Enabled = true
	config.InferenceGateway.ListenAddr = "127.0.0.1:50151"
	config.InferenceGateway.PublicBaseURL = "http://127.0.0.1:50151"
	config.Inference.DomainAgents.UseProfile = "forge"
	config.Inference.Models = map[string]ModelProfile{
		"forge": {
			Provider: "redacted-openai-compatible",
			Model:    "redacted-model",
			Endpoint: "https://example.invalid/v1",
		},
	}

	router := hub.NewRouter(filepath.Join(root, "logs"))
	defer router.Close()
	allocator := NewAllocator(config, root, router, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer allocator.StopAll()

	err := allocator.AllocateWithOptions(ctx,
		[]coordinator.Domain{{DomainID: "api"}},
		map[string]string{"api": "initial prompt"},
		AllocationOptions{Mode: AllocationModeInitial},
	)
	if err != nil {
		t.Fatalf("AllocateWithOptions: %v", err)
	}

	launchPath := filepath.Join(config.Agents.SessionDir, "agent-api", "launch.env")
	launch := waitForFileContains(t, launchPath, "target_model=redacted-model")
	for _, want := range []string{
		"provider=aion-gateway",
		"model=redacted-model",
		"target_provider=redacted-openai-compatible",
		"target_profile=forge",
		"target_api=openai-completions",
		"gateway_url=http://127.0.0.1:50151",
	} {
		if !strings.Contains(launch, want) {
			t.Fatalf("launch env missing %q:\n%s", want, launch)
		}
	}
}

func waitForFileContains(t *testing.T, path, want string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil && strings.Contains(string(data), want) {
			return string(data)
		}
		time.Sleep(20 * time.Millisecond)
	}
	data, _ := os.ReadFile(path)
	t.Fatalf("timed out waiting for %q in %s; got:\n%s", want, path, string(data))
	return ""
}
