package orchestrator

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	coordination "aion-coordination"
	"aion-isolation/isolationtest"
	"aion-kernel/internal/coordinator"
	"aion-kernel/internal/dag"
	"aion-kernel/internal/hub"
	"aion-kernel/internal/supervisor"
)

func TestIncompleteAssignedTurnGetsOneDirectRetry(t *testing.T) {
	root := t.TempDir()
	script := filepath.Join(root, "dummy-pi.sh")
	if err := os.WriteFile(script, []byte(`#!/usr/bin/env bash
session=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --session-dir) session="$2"; shift 2 ;;
    *) shift ;;
  esac
done
mkdir -p "$session"
while IFS= read -r line; do
  echo "$line" >> "$session/received.jsonl"
done
`), 0o755); err != nil {
		t.Fatalf("write dummy pi: %v", err)
	}

	config := &Config{}
	config.Agents.SessionDir = filepath.Join(root, "sessions")
	config.Agents.CommandPath = script
	config.Agents.IncompleteTurnRetries = 1
	config.Health.HeartbeatTimeoutSec = 60
	config.Health.ProgressTimeoutSec = 60
	config.Cgroups.Mode = "disabled"
	config.Isolation.Network = "shared"

	router := hub.NewRouter(filepath.Join(root, "logs"))
	t.Cleanup(router.Close)
	mgr, err := dag.NewManager(dag.ManagerConfig{
		StoreFilePath: filepath.Join(root, "dag.dat"),
		WalFilePath:   filepath.Join(root, "dag.wal"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	if err := mgr.AddNode(dag.DagNode{
		ID: "api-work", DomainID: "api", TaskSpec: "finish API work",
		Status: dag.StatusInProgress, AssignedAgent: "agent-api",
	}); err != nil {
		t.Fatal(err)
	}

	allocator := NewAllocator(config, root, router, mgr)
	allocator.SetIsolationEngine(&isolationtest.FakeEngine{})
	t.Cleanup(allocator.StopAll)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := allocator.AllocateWithOptions(ctx,
		[]coordinator.Domain{{DomainID: "api", AssignedPaths: []string{"api"}}},
		map[string]string{"api": "domain orientation"},
		AllocationOptions{Mode: AllocationModeInitial},
	); err != nil {
		t.Fatal(err)
	}

	allocator.handleAgentLifecycle("agent-api", "api", supervisor.AgentLifecycleEvent{Kind: "agent_end"})
	received := waitForFileContains(t, filepath.Join(config.Agents.SessionDir, "agent-api", "received.jsonl"), "AUTOMATIC INCOMPLETE-TURN RETRY 1/1")
	if !strings.Contains(received, `"type":"prompt"`) || !strings.Contains(received, "api-work") {
		t.Fatalf("incomplete turn was not retried with assigned-node context:\n%s", received)
	}
}

func TestAllocateResumeUsesConcisePromptForExistingPiSession(t *testing.T) {
	root := t.TempDir()
	script := filepath.Join(root, "dummy-pi.sh")
	if err := os.WriteFile(script, []byte(`#!/usr/bin/env bash
session=""
resume="false"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --session-dir)
      session="$2"
      shift 2
      ;;
    --continue)
      resume="true"
      shift
      ;;
    *)
      shift
      ;;
  esac
done
mkdir -p "$session"
echo "resume=$resume" > "$session/launch.env"
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
	config.Isolation.Network = "shared"

	agentDir := filepath.Join(config.Agents.SessionDir, "agent-api")
	if err := os.MkdirAll(agentDir, 0o755); err != nil {
		t.Fatalf("mkdir agent dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(agentDir, "prior-session.jsonl"), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("seed session state: %v", err)
	}

	router := hub.NewRouter(filepath.Join(root, "logs"))
	defer router.Close()
	mgr, err := dag.NewManager(dag.ManagerConfig{
		StoreFilePath: filepath.Join(root, "dag.dat"),
		WalFilePath:   filepath.Join(root, "dag.wal"),
	})
	if err != nil {
		t.Fatalf("new DAG manager: %v", err)
	}
	defer mgr.Close()
	if err := mgr.AddNode(dag.DagNode{
		ID: "api-work", DomainID: "api", TaskSpec: "finish API work",
		Status: dag.StatusInProgress, AssignedAgent: "agent-api",
	}); err != nil {
		t.Fatalf("add assigned node: %v", err)
	}
	allocator := NewAllocator(config, root, router, mgr)
	allocator.SetIsolationEngine(&isolationtest.FakeEngine{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer allocator.StopAll()

	err = allocator.AllocateWithOptions(ctx,
		[]coordinator.Domain{{DomainID: "api", AssignedPaths: []string{"api"}}},
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
	if count := strings.Count(received, `"type":"prompt"`); count != 1 {
		t.Fatalf("resume path sent %d prompt commands, want exactly one:\n%s", count, received)
	}
	launch := waitForFileContains(t, filepath.Join(agentDir, "launch.env"), "resume=true")
	if !strings.Contains(launch, "resume=true") {
		t.Fatalf("resume path did not pass Pi --continue:\n%s", launch)
	}
}

func TestUserInterventionRehydratesStoppedDomainAgent(t *testing.T) {
	root := t.TempDir()
	script := filepath.Join(root, "dummy-pi.sh")
	if err := os.WriteFile(script, []byte(`#!/usr/bin/env bash
session=""
resume="false"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --session-dir)
      session="$2"
      shift 2
      ;;
    --continue)
      resume="true"
      shift
      ;;
    *)
      shift
      ;;
  esac
done
mkdir -p "$session"
echo "resume=$resume" >> "$session/launches.log"
while IFS= read -r line; do
  echo "$line" >> "$session/received.jsonl"
  if [[ "$line" == *'"type":"prompt"'* ]]; then
    echo '{"type":"turn_start"}'
    echo '{"type":"agent_end"}'
  fi
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
	config.Isolation.Network = "shared"

	router := hub.NewRouter(filepath.Join(root, "logs"))
	t.Cleanup(router.Close)
	mgr, err := dag.NewManager(dag.ManagerConfig{
		StoreFilePath: filepath.Join(root, "dag.dat"),
		WalFilePath:   filepath.Join(root, "dag.wal"),
	})
	if err != nil {
		t.Fatalf("new DAG manager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	if err := mgr.AddNode(dag.DagNode{
		ID: "api-work", DomainID: "api", TaskSpec: "finish API work",
		Status: dag.StatusInProgress, AssignedAgent: "agent-api",
	}); err != nil {
		t.Fatalf("add assigned node: %v", err)
	}

	allocator := NewAllocator(config, root, router, mgr)
	allocator.SetIsolationEngine(&isolationtest.FakeEngine{})
	t.Cleanup(allocator.StopAll)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := allocator.AllocateWithOptions(ctx,
		[]coordinator.Domain{{DomainID: "api", AssignedPaths: []string{"api"}}},
		map[string]string{"api": "domain orientation"},
		AllocationOptions{Mode: AllocationModeInitial},
	); err != nil {
		t.Fatalf("allocate agent: %v", err)
	}

	allocator.mu.Lock()
	agent := allocator.activeAgents["agent-api"]
	allocator.mu.Unlock()
	if agent == nil {
		t.Fatal("allocated agent not found")
	}
	if err := agent.Stop(); err != nil {
		t.Fatalf("stop agent: %v", err)
	}

	daemon := &Daemon{allocator: allocator, hubRouter: router, ctx: ctx}
	message := hub.Message{
		ID:        "user-recovery",
		Type:      hub.MsgContextShare,
		FromAgent: "user",
		ToAgent:   "agent-api",
		Payload:   json.RawMessage(`{"content":"finish the missing focused test"}`),
	}
	if err := daemon.routeHubMessage(message); err != nil {
		t.Fatalf("route stopped-agent intervention: %v", err)
	}

	agentDir := filepath.Join(config.Agents.SessionDir, "agent-api")
	received := waitForFileContains(t, filepath.Join(agentDir, "received.jsonl"), "finish the missing focused test")
	if !strings.Contains(received, `"type":"prompt"`) {
		t.Fatalf("intervention was not delivered as a direct prompt:\n%s", received)
	}
	launches := waitForFileContains(t, filepath.Join(agentDir, "launches.log"), "resume=true")
	if !strings.Contains(launches, "resume=false") || !strings.Contains(launches, "resume=true") {
		t.Fatalf("agent did not reopen its persisted session:\n%s", launches)
	}

	if err := allocator.ResetAgentSessionWithPrompt(ctx, "agent-api", "Operator correction: repair the focused test only."); err != nil {
		t.Fatalf("reset agent session: %v", err)
	}
	launches = waitForFileOccurrences(t, filepath.Join(agentDir, "launches.log"), "resume=false", 2)
	if strings.Count(launches, "resume=false") != 2 {
		t.Fatalf("fresh recovery did not start a new Pi conversation:\n%s", launches)
	}
	received = waitForFileContains(t, filepath.Join(agentDir, "received.jsonl"), "api-work")
	if !strings.Contains(received, "Operator correction: repair the focused test only.") || !strings.Contains(received, "previous agent loop ended before reporting completion") {
		t.Fatalf("fresh recovery prompt omitted authoritative node context:\n%s", received)
	}
	if correction := strings.LastIndex(received, "AUTHORITATIVE OPERATOR CORRECTION:"); correction < strings.LastIndex(received, "previous agent loop ended before reporting completion") {
		t.Fatalf("operator correction must follow generated recovery context:\n%s", received)
	}
}

func TestUserInterventionSteersActiveDomainAgent(t *testing.T) {
	root := t.TempDir()
	script := filepath.Join(root, "dummy-pi.sh")
	if err := os.WriteFile(script, []byte(`#!/usr/bin/env bash
session=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --session-dir) session="$2"; shift 2 ;;
    *) shift ;;
  esac
done
mkdir -p "$session"
while IFS= read -r line; do
  echo "$line" >> "$session/received.jsonl"
  if [[ "$line" == *'"type":"prompt"'* ]]; then
    echo '{"type":"turn_start"}'
  fi
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
	config.Isolation.Network = "shared"

	router := hub.NewRouter(filepath.Join(root, "logs"))
	t.Cleanup(router.Close)
	mgr, err := dag.NewManager(dag.ManagerConfig{
		StoreFilePath: filepath.Join(root, "dag.dat"),
		WalFilePath:   filepath.Join(root, "dag.wal"),
	})
	if err != nil {
		t.Fatalf("new DAG manager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	if err := mgr.AddNode(dag.DagNode{
		ID: "api-work", DomainID: "api", TaskSpec: "finish API work",
		Status: dag.StatusInProgress, AssignedAgent: "agent-api",
	}); err != nil {
		t.Fatalf("add assigned node: %v", err)
	}

	allocator := NewAllocator(config, root, router, mgr)
	allocator.SetIsolationEngine(&isolationtest.FakeEngine{})
	t.Cleanup(allocator.StopAll)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := allocator.AllocateWithOptions(ctx,
		[]coordinator.Domain{{DomainID: "api", AssignedPaths: []string{"api"}}},
		map[string]string{"api": "domain orientation"},
		AllocationOptions{Mode: AllocationModeInitial},
	); err != nil {
		t.Fatalf("allocate agent: %v", err)
	}

	allocator.mu.Lock()
	agent := allocator.activeAgents["agent-api"]
	allocator.mu.Unlock()
	if agent == nil {
		t.Fatal("allocated agent not found")
	}
	if err := agent.SendPrompt("start active work"); err != nil {
		t.Fatalf("start active turn: %v", err)
	}
	agentDir := filepath.Join(config.Agents.SessionDir, "agent-api")
	waitForFileContains(t, filepath.Join(agentDir, "received.jsonl"), `"type":"prompt"`)
	daemon := &Daemon{allocator: allocator, hubRouter: router, ctx: ctx}
	message := hub.Message{
		ID:        "user-steer",
		Type:      hub.MsgContextShare,
		FromAgent: "user",
		ToAgent:   "agent-api",
		Payload:   json.RawMessage(`{"content":"stop exploring and finish the assigned node"}`),
	}
	if err := daemon.routeHubMessage(message); err != nil {
		t.Fatalf("route active-agent intervention: %v", err)
	}

	received := waitForFileContains(t, filepath.Join(agentDir, "received.jsonl"), "stop exploring and finish the assigned node")
	if !strings.Contains(received, `"type":"steer"`) {
		t.Fatalf("active intervention was not delivered as a steer:\n%s", received)
	}
}

func TestDomainNeedsResumeSkipsTerminalDomain(t *testing.T) {
	root := t.TempDir()
	mgr, err := dag.NewManager(dag.ManagerConfig{
		StoreFilePath: filepath.Join(root, "dag.dat"),
		WalFilePath:   filepath.Join(root, "dag.wal"),
	})
	if err != nil {
		t.Fatalf("new DAG manager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	if err := mgr.AddNode(dag.DagNode{ID: "done", DomainID: "api", Status: dag.StatusDone, AssignedAgent: "agent-api"}); err != nil {
		t.Fatalf("add done node: %v", err)
	}
	if err := mgr.AddNode(dag.DagNode{ID: "pending", DomainID: "worker", Status: dag.StatusPending}); err != nil {
		t.Fatalf("add pending node: %v", err)
	}
	allocator := &Allocator{dagManager: mgr}
	if allocator.domainNeedsResume("api", "agent-api") {
		t.Fatal("terminal domain was considered resumable")
	}
	if !allocator.domainNeedsResume("worker", "agent-worker") {
		t.Fatal("pending domain was skipped during resume")
	}
}

func TestRecoveryPromptTargetsAssignedInProgressNode(t *testing.T) {
	root := t.TempDir()
	mgr, err := dag.NewManager(dag.ManagerConfig{
		StoreFilePath: filepath.Join(root, "dag.dat"),
		WalFilePath:   filepath.Join(root, "dag.wal"),
	})
	if err != nil {
		t.Fatalf("new DAG manager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	node := dag.DagNode{
		ID:            "api-implementation",
		DomainID:      "api",
		TaskSpec:      "Implement the API and its focused tests",
		Status:        dag.StatusInProgress,
		AssignedAgent: "agent-api",
		TargetFiles:   []string{"api/service.go", "api/service_test.go"},
	}
	if err := mgr.AddNode(node); err != nil {
		t.Fatalf("add node: %v", err)
	}

	allocator := &Allocator{dagManager: mgr}
	prompt := allocator.resumePromptForAgent("agent-api", "api", "Resume persisted build-spec work.")
	for _, want := range []string{
		"Resume persisted build-spec work.",
		"api-implementation",
		"Implement the API and its focused tests",
		"api/service.go, api/service_test.go",
		"Do not create /workspace/tests or another shared root",
		"place focused tests under the owned domain path instead",
		"Finish or explicitly fail/block this node",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("recovery prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestResumePromptWithoutAssignmentWaitsForAllocator(t *testing.T) {
	allocator := &Allocator{}
	if prompt := allocator.resumeLaunchPrompt("agent-api", "api", "Resume persisted work."); prompt != "" {
		t.Fatalf("unassigned resumed agent received a launch prompt: %q", prompt)
	}
}

func TestAllocateResumeRecordsContractWorkEmbeddedInLaunchPrompt(t *testing.T) {
	root := t.TempDir()
	script := filepath.Join(root, "dummy-pi.sh")
	if err := os.WriteFile(script, []byte(`#!/usr/bin/env bash
session=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --session-dir) session="$2"; shift 2 ;;
    *) shift ;;
  esac
done
mkdir -p "$session"
while IFS= read -r line; do
  echo "$line" >> "$session/received.jsonl"
  echo '{"type":"turn_start"}'
done
`), 0o755); err != nil {
		t.Fatal(err)
	}

	config := &Config{}
	config.Agents.SessionDir = filepath.Join(root, "sessions")
	config.Agents.CommandPath = script
	config.Health.HeartbeatTimeoutSec = 60
	config.Health.ProgressTimeoutSec = 60
	config.Cgroups.Mode = "disabled"
	config.Isolation.Network = "shared"

	agentDir := filepath.Join(config.Agents.SessionDir, "agent-api")
	if err := os.MkdirAll(agentDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(agentDir, "prior-session.jsonl"), []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	router := hub.NewRouter(filepath.Join(root, "logs"))
	t.Cleanup(router.Close)
	mgr, err := dag.NewManager(dag.ManagerConfig{
		StoreFilePath: filepath.Join(root, "dag.dat"),
		WalFilePath:   filepath.Join(root, "dag.wal"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	for _, node := range []dag.DagNode{
		{ID: "api-node", DomainID: "api", Status: dag.StatusPending},
		{ID: "client-node", DomainID: "client", Status: dag.StatusPending},
	} {
		if err := mgr.AddNode(node); err != nil {
			t.Fatal(err)
		}
	}
	coordinationManager, err := coordination.Open(coordination.Options{StateDir: filepath.Join(root, "coordination")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = coordinationManager.Close() })
	contract := coordinationContract("contract-api", "agent-api", "api-node", "agent-client", "client-node")
	if _, err := coordinationManager.Request(context.Background(),
		coordination.Principal{Role: coordination.RoleAgent, AgentID: "agent-client", NodeIDs: []string{"client-node"}},
		coordination.RequestCommand{Contract: contract},
	); err != nil {
		t.Fatal(err)
	}

	allocator := NewAllocator(config, root, router, mgr)
	allocator.SetIsolationEngine(&isolationtest.FakeEngine{})
	allocator.SetWorkInboxService(NewWorkInboxService(mgr, coordinationManager, ""))
	t.Cleanup(allocator.StopAll)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := allocator.AllocateWithOptions(ctx,
		[]coordinator.Domain{{DomainID: "api", AssignedPaths: []string{"api"}}},
		map[string]string{"api": "domain orientation"},
		AllocationOptions{Mode: AllocationModeResume},
	); err != nil {
		t.Fatal(err)
	}

	allocator.mu.Lock()
	dispatched := allocator.dispatchedWork["agent-api"]
	allocator.mu.Unlock()
	if dispatched != "contract:contract-api:contract_publish" {
		t.Fatalf("resumed work = %q", dispatched)
	}
	received := waitForFileContains(t, filepath.Join(agentDir, "received.jsonl"), "contract:contract-api:contract_publish")
	if !strings.Contains(received, "WORK ITEM DISPATCH") {
		t.Fatalf("resume prompt omitted contract work:\n%s", received)
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
  echo "target_model=$AION_TARGET_MODEL"
  echo "target_profile=$AION_TARGET_PROFILE"
  echo "target_api=$AION_TARGET_API"
  echo "inference_socket=$AION_INFERENCE_SOCKET"
  echo "control_socket=$AION_ORCHESTRATOR_SOCKET"
  echo "upstream_credential=${RESOURCE_CREDENTIAL:+set}"
  echo "capability=${AION_AGENT_CAPABILITY:+set}"
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
			Env:      map[string]string{"RESOURCE_CREDENTIAL": "test-value"},
		},
	}

	router := hub.NewRouter(filepath.Join(root, "logs"))
	defer router.Close()
	allocator := NewAllocator(config, root, router, nil)
	allocator.SetIsolationEngine(&isolationtest.FakeEngine{})
	allocator.SetAgentCapabilityFunc(func(agentID string) (string, error) {
		return "test-capability-" + agentID, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer allocator.StopAll()

	err := allocator.AllocateWithOptions(ctx,
		[]coordinator.Domain{{DomainID: "api", AssignedPaths: []string{"api"}}},
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
		"target_profile=forge",
		"target_api=openai-completions",
		"upstream_credential=",
		"capability=set",
	} {
		if !strings.Contains(launch, want) {
			t.Fatalf("launch env missing %q:\n%s", want, launch)
		}
	}
	if !strings.Contains(launch, "inference_socket="+filepath.Join(root, ".aion", "ipc", "inference.sock")) ||
		!strings.Contains(launch, "control_socket="+filepath.Join(root, ".aion", "ipc", "control.sock")) {
		t.Fatalf("fake isolation did not map agent sockets to host fixtures:\n%s", launch)
	}
	contextPath := filepath.Join(config.Agents.SessionDir, "agent-api", "config", "AGENTS.md")
	contextData, err := os.ReadFile(contextPath)
	if err != nil {
		t.Fatalf("read domain context: %v", err)
	}
	if string(contextData) != "initial prompt" {
		t.Fatalf("domain context = %q", contextData)
	}
	time.Sleep(50 * time.Millisecond)
	receivedPath := filepath.Join(config.Agents.SessionDir, "agent-api", "received.jsonl")
	if received, err := os.ReadFile(receivedPath); err == nil && strings.Contains(string(received), `"type":"prompt"`) {
		t.Fatalf("initial allocation spent a bootstrap inference turn:\n%s", received)
	} else if err != nil && !os.IsNotExist(err) {
		t.Fatalf("read initial RPC input: %v", err)
	}
}

func TestPrepareIsolatedAgentRuntimeOwnsOnlyAssignedPaths(t *testing.T) {
	root := t.TempDir()
	router := hub.NewRouter(filepath.Join(root, "logs"))
	defer router.Close()
	config := &Config{}
	config.Agents.CommandPath = filepath.Join(root, "pi")
	config.Agents.CLIPath = filepath.Join(root, "orchestrator-cli")
	config.Execution.Mode = "gateway"
	config.InferenceGateway.Enabled = true
	if err := os.WriteFile(config.Agents.CommandPath, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config.Agents.CLIPath, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	allocator := NewAllocator(config, root, router, nil)
	runtime, err := allocator.prepareIsolatedAgentRuntime("agent-storage", coordinator.Domain{
		DomainID:      "storage",
		AssignedPaths: []string{"oltp/storage"},
	}, filepath.Join(root, ".aion", "sessions", "agent-storage"))
	if err != nil {
		t.Fatalf("prepareIsolatedAgentRuntime: %v", err)
	}
	if runtime.policy.WorkingDir != agentSandboxRoot || runtime.policy.SourceRoot != root {
		t.Fatalf("unexpected isolation policy roots: %#v", runtime.policy)
	}
	if len(runtime.policy.Writable) != 2 {
		t.Fatalf("expected owned path and state mount, got %#v", runtime.policy.Writable)
	}
	if runtime.policy.Writable[0].Source != filepath.Join(root, "oltp", "storage") || runtime.policy.Writable[0].Target != "/workspace/oltp/storage" {
		t.Fatalf("unexpected owned mount: %#v", runtime.policy.Writable[0])
	}
	if runtime.policy.Writable[1].Target != agentSandboxSessionDir {
		t.Fatalf("unexpected session mount: %#v", runtime.policy.Writable[1])
	}
	foundCLI := false
	for _, mount := range runtime.policy.ReadOnly {
		if mount.Source == config.Agents.CLIPath && mount.Target == "/runtime/bin/orchestrator-cli" {
			foundCLI = true
		}
	}
	if !foundCLI || !strings.HasPrefix(runtime.policy.Environment["PATH"], "/runtime/bin:") {
		t.Fatalf("matching orchestrator CLI was not prioritized: mounts=%#v path=%q", runtime.policy.ReadOnly, runtime.policy.Environment["PATH"])
	}
}

func TestPrepareIsolatedAgentRuntimeRequiresGateway(t *testing.T) {
	root := t.TempDir()
	router := hub.NewRouter(filepath.Join(root, "logs"))
	defer router.Close()
	config := &Config{Agents: AgentsConfig{CommandPath: filepath.Join(root, "pi")}}
	if err := os.WriteFile(config.Agents.CommandPath, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	allocator := NewAllocator(config, root, router, nil)
	_, err := allocator.prepareIsolatedAgentRuntime("agent-offline", coordinator.Domain{
		DomainID:      "offline",
		AssignedPaths: []string{"offline"},
	}, filepath.Join(root, ".aion", "sessions", "agent-offline"))
	if err == nil || !strings.Contains(err.Error(), "requires the inference gateway") {
		t.Fatalf("prepare isolated runtime error = %v", err)
	}
}

func TestPrepareIsolatedAgentRuntimeRejectsBroadOrEscapingOwnership(t *testing.T) {
	root := t.TempDir()
	router := hub.NewRouter(filepath.Join(root, "logs"))
	defer router.Close()
	config := &Config{Agents: AgentsConfig{CommandPath: filepath.Join(root, "pi")}}
	config.Isolation.Network = "shared"
	if err := os.WriteFile(config.Agents.CommandPath, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	allocator := NewAllocator(config, root, router, nil)

	for _, assignedPath := range []string{".", "../outside", filepath.Join(root, "absolute")} {
		_, err := allocator.prepareIsolatedAgentRuntime("agent-escape", coordinator.Domain{
			DomainID:      "escape",
			AssignedPaths: []string{assignedPath},
		}, filepath.Join(root, ".aion", "sessions", "agent-escape"))
		if err == nil {
			t.Fatalf("expected broad or escaping assigned path %q to fail", assignedPath)
		}
	}
}

func TestResumePromptIncludesNodeRecoveryContext(t *testing.T) {
	root := t.TempDir()
	manager, err := dag.NewManager(dag.ManagerConfig{
		StoreFilePath: filepath.Join(root, "dag.dat"),
		WalFilePath:   filepath.Join(root, "dag.wal"),
		MaxNodes:      10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	if err := manager.AddNode(dag.DagNode{
		ID: "api-node", DomainID: "api", TaskSpec: "repair API acceptance",
		Status: dag.StatusInProgress, AssignedAgent: "agent-api",
	}); err != nil {
		t.Fatal(err)
	}
	allocator := NewAllocator(&Config{}, root, nil, manager)
	allocator.SetNodeRecoveryContext("api-node", "Directive api-smoke failed: import boundary was missing")

	prompt := allocator.resumePromptForAgent("agent-api", "api", "")
	for _, want := range []string{"AUTHORITATIVE RECOVERY CONTEXT", "api-smoke", "import boundary was missing"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("recovery prompt missing %q:\n%s", want, prompt)
		}
	}
	allocator.ClearNodeRecoveryContext("api-node")
	if prompt = allocator.resumePromptForAgent("agent-api", "api", ""); strings.Contains(prompt, "api-smoke") {
		t.Fatalf("cleared recovery context remained in prompt:\n%s", prompt)
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

func waitForFileOccurrences(t *testing.T, path, want string, count int) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil && strings.Count(string(data), want) >= count {
			return string(data)
		}
		time.Sleep(20 * time.Millisecond)
	}
	data, _ := os.ReadFile(path)
	t.Fatalf("timed out waiting for %d occurrences of %q in %s; got:\n%s", count, want, path, string(data))
	return ""
}
