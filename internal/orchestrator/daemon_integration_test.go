package orchestrator_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"aion-isolation/isolationtest"
	"aion-kernel/internal/coordinator"
	"aion-kernel/internal/dag"
	"aion-kernel/internal/orchestrator"
)

func TestDaemonLifecycleAndRecovery(t *testing.T) {
	tempDir := t.TempDir()

	config := &orchestrator.Config{}
	config.Orchestrator.ListenAddr = "127.0.0.1:0"
	config.Orchestrator.DagFile = "dag.bin"
	config.Orchestrator.WalFile = "dag.wal"
	config.Orchestrator.MaxActiveNodes = 100
	config.Orchestrator.FlushDeadlineMs = 10
	config.Agents.SessionDir = "sessions"
	config.Health.ProgressTimeoutSec = 1

	// 1. Start Daemon
	daemon1, err := orchestrator.NewDaemon(config, tempDir)
	if err != nil {
		t.Fatalf("NewDaemon: %v", err)
	}

	if err := daemon1.Start(); err != nil {
		t.Fatalf("Daemon Start: %v", err)
	}

	// Wait for daemon to initialize
	time.Sleep(100 * time.Millisecond)

	// Inject a node directly to the DAG Manager to test WAL replay later
	err = daemon1.DagManager().AddNode(dag.DagNode{
		ID:            "test-node-1",
		DomainID:      "test-domain",
		TaskSpec:      "echo hello",
		Priority:      1,
		Status:        dag.StatusPending,
		AssignedAgent: "",
	})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// 2. Graceful Shutdown
	daemon1.Shutdown()

	// 3. Restart Daemon to check if WAL replay recovers the node
	daemon2, err := orchestrator.NewDaemon(config, tempDir)
	if err != nil {
		t.Fatalf("NewDaemon 2: %v", err)
	}

	snap := daemon2.DagManager().Snapshot()
	if len(snap.Nodes) != 1 {
		for i, n := range snap.Nodes {
			t.Logf("Node %d: %s", i, n.ID)
		}
		t.Errorf("expected 1 node recovered from WAL/Store, got %d", len(snap.Nodes))
	} else if snap.Nodes[0].ID != "test-node-1" {
		t.Errorf("expected node test-node-1, got %s", snap.Nodes[0].ID)
	}

	daemon2.Shutdown()
}

func TestParametricDispatcher(t *testing.T) {
	tempDir := t.TempDir()

	// Creates a dummy script to act as the Pi Agent
	dummyAgentScript := filepath.Join(tempDir, "dummy-agent.sh")
	os.WriteFile(dummyAgentScript, []byte(`#!/bin/bash
# Read commands from stdin and write to a file we can inspect
while read line; do
    echo "$line" >> "$AION_AGENT_SESSION_DIR/received.jsonl"
	# emit a ready event so supervisor knows we are alive
    echo '{"type":"turn_start","message":"ready"}'
done
`), 0755)

	config := &orchestrator.Config{}
	config.Orchestrator.ListenAddr = "127.0.0.1:0"
	config.Orchestrator.DagFile = "dag.bin"
	config.Orchestrator.WalFile = "dag.wal"
	config.Orchestrator.MaxActiveNodes = 100
	config.Orchestrator.FlushDeadlineMs = 10
	config.Agents.SessionDir = filepath.Join(tempDir, "sessions")
	config.Agents.CommandPath = dummyAgentScript
	config.Isolation.Network = "shared"
	os.MkdirAll(config.Agents.SessionDir, 0755)

	daemon, err := orchestrator.NewDaemon(config, tempDir)
	if err != nil {
		t.Fatalf("NewDaemon: %v", err)
	}

	if err := daemon.Start(); err != nil {
		t.Fatalf("Daemon Start: %v", err)
	}
	defer daemon.Shutdown()

	time.Sleep(100 * time.Millisecond)

	// Mock a multi-node DAG (Node-A -> Node-B)
	err = daemon.DagManager().AddNode(dag.DagNode{
		ID:       "Node-A",
		DomainID: "test",
		TaskSpec: "Do part A",
		Priority: 1,
		Status:   dag.StatusPending,
	})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	err = daemon.DagManager().AddNode(dag.DagNode{
		ID:       "Node-B",
		DomainID: "test",
		TaskSpec: "Do part B",
		Priority: 2,
		Status:   dag.StatusPending,
	})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	err = daemon.DagManager().AddEdge(dag.DagEdge{
		FromNode: "Node-A",
		ToNode:   "Node-B",
		Type:     dag.EdgeDependency,
	})
	if err != nil {
		t.Fatalf("AddEdge: %v", err)
	}

	// Start Allocator to trigger dispatch loop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create prompt
	prompts := map[string]string{"test": "System instruction"}
	daemon.Allocator().SetIsolationEngine(&isolationtest.FakeEngine{})
	// Allocate will spawn agent-test
	if err := daemon.Allocator().Allocate(ctx, []coordinator.Domain{{DomainID: "test", AssignedPaths: []string{"src"}}}, prompts); err != nil {
		t.Fatalf("Allocate: %v", err)
	}

	go daemon.Allocator().MonitorExecution(ctx, 100*time.Millisecond)

	time.Sleep(500 * time.Millisecond)

	// Simulate successful StatusDone emission of Node-A
	err = daemon.DagManager().UpdateNode("Node-A", dag.StatusDone)
	if err != nil {
		t.Fatalf("UpdateNode A: %v", err)
	}

	// Wait for Dispatcher to assign Node-B to the agent
	time.Sleep(1 * time.Second)

	// Check if the agent received the dispatch
	agentSessionDir := filepath.Join(config.Agents.SessionDir, "agent-test")
	receivedBytes, err := os.ReadFile(filepath.Join(agentSessionDir, "received.jsonl"))
	if err != nil {
		t.Fatalf("Failed to read agent received file: %v", err)
	}

	receivedData := string(receivedBytes)
	if !strings.Contains(receivedData, "TASK DISPATCH (Node Node-B)") {
		t.Errorf("Agent did not receive task dispatch for Node-B. Got: %s", receivedData)
	}
}
