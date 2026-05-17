package orchestrator

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"aion-kernel/internal/dag"
	"aion-kernel/internal/hub"
)

func TestBuildSpecAttemptSaveLoad(t *testing.T) {
	root := t.TempDir()
	attempt := newBuildSpecAttempt("run_1", "docs/build_spec.md", []byte("hello world"))
	attempt.Status = BuildSpecAttemptPlanning
	if err := saveBuildSpecAttempt(root, attempt); err != nil {
		t.Fatalf("saveBuildSpecAttempt: %v", err)
	}
	loaded, err := loadBuildSpecAttempt(root)
	if err != nil {
		t.Fatalf("loadBuildSpecAttempt: %v", err)
	}
	if loaded == nil || loaded.SpecHash != attempt.SpecHash || loaded.Status != BuildSpecAttemptPlanning {
		t.Fatalf("unexpected loaded attempt: %#v", loaded)
	}
}

func TestAppendBuildSpecTraceWritesProjectDebugLog(t *testing.T) {
	projectRoot := t.TempDir()
	runRoot := filepath.Join(projectRoot, ".aion", "runs", "run_1")
	if err := os.MkdirAll(runRoot, 0o755); err != nil {
		t.Fatalf("mkdir run root: %v", err)
	}

	if err := appendBuildSpecTrace(runRoot, "trace line"); err != nil {
		t.Fatalf("appendBuildSpecTrace: %v", err)
	}

	for _, path := range []string{
		filepath.Join(runRoot, "build_spec_trace.log"),
		filepath.Join(projectRoot, "build_spec_debug.log"),
	} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if !strings.Contains(string(data), "trace line") {
			t.Fatalf("expected trace line in %s, got %q", path, string(data))
		}
	}
}

func TestPrepareBuildSpecAttemptRefusesActiveAttempt(t *testing.T) {
	daemon := testBuildSpecDaemon(t)
	attempt := newBuildSpecAttempt(daemon.runState.RunID, "docs/build_spec.md", []byte("hello"))
	attempt.Status = BuildSpecAttemptActive
	if err := saveBuildSpecAttempt(daemon.runState.Root, attempt); err != nil {
		t.Fatalf("saveBuildSpecAttempt: %v", err)
	}
	if _, err := daemon.prepareBuildSpecAttempt("docs/build_spec.md", []byte("hello")); err == nil {
		t.Fatal("expected active attempt refusal")
	}
}

func TestPrepareBuildSpecAttemptRetriesStaleAttempt(t *testing.T) {
	daemon := testBuildSpecDaemon(t)
	old := newBuildSpecAttempt(daemon.runState.RunID, "docs/build_spec.md", []byte("hello"))
	old.Status = BuildSpecAttemptPlanning
	old.UpdatedAt = time.Now().Add(-buildSpecStaleAfter - time.Minute)
	data, err := json.MarshalIndent(old, "", "  ")
	if err != nil {
		t.Fatalf("marshal stale attempt: %v", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(buildSpecAttemptPath(daemon.runState.Root), data, 0o644); err != nil {
		t.Fatalf("write stale attempt: %v", err)
	}
	if err := os.WriteFile(daemon.runState.DagFile, []byte("dag"), 0o644); err != nil {
		t.Fatalf("write dag: %v", err)
	}
	if err := os.WriteFile(daemon.runState.WalFile, []byte("wal"), 0o644); err != nil {
		t.Fatalf("write wal: %v", err)
	}
	attempt, err := daemon.prepareBuildSpecAttempt("docs/build_spec.md", []byte("hello"))
	if err != nil {
		t.Fatalf("prepareBuildSpecAttempt: %v", err)
	}
	if attempt == nil || attempt.Status != BuildSpecAttemptPlanning {
		t.Fatalf("expected fresh planning attempt, got %#v", attempt)
	}
	if snapshot := daemon.dagManager.Snapshot(); len(snapshot.Nodes) != 0 || len(snapshot.Edges) != 0 {
		t.Fatalf("expected cleared dag snapshot, got %#v", snapshot)
	}
}

func testBuildSpecDaemon(t *testing.T) *Daemon {
	t.Helper()
	root := t.TempDir()
	cfg := &Config{}
	applyDefaults(cfg)
	cfg.Orchestrator.ListenAddr = "127.0.0.1:0"
	run, err := CreateNewCurrentRun(root, cfg)
	if err != nil {
		t.Fatalf("CreateNewCurrentRun: %v", err)
	}
	dagMgr, err := dag.NewManager(dag.ManagerConfig{
		StoreFilePath: run.DagFile,
		WalFilePath:   run.WalFile,
		MaxNodes:      cfg.Orchestrator.MaxActiveNodes,
		FlushDeadline: cfg.FlushDeadline(),
	})
	if err != nil {
		t.Fatalf("dag.NewManager: %v", err)
	}
	router := hub.NewRouter(run.LogsDir)
	alloc := NewAllocator(cfg, root, router, dagMgr)
	server := NewServer(dagMgr, nil, nil, nil)
	d := &Daemon{
		config:      cfg,
		dagManager:  dagMgr,
		allocator:   alloc,
		server:      server,
		hubRouter:   router,
		projectRoot: root,
		runState:    run,
	}
	return d
}

func TestPrepareBuildSpecAttemptRefusesDagWithoutMetadata(t *testing.T) {
	daemon := testBuildSpecDaemon(t)
	if err := daemon.dagManager.AddNode(dag.DagNode{ID: "node-1", DomainID: "domain-1"}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if _, err := daemon.prepareBuildSpecAttempt("docs/build_spec.md", []byte("hello")); err == nil {
		t.Fatal("expected refusal when DAG exists without attempt metadata")
	}
}
