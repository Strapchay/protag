package orchestrator

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRunStateCreateAndDeleteCurrentRun(t *testing.T) {
	root := t.TempDir()
	cfg := &Config{}
	applyDefaults(cfg)

	run, err := CreateNewCurrentRun(root, cfg)
	if err != nil {
		t.Fatalf("CreateNewCurrentRun: %v", err)
	}
	if run.RunID == "" {
		t.Fatal("expected run id")
	}
	for _, path := range []string{run.Root, run.AgentSessionsDir, run.PiSessionsDir, run.LogsDir} {
		if info, err := os.Stat(path); err != nil || !info.IsDir() {
			t.Fatalf("expected dir %s, info=%v err=%v", path, info, err)
		}
	}
	data, err := os.ReadFile(filepath.Join(root, ".aion", "current_run"))
	if err != nil {
		t.Fatalf("read current_run: %v", err)
	}
	if got := string(data); got != run.RunID+"\n" {
		t.Fatalf("current_run=%q want %q", got, run.RunID+"\n")
	}
	if err := run.Delete(); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := os.Stat(run.Root); !os.IsNotExist(err) {
		t.Fatalf("expected run root removed, err=%v", err)
	}
}

func TestLoadOrCreateCurrentRunReusesPointer(t *testing.T) {
	root := t.TempDir()
	cfg := &Config{}
	applyDefaults(cfg)

	first, err := LoadOrCreateCurrentRun(root, cfg)
	if err != nil {
		t.Fatalf("first LoadOrCreateCurrentRun: %v", err)
	}
	second, err := LoadOrCreateCurrentRun(root, cfg)
	if err != nil {
		t.Fatalf("second LoadOrCreateCurrentRun: %v", err)
	}
	if first.RunID != second.RunID {
		t.Fatalf("expected same run id, first=%s second=%s", first.RunID, second.RunID)
	}
}
