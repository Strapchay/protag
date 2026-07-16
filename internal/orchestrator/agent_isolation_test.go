package orchestrator

import (
	"os"
	"path/filepath"
	"testing"
)

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
