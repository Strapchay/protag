package coordinator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnsureAgentIgnoreFileCreatesDefaults(t *testing.T) {
	dir := t.TempDir()
	if err := EnsureAgentIgnoreFile(dir); err != nil {
		t.Fatalf("EnsureAgentIgnoreFile: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, AgentIgnoreFileName))
	if err != nil {
		t.Fatalf("read ignore file: %v", err)
	}
	text := string(data)
	for _, want := range []string{".aion/", ".git/", ".agents/", ".codex/"} {
		if !strings.Contains(text, want) {
			t.Fatalf("ignore file missing %q:\n%s", want, text)
		}
	}
}

func TestLoadAgentExcludePathsMergesProjectFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, AgentIgnoreFileName), []byte("# local\nfixtures/cache/\n"), 0o644); err != nil {
		t.Fatalf("write ignore file: %v", err)
	}
	excludes := LoadAgentExcludePaths(dir)
	if !IsAgentExcludedPath(".aion/runs/current", excludes) {
		t.Fatalf("expected default .aion exclusion")
	}
	if !IsAgentExcludedPath("fixtures/cache/output.json", excludes) {
		t.Fatalf("expected project ignore exclusion")
	}
	if IsAgentExcludedPath("internal/service/file.go", excludes) {
		t.Fatalf("unexpected source exclusion")
	}
}
