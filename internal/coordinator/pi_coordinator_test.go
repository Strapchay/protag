package coordinator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPiCoordinator_Plan(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aion-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create docs dir
	docsDir := filepath.Join(tmpDir, "docs")
	os.MkdirAll(docsDir, 0755)

	// Create a structured dummy spec
	specContent := `
## Domains
- Backend: API server (Paths: internal/api, cmd/server)
## Tasks
- Init [Backend]: Setup project structure (Priority: 1)
- Health [Backend]: Add health endpoint (Priority: 2)
`
	specPath := filepath.Join(docsDir, "build_spec.md")
	os.WriteFile(specPath, []byte(specContent), 0644)

	coord := NewPiCoordinator(tmpDir, PiCoordinatorConfig{})

	req := PlanRequest{
		ProjectScan: &ProjectScan{
			FileCount:   1,
			EntryPoints: []string{"main.go"},
		},
	}

	resp, err := coord.Plan(context.Background(), req)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	if len(resp.Nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(resp.Nodes))
	}

	if resp.Nodes[0].ID != "Init" || resp.Nodes[0].DomainID != "Backend" {
		t.Errorf("first node mismatch: ID=%s, Domain=%s", resp.Nodes[0].ID, resp.Nodes[0].DomainID)
	}

	if len(resp.Domains) != 1 || resp.Domains[0].DomainID != "Backend" {
		t.Errorf("domain mismatch: count=%d, ID=%s", len(resp.Domains), resp.Domains[0].DomainID)
	}
}

func TestPiCoordinator_PlanMissingBuildSpec(t *testing.T) {
	tmpDir := t.TempDir()
	coord := NewPiCoordinator(tmpDir, PiCoordinatorConfig{})

	_, err := coord.Plan(context.Background(), PlanRequest{})
	if err == nil {
		t.Fatal("expected missing build spec error")
	}
	if got := err.Error(); !strings.Contains(got, "docs/build_spec.md is missing") {
		t.Fatalf("unexpected error: %s", got)
	}
}

func TestPiCoordinator_PlanEmptyBuildSpec(t *testing.T) {
	tmpDir := t.TempDir()
	docsDir := filepath.Join(tmpDir, "docs")
	if err := os.MkdirAll(docsDir, 0755); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(docsDir, "build_spec.md"), []byte(" \n\t"), 0644); err != nil {
		t.Fatalf("write empty spec: %v", err)
	}
	coord := NewPiCoordinator(tmpDir, PiCoordinatorConfig{})

	_, err := coord.Plan(context.Background(), PlanRequest{})
	if err == nil {
		t.Fatal("expected empty build spec error")
	}
	if got := err.Error(); !strings.Contains(got, "docs/build_spec.md is empty") {
		t.Fatalf("unexpected error: %s", got)
	}
}

func TestPiCoordinator_Refine_NotStarted(t *testing.T) {
	coord := NewPiCoordinator(".", PiCoordinatorConfig{})
	err := coord.Refine(context.Background(), "hello")
	if err == nil {
		t.Error("expected error when refining without starting architect")
	}
}
