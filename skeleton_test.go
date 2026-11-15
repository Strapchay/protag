package isolation

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCopyWorkspaceSkeletonSkipsDeniedAndMountTargets(t *testing.T) {
	source := t.TempDir()
	destination := t.TempDir()

	write := func(rel, content string) {
		t.Helper()
		path := filepath.Join(source, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("README.md", "project")
	write("src/main.go", "package main")
	write(".env", "SECRET=value")
	write(".aion/state.json", "{}")
	write("docs/spec.md", "spec")

	if err := copyWorkspaceSkeleton(source, destination, []string{
		filepath.Join(source, ".env"),
		filepath.Join(source, ".aion"),
	}, []string{"docs"}); err != nil {
		t.Fatalf("copy skeleton: %v", err)
	}

	for _, rel := range []string{"README.md", "src/main.go"} {
		if _, err := os.Stat(filepath.Join(destination, rel)); err != nil {
			t.Fatalf("expected %s in skeleton: %v", rel, err)
		}
	}
	for _, rel := range []string{".env", ".aion", "docs"} {
		if _, err := os.Stat(filepath.Join(destination, rel)); !os.IsNotExist(err) {
			t.Fatalf("denied/skipped path %s leaked into skeleton", rel)
		}
	}
}
