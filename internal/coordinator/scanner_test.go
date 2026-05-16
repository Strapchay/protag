package coordinator

import (
	"os"
	"path/filepath"
	"testing"
)

func setupMockProject(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	// Create directory structure
	dirs := []string{
		"cmd/server",
		"internal/auth",
		"internal/db",
		"pkg/utils",
		"web/static",
	}
	for _, d := range dirs {
		os.MkdirAll(filepath.Join(dir, d), 0755)
	}

	// Create files
	files := map[string]string{
		"go.mod":                        "module example",
		"Makefile":                      "build:",
		"cmd/server/main.go":            "package main",
		"internal/auth/handler.go":      "package auth",
		"internal/auth/handler_test.go": "package auth",
		"internal/db/repo.go":           "package db",
		"pkg/utils/helper.go":           "package utils",
		"web/static/index.html":         "<html>",
		"web/static/style.css":          "body {}",
		"README.md":                     "# Project",
	}

	for path, content := range files {
		full := filepath.Join(dir, path)
		os.MkdirAll(filepath.Dir(full), 0755)
		os.WriteFile(full, []byte(content), 0644)
	}

	return dir
}

func TestScanProject(t *testing.T) {
	dir := setupMockProject(t)

	scan, err := ScanProject(dir)
	if err != nil {
		t.Fatalf("ScanProject: %v", err)
	}

	if scan.FileCount == 0 {
		t.Fatal("expected non-zero file count")
	}

	if scan.TotalSize == 0 {
		t.Fatal("expected non-zero total size")
	}

	if scan.DirectoryTree == "" {
		t.Fatal("expected non-empty directory tree")
	}
}

func TestScanDetectsLanguages(t *testing.T) {
	dir := setupMockProject(t)

	scan, err := ScanProject(dir)
	if err != nil {
		t.Fatalf("ScanProject: %v", err)
	}

	foundGo := false
	foundHTML := false
	for _, lang := range scan.Languages {
		if lang == "Go" {
			foundGo = true
		}
		if lang == "HTML" {
			foundHTML = true
		}
	}

	if !foundGo {
		t.Fatal("expected Go language detected")
	}
	if !foundHTML {
		t.Fatal("expected HTML language detected")
	}
}

func TestScanDetectsEntryPoints(t *testing.T) {
	dir := setupMockProject(t)

	scan, err := ScanProject(dir)
	if err != nil {
		t.Fatalf("ScanProject: %v", err)
	}

	if len(scan.EntryPoints) == 0 {
		t.Fatal("expected at least one entry point (main.go)")
	}

	foundMain := false
	for _, ep := range scan.EntryPoints {
		if filepath.Base(ep) == "main.go" {
			foundMain = true
		}
	}
	if !foundMain {
		t.Fatal("expected main.go as entry point")
	}
}

func TestScanDetectsDependencyFiles(t *testing.T) {
	dir := setupMockProject(t)

	scan, err := ScanProject(dir)
	if err != nil {
		t.Fatalf("ScanProject: %v", err)
	}

	if len(scan.DependencyFiles) == 0 {
		t.Fatal("expected at least one dependency file (go.mod)")
	}

	found := false
	for _, df := range scan.DependencyFiles {
		if filepath.Base(df) == "go.mod" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected go.mod as dependency file")
	}
}

func TestScanNonExistent(t *testing.T) {
	_, err := ScanProject("/nonexistent/path")
	if err == nil {
		t.Fatal("expected error for non-existent path")
	}
}
