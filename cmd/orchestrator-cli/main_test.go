package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestResolveDashboardAddrFromProjectServerInfo(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".aion"), 0o755); err != nil {
		t.Fatalf("mkdir .aion: %v", err)
	}

	info := map[string]string{"addr": "127.0.0.1:6123"}
	data, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, ".aion", "server.json"), data, 0o644); err != nil {
		t.Fatalf("write server info: %v", err)
	}

	oldwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(oldwd)
	})
	if err := os.Chdir(root); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	if got := resolveDashboardAddr(); got != "127.0.0.1:6123" {
		t.Fatalf("resolveDashboardAddr = %q", got)
	}
}

func TestResolveDashboardAddrFallsBackToEnv(t *testing.T) {
	t.Setenv("AION_ORCHESTRATOR_CORE_ADDR", "127.0.0.1:7001")
	oldwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(oldwd)
	})
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	if got := resolveDashboardAddr(); got != "127.0.0.1:7001" {
		t.Fatalf("resolveDashboardAddr = %q", got)
	}
}

func TestParseDebugStatusCommand(t *testing.T) {
	params, err := parseCommand("debug-status", nil)
	if err != nil {
		t.Fatalf("parse debug-status: %v", err)
	}
	if len(params) != 0 {
		t.Fatalf("debug-status params = %#v", params)
	}
}

func TestResolveDebugStatusAddrFromProjectServerInfo(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".aion"), 0o755); err != nil {
		t.Fatalf("mkdir .aion: %v", err)
	}
	data, err := json.Marshal(map[string]string{"addr": "127.0.0.1:6124"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, ".aion", "server.json"), data, 0o644); err != nil {
		t.Fatalf("write server info: %v", err)
	}

	oldwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(oldwd)
	})
	if err := os.Chdir(root); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	if got := resolveCommandAddr("debug-status"); got != "127.0.0.1:6124" {
		t.Fatalf("resolveCommandAddr(debug-status) = %q", got)
	}
}
