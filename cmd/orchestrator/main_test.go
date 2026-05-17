package main

import (
	"os"
	"path/filepath"
	"testing"

	"aion-kernel/internal/orchestrator"
)

func TestResolveKernelRootFromExecutable(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "configs"), 0o755); err != nil {
		t.Fatalf("mkdir configs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, ".env"), []byte("AION_KERNEL_ROOT_TEST=1\n"), 0o644); err != nil {
		t.Fatalf("write kernel env: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "configs", "aion.yaml"), []byte("orchestrator:\n  listen_addr: '127.0.0.1:0'\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	binDir := filepath.Join(root, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("mkdir bin: %v", err)
	}
	exe := filepath.Join(binDir, "aion-kernel")
	if err := os.WriteFile(exe, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("write exe: %v", err)
	}

	got, err := resolveKernelRootFromExecutable(exe)
	if err != nil {
		t.Fatalf("resolveKernelRootFromExecutable: %v", err)
	}
	if got != root {
		t.Fatalf("got %q want %q", got, root)
	}
}

func TestLoadEnvironmentPrefersKernelDefaultsAndProjectOverrides(t *testing.T) {
	oldHome := os.Getenv("HOME")
	t.Cleanup(func() {
		_ = os.Setenv("HOME", oldHome)
	})

	kernelRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(kernelRoot, "configs"), 0o755); err != nil {
		t.Fatalf("mkdir kernel configs: %v", err)
	}
	kernelEnv := []byte("" +
		"AION_PROFILE_ORACLE_ENV_KEY=KERNEL_ORACLE_KEY\n" +
		"AION_PROFILE_ORACLE_PROVIDER=kernel-provider\n" +
		"AION_PROFILE_ORACLE_MODEL=kernel-model\n" +
		"AION_PROFILE_ORACLE_API_KEY=kernel-placeholder-value\n" +
		"AION_ORCHESTRATOR_HOST=127.0.0.1\n" +
		"AION_ORCHESTRATOR_PORT=0\n")
	if err := os.WriteFile(filepath.Join(kernelRoot, ".env"), kernelEnv, 0o644); err != nil {
		t.Fatalf("write kernel env: %v", err)
	}
	if err := os.WriteFile(filepath.Join(kernelRoot, "configs", "aion.yaml"), []byte("orchestrator:\n  listen_addr: '127.0.0.1:0'\n"), 0o644); err != nil {
		t.Fatalf("write kernel config sentinel: %v", err)
	}

	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, ".env"), []byte("AION_ORCHESTRATOR_PORT=6000\n"), 0o644); err != nil {
		t.Fatalf("write project env: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectRoot, "aion.yaml"), []byte(`orchestrator:
  listen_addr: "${AION_ORCHESTRATOR_HOST}:${AION_ORCHESTRATOR_PORT}"
inference:
  models:
    oracle:
      provider: "${AION_PROFILE_ORACLE_PROVIDER}"
      model: "${AION_PROFILE_ORACLE_MODEL}"
      env:
        ${AION_PROFILE_ORACLE_ENV_KEY}: "${AION_PROFILE_ORACLE_API_KEY}"
`), 0o644); err != nil {
		t.Fatalf("write project config: %v", err)
	}

	homeDir := t.TempDir()
	if err := os.Setenv("HOME", homeDir); err != nil {
		t.Fatalf("set HOME: %v", err)
	}
	t.Setenv("AION_KERNEL_ROOT", kernelRoot)

	loadEnvironment(kernelRoot, projectRoot)

	if got := os.Getenv("AION_PROFILE_ORACLE_ENV_KEY"); got != "KERNEL_ORACLE_KEY" {
		t.Fatalf("kernel env not loaded, got %q", got)
	}
	if got := os.Getenv("AION_PROFILE_ORACLE_PROVIDER"); got != "kernel-provider" {
		t.Fatalf("kernel provider env not loaded, got %q", got)
	}
	if got := os.Getenv("AION_ORCHESTRATOR_PORT"); got != "6000" {
		t.Fatalf("project env override not applied, got %q", got)
	}

	config, err := orchestrator.LoadConfig(filepath.Join(projectRoot, "aion.yaml"))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if got := config.Orchestrator.ListenAddr; got != "127.0.0.1:6000" {
		t.Fatalf("dynamic YAML env expansion failed, got %q", got)
	}
	if got := config.Inference.Models["oracle"].Env["KERNEL_ORACLE_KEY"]; got != "kernel-placeholder-value" {
		t.Fatalf("dynamic YAML key resolution failed, got %q", got)
	}
}

func TestValidateKernelRootRejectsMissingSentinels(t *testing.T) {
	root := t.TempDir()
	if _, err := validateKernelRoot(root); err == nil {
		t.Fatal("expected error for missing kernel sentinels")
	}
}
