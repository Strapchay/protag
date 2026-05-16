package orchestrator

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/joho/godotenv"
)

func setDefaultConfigEnv(t *testing.T) {
	t.Helper()
	envPath := filepath.Join("..", "..", ".env.test")
	if err := godotenv.Overload(envPath); err != nil {
		t.Fatalf("load test env %s: %v", envPath, err)
	}
}

func TestLoadConfig(t *testing.T) {
	setDefaultConfigEnv(t)
	// Get the path relative to this test file
	config, err := LoadConfig(filepath.Join("..", "..", "configs", "aion.yaml"))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if want := os.Getenv("AION_ORCHESTRATOR_CORE_ADDR"); config.Orchestrator.ListenAddr != want {
		t.Fatalf("expected %s, got %s", want, config.Orchestrator.ListenAddr)
	}
	if config.Orchestrator.MaxActiveNodes != 200 {
		t.Fatalf("expected 200 max nodes, got %d", config.Orchestrator.MaxActiveNodes)
	}
	if config.Agents.MaxAgents != 4 {
		t.Fatalf("expected 4 max agents, got %d", config.Agents.MaxAgents)
	}
}

func TestConfigDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "minimal.yaml")
	os.WriteFile(path, []byte("orchestrator:\n  listen_addr: 'localhost:9999'\n"), 0644)

	config, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if config.Orchestrator.ListenAddr != "localhost:9999" {
		t.Fatalf("expected localhost:9999, got %s", config.Orchestrator.ListenAddr)
	}
	if config.Orchestrator.MaxActiveNodes != 200 {
		t.Fatalf("expected default 200, got %d", config.Orchestrator.MaxActiveNodes)
	}
	if config.Health.HeartbeatTimeoutSec != 30 {
		t.Fatalf("expected default 30s, got %d", config.Health.HeartbeatTimeoutSec)
	}
}

func TestConfigListenAddrFromEnv(t *testing.T) {
	setDefaultConfigEnv(t)
	t.Setenv("AION_ORCHESTRATOR_HOST", "10.1.2.3")
	t.Setenv("AION_ORCHESTRATOR_PORT", "6000")

	config, err := LoadConfig(filepath.Join("..", "..", "configs", "aion.yaml"))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if config.Orchestrator.ListenAddr != "10.1.2.3:6000" {
		t.Fatalf("expected env-derived listen addr, got %s", config.Orchestrator.ListenAddr)
	}
}

func TestLoadConfigWithFallbackProjectConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "aion.yaml")
	if err := os.WriteFile(path, []byte("orchestrator:\n  listen_addr: 'localhost:7777'\n"), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	config, source, err := LoadConfigWithFallback("", dir)
	if err != nil {
		t.Fatalf("LoadConfigWithFallback: %v", err)
	}
	if source != path {
		t.Fatalf("source=%q want %q", source, path)
	}
	if config.Orchestrator.ListenAddr != "localhost:7777" {
		t.Fatalf("expected localhost:7777, got %s", config.Orchestrator.ListenAddr)
	}
}

func TestLoadConfigWithFallbackBuiltInDefaults(t *testing.T) {
	setDefaultConfigEnv(t)
	config, source, err := LoadConfigWithFallback("", t.TempDir())
	if err != nil {
		t.Fatalf("LoadConfigWithFallback: %v", err)
	}
	if source != "built-in defaults" {
		t.Fatalf("source=%q", source)
	}
	if config.Agents.CommandPath != "pi" {
		t.Fatalf("expected default pi command, got %q", config.Agents.CommandPath)
	}
	if config.Inference.Coordinator.UseProfile == "" {
		t.Fatal("expected built-in coordinator profile")
	}
	if want := os.Getenv("AION_ORCHESTRATOR_CORE_ADDR"); config.Orchestrator.ListenAddr != want {
		t.Fatalf("expected %s, got %s", want, config.Orchestrator.ListenAddr)
	}
}

func TestConfigHelpers(t *testing.T) {
	config, err := LoadConfig(filepath.Join("..", "..", "configs", "aion.yaml"))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if config.FlushDeadline().Milliseconds() != 50 {
		t.Fatalf("expected 50ms, got %v", config.FlushDeadline())
	}
	if config.HeartbeatTimeout().Seconds() != 30 {
		t.Fatalf("expected 30s, got %v", config.HeartbeatTimeout())
	}
	if config.MemoryMaxBytes() != 2048*1024*1024 {
		t.Fatalf("expected %d, got %d", 2048*1024*1024, config.MemoryMaxBytes())
	}
}
