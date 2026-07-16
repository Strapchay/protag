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
	if !config.GatewayEnabled() {
		t.Fatal("expected inference gateway to be enabled from test config")
	}
	if config.InferenceGateway.PublicBaseURL == "" {
		t.Fatal("expected inference gateway public base URL")
	}
	if config.InferenceGateway.MaxRetries != 2 {
		t.Fatalf("expected two gateway retries, got %d", config.InferenceGateway.MaxRetries)
	}
	if config.InferenceGateway.UpstreamTimeoutSec != 300 {
		t.Fatalf("expected 300s gateway upstream timeout, got %d", config.InferenceGateway.UpstreamTimeoutSec)
	}
}

func TestEnvExampleLoadsConfig(t *testing.T) {
	envPath := filepath.Join("..", "..", ".env.example")
	if err := godotenv.Overload(envPath); err != nil {
		t.Fatalf("load env example %s: %v", envPath, err)
	}

	config, err := LoadConfig(filepath.Join("..", "..", "configs", "aion.yaml"))
	if err != nil {
		t.Fatalf("LoadConfig with env example: %v", err)
	}
	if config.Orchestrator.ListenAddr != "127.0.0.1:50051" {
		t.Fatalf("unexpected listen addr from env example: %s", config.Orchestrator.ListenAddr)
	}
	if !config.GatewayEnabled() {
		t.Fatal("expected env example to enable gateway mode")
	}
	if config.Inference.Coordinator.UseProfile != "oracle" {
		t.Fatalf("unexpected coordinator profile: %s", config.Inference.Coordinator.UseProfile)
	}
	if config.Inference.Models["forge"].Provider != "provider-beta" {
		t.Fatalf("env example should use neutral provider labels, got %q", config.Inference.Models["forge"].Provider)
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
	if config.Health.ExternalActivityStaleTimeoutSec != 45 {
		t.Fatalf("expected default external activity stale timeout 45s, got %d", config.Health.ExternalActivityStaleTimeoutSec)
	}
	if config.Health.ExternalActivityMaxDurationSec != 900 {
		t.Fatalf("expected default external activity max duration 900s, got %d", config.Health.ExternalActivityMaxDurationSec)
	}
	if config.Health.CoordinatorPlannerStartTimeoutSec != 30 {
		t.Fatalf("expected default coordinator planner start timeout 30s, got %d", config.Health.CoordinatorPlannerStartTimeoutSec)
	}
	if config.Health.CoordinatorPlannerFirstRequestTimeoutSec != 60 {
		t.Fatalf("expected default coordinator first request timeout 60s, got %d", config.Health.CoordinatorPlannerFirstRequestTimeoutSec)
	}
	if config.Health.CoordinatorPlannerArtifactTimeoutSec != 300 {
		t.Fatalf("expected default coordinator artifact timeout 300s, got %d", config.Health.CoordinatorPlannerArtifactTimeoutSec)
	}
	if config.Cgroups.Mode != "direct" {
		t.Fatalf("expected default cgroup mode direct, got %q", config.Cgroups.Mode)
	}
	if config.Cgroups.BasePath != "/sys/fs/cgroup/aion" {
		t.Fatalf("expected default cgroup base path, got %q", config.Cgroups.BasePath)
	}
	if config.Execution.Mode != "gateway" {
		t.Fatalf("expected default execution mode gateway, got %q", config.Execution.Mode)
	}
	if config.InferenceGateway.MaxRetries != 2 || config.InferenceGateway.RetryBaseDelayMS != 1000 || config.InferenceGateway.RetryMaxDelayMS != 30000 {
		t.Fatalf("unexpected gateway retry defaults: %#v", config.InferenceGateway)
	}
	if config.InferenceGateway.UpstreamTimeoutSec != 300 {
		t.Fatalf("unexpected gateway upstream timeout default: %d", config.InferenceGateway.UpstreamTimeoutSec)
	}
}

func TestConfigRejectsExcessiveGatewayRetries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "invalid-retries.yaml")
	if err := os.WriteFile(path, []byte("inference_gateway:\n  max_retries: 6\n"), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("expected max_retries validation error")
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
