package orchestrator

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the root configuration for the Aion-Kernel.
type Config struct {
	Orchestrator OrchestratorConfig `yaml:"orchestrator"`
	Agents       AgentsConfig       `yaml:"agents"`
	Health       HealthConfig       `yaml:"health"`
	Cgroups      CgroupsConfig      `yaml:"cgroups"`
	Inference    InferenceConfig    `yaml:"inference"`
	Memory       MemoryConfig       `yaml:"memory"`
}

// MemoryConfig configures the semantic memory store.
type MemoryConfig struct {
	Enabled            bool    `yaml:"enabled"`
	StorePath          string  `yaml:"store_path"`
	EmbedderType       string  `yaml:"embedder_type"` // backend codename, e.g. "harbor", "mock"
	EmbedderModel      string  `yaml:"embedder_model"`
	EmbedderBaseURL    string  `yaml:"embedder_base_url"`
	RelevanceThreshold float64 `yaml:"relevance_threshold"`
	ReadEnabled        bool    `yaml:"read_enabled"` // For Phase 10.4
}

// OrchestratorConfig configures the Orchestrator daemon.
type OrchestratorConfig struct {
	ListenAddr      string `yaml:"listen_addr"`
	DagFile         string `yaml:"dag_file"`
	WalFile         string `yaml:"wal_file"`
	MaxActiveNodes  uint32 `yaml:"max_active_nodes"`
	FlushDeadlineMs int    `yaml:"flush_deadline_ms"`
}

// AgentsConfig configures agent management.
type AgentsConfig struct {
	SessionDir  string   `yaml:"session_dir"`
	MaxAgents   int      `yaml:"max_agents"`
	SharedFiles []string `yaml:"shared_files"`
	CommandPath string   `yaml:"command_path"`
	CommandArgs []string `yaml:"command_args"`
	SkillPaths  []string `yaml:"skill_paths,omitempty"`
}

// HealthConfig configures agent health monitoring.
type HealthConfig struct {
	HeartbeatTimeoutSec int `yaml:"heartbeat_timeout_sec"`
	ProgressTimeoutSec  int `yaml:"progress_timeout_sec"`
}

// CgroupsConfig configures cgroup resource limits.
type CgroupsConfig struct {
	Enabled     bool   `yaml:"enabled"`
	Mode        string `yaml:"mode,omitempty"`
	BasePath    string `yaml:"base_path,omitempty"`
	MemoryMaxMB int64  `yaml:"memory_max_mb"`
	PidsMax     int64  `yaml:"pids_max"`
}

// InferenceConfig configures LLM inference providers.
type InferenceConfig struct {
	Models       map[string]ModelProfile `yaml:"models"`
	Coordinator  ProviderConfig          `yaml:"coordinator"`
	Architect    ProviderConfig          `yaml:"architect"`
	DomainAgents ProviderConfig          `yaml:"domain_agents"`
	Fallback     ProviderConfig          `yaml:"fallback"`
}

// ModelProfile defines a reusable inference configuration.
type ModelProfile struct {
	Provider string            `yaml:"provider"`
	Model    string            `yaml:"model"`
	Endpoint string            `yaml:"endpoint,omitempty"`
	Env      map[string]string `yaml:"env,omitempty"`
}

// ProviderConfig configures a single inference provider.
type ProviderConfig struct {
	UseProfile string `yaml:"use_profile,omitempty"`
	Provider   string `yaml:"provider,omitempty"`
	Model      string `yaml:"model,omitempty"`
	Endpoint   string `yaml:"endpoint,omitempty"`
}

// LoadConfig reads and parses a YAML configuration file with environment variable expansion.
func LoadConfig(filePath string) (*Config, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("config: read file: %w", err)
	}
	return parseConfigData(data)
}

func LoadConfigWithFallback(configPath, projectRoot string) (*Config, string, error) {
	if configPath != "" {
		resolved := configPath
		if !filepath.IsAbs(resolved) {
			resolved = filepath.Join(projectRoot, resolved)
		}
		config, err := LoadConfig(resolved)
		return config, resolved, err
	}

	candidates := []string{
		filepath.Join(projectRoot, "aion.yaml"),
		filepath.Join(projectRoot, ".aion", "aion.yaml"),
		filepath.Join(projectRoot, "configs", "aion.yaml"),
	}
	if exe, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exe)
		candidates = append(candidates,
			filepath.Join(exeDir, "configs", "aion.yaml"),
			filepath.Join(exeDir, "..", "configs", "aion.yaml"),
		)
	}

	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err != nil {
			continue
		}
		config, err := LoadConfig(candidate)
		return config, candidate, err
	}

	config, err := parseConfigData([]byte(defaultConfigYAML))
	if err != nil {
		return nil, "", err
	}
	return config, "built-in defaults", nil
}

func parseConfigData(data []byte) (*Config, error) {
	// Expand environment variables in the YAML content
	expandedData := []byte(os.ExpandEnv(string(data)))

	var config Config
	if err := yaml.Unmarshal(expandedData, &config); err != nil {
		return nil, fmt.Errorf("config: parse yaml: %w", err)
	}

	applyDefaults(&config)

	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("config: validate: %w", err)
	}

	return &config, nil
}

const defaultConfigYAML = `
orchestrator:
  listen_addr: "${AION_ORCHESTRATOR_CORE_ADDR}"
  dag_file: ".aion/dag.fbs"
  wal_file: ".aion/orchestrator.wal"
  max_active_nodes: 200
  flush_deadline_ms: 50

health:
  heartbeat_timeout_sec: 30
  progress_timeout_sec: 120

cgroups:
  enabled: true
  mode: "${AION_CGROUPS_MODE}"
  base_path: "${AION_CGROUPS_BASE_PATH}"
  memory_max_mb: 2048
  pids_max: 256

inference:
  models:
    # oracle
    oracle:
      provider: "${AION_PROFILE_ORACLE_PROVIDER}"
      model: "${AION_PROFILE_ORACLE_MODEL}"
      env:
        ${AION_PROFILE_ORACLE_ENV_KEY}: "${AION_PROFILE_ORACLE_API_KEY}"
    # forge
    forge:
      provider: "${AION_PROFILE_FORGE_PROVIDER}"
      model: "${AION_PROFILE_FORGE_MODEL}"
      env:
        ${AION_PROFILE_FORGE_ENV_KEY}: "${AION_PROFILE_FORGE_API_KEY}"
    # glimmer
    glimmer:
      provider: "${AION_PROFILE_GLIMMER_PROVIDER}"
      model: "${AION_PROFILE_GLIMMER_MODEL}"
      env:
        ${AION_PROFILE_GLIMMER_ACCOUNT_ENV_KEY}: "${AION_PROFILE_GLIMMER_ACCOUNT_ID}"
        ${AION_PROFILE_GLIMMER_API_KEY_ENV_KEY}: "${AION_PROFILE_GLIMMER_API_KEY}"
    # ember
    ember:
      provider: "${AION_PROFILE_EMBER_PROVIDER}"
      model: "${AION_PROFILE_EMBER_MODEL}"
      env:
        ${AION_PROFILE_EMBER_ENV_KEY}: "${AION_PROFILE_EMBER_API_KEY}"
    # lyric
    lyric:
      provider: "${AION_PROFILE_LYRIC_PROVIDER}"
      model: "${AION_PROFILE_LYRIC_MODEL}"
      env:
        ${AION_PROFILE_LYRIC_ENV_KEY}: "${AION_PROFILE_LYRIC_API_KEY}"

  coordinator:
    use_profile: "${AION_COORDINATOR_PROFILE}"
  domain_agents:
    use_profile: "${AION_DOMAIN_AGENTS_PROFILE}"
  fallback:
    provider: "${AION_PROFILE_HARBOR_PROVIDER}"
    model: "${AION_PROFILE_HARBOR_MODEL}"
    endpoint: "${AION_PROFILE_HARBOR_ENDPOINT}"

agents:
  session_dir: "${AION_AGENT_SESSION_DIR}"
  max_agents: ${AION_MAX_AGENTS}
  command_path: "${AION_AGENT_COMMAND_PATH}"
  skill_paths:
    - "${AION_AGENT_SKILL_PATH}"
  shared_files:
    - "go.mod"
    - "go.sum"

memory:
  enabled: ${AION_MEMORY_ENABLED}
  read_enabled: ${AION_MEMORY_READ_ENABLED}
  embedder_type: "${AION_MEMORY_BACKEND_PROFILE}"
  embedder_model: "${AION_MEMORY_BACKEND_MODEL}"
  embedder_base_url: "${AION_MEMORY_BACKEND_ENDPOINT}"
  relevance_threshold: ${AION_MEMORY_RELEVANCE_THRESHOLD}
`

func applyDefaults(c *Config) {
	if c.Orchestrator.ListenAddr == "" {
		c.Orchestrator.ListenAddr = os.Getenv("AION_ORCHESTRATOR_CORE_ADDR")
	}
	if c.Orchestrator.DagFile == "" {
		c.Orchestrator.DagFile = ".aion/dag.bin"
	}
	if c.Orchestrator.WalFile == "" {
		c.Orchestrator.WalFile = ".aion/orchestrator.wal"
	}
	if c.Orchestrator.MaxActiveNodes == 0 {
		c.Orchestrator.MaxActiveNodes = 200
	}
	if c.Orchestrator.FlushDeadlineMs == 0 {
		c.Orchestrator.FlushDeadlineMs = 50
	}
	if c.Agents.SessionDir == "" {
		c.Agents.SessionDir = ".aion/sessions/"
	}
	if c.Agents.MaxAgents == 0 {
		c.Agents.MaxAgents = 4
	}
	if c.Health.HeartbeatTimeoutSec == 0 {
		c.Health.HeartbeatTimeoutSec = 30
	}
	if c.Health.ProgressTimeoutSec == 0 {
		c.Health.ProgressTimeoutSec = 120
	}
	if c.Cgroups.MemoryMaxMB == 0 {
		c.Cgroups.MemoryMaxMB = 2048
	}
	if c.Cgroups.PidsMax == 0 {
		c.Cgroups.PidsMax = 256
	}
	if c.Cgroups.Mode == "" {
		c.Cgroups.Mode = "direct"
	}
	if c.Cgroups.BasePath == "" {
		c.Cgroups.BasePath = "/sys/fs/cgroup/aion"
	}
	if c.Memory.Enabled {
		if c.Memory.StorePath == "" {
			c.Memory.StorePath = filepath.Join(filepath.Dir(c.Orchestrator.DagFile), "memory")
		}
		if c.Memory.EmbedderType == "" {
			c.Memory.EmbedderType = os.Getenv("AION_MEMORY_BACKEND_PROFILE")
		}
		if c.Memory.EmbedderModel == "" {
			c.Memory.EmbedderModel = os.Getenv("AION_MEMORY_BACKEND_MODEL")
		}
		if c.Memory.EmbedderBaseURL == "" {
			c.Memory.EmbedderBaseURL = os.Getenv("AION_MEMORY_BACKEND_ENDPOINT")
		}
		if c.Memory.RelevanceThreshold == 0.0 {
			c.Memory.RelevanceThreshold = 0.7
		}
	}
}

func validateConfig(c *Config) error {
	if c.Orchestrator.MaxActiveNodes > 10000 {
		return fmt.Errorf("max_active_nodes %d is unreasonably high", c.Orchestrator.MaxActiveNodes)
	}
	if c.Agents.MaxAgents > 32 {
		return fmt.Errorf("max_agents %d exceeds reasonable limit", c.Agents.MaxAgents)
	}
	switch strings.ToLower(strings.TrimSpace(c.Cgroups.Mode)) {
	case "", "direct", "systemd", "disabled":
	default:
		return fmt.Errorf("invalid cgroups.mode %q", c.Cgroups.Mode)
	}
	return nil
}

// FlushDeadline returns the flush deadline as a time.Duration.
func (c *Config) FlushDeadline() time.Duration {
	return time.Duration(c.Orchestrator.FlushDeadlineMs) * time.Millisecond
}

// HeartbeatTimeout returns the heartbeat timeout as a time.Duration.
func (c *Config) HeartbeatTimeout() time.Duration {
	return time.Duration(c.Health.HeartbeatTimeoutSec) * time.Second
}

// ProgressTimeout returns the progress timeout as a time.Duration.
func (c *Config) ProgressTimeout() time.Duration {
	return time.Duration(c.Health.ProgressTimeoutSec) * time.Second
}

// MemoryMaxBytes returns the memory limit in bytes.
func (c *Config) MemoryMaxBytes() int64 {
	return c.Cgroups.MemoryMaxMB * 1024 * 1024
}
