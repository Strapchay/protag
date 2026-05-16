package orchestrator

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type RunState struct {
	RunID            string `json:"run_id"`
	Root             string `json:"root"`
	AgentSessionsDir string `json:"agent_sessions_dir"`
	PiSessionsDir    string `json:"pi_sessions_dir"`
	LogsDir          string `json:"logs_dir"`
	DagFile          string `json:"dag_file"`
	WalFile          string `json:"wal_file"`
	CreatedAt        string `json:"created_at"`
}

func LoadOrCreateCurrentRun(projectRoot string, config *Config) (*RunState, error) {
	aionRoot := filepath.Join(projectRoot, ".aion")
	if err := os.MkdirAll(filepath.Join(aionRoot, "runs"), 0755); err != nil {
		return nil, fmt.Errorf("run state: create runs dir: %w", err)
	}
	currentPath := filepath.Join(aionRoot, "current_run")
	data, err := os.ReadFile(currentPath)
	if err == nil {
		runID := strings.TrimSpace(string(data))
		if runID != "" {
			run := newRunState(projectRoot, config, runID)
			if err := run.ensure(); err != nil {
				return nil, err
			}
			return run, nil
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("run state: read current run: %w", err)
	}
	return CreateNewCurrentRun(projectRoot, config)
}

func CreateNewCurrentRun(projectRoot string, config *Config) (*RunState, error) {
	run := newRunState(projectRoot, config, newRunID())
	if err := run.ensure(); err != nil {
		return nil, err
	}
	if err := writeJSONFile(filepath.Join(run.Root, "metadata.json"), run); err != nil {
		return nil, fmt.Errorf("run state: write metadata: %w", err)
	}
	currentPath := filepath.Join(projectRoot, ".aion", "current_run")
	if err := writeFileAtomic(currentPath, []byte(run.RunID+"\n")); err != nil {
		return nil, fmt.Errorf("run state: write current pointer: %w", err)
	}
	return run, nil
}

func (r *RunState) Delete() error {
	if r == nil || r.Root == "" {
		return nil
	}
	return os.RemoveAll(r.Root)
}

func newRunState(projectRoot string, config *Config, runID string) *RunState {
	root := filepath.Join(projectRoot, ".aion", "runs", runID)
	return &RunState{
		RunID:            runID,
		Root:             root,
		AgentSessionsDir: filepath.Join(root, "agent_sessions"),
		PiSessionsDir:    filepath.Join(root, "pi_sessions"),
		LogsDir:          filepath.Join(root, "logs"),
		DagFile:          filepath.Join(root, filepath.Base(config.Orchestrator.DagFile)),
		WalFile:          filepath.Join(root, filepath.Base(config.Orchestrator.WalFile)),
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
	}
}

func (r *RunState) ensure() error {
	for _, dir := range []string{r.Root, r.AgentSessionsDir, r.PiSessionsDir, r.LogsDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("run state: create %s: %w", dir, err)
		}
	}
	return nil
}

func newRunID() string {
	return "run_" + time.Now().UTC().Format("20060102T150405.000000000Z")
}

func writeJSONFile(path string, value interface{}) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeFileAtomic(path, data)
}

func writeFileAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func resolveProjectPath(projectRoot, path string) string {
	if path == "" {
		return projectRoot
	}
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(projectRoot, path)
}
