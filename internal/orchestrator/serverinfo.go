package orchestrator

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type ServerInfo struct {
	ProjectRoot string `json:"project_root"`
	RunID       string `json:"run_id"`
	Addr        string `json:"addr"`
	PID         int    `json:"pid"`
	StartedAt   string `json:"started_at"`
}

func writeServerInfo(projectRoot string, runState *RunState, addr string) error {
	if projectRoot == "" || runState == nil {
		return fmt.Errorf("server info: missing project context")
	}
	info := ServerInfo{
		ProjectRoot: projectRoot,
		RunID:       runState.RunID,
		Addr:        addr,
		PID:         os.Getpid(),
		StartedAt:   time.Now().UTC().Format(time.RFC3339),
	}
	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return fmt.Errorf("server info: marshal: %w", err)
	}
	data = append(data, '\n')
	path := filepath.Join(projectRoot, ".aion", "server.json")
	return writeFileAtomic(path, data)
}

func deleteServerInfo(projectRoot string) error {
	if projectRoot == "" {
		return nil
	}
	return os.Remove(filepath.Join(projectRoot, ".aion", "server.json"))
}
