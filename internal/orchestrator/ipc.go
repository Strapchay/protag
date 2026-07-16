package orchestrator

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	agentIPCMountPath       = "/run/aion"
	agentInferenceSocket    = agentIPCMountPath + "/inference.sock"
	agentOrchestratorSocket = agentIPCMountPath + "/control.sock"
)

type daemonIPCPaths struct {
	Dir             string
	InferenceSocket string
	ControlSocket   string
}

func prepareDaemonIPC(projectRoot string) (daemonIPCPaths, error) {
	dir := filepath.Join(projectRoot, ".aion", "ipc")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return daemonIPCPaths{}, fmt.Errorf("create IPC directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return daemonIPCPaths{}, fmt.Errorf("protect IPC directory: %w", err)
	}
	return daemonIPCPaths{
		Dir:             dir,
		InferenceSocket: filepath.Join(dir, "inference.sock"),
		ControlSocket:   filepath.Join(dir, "control.sock"),
	}, nil
}

func prepareUnixSocketPath(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return fmt.Errorf("refusing to replace non-socket path %s", path)
		}
		return os.Remove(path)
	}
	if !os.IsNotExist(err) {
		return err
	}
	return nil
}
