//go:build scenario_process

package scenario

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// ProcessHarness runs the real kernel binary in an isolated project. It is
// intentionally opt-in so normal scenario runs stay fast and model-free.
type ProcessHarness struct {
	t       *testing.T
	Root    string
	Binary  string
	Config  string
	LogPath string
	cmd     *exec.Cmd
	done    chan error
	logFile *os.File
}

func NewProcessHarness(t *testing.T) *ProcessHarness {
	t.Helper()
	binary := os.Getenv("AION_KERNEL_BIN")
	if binary == "" {
		binary = filepath.Join("..", "bin", "aion-kernel")
	}
	config := os.Getenv("AION_KERNEL_CONFIG")
	if config == "" {
		config = filepath.Join("..", "configs", "aion.yaml")
	}
	if _, err := os.Stat(binary); err != nil {
		t.Skipf("kernel binary not available: %v", err)
	}
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "docs"), 0o755); err != nil {
		t.Fatal(err)
	}
	return &ProcessHarness{t: t, Root: root, Binary: binary, Config: config, LogPath: filepath.Join(t.TempDir(), "kernel.log")}
}

func (h *ProcessHarness) Start() {
	h.t.Helper()
	logFile, err := os.Create(h.LogPath)
	if err != nil {
		h.t.Fatal(err)
	}
	h.logFile = logFile
	h.cmd = exec.Command(h.Binary, "server", "--workdir", h.Root, "--config", h.Config, "--log-level", "debug")
	h.cmd.Stdout = logFile
	h.cmd.Stderr = logFile
	h.cmd.Env = append(os.Environ(),
		"AION_ORCHESTRATOR_PORT=0",
		"AION_INFERENCE_GATEWAY_ENABLED=false",
	)
	h.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := h.cmd.Start(); err != nil {
		h.t.Fatal(err)
	}
	h.done = make(chan error, 1)
	go func() { h.done <- h.cmd.Wait() }()
	serverInfo := filepath.Join(h.Root, ".aion", "server.json")
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(serverInfo); err == nil {
			var info struct {
				Addr string `json:"addr"`
			}
			if json.Unmarshal(data, &info) == nil && info.Addr != "" {
				return
			}
		}
		select {
		case err := <-h.done:
			h.t.Fatalf("kernel exited before readiness: %v (log: %s)", err, h.LogPath)
		case <-time.After(100 * time.Millisecond):
		}
	}
	h.Stop()
	h.t.Fatalf("kernel did not become ready; log: %s", h.LogPath)
}

func (h *ProcessHarness) Stop() {
	if h == nil || h.cmd == nil || h.cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-h.cmd.Process.Pid, syscall.SIGTERM)
	select {
	case <-h.done:
	case <-time.After(10 * time.Second):
		_ = syscall.Kill(-h.cmd.Process.Pid, syscall.SIGKILL)
		<-h.done
	}
	h.cmd = nil
	if h.logFile != nil {
		_ = h.logFile.Close()
		h.logFile = nil
	}
}

func TestProcessScenarioSmoke(t *testing.T) {
	if os.Getenv("AION_PROCESS_SCENARIO") != "1" {
		t.Skip("set AION_PROCESS_SCENARIO=1 to run process-level scenarios")
	}
	h := NewProcessHarness(t)
	t.Cleanup(h.Stop)
	h.Start()
	if _, err := os.Stat(filepath.Join(h.Root, ".aion", "server.json")); err != nil {
		t.Fatal(err)
	}
	fmt.Println("process scenario server ready at", h.Root)
}
