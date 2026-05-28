package supervisor

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// CgroupConfig configures cgroup resource limits for an agent.
type CgroupConfig struct {
	// Enabled controls whether cgroups are applied.
	Enabled bool
	// Mode describes how the cgroup subtree is provisioned.
	Mode string
	// BasePath is the root cgroup path for Aion-managed agents.
	BasePath string
	// AgentID is used as the cgroup directory name.
	AgentID string
	// MemoryMaxBytes is the memory limit in bytes.
	MemoryMaxBytes int64
	// PidsMax is the maximum number of processes.
	PidsMax int64
}

const defaultCgroupBasePath = "/sys/fs/cgroup/aion"
const cgroupBasePath = defaultCgroupBasePath

var (
	cgroupSkipMu    sync.Mutex
	skippedCgroups  = make(map[string]string)
	reportedCgroups = make(map[string]bool)
)

func (c CgroupConfig) rootPath() string {
	if strings.TrimSpace(c.BasePath) != "" {
		return c.BasePath
	}
	return defaultCgroupBasePath
}

func (c CgroupConfig) mode() string {
	mode := strings.ToLower(strings.TrimSpace(c.Mode))
	if mode == "" {
		return "direct"
	}
	return mode
}

func (c CgroupConfig) Strict() bool {
	return c.Enabled && c.mode() == "systemd"
}

// CreateCgroup creates a cgroup v2 directory with resource limits.
func CreateCgroup(config CgroupConfig) error {
	if !config.Enabled || config.mode() == "disabled" {
		return nil
	}

	if !IsCgroupAvailable() {
		err := fmt.Errorf("cgroup: cgroupv2 not available")
		if config.Strict() {
			return err
		}
		markCgroupSkipped(config.rootPath(), err.Error())
		return nil
	}

	rootPath := config.rootPath()
	if isCgroupSkipped(rootPath) {
		return nil
	}
	if err := os.MkdirAll(rootPath, 0755); err != nil {
		err = fmt.Errorf("cgroup: root not writable: %w", err)
		if config.Strict() {
			return err
		}
		markCgroupSkipped(rootPath, err.Error())
		return nil
	}

	cgroupPath := filepath.Join(rootPath, config.AgentID)

	if err := os.MkdirAll(cgroupPath, 0755); err != nil {
		err = fmt.Errorf("cgroup: agent cgroup not writable: %w", err)
		if config.Strict() {
			return err
		}
		markCgroupSkipped(rootPath, err.Error())
		return nil
	}

	// Set memory limit
	if config.MemoryMaxBytes > 0 {
		memFile := filepath.Join(cgroupPath, "memory.max")
		if err := os.WriteFile(memFile, []byte(strconv.FormatInt(config.MemoryMaxBytes, 10)), 0644); err != nil {
			if config.Strict() {
				return fmt.Errorf("cgroup: set memory.max: %w", err)
			}
			log.Printf("cgroup: failed to set memory.max: %v", err)
		}
	}

	// Set PID limit
	if config.PidsMax > 0 {
		pidFile := filepath.Join(cgroupPath, "pids.max")
		if err := os.WriteFile(pidFile, []byte(strconv.FormatInt(config.PidsMax, 10)), 0644); err != nil {
			if config.Strict() {
				return fmt.Errorf("cgroup: set pids.max: %w", err)
			}
			log.Printf("cgroup: failed to set pids.max: %v", err)
		}
	}

	return nil
}

// AssignProcess assigns a process to the agent's cgroup.
func AssignProcess(basePath, agentID string, pid int) error {
	if !IsCgroupAvailable() {
		return nil
	}

	if strings.TrimSpace(basePath) == "" {
		basePath = defaultCgroupBasePath
	}
	if isCgroupSkipped(basePath) {
		return nil
	}
	procsFile := filepath.Join(basePath, agentID, "cgroup.procs")
	return os.WriteFile(procsFile, []byte(strconv.Itoa(pid)), 0644)
}

func AssignProcessWithConfig(config CgroupConfig, pid int) error {
	if !config.Enabled || config.mode() == "disabled" {
		return nil
	}
	if !IsCgroupAvailable() {
		if config.Strict() {
			return fmt.Errorf("cgroup: cgroupv2 not available")
		}
		return nil
	}
	err := AssignProcess(config.rootPath(), config.AgentID, pid)
	if err != nil && config.Strict() {
		return err
	}
	return nil
}

// DestroyCgroup removes the agent's cgroup directory.
func DestroyCgroup(basePath, agentID string) error {
	if !IsCgroupAvailable() {
		return nil
	}

	if strings.TrimSpace(basePath) == "" {
		basePath = defaultCgroupBasePath
	}
	if isCgroupSkipped(basePath) {
		return nil
	}
	cgroupPath := filepath.Join(basePath, agentID)
	return os.RemoveAll(cgroupPath)
}

// IsCgroupAvailable checks whether cgroupv2 is mounted.
func IsCgroupAvailable() bool {
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return false
	}
	return strings.Contains(string(data), "cgroup2")
}

func markCgroupSkipped(rootPath, reason string) {
	rootPath = strings.TrimSpace(rootPath)
	if rootPath == "" {
		rootPath = defaultCgroupBasePath
	}
	cgroupSkipMu.Lock()
	skippedCgroups[rootPath] = reason
	if !reportedCgroups[rootPath] {
		reportedCgroups[rootPath] = true
		log.Printf("cgroup: skipping resource limits for root %s: %s", rootPath, reason)
	}
	cgroupSkipMu.Unlock()
}

func isCgroupSkipped(rootPath string) bool {
	rootPath = strings.TrimSpace(rootPath)
	if rootPath == "" {
		rootPath = defaultCgroupBasePath
	}
	cgroupSkipMu.Lock()
	_, skipped := skippedCgroups[rootPath]
	cgroupSkipMu.Unlock()
	return skipped
}
