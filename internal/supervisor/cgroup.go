package supervisor

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

func (c CgroupConfig) rootPath() string {
	if strings.TrimSpace(c.BasePath) != "" {
		return c.BasePath
	}
	return defaultCgroupBasePath
}

// CreateCgroup creates a cgroup v2 directory with resource limits.
func CreateCgroup(config CgroupConfig) error {
	if !config.Enabled {
		return nil
	}

	if !IsCgroupAvailable() {
		log.Printf("cgroup: cgroupv2 not available, skipping resource limits for agent %s", config.AgentID)
		return nil
	}

	cgroupPath := filepath.Join(config.rootPath(), config.AgentID)

	if err := os.MkdirAll(cgroupPath, 0755); err != nil {
		return fmt.Errorf("cgroup: create directory: %w", err)
	}

	// Set memory limit
	if config.MemoryMaxBytes > 0 {
		memFile := filepath.Join(cgroupPath, "memory.max")
		if err := os.WriteFile(memFile, []byte(strconv.FormatInt(config.MemoryMaxBytes, 10)), 0644); err != nil {
			log.Printf("cgroup: failed to set memory.max: %v", err)
		}
	}

	// Set PID limit
	if config.PidsMax > 0 {
		pidFile := filepath.Join(cgroupPath, "pids.max")
		if err := os.WriteFile(pidFile, []byte(strconv.FormatInt(config.PidsMax, 10)), 0644); err != nil {
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
	procsFile := filepath.Join(basePath, agentID, "cgroup.procs")
	return os.WriteFile(procsFile, []byte(strconv.Itoa(pid)), 0644)
}

// DestroyCgroup removes the agent's cgroup directory.
func DestroyCgroup(basePath, agentID string) error {
	if !IsCgroupAvailable() {
		return nil
	}

	if strings.TrimSpace(basePath) == "" {
		basePath = defaultCgroupBasePath
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
