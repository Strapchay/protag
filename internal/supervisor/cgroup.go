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
	// AgentID is used as the cgroup directory name.
	AgentID string
	// MemoryMaxBytes is the memory limit in bytes.
	MemoryMaxBytes int64
	// PidsMax is the maximum number of processes.
	PidsMax int64
}

const cgroupBasePath = "/sys/fs/cgroup/aion"

// CreateCgroup creates a cgroup v2 directory with resource limits.
func CreateCgroup(config CgroupConfig) error {
	if !config.Enabled {
		return nil
	}

	if !IsCgroupAvailable() {
		log.Printf("cgroup: cgroupv2 not available, skipping resource limits for agent %s", config.AgentID)
		return nil
	}

	cgroupPath := filepath.Join(cgroupBasePath, config.AgentID)

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
func AssignProcess(agentID string, pid int) error {
	if !IsCgroupAvailable() {
		return nil
	}

	procsFile := filepath.Join(cgroupBasePath, agentID, "cgroup.procs")
	return os.WriteFile(procsFile, []byte(strconv.Itoa(pid)), 0644)
}

// DestroyCgroup removes the agent's cgroup directory.
func DestroyCgroup(agentID string) error {
	if !IsCgroupAvailable() {
		return nil
	}

	cgroupPath := filepath.Join(cgroupBasePath, agentID)
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
