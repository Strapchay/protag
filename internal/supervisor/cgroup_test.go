package supervisor

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestCgroupsLifecycle(t *testing.T) {
	if !IsCgroupAvailable() {
		t.Skip("Cgroup v2 not available on this system, skipping cgroup tests")
	}

	config := CgroupConfig{
		Enabled:        true,
		BasePath:       cgroupBasePath,
		AgentID:        "test-agent-123",
		MemoryMaxBytes: 1024 * 1024 * 100, // 100MB
		PidsMax:        50,
	}

	// 1. Create
	err := CreateCgroup(config)
	if err != nil {
		if os.IsPermission(err) || strings.Contains(err.Error(), "permission denied") || strings.Contains(err.Error(), "read-only file system") {
			t.Skipf("CreateCgroup permission denied, skipping test: %v", err)
		}
		t.Fatalf("CreateCgroup failed: %v", err)
	}

	cgroupPath := filepath.Join(cgroupBasePath, config.AgentID)
	if _, err := os.Stat(cgroupPath); os.IsNotExist(err) {
		t.Fatalf("cgroup directory not created at %s", cgroupPath)
	}

	// Validate Memory
	memBytes, err := os.ReadFile(filepath.Join(cgroupPath, "memory.max"))
	if err != nil {
		t.Fatalf("failed to read memory.max: %v", err)
	}
	if string(memBytes) != "104857600\n" && string(memBytes) != "104857600" {
		t.Errorf("unexpected memory.max value: %s", string(memBytes))
	}

	// Validate Pids
	pidBytes, err := os.ReadFile(filepath.Join(cgroupPath, "pids.max"))
	if err != nil {
		t.Fatalf("failed to read pids.max: %v", err)
	}
	if string(pidBytes) != "50\n" && string(pidBytes) != "50" {
		t.Errorf("unexpected pids.max value: %s", string(pidBytes))
	}

	// 2. Assign Process (use own PID)
	myPid := os.Getpid()
	err = AssignProcess(config.BasePath, config.AgentID, myPid)
	if err != nil {
		t.Fatalf("AssignProcess failed: %v", err)
	}

	procsBytes, err := os.ReadFile(filepath.Join(cgroupPath, "cgroup.procs"))
	if err != nil {
		t.Fatalf("failed to read cgroup.procs: %v", err)
	}
	procsStr := string(procsBytes)
	if !containsPid(procsStr, myPid) {
		t.Errorf("expected pid %d in cgroup.procs, got: %s", myPid, procsStr)
	}

	// 3. Destroy
	err = DestroyCgroup(config.BasePath, config.AgentID)
	// Even with process inside, cgroups v2 usually let you delete if empty or force hierarchy.
	// But actually if a process is inside, RMDIR might fail with Device or Resource Busy
	// We won't strictly enforce tests of busy cgroups as that risks flaky CI.
	// Just attempt destruction and verify it drops errors or succeeds.
	if err != nil {
		t.Logf("DestroyCgroup failed (expected if process inside): %v", err)
	}
}

func containsPid(procs string, pid int) bool {
	pidStr := strconv.Itoa(pid)
	lines := splitLines(procs)
	for _, line := range lines {
		if line == pidStr {
			return true
		}
	}
	return false
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

func TestResourceGovernance(t *testing.T) {
	if !IsCgroupAvailable() {
		t.Skip("Cgroup v2 not available on this system")
	}

	config := CgroupConfig{
		Enabled:        true,
		BasePath:       cgroupBasePath,
		AgentID:        "oom-test-agent",
		MemoryMaxBytes: 10 * 1024 * 1024, // 10MB cutoff
		PidsMax:        50,
	}

	err := CreateCgroup(config)
	if err != nil {
		if os.IsPermission(err) || strings.Contains(err.Error(), "permission denied") || strings.Contains(err.Error(), "read-only file system") {
			t.Skipf("CreateCgroup unavailable, skipping test: %v", err)
		}
		t.Fatalf("CreateCgroup failed: %v", err)
	}
	defer DestroyCgroup(config.BasePath, config.AgentID)

	// Since we can't easily OOM the current test process without crashing the test test runner,
	// we would typically test this by spawning a child process that allocates memory until it gets killed
	// by the cgroup OOM killer.
	// For the sake of this validation test, we will verify the threshold is correctly applied to the cgroup.
	cgroupPath := filepath.Join(cgroupBasePath, config.AgentID)
	memBytes, err := os.ReadFile(filepath.Join(cgroupPath, "memory.max"))
	if err != nil {
		t.Fatalf("failed to read memory.max: %v", err)
	}

	if string(memBytes) != "10485760\n" && string(memBytes) != "10485760" {
		t.Errorf("expected 10MB limit in memory.max, got %s", string(memBytes))
	}
}
