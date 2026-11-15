//go:build scenario_live

package scenario

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestLiveBuildSmoke is the opt-in real-model smoke test. It is intentionally
// excluded from deterministic CI because it spends tokens and depends on the
// configured provider.
func TestLiveBuildSmoke(t *testing.T) {
	if os.Getenv("AION_LIVE_SCENARIO") != "1" {
		t.Skip("set AION_LIVE_SCENARIO=1 to run live model scenarios")
	}
	h := NewProcessHarness(t)
	t.Cleanup(h.Stop)
	h.Start()
	spec := "# Live Smoke\n\nPython 3.10+, standard library only. One domain `alpha` owning `alpha/`. Implement `alpha/__init__.py` exporting `answer()` returning 42, `alpha/tests/test_answer.py`, and `alpha/README.md`. No other domains or shared files.\n"
	if err := os.WriteFile(filepath.Join(h.Root, "docs", "build_spec.md"), []byte(spec), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Request("send-message", map[string]any{"agent_id": "orchestrator", "text": "/build-spec"}); err != nil {
		t.Fatalf("trigger build: %v", err)
	}
	waitForBuildStatus(t, h, func(status map[string]any) bool {
		if status["status"] == "failed" {
			t.Fatalf("live build failed: %v", status)
		}
		return status["status"] == "completed"
	}, 15*time.Minute)
}

func TestLiveProcessRestart(t *testing.T) {
	if os.Getenv("AION_LIVE_SCENARIO") != "1" {
		t.Skip("set AION_LIVE_SCENARIO=1 to run live model scenarios")
	}
	h := NewProcessHarness(t)
	t.Cleanup(h.Stop)
	h.Start()
	spec := "# Live Restart\n\nPython 3.10+, standard library only. One domain `alpha` owning `alpha/`. Implement `alpha/__init__.py` exporting `answer()` returning 42, `alpha/tests/test_answer.py`, and `alpha/README.md`. No other domains or shared files.\n"
	if err := os.WriteFile(filepath.Join(h.Root, "docs", "build_spec.md"), []byte(spec), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Request("send-message", map[string]any{"agent_id": "orchestrator", "text": "/build-spec"}); err != nil {
		t.Fatalf("trigger build: %v", err)
	}
	waitForBuildStatus(t, h, func(status map[string]any) bool {
		return status["status"] == "active"
	}, 5*time.Minute)
	h.Stop()
	h.Start()
	if _, err := h.Request("resume-preflight", map[string]any{}); err != nil {
		t.Fatalf("restart preflight: %v", err)
	}
	if _, err := h.Request("build-spec-continue-agents", map[string]any{"confirm": true}); err != nil {
		t.Fatalf("restart confirm: %v", err)
	}
	waitForBuildStatus(t, h, func(status map[string]any) bool {
		return status["status"] == "completed"
	}, 10*time.Minute)
}

func waitForDebugBuildStatus(t *testing.T, h *ProcessHarness, predicate func(map[string]any) bool, timeout time.Duration) map[string]any {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		raw, err := h.Request("debug-status", map[string]any{})
		if err == nil {
			var outer map[string]any
			var inner map[string]any
			if json.Unmarshal(raw, &outer) == nil {
				switch value := outer["build_spec_status"].(type) {
				case string:
					_ = json.Unmarshal([]byte(value), &inner)
				case map[string]any:
					inner = value
				}
				if inner != nil && predicate(inner) {
					return inner
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatal("build status condition not reached")
	return nil
}

func waitForBuildStatus(t *testing.T, h *ProcessHarness, predicate func(map[string]any) bool, timeout time.Duration) map[string]any {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last map[string]any
	var lastErr error
	for time.Now().Before(deadline) {
		raw, err := h.Request("build-spec-status", map[string]any{})
		if err == nil {
			var wrapper struct {
				Status string `json:"status"`
			}
			var status map[string]any
			if json.Unmarshal(raw, &wrapper) == nil && json.Unmarshal([]byte(wrapper.Status), &status) == nil {
				last = status
				if predicate(status) {
					return status
				}
			}
		} else {
			lastErr = err
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("build status condition not reached; last=%v err=%v log=%s", last, lastErr, h.LogPath)
	return nil
}

func findAgentPID(agentID string) int {
	for _, entry := range mustGlob("/proc/[0-9]*") {
		pid := strings.TrimPrefix(entry, "/proc/")
		data, err := os.ReadFile(filepath.Join(entry, "environ"))
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\x00") {
			if line == "AION_AGENT_ID="+agentID {
				var value int
				_, _ = fmt.Sscanf(pid, "%d", &value)
				return value
			}
		}
	}
	return 0
}

func mustGlob(pattern string) []string {
	matches, _ := filepath.Glob(pattern)
	return matches
}

func TestLiveAgentCrash(t *testing.T) {
	if os.Getenv("AION_LIVE_SCENARIO") != "1" {
		t.Skip("set AION_LIVE_SCENARIO=1 to run live model scenarios")
	}
	h := NewProcessHarness(t)
	t.Cleanup(h.Stop)
	h.Start()
	spec := "# Live Agent Crash\n\nPython 3.10+, standard library only. One domain `alpha` owning `alpha/`. Implement `alpha/__init__.py` exporting `answer()` returning 42, `alpha/tests/test_answer.py`, and `alpha/README.md`. No other domains or shared files.\n"
	if err := os.WriteFile(filepath.Join(h.Root, "docs", "build_spec.md"), []byte(spec), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Request("send-message", map[string]any{"agent_id": "orchestrator", "text": "/build-spec"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		if pid := findAgentPID("agent-alpha"); pid != 0 {
			_ = syscall.Kill(pid, syscall.SIGKILL)
			break
		}
		time.Sleep(1 * time.Second)
	}
	waitForBuildStatus(t, h, func(status map[string]any) bool {
		return status["status"] == "completed" || status["status"] == "failed"
	}, 10*time.Minute)
}

func TestLiveRestartDuringAcceptance(t *testing.T) {
	if os.Getenv("AION_LIVE_SCENARIO") != "1" {
		t.Skip("set AION_LIVE_SCENARIO=1 to run live model scenarios")
	}
	h := NewProcessHarness(t)
	t.Cleanup(h.Stop)
	h.Start()
	spec := "# Live Acceptance Restart\n\nPython 3.10+, standard library only. One domain `alpha` owning `alpha/`. Implement `alpha/__init__.py` exporting `answer()` returning 42, `alpha/tests/test_answer.py`, and `alpha/README.md`. No other domains or shared files.\n"
	if err := os.WriteFile(filepath.Join(h.Root, "docs", "build_spec.md"), []byte(spec), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Request("send-message", map[string]any{"agent_id": "orchestrator", "text": "/build-spec"}); err != nil {
		t.Fatal(err)
	}
	waitForDebugBuildStatus(t, h, func(status map[string]any) bool {
		return status["acceptance_status"] == "running"
	}, 10*time.Minute)
	h.Stop()
	h.Start()
	if _, err := h.Request("resume-preflight", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Request("build-spec-continue-agents", map[string]any{"confirm": true}); err != nil {
		t.Fatal(err)
	}
}

func TestLiveArchitectBuild(t *testing.T) {
	if os.Getenv("AION_LIVE_SCENARIO") != "1" {
		t.Skip("set AION_LIVE_SCENARIO=1 to run live model scenarios")
	}
	h := NewProcessHarness(t)
	t.Cleanup(h.Stop)
	h.Start()
	prompt := "Create and finalize docs/build_spec.md for a compact Python 3.10+ project named Architect Live. Use exactly one domain alpha owning alpha/. Implement alpha/__init__.py exporting answer() returning 42, alpha/tests/test_answer.py, and alpha/README.md. All decisions are fixed; do not ask questions. Write the finalized spec file directly."
	if _, err := h.Request("send-message", map[string]any{"agent_id": "orchestrator", "text": prompt}); err != nil {
		t.Fatalf("architect prompt: %v", err)
	}
	specPath := filepath.Join(h.Root, "docs", "build_spec.md")
	deadline := time.Now().Add(8 * time.Minute)
	for time.Now().Before(deadline) {
		if info, err := os.Stat(specPath); err == nil && info.Size() > 500 {
			break
		}
		time.Sleep(3 * time.Second)
	}
	if info, err := os.Stat(specPath); err != nil || info.Size() <= 500 {
		t.Fatalf("architect or fallback did not write the spec; log=%s", h.LogPath)
	}
	if _, err := h.Request("send-message", map[string]any{"agent_id": "orchestrator", "text": "/build-spec"}); err != nil {
		t.Fatalf("trigger build: %v", err)
	}
	waitForBuildStatus(t, h, func(status map[string]any) bool {
		return status["status"] == "completed"
	}, 10*time.Minute)
}
