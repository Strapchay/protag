//go:build scenario_live

package scenario

import (
	"encoding/json"
	"os"
	"path/filepath"
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
	deadline := time.Now().Add(15 * time.Minute)
	for time.Now().Before(deadline) {
		raw, err := h.Request("build-spec-status", map[string]any{})
		if err == nil {
			var status struct {
				Status string `json:"status"`
			}
			if json.Unmarshal(raw, &status) == nil && status.Status == "completed" {
				return
			}
		}
		time.Sleep(5 * time.Second)
	}
	t.Fatal("live build did not complete within 15 minutes")
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
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		raw, err := h.Request("build-spec-status", map[string]any{})
		if err == nil {
			var status struct {
				Status string `json:"status"`
			}
			if json.Unmarshal(raw, &status) == nil && status.Status == "active" {
				h.Stop()
				h.Start()
				if _, err := h.Request("resume-preflight", map[string]any{}); err != nil {
					t.Fatalf("restart preflight: %v", err)
				}
				if _, err := h.Request("build-spec-continue-agents", map[string]any{"confirm": true}); err != nil {
					t.Fatalf("restart confirm: %v", err)
				}
				return
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatal("build did not become active before restart")
}
