package supervisor

import (
	"encoding/json"
	"testing"
)

func TestSupervisorResilienceLooping(t *testing.T) {
	config := AgentConfig{
		AgentID: "test-agent",
	}
	s := NewAgentSupervisor(config)

	toolData, _ := json.Marshal(map[string]string{
		"name":  "run_command",
		"input": "go test ./...",
	})

	loopingEvent := PiAgentEvent{
		Type: "tool_execution_start",
		Data: toolData,
	}

	// Send 6 identical events - should accumulating in window
	for i := 0; i < 6; i++ {
		s.handleEvent(loopingEvent)
		if len(s.recentTools) != i+1 {
			t.Fatalf("expected recentTools length %d, got %d", i+1, len(s.recentTools))
		}
	}

	// The 7th identical event triggers a steer and resets the sliding window
	s.handleEvent(loopingEvent)
	if len(s.recentTools) != 0 {
		t.Fatalf("expected recentTools to reset to 0 after 7 loops, got %d", len(s.recentTools))
	}
}

func TestSupervisorResilienceNetworkFault(t *testing.T) {
	config := AgentConfig{
		AgentID: "test-agent",
	}
	s := NewAgentSupervisor(config)

	netEvent := PiAgentEvent{
		Type:    "tool_error",
		Message: "dial tcp 10.0.0.1:443: connect: ECONNRESET",
	}

	// Send the fault event. Without an active PiAgentProcess, this is just a
	// state-check to ensure it logs and handles the branch without panicking.
	s.handleEvent(netEvent)

	// In a real environment, the backoff goroutine would fire.
	// Since piAgent is nil, it skips SendSteer safely.
}

func TestSupervisorLoopBreakerFileModified(t *testing.T) {
	config := AgentConfig{
		AgentID: "test-agent",
	}
	s := NewAgentSupervisor(config)

	toolData, _ := json.Marshal(map[string]string{
		"name": "run_command",
	})

	s.handleEvent(PiAgentEvent{Type: "tool_execution_start", Data: toolData})
	s.handleEvent(PiAgentEvent{Type: "tool_execution_start", Data: toolData})

	if len(s.recentTools) != 2 {
		t.Fatal("expected 2 tools")
	}

	// File modification resets the loop heuristic safely
	s.handleEvent(PiAgentEvent{Type: "file_modified", Message: "saved auth.go"})

	if len(s.recentTools) != 0 {
		t.Fatalf("expected clear after file_modified, got %d", len(s.recentTools))
	}
}
