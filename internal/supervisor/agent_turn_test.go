package supervisor

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestMessageEndSurfacesNestedModelError(t *testing.T) {
	supervisor := &AgentSupervisor{config: AgentConfig{AgentID: "agent-test", DomainID: "test"}}
	events := make(chan AgentLifecycleEvent, 1)
	supervisor.lifecycleFn = func(event AgentLifecycleEvent) {
		if event.Kind == "model_error" {
			events <- event
		}
	}
	supervisor.handleEvent(PiAgentEvent{
		Type:    "message_end",
		Message: json.RawMessage(`{"role":"assistant","content":[],"stopReason":"error","errorMessage":"ResourceExhausted: request limit"}`),
	})
	select {
	case event := <-events:
		if !event.IsError || !strings.Contains(event.Error, "ResourceExhausted") {
			t.Fatalf("event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("model error lifecycle event was not emitted")
	}
}

func TestWaitForTurnIdleObservesTurnCompletion(t *testing.T) {
	supervisor := &AgentSupervisor{isThinking: true}
	go func() {
		time.Sleep(20 * time.Millisecond)
		supervisor.mu.Lock()
		supervisor.isThinking = false
		supervisor.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := supervisor.WaitForTurnIdle(ctx); err != nil {
		t.Fatalf("wait for idle: %v", err)
	}
}

func TestWaitForTurnIdleHonorsCancellation(t *testing.T) {
	supervisor := &AgentSupervisor{isThinking: true}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := supervisor.WaitForTurnIdle(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait error = %v, want deadline exceeded", err)
	}
}

func TestTurnIdleReportsAgentLoopState(t *testing.T) {
	supervisor := &AgentSupervisor{isThinking: true}
	if supervisor.TurnIdle() {
		t.Fatal("active turn reported idle")
	}
	supervisor.mu.Lock()
	supervisor.isThinking = false
	supervisor.mu.Unlock()
	if !supervisor.TurnIdle() {
		t.Fatal("completed turn did not report idle")
	}
}

func TestAutoRetryKeepsTurnActiveUntilFinalAgentEnd(t *testing.T) {
	supervisor := &AgentSupervisor{
		config: AgentConfig{AgentID: "agent-test", DomainID: "test"},
	}

	supervisor.handleEvent(PiAgentEvent{
		Type:         "auto_retry_start",
		Attempt:      1,
		MaxAttempts:  3,
		ErrorMessage: "temporary upstream failure",
	})
	if supervisor.TurnIdle() {
		t.Fatal("Pi auto-retry was reported as idle")
	}
	if !supervisor.TurnRetryPending() {
		t.Fatal("Pi auto-retry was not exposed to the allocator")
	}

	supervisor.handleEvent(PiAgentEvent{Type: "agent_end"})
	if !supervisor.TurnIdle() {
		t.Fatal("final agent_end did not close the auto-retry loop")
	}
	if supervisor.TurnRetryPending() {
		t.Fatal("final agent_end left auto-retry pending")
	}
}

func TestTurnEndDoesNotMarkAgentLoopIdle(t *testing.T) {
	supervisor := &AgentSupervisor{
		config:     AgentConfig{AgentID: "agent-test", DomainID: "test"},
		isThinking: true,
	}

	supervisor.handleEvent(PiAgentEvent{Type: "turn_end"})
	supervisor.mu.Lock()
	activeAfterTurn := supervisor.isThinking
	supervisor.mu.Unlock()
	if !activeAfterTurn {
		t.Fatal("turn_end marked an active Pi agent loop idle")
	}

	supervisor.handleEvent(PiAgentEvent{Type: "agent_end"})
	supervisor.mu.Lock()
	activeAfterAgent := supervisor.isThinking
	supervisor.mu.Unlock()
	if activeAfterAgent {
		t.Fatal("agent_end did not mark the Pi agent loop idle")
	}
}

func TestSendPromptMarksTurnActiveBeforeEvent(t *testing.T) {
	supervisor := &AgentSupervisor{}
	if err := supervisor.SendPrompt("work"); err == nil {
		t.Fatal("expected missing-agent error")
	}
	supervisor.mu.Lock()
	active := supervisor.isThinking
	supervisor.mu.Unlock()
	if active {
		t.Fatal("failed prompt left turn marked active")
	}
}
