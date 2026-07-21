package supervisor

import (
	"context"
	"errors"
	"testing"
	"time"
)

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
