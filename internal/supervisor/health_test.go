package supervisor

import (
	"context"
	"testing"
	"time"
)

func TestHealthMonitorOK(t *testing.T) {
	hm := NewHealthMonitor("agent-1", 2*time.Second, 5*time.Second)

	if hm.Status() != HealthOK {
		t.Fatal("expected HealthOK initially")
	}
}

func TestHealthMonitorUnresponsive(t *testing.T) {
	hm := NewHealthMonitor("agent-1", 50*time.Millisecond, 5*time.Second)

	// Wait for heartbeat timeout
	time.Sleep(100 * time.Millisecond)

	if hm.Status() != HealthUnresponsive {
		t.Fatalf("expected HealthUnresponsive, got %d", hm.Status())
	}
}

func TestHealthMonitorHeartbeatKeepsAlive(t *testing.T) {
	hm := NewHealthMonitor("agent-1", 100*time.Millisecond, 5*time.Second)

	// Keep sending heartbeats
	for i := 0; i < 5; i++ {
		time.Sleep(50 * time.Millisecond)
		hm.RecordHeartbeat()
	}

	if hm.Status() != HealthOK {
		t.Fatal("expected HealthOK with regular heartbeats")
	}
}

func TestHealthMonitorStalled(t *testing.T) {
	hm := NewHealthMonitor("agent-1", 5*time.Second, 50*time.Millisecond)

	// Heartbeat but no progress
	hm.RecordHeartbeat()
	time.Sleep(100 * time.Millisecond)

	if hm.Status() != HealthStalled {
		t.Fatalf("expected HealthStalled, got %d", hm.Status())
	}
}

func TestHealthMonitorProgressResetsStall(t *testing.T) {
	hm := NewHealthMonitor("agent-1", 5*time.Second, 100*time.Millisecond)

	time.Sleep(50 * time.Millisecond)
	hm.RecordProgress()

	if hm.Status() != HealthOK {
		t.Fatal("progress should reset stall timer")
	}
}

func TestHealthMonitorCallback(t *testing.T) {
	hm := NewHealthMonitor("agent-1", 50*time.Millisecond, 5*time.Second)

	callbackCalled := make(chan HealthStatus, 1)
	hm.OnTimeout(func(agentID string, status HealthStatus) {
		callbackCalled <- status
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hm.Start(ctx)

	select {
	case status := <-callbackCalled:
		if status != HealthUnresponsive {
			t.Fatalf("expected HealthUnresponsive callback, got %d", status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for callback")
	}
}

func TestHealthMonitorStop(t *testing.T) {
	hm := NewHealthMonitor("agent-1", 5*time.Second, 5*time.Second)

	ctx := context.Background()
	hm.Start(ctx)
	hm.Stop()
	// Should not panic or hang
}
