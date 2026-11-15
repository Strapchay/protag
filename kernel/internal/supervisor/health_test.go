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

func TestHealthMonitorPausesDeadlinesBetweenTurns(t *testing.T) {
	hm := NewHealthMonitor("agent-idle", 50*time.Millisecond, 50*time.Millisecond)
	hm.SetMonitoring(false)
	time.Sleep(100 * time.Millisecond)
	if got := hm.Status(); got != HealthOK {
		t.Fatalf("paused health status = %d, want HealthOK", got)
	}

	hm.SetMonitoring(true)
	if got := hm.Status(); got != HealthOK {
		t.Fatalf("resumed health status = %d, want HealthOK", got)
	}
	time.Sleep(100 * time.Millisecond)
	if got := hm.Status(); got != HealthUnresponsive {
		t.Fatalf("active health status = %d, want HealthUnresponsive", got)
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

func TestHealthMonitorExternalActivitySuppressesProgressStall(t *testing.T) {
	hm := NewHealthMonitor("agent-1", 5*time.Second, 50*time.Millisecond)
	hm.SetExternalActivityTimeouts(200*time.Millisecond, time.Second)

	time.Sleep(80 * time.Millisecond)
	hm.RecordExternalActivity("forwarding")

	if hm.Status() != HealthOK {
		t.Fatalf("expected active external request to suppress progress stall, got %d", hm.Status())
	}
}

func TestHealthMonitorExternalActivityCanGoStale(t *testing.T) {
	hm := NewHealthMonitor("agent-1", 5*time.Second, 50*time.Millisecond)
	hm.SetExternalActivityTimeouts(40*time.Millisecond, time.Second)

	hm.RecordExternalActivity("active")
	time.Sleep(80 * time.Millisecond)

	if hm.Status() != HealthStalled {
		t.Fatalf("expected stale external request to allow progress stall, got %d", hm.Status())
	}
}

func TestHealthMonitorExternalActivityMaxDuration(t *testing.T) {
	hm := NewHealthMonitor("agent-1", 5*time.Second, 50*time.Millisecond)
	hm.SetExternalActivityTimeouts(time.Second, 70*time.Millisecond)

	hm.RecordExternalActivity("active")
	time.Sleep(40 * time.Millisecond)
	hm.RecordExternalActivity("active")
	time.Sleep(40 * time.Millisecond)

	if hm.Status() != HealthStalled {
		t.Fatalf("expected overlong external request to allow progress stall, got %d", hm.Status())
	}
}

func TestHealthMonitorExternalActivityCompletedClearsShield(t *testing.T) {
	hm := NewHealthMonitor("agent-1", 5*time.Second, 50*time.Millisecond)
	hm.SetExternalActivityTimeouts(time.Second, time.Second)

	hm.RecordExternalActivity("active")
	hm.RecordExternalActivity("completed")
	time.Sleep(80 * time.Millisecond)

	if hm.Status() != HealthStalled {
		t.Fatalf("expected completed external request to clear progress shield, got %d", hm.Status())
	}
}

func TestHealthMonitorRetryWaitCountsAsExternalActivity(t *testing.T) {
	hm := NewHealthMonitor("agent-retry", time.Second, time.Millisecond)
	hm.SetExternalActivityTimeouts(time.Second, time.Second)
	time.Sleep(2 * time.Millisecond)
	hm.RecordExternalActivity("retry_wait")
	if got := hm.Status(); got != HealthOK {
		t.Fatalf("retry_wait status = %d, want HealthOK", got)
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
