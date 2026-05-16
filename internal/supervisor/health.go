package supervisor

import (
	"context"
	"log"
	"sync"
	"time"
)

// HealthStatus represents the health state of an agent.
type HealthStatus int

const (
	HealthOK HealthStatus = iota
	HealthUnresponsive
	HealthStalled
)

// HealthMonitor tracks agent liveness via heartbeats and progress.
type HealthMonitor struct {
	mu               sync.Mutex
	agentID          string
	lastHeartbeat    time.Time
	lastProgress     time.Time
	heartbeatTimeout time.Duration
	progressTimeout  time.Duration
	onTimeout        func(agentID string, status HealthStatus)
	cancel           context.CancelFunc
}

// NewHealthMonitor creates a health monitor for an agent.
func NewHealthMonitor(agentID string, heartbeatTimeout, progressTimeout time.Duration) *HealthMonitor {
	return &HealthMonitor{
		agentID:          agentID,
		lastHeartbeat:    time.Now(),
		lastProgress:     time.Now(),
		heartbeatTimeout: heartbeatTimeout,
		progressTimeout:  progressTimeout,
	}
}

// OnTimeout sets the callback invoked when a health timeout occurs.
func (h *HealthMonitor) OnTimeout(cb func(agentID string, status HealthStatus)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onTimeout = cb
}

// RecordHeartbeat records that a heartbeat was received.
func (h *HealthMonitor) RecordHeartbeat() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastHeartbeat = time.Now()
}

// RecordProgress records that meaningful progress occurred
// (lock acquisition, file write, node update).
func (h *HealthMonitor) RecordProgress() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastProgress = time.Now()
}

// Start begins the health monitoring loop.
func (h *HealthMonitor) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	h.mu.Lock()
	h.cancel = cancel
	h.mu.Unlock()

	checkInterval := minDuration(h.heartbeatTimeout, h.progressTimeout) / 2
	if checkInterval < time.Second {
		checkInterval = time.Second
	}

	go func() {
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h.check()
			}
		}
	}()
}

// Stop stops the health monitoring loop.
func (h *HealthMonitor) Stop() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cancel != nil {
		h.cancel()
	}
}

func (h *HealthMonitor) check() {
	h.mu.Lock()
	now := time.Now()
	heartbeatAge := now.Sub(h.lastHeartbeat)
	progressAge := now.Sub(h.lastProgress)
	cb := h.onTimeout
	h.mu.Unlock()

	if cb == nil {
		return
	}

	if heartbeatAge > h.heartbeatTimeout {
		log.Printf("health: agent %s unresponsive (no heartbeat for %v)", h.agentID, heartbeatAge)
		cb(h.agentID, HealthUnresponsive)
		return
	}

	if progressAge > h.progressTimeout {
		log.Printf("health: agent %s stalled (no progress for %v)", h.agentID, progressAge)
		cb(h.agentID, HealthStalled)
		return
	}
}

// Status returns the current health status.
func (h *HealthMonitor) Status() HealthStatus {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now()
	if now.Sub(h.lastHeartbeat) > h.heartbeatTimeout {
		return HealthUnresponsive
	}
	if now.Sub(h.lastProgress) > h.progressTimeout {
		return HealthStalled
	}
	return HealthOK
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
