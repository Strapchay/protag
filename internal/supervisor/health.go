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
	activityStale    time.Duration
	activityMax      time.Duration
	activityPhase    string
	activityStarted  time.Time
	activityLastSeen time.Time
	monitoring       bool
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
		activityStale:    45 * time.Second,
		activityMax:      15 * time.Minute,
		monitoring:       true,
	}
}

// SetMonitoring enables health deadlines only while an RPC agent is handling
// a turn. A resident Pi process is expected to be silent between turns; its
// process lifetime remains covered by the supervisor's crash detector.
func (h *HealthMonitor) SetMonitoring(active bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.monitoring = active
	if active {
		now := time.Now()
		h.lastHeartbeat = now
		h.lastProgress = now
	}
}

// SetExternalActivityTimeouts configures how external runtime activity, such
// as a gateway inference request, shields the progress timeout.
func (h *HealthMonitor) SetExternalActivityTimeouts(stale, max time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if stale > 0 {
		h.activityStale = stale
	}
	if max > 0 {
		h.activityMax = max
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

// RecordExternalActivity records externally observable work for the agent.
// Active phases keep the heartbeat fresh and suppress progress stalls while
// their pulses remain fresh and below the configured maximum duration.
func (h *HealthMonitor) RecordExternalActivity(phase string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now()
	h.lastHeartbeat = now
	if !isActiveExternalPhase(phase) {
		h.activityPhase = phase
		h.activityStarted = time.Time{}
		h.activityLastSeen = time.Time{}
		return
	}
	if h.activityStarted.IsZero() || !isActiveExternalPhase(h.activityPhase) {
		h.activityStarted = now
	}
	h.activityPhase = phase
	h.activityLastSeen = now
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
	externalActive := h.externalActivityActiveLocked(now)
	monitoring := h.monitoring
	cb := h.onTimeout
	h.mu.Unlock()

	if cb == nil || !monitoring {
		return
	}

	if heartbeatAge > h.heartbeatTimeout {
		log.Printf("health: agent %s unresponsive (no heartbeat for %v)", h.agentID, heartbeatAge)
		cb(h.agentID, HealthUnresponsive)
		return
	}

	if progressAge > h.progressTimeout && !externalActive {
		log.Printf("health: agent %s stalled (no progress for %v)", h.agentID, progressAge)
		cb(h.agentID, HealthStalled)
		return
	}
}

// Status returns the current health status.
func (h *HealthMonitor) Status() HealthStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.monitoring {
		return HealthOK
	}

	now := time.Now()
	if now.Sub(h.lastHeartbeat) > h.heartbeatTimeout {
		return HealthUnresponsive
	}
	if now.Sub(h.lastProgress) > h.progressTimeout && !h.externalActivityActiveLocked(now) {
		return HealthStalled
	}
	return HealthOK
}

func (h *HealthMonitor) externalActivityActiveLocked(now time.Time) bool {
	if !isActiveExternalPhase(h.activityPhase) || h.activityLastSeen.IsZero() || h.activityStarted.IsZero() {
		return false
	}
	if h.activityStale > 0 && now.Sub(h.activityLastSeen) > h.activityStale {
		return false
	}
	if h.activityMax > 0 && now.Sub(h.activityStarted) > h.activityMax {
		return false
	}
	return true
}

func isActiveExternalPhase(phase string) bool {
	switch phase {
	case "queued", "admitted", "forwarding", "retry_wait", "active", "waiting_inference", "running_tool", "waiting_orchestrator_rpc":
		return true
	default:
		return false
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
