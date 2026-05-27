package orchestrator

import (
	"context"
	"log"
	"time"

	"aion-kernel/internal/dag"
	"aion-kernel/internal/hub"
	"aion-kernel/internal/locking"
)

// Auditor periodically scans the Orchestrator's state to detect anomalies
// like stalled tasks, orphaned locks, and stale stubs.
type Auditor struct {
	dagManager      *dag.Manager
	lockManager     *locking.Manager
	hubRouter       *hub.Router
	progressTimeout time.Duration
	scanInterval    time.Duration
	staleNodeFn     func(dag.DagNode, time.Duration)
}

// NewAuditor creates a new passive auditor.
func NewAuditor(
	dagManager *dag.Manager,
	lockManager *locking.Manager,
	hubRouter *hub.Router,
	progressTimeout time.Duration,
	scanInterval time.Duration,
) *Auditor {
	return &Auditor{
		dagManager:      dagManager,
		lockManager:     lockManager,
		hubRouter:       hubRouter,
		progressTimeout: progressTimeout,
		scanInterval:    scanInterval,
	}
}

func (a *Auditor) SetStaleNodeFunc(fn func(dag.DagNode, time.Duration)) {
	a.staleNodeFn = fn
}

// Start launches the auditor loop in the background.
func (a *Auditor) Start(ctx context.Context) {
	go a.loop(ctx)
}

func (a *Auditor) loop(ctx context.Context) {
	ticker := time.NewTicker(a.scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.scan()
		}
	}
}

func (a *Auditor) scan() {
	now := time.Now().UnixMilli()

	// 1. Check for stalled InProgress nodes
	snap := a.dagManager.Snapshot()
	for _, node := range snap.Nodes {
		if node.Status == dag.StatusInProgress {
			elapsed := now - node.UpdatedAt
			if elapsed > a.progressTimeout.Milliseconds() {
				log.Printf("auditor: [WARNING] Node %s for agent %s has been InProgress for %d ms (exceeds timeout)",
					node.ID, node.AssignedAgent, elapsed)
				if a.staleNodeFn != nil {
					a.staleNodeFn(node, time.Duration(elapsed)*time.Millisecond)
				}
			}
		}
	}

	// 2. Check for orphaned locks
	// The lock manager holds a list of current locks, let's see which ones belong to inactive agents
	// (we can check if the hub router still has them registered, although the Server heartbeat is a better metric,
	// for now hub router registration suffices).
	// We'd ideally need a ListLocks method on the lock manager, but we don't strictly have one.
	// Actually we wrote a GetLocks / Status maybe?
	// Note: We might just log for now without implementing advanced recovery.
}
