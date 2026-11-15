package orchestrator

import (
	"context"
	"fmt"
	"log"
	"time"

	coordination "aion-coordination"
	"aion-kernel/internal/dag"
)

const coordinationDeadlockScanTicks = 5

// scanCoordinationDeadlocks turns a silent contract stall into one kernel-owned
// reconciliation lineage. It only escalates when both participant agents have
// been idle for the configured window and the contract has made no progress.
func (d *Daemon) scanCoordinationDeadlocks(now time.Time) int {
	if d == nil || d.coordinationManager == nil || d.dagManager == nil {
		return 0
	}
	timeout := 300 * time.Second
	if d.config != nil && d.config.Coordination.DeadlockTimeoutSec > 0 {
		timeout = time.Duration(d.config.Coordination.DeadlockTimeoutSec) * time.Second
	}
	ctx := d.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	contracts, err := d.coordinationManager.List(ctx, coordination.Principal{Role: coordination.RoleAdmin})
	if err != nil {
		log.Printf("coordination: deadlock scan: %v", err)
		return 0
	}
	agents := make(map[string]AgentInfo)
	if d.allocator != nil {
		for _, info := range d.allocator.AgentInfos() {
			agents[info.AgentID] = info
		}
	}
	created := 0
	for _, contract := range contracts {
		lastActivity, ok := d.coordinationDeadlock(contract, agents, now, timeout)
		if !ok {
			continue
		}
		reason := fmt.Sprintf("automatic deadlock escalation: both participants idle for %s without contract progress",
			now.Sub(lastActivity).Round(time.Second))
		evidence := []string{
			fmt.Sprintf("contract_status=%s", contract.Status),
			fmt.Sprintf("contract_revision=%d", contract.Revision),
			fmt.Sprintf("producer_node=%s", contract.ProducerNodeID),
			fmt.Sprintf("consumer_node=%s", contract.ConsumerNodeID),
			fmt.Sprintf("last_activity=%s", lastActivity.UTC().Format(time.RFC3339)),
			fmt.Sprintf("idle_for=%s", now.Sub(lastActivity).Round(time.Second)),
		}
		resolution, isNew, err := d.coordinationManager.EscalateDeadlock(ctx, contract.ID, reason, evidence, now)
		if err != nil {
			log.Printf("coordination: escalate deadlock for %s: %v", contract.ID, err)
			continue
		}
		if isNew {
			created++
			log.Printf("coordination: automatically escalated contract %s after %s idle (resolution %s)",
				contract.ID, now.Sub(lastActivity).Round(time.Second), resolution.ID)
		}
	}
	return created
}

func (d *Daemon) coordinationDeadlock(contract coordination.Contract, agents map[string]AgentInfo, now time.Time, timeout time.Duration) (time.Time, bool) {
	switch contract.Status {
	case coordination.StatusRequested, coordination.StatusProduced, coordination.StatusCorrectionRequested:
	default:
		return time.Time{}, false
	}
	producer, err := d.dagManager.GetNode(contract.ProducerNodeID)
	if err != nil || producer == nil {
		return time.Time{}, false
	}
	consumer, err := d.dagManager.GetNode(contract.ConsumerNodeID)
	if err != nil || consumer == nil {
		return time.Time{}, false
	}
	if producer.Status.IsTerminal() || consumer.Status.IsTerminal() {
		return time.Time{}, false
	}
	// A plan-created contract is Requested while both nodes are still Pending.
	// That is an allocation concern, not a two-agent deadlock.
	if contract.Status == coordination.StatusRequested && producer.Status == dag.StatusPending && consumer.Status == dag.StatusPending {
		return time.Time{}, false
	}
	lastActivity := contract.UpdatedAt
	if producerUpdate := time.UnixMilli(producer.UpdatedAt); producerUpdate.After(lastActivity) {
		lastActivity = producerUpdate
	}
	if consumerUpdate := time.UnixMilli(consumer.UpdatedAt); consumerUpdate.After(lastActivity) {
		lastActivity = consumerUpdate
	}
	for _, agentID := range []string{contract.ProducerAgentID, contract.ConsumerAgentID} {
		info, ok := agents[agentID]
		if !ok {
			continue
		}
		if info.InTurn || info.ToolActive {
			return time.Time{}, false
		}
		if info.LastProgress.After(lastActivity) {
			lastActivity = info.LastProgress
		}
	}
	if now.Sub(lastActivity) < timeout {
		return time.Time{}, false
	}
	return lastActivity, true
}
