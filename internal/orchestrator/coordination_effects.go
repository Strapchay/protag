package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	coordination "aion-coordination"
	"aion-kernel/internal/dag"
	"aion-kernel/internal/hub"
)

func (d *Daemon) startCoordinationEffectLoop() {
	if d == nil || d.coordinationManager == nil || d.ctx == nil {
		return
	}
	d.coordinationWG.Add(1)
	go func() {
		defer d.coordinationWG.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		d.drainCoordinationEffects()
		for {
			select {
			case <-d.ctx.Done():
				return
			case <-ticker.C:
				d.expireCoordinationContracts()
				d.drainCoordinationEffects()
			}
		}
	}()
}

func (d *Daemon) expireCoordinationContracts() {
	if d == nil {
		return
	}
	d.coordinationEffectMu.Lock()
	defer d.coordinationEffectMu.Unlock()
	if d.coordinationManager != nil {
		_, _ = d.coordinationManager.ExpireDue(d.ctx, time.Now().UTC())
	}
}

func (d *Daemon) drainCoordinationEffects() {
	if d == nil || d.coordinationManager == nil {
		return
	}
	d.coordinationEffectMu.Lock()
	defer d.coordinationEffectMu.Unlock()

	ctx := d.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	for batch := 0; batch < 10; batch++ {
		effects, err := d.coordinationManager.PendingEffects(ctx, 100)
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("coordination: load pending effects: %v", err)
			}
			return
		}
		if len(effects) == 0 {
			return
		}
		progress := false
		for _, effect := range effects {
			if err := d.applyCoordinationEffect(effect); err != nil {
				next := time.Now().UTC().Add(coordinationRetryDelay(effect.Attempts))
				_ = d.coordinationManager.FailEffect(ctx, effect.ID, next, err)
				continue
			}
			if err := d.coordinationManager.AckEffect(ctx, effect.ID); err != nil {
				log.Printf("coordination: acknowledge effect %s: %v", effect.ID, err)
				continue
			}
			progress = true
		}
		if !progress {
			return
		}
	}
}

func coordinationRetryDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt > 6 {
		attempt = 6
	}
	return time.Duration(1<<attempt) * time.Second
}

func (d *Daemon) applyCoordinationEffect(effect coordination.Effect) error {
	switch effect.Type {
	case coordination.EffectWaitDeclared:
		if err := d.setCoordinationNodeStatus(effect.TargetNodeID, dag.StatusBlocked); err != nil {
			return err
		}
		d.recordCoordinationRecovery(effect, "contract_waiting", "Contract wait is blocking task execution.", "warn")
	case coordination.EffectWaitCleared:
		if err := d.resumeCoordinationNode(effect.TargetNodeID); err != nil {
			return err
		}
		if d.server.recoveryResolvedCb != nil {
			d.server.recoveryResolvedCb("contract_waiting", effect.TargetNodeID, effect.TargetAgentID)
		}
	case coordination.EffectCorrectionRequested:
		if err := d.resumeCoordinationNode(effect.TargetNodeID); err != nil {
			return err
		}
	case coordination.EffectContractTerminal:
		if err := d.setCoordinationNodeStatus(effect.TargetNodeID, dag.StatusBlocked); err != nil {
			return err
		}
		d.recordCoordinationRecovery(effect, "contract_terminal", "A required contract reached a terminal failure state.", "error")
	case coordination.EffectCycleDetected:
		var cycle coordination.Cycle
		if err := json.Unmarshal(effect.Payload, &cycle); err != nil {
			return fmt.Errorf("decode cycle effect: %w", err)
		}
		for _, nodeID := range cycle.NodeIDs {
			if err := d.setCoordinationNodeStatus(nodeID, dag.StatusBlocked); err != nil {
				return err
			}
			d.recordCoordinationRecovery(coordination.Effect{ID: effect.ID, TargetNodeID: nodeID, Payload: effect.Payload}, "contract_cycle", "Cross-domain contract waits contain a cycle.", "error")
		}
		d.server.BroadcastStatus(fmt.Sprintf("Contract cycle detected across nodes: %s", strings.Join(cycle.NodeIDs, ", ")), "error")
	case coordination.EffectNodeCompletionReady:
		if err := d.completeCoordinationNode(effect); err != nil {
			return err
		}
	}

	if coordinationEffectRequiresDelivery(effect) {
		if strings.TrimSpace(effect.TargetAgentID) == "" {
			return fmt.Errorf("coordination effect %s has no target agent", effect.ID)
		}
		if !d.hubRouter.HasAgent(effect.TargetAgentID) {
			return fmt.Errorf("target agent %s is not active", effect.TargetAgentID)
		}
		message := hub.Message{
			ID:        effect.ID,
			Type:      coordinationHubMessageType(effect.Type),
			FromAgent: "orchestrator",
			ToAgent:   effect.TargetAgentID,
			Payload:   effect.Payload,
			Timestamp: effect.CreatedAt,
		}
		if err := d.hubRouter.Route(message); err != nil {
			return err
		}
		d.server.BroadcastHubEvent(message)
	} else if effect.Type == coordination.EffectCycleDetected {
		d.server.BroadcastHubEvent(hub.Message{
			ID:        effect.ID,
			Type:      hub.MsgContractCycle,
			FromAgent: "orchestrator",
			Payload:   effect.Payload,
			Timestamp: effect.CreatedAt,
		})
	}
	if d.server.executionEventCb != nil {
		d.server.executionEventCb(ExecutionJournalEvent{
			Kind:     strings.ToLower(string(effect.Type)),
			Severity: coordinationEffectSeverity(effect.Type),
			AgentID:  effect.TargetAgentID,
			NodeID:   effect.TargetNodeID,
			Summary:  fmt.Sprintf("Coordination effect %s applied for contract %s.", effect.Type, effect.ContractID),
		})
	}
	if d.server.progressChangedCb != nil {
		go d.server.progressChangedCb("coordination_effect")
	}
	return nil
}

func coordinationEffectRequiresDelivery(effect coordination.Effect) bool {
	switch effect.Type {
	case coordination.EffectContractRequested, coordination.EffectContractProduced,
		coordination.EffectContractFulfilled, coordination.EffectCorrectionRequested,
		coordination.EffectContractTerminal:
		return true
	default:
		return false
	}
}

func coordinationHubMessageType(effectType coordination.EffectType) hub.MessageType {
	switch effectType {
	case coordination.EffectContractRequested:
		return hub.MsgContractRequested
	case coordination.EffectContractProduced:
		return hub.MsgContractProduced
	case coordination.EffectContractFulfilled:
		return hub.MsgContractFulfilled
	case coordination.EffectCorrectionRequested:
		return hub.MsgCorrectionRequest
	case coordination.EffectContractTerminal:
		return hub.MsgContractTerminal
	default:
		return hub.MsgContractWait
	}
}

func coordinationEffectSeverity(effectType coordination.EffectType) string {
	switch effectType {
	case coordination.EffectContractFulfilled, coordination.EffectNodeCompletionReady, coordination.EffectWaitCleared:
		return "ok"
	case coordination.EffectContractTerminal, coordination.EffectCycleDetected:
		return "error"
	case coordination.EffectCorrectionRequested, coordination.EffectWaitDeclared:
		return "warn"
	default:
		return "info"
	}
}

func (d *Daemon) setCoordinationNodeStatus(nodeID string, status dag.NodeStatus) error {
	if strings.TrimSpace(nodeID) == "" {
		return nil
	}
	node, err := d.dagManager.GetNode(nodeID)
	if err != nil {
		return err
	}
	if node.Status.IsTerminal() || node.Status == status {
		return nil
	}
	if err := d.dagManager.UpdateNode(nodeID, status); err != nil {
		return err
	}
	if updated, err := d.dagManager.GetNode(nodeID); err == nil && updated != nil {
		d.server.recordNodeProgressFact(*updated, status)
	}
	return nil
}

func (d *Daemon) resumeCoordinationNode(nodeID string) error {
	if strings.TrimSpace(nodeID) == "" {
		return nil
	}
	node, err := d.dagManager.GetNode(nodeID)
	if err != nil {
		return err
	}
	if node.Status != dag.StatusBlocked {
		return nil
	}
	return d.setCoordinationNodeStatus(nodeID, dag.StatusInProgress)
}

func (d *Daemon) completeCoordinationNode(effect coordination.Effect) error {
	var payload struct {
		NodeID   string          `json:"node_id"`
		AgentID  string          `json:"agent_id"`
		Metadata json.RawMessage `json:"metadata"`
	}
	if err := json.Unmarshal(effect.Payload, &payload); err != nil {
		return fmt.Errorf("decode completion effect: %w", err)
	}
	var metrics dag.NodeMetrics
	if len(payload.Metadata) > 0 {
		_ = json.Unmarshal(payload.Metadata, &metrics)
	}
	if err := d.dagManager.UpdateNodeForAgent(payload.NodeID, payload.AgentID, dag.StatusDone, &metrics); err != nil {
		node, getErr := d.dagManager.GetNode(payload.NodeID)
		if getErr == nil && node.Status == dag.StatusDone {
			return nil
		}
		return err
	}
	if node, err := d.dagManager.GetNode(payload.NodeID); err == nil && node != nil {
		d.server.recordNodeProgressFact(*node, dag.StatusDone)
	}
	return nil
}

func (d *Daemon) recordCoordinationRecovery(effect coordination.Effect, kind, summary, severity string) {
	if d.server.recoveryCb == nil {
		return
	}
	d.server.recoveryCb(RecoveryRecord{
		FailureID:        recoveryID(kind, effect.TargetNodeID, effect.TargetAgentID),
		Kind:             kind,
		Severity:         severity,
		Status:           "open",
		AgentID:          effect.TargetAgentID,
		NodeID:           effect.TargetNodeID,
		Summary:          summary,
		SuggestedCommand: "/progress",
	})
}
