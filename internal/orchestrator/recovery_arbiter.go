package orchestrator

import (
	"fmt"
	"strings"
	"time"

	"aion-kernel/internal/dag"
)

const agentRecoveryBudget = 3

// AgentRecoveryState is the arbiter's compact, persisted view of one agent
// recovery lineage. It is coordination metadata, not a second node lifecycle.
type AgentRecoveryState struct {
	AgentID            string    `json:"agent_id"`
	DomainID           string    `json:"domain_id,omitempty"`
	Epoch              int64     `json:"epoch"`
	Phase              string    `json:"phase"`
	Owner              string    `json:"owner,omitempty"`
	FailureCode        string    `json:"failure_code,omitempty"`
	FailureClass       string    `json:"failure_class,omitempty"`
	Cause              string    `json:"cause,omitempty"`
	Attempts           int       `json:"attempts"`
	Budget             int       `json:"budget"`
	SupervisorRestarts int       `json:"supervisor_restarts"`
	LastError          string    `json:"last_error,omitempty"`
	UpdatedAt          time.Time `json:"updated_at"`
}

type AgentResumePreflight struct {
	AgentID    string             `json:"agent_id"`
	DomainID   string             `json:"domain_id,omitempty"`
	State      string             `json:"state"`
	InTurn     bool               `json:"in_turn"`
	ToolActive bool               `json:"tool_active"`
	Recovery   AgentRecoveryState `json:"recovery"`
}

type ResumePreflight struct {
	AttemptID         string                 `json:"attempt_id,omitempty"`
	Status            string                 `json:"status,omitempty"`
	FailureReason     string                 `json:"failure_reason,omitempty"`
	AcceptanceStatus  string                 `json:"acceptance_status,omitempty"`
	AcceptanceSummary string                 `json:"acceptance_summary,omitempty"`
	CanResume         bool                   `json:"can_resume"`
	BlockingReasons   []string               `json:"blocking_reasons,omitempty"`
	Warnings          []string               `json:"warnings,omitempty"`
	NodeCounts        map[string]int         `json:"node_counts,omitempty"`
	ProblemNodes      []string               `json:"problem_nodes,omitempty"`
	Agents            []AgentResumePreflight `json:"agents,omitempty"`
	OpenRecoveries    []RecoveryRecord       `json:"open_recoveries,omitempty"`
	Integration       *IntegrationRunStatus  `json:"integration,omitempty"`
	ResumePlan        []string               `json:"resume_plan,omitempty"`
	ConfirmationHint  string                 `json:"confirmation_hint,omitempty"`
	GeneratedAt       time.Time              `json:"generated_at"`
}

func (d *Daemon) observeAgentRecovery(agentID, domainID, state, reason string) {
	if d == nil || strings.TrimSpace(agentID) == "" {
		return
	}
	now := time.Now().UTC()
	d.recoveryMu.Lock()
	defer d.recoveryMu.Unlock()
	if d.recoveryStates == nil {
		d.recoveryStates = make(map[string]AgentRecoveryState)
	}
	recovery := d.recoveryStates[agentID]
	if recovery.AgentID == "" {
		recovery = AgentRecoveryState{AgentID: agentID, DomainID: domainID, Budget: agentRecoveryBudget, Epoch: 1}
	}
	recovery.DomainID = firstNonEmpty(domainID, recovery.DomainID)
	recovery.LastError = reason
	recovery.UpdatedAt = now
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "crashed":
		recovery.Epoch++
		recovery.Phase = "process_recovering"
		recovery.Owner = "supervisor"
		recovery.FailureCode = "agent.process_crashed"
		recovery.FailureClass = "retry"
		recovery.Cause = reason
		recovery.Attempts++
		recovery.SupervisorRestarts++
	case "running":
		recovery.Phase = "idle"
		recovery.Owner = ""
		recovery.FailureCode = ""
		recovery.FailureClass = ""
		recovery.Cause = ""
	case "stopped":
		if strings.TrimSpace(reason) == "" {
			recovery.Phase = "idle"
			recovery.Owner = ""
			recovery.FailureCode = ""
			recovery.FailureClass = ""
			recovery.Cause = ""
			break
		}
		recovery.Phase = "work_recovery_pending"
		recovery.Owner = "arbiter"
		recovery.FailureCode = "agent.stopped"
		recovery.FailureClass = "retry"
		if recovery.Attempts >= recovery.Budget {
			recovery.Phase = "awaiting_operator"
			recovery.FailureClass = "attention"
			recovery.FailureCode = "supervisor.restart_budget_exhausted"
		}
	}
	d.recoveryStates[agentID] = recovery
}

func (d *Daemon) recoverySnapshot(agentID string) AgentRecoveryState {
	d.recoveryMu.Lock()
	defer d.recoveryMu.Unlock()
	return d.recoveryStates[strings.TrimSpace(agentID)]
}

func (d *Daemon) resetRecoveryEpochForOperator(agentID string) {
	d.recoveryMu.Lock()
	defer d.recoveryMu.Unlock()
	state := d.recoveryStates[strings.TrimSpace(agentID)]
	if state.AgentID == "" {
		return
	}
	state.Epoch++
	state.Phase = "work_recovery_pending"
	state.Owner = "operator"
	state.Attempts = 0
	state.FailureClass = "retry"
	state.FailureCode = "operator.continue"
	state.Cause = "operator approved continuation"
	state.UpdatedAt = time.Now().UTC()
	d.recoveryStates[agentID] = state
}

func (d *Daemon) recoveryStateBlocked(agentID string) bool {
	state := d.recoverySnapshot(agentID)
	return state.Phase == "process_recovering" || state.Phase == "awaiting_operator"
}

func (d *Daemon) resumeBuildWorkAfterAcceptance(attempt *BuildSpecAttempt) error {
	if d == nil || attempt == nil {
		return nil
	}
	attempt.Status = BuildSpecAttemptActive
	live := d.allocator.AgentInfos()
	if len(live) == 0 {
		if err := d.resumeActiveBuildSpecAgents(attempt); err != nil {
			return err
		}
	} else {
		for _, info := range live {
			state := d.recoverySnapshot(info.AgentID)
			if state.Phase == "process_recovering" || state.Phase == "awaiting_operator" {
				continue
			}
			switch strings.ToLower(strings.TrimSpace(info.State)) {
			case "stopped", "crashed", "failed", "failed_terminal":
				if err := d.allocator.ReviveAgent(d.ctx, info.AgentID); err != nil {
					return fmt.Errorf("acceptance resume revive %s: %w", info.AgentID, err)
				}
			}
		}
	}
	d.startBuildSpecExecutionMonitor()
	return nil
}

func (d *Daemon) reopenFailedNodesForResume() error {
	if d == nil || d.dagManager == nil {
		return nil
	}
	for _, node := range d.dagManager.Snapshot().Nodes {
		if node.Status != dag.StatusFailed {
			continue
		}
		if err := d.dagManager.UpdateNode(node.ID, dag.StatusPending); err != nil {
			return fmt.Errorf("reopen failed node %s: %w", node.ID, err)
		}
		if updated, err := d.dagManager.GetNode(node.ID); err == nil && updated != nil && d.server != nil {
			d.server.recordNodeProgressFact(*updated, dag.StatusPending)
		}
	}
	return nil
}

func (d *Daemon) initRecoveryArbiter() {
	if d == nil || d.runState == nil {
		return
	}
	attempt, err := loadBuildSpecAttempt(d.runState.Root)
	if err != nil || attempt == nil {
		return
	}
	d.recoveryMu.Lock()
	if d.recoveryStates == nil {
		d.recoveryStates = make(map[string]AgentRecoveryState)
	}
	for agentID, persisted := range attempt.AgentStates {
		state := AgentRecoveryState{
			AgentID: agentID, DomainID: persisted.DomainID, Epoch: persisted.RecoveryEpoch,
			Phase: persisted.RecoveryPhase, Owner: persisted.RecoveryOwner, FailureCode: persisted.FailureCode,
			FailureClass: persisted.FailureClass, Attempts: persisted.RecoveryAttempts, Budget: persisted.RecoveryBudget,
			SupervisorRestarts: persisted.SupervisorRestarts, LastError: persisted.Reason, UpdatedAt: persisted.UpdatedAt,
		}
		if state.Epoch == 0 {
			state.Epoch = 1
		}
		if state.Budget == 0 {
			state.Budget = agentRecoveryBudget
		}
		if state.Phase == "process_recovering" {
			state.Phase = "work_recovery_pending"
			state.Owner = "arbiter"
			state.Epoch++
		}
		if state.Attempts >= state.Budget && (state.Phase == "work_recovery_pending" || state.Phase == "process_recovering") {
			state.Phase = "awaiting_operator"
			state.FailureClass = "attention"
			state.FailureCode = "supervisor.restart_budget_exhausted"
		}
		d.recoveryStates[agentID] = state
	}
	d.recoveryMu.Unlock()
}

func (d *Daemon) buildResumePreflight() ResumePreflight {
	preflight := ResumePreflight{
		GeneratedAt: time.Now().UTC(), CanResume: true, NodeCounts: make(map[string]int),
		ConfirmationHint: "Review this state, then run /continue-agents confirm to proceed.",
	}
	attempt, _ := loadBuildSpecAttempt(d.runState.Root)
	if attempt != nil {
		preflight.AttemptID = attempt.AttemptID
		preflight.Status = string(attempt.Status)
		preflight.FailureReason = attempt.FailureReason
		preflight.AcceptanceStatus = attempt.AcceptanceStatus
		preflight.AcceptanceSummary = attempt.AcceptanceSummary
	}
	if d.dagManager != nil {
		for _, node := range d.dagManager.Snapshot().Nodes {
			preflight.NodeCounts[node.Status.String()]++
			if node.Status == dag.StatusFailed || node.Status == dag.StatusBlocked || node.Status == dag.StatusInProgress {
				preflight.ProblemNodes = append(preflight.ProblemNodes, node.ID+"="+node.Status.String())
			}
		}
	}
	seenAgents := make(map[string]bool)
	for _, info := range d.allocator.AgentInfos() {
		seenAgents[info.AgentID] = true
		preflight.Agents = append(preflight.Agents, AgentResumePreflight{
			AgentID: info.AgentID, DomainID: info.DomainID, State: info.State,
			InTurn: info.InTurn, ToolActive: info.ToolActive, Recovery: d.recoverySnapshot(info.AgentID),
		})
	}
	d.recoveryMu.Lock()
	for agentID, recovery := range d.recoveryStates {
		if seenAgents[agentID] {
			continue
		}
		preflight.Agents = append(preflight.Agents, AgentResumePreflight{
			AgentID: agentID, DomainID: recovery.DomainID, State: "NotRunning", Recovery: recovery,
		})
	}
	d.recoveryMu.Unlock()
	preflight.OpenRecoveries = d.openRecoveryRecords()
	if d.integrationRunner != nil {
		status := d.integrationRunner.Status()
		preflight.Integration = &status
	}
	if attempt == nil {
		preflight.CanResume = false
		preflight.BlockingReasons = append(preflight.BlockingReasons, "no persisted build-spec attempt")
		return preflight
	}
	switch attempt.Status {
	case BuildSpecAttemptCompleted, BuildSpecAttemptCanceled:
		preflight.CanResume = false
		preflight.BlockingReasons = append(preflight.BlockingReasons, "attempt is already terminal")
	}
	if attempt.AcceptanceStatus == "running" {
		preflight.CanResume = false
		preflight.BlockingReasons = append(preflight.BlockingReasons, "acceptance review is running")
	}
	for _, agent := range preflight.Agents {
		if agent.Recovery.Phase == "process_recovering" {
			preflight.CanResume = false
			preflight.BlockingReasons = append(preflight.BlockingReasons, "supervisor is recovering "+agent.AgentID)
		}
		if agent.Recovery.Phase == "awaiting_operator" {
			preflight.Warnings = append(preflight.Warnings, "agent "+agent.AgentID+" exhausted its recovery budget; confirmation grants a new operator-approved epoch")
		}
	}
	if failedCount := preflight.NodeCounts[dag.StatusFailed.String()]; failedCount > 0 {
		preflight.Warnings = append(preflight.Warnings, fmt.Sprintf("%d failed node(s) will be reopened for an operator-approved retry", failedCount))
	}
	if attempt.AcceptanceStatus == "passed" {
		preflight.ResumePlan = append(preflight.ResumePlan, "finalize completed build")
	} else if attempt.AcceptanceStatus == "attention" {
		preflight.ResumePlan = append(preflight.ResumePlan, "rerun acceptance after operator review")
	} else if attempt.AcceptanceStatus == "retry" || attempt.AcceptanceStatus == "successor" {
		preflight.ResumePlan = append(preflight.ResumePlan, "resume rework nodes and rerun acceptance")
	} else if len(preflight.ProblemNodes) > 0 {
		preflight.ResumePlan = append(preflight.ResumePlan, "recover failed/blocked nodes, then rerun integration and acceptance")
	} else {
		preflight.ResumePlan = append(preflight.ResumePlan, "start execution monitor")
	}
	if preflight.Integration != nil {
		switch preflight.Integration.Status {
		case IntegrationFailed:
			preflight.ResumePlan = append(preflight.ResumePlan, "retry integration validation after owner recovery")
		case IntegrationSkipped:
			preflight.Warnings = append(preflight.Warnings, "integration was skipped because no directives were published")
		case IntegrationDisabled:
			preflight.Warnings = append(preflight.Warnings, "integration is disabled; acceptance is the only semantic gate")
		}
	}
	return preflight
}
