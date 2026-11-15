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

type RecoveryRequest struct {
	ID          string    `json:"id"`
	AgentID     string    `json:"agent_id"`
	NodeID      string    `json:"node_id,omitempty"`
	Action      string    `json:"action"`
	Cause       string    `json:"cause"`
	FailureCode string    `json:"failure_code,omitempty"`
	Priority    int       `json:"priority,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
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
	AttemptID             string                 `json:"attempt_id,omitempty"`
	Status                string                 `json:"status,omitempty"`
	FailureReason         string                 `json:"failure_reason,omitempty"`
	AcceptanceStatus      string                 `json:"acceptance_status,omitempty"`
	AcceptanceSummary     string                 `json:"acceptance_summary,omitempty"`
	AcceptanceFailureCode string                 `json:"acceptance_failure_code,omitempty"`
	CanResume             bool                   `json:"can_resume"`
	BlockingReasons       []string               `json:"blocking_reasons,omitempty"`
	Warnings              []string               `json:"warnings,omitempty"`
	NodeCounts            map[string]int         `json:"node_counts,omitempty"`
	ProblemNodes          []string               `json:"problem_nodes,omitempty"`
	Agents                []AgentResumePreflight `json:"agents,omitempty"`
	OpenRecoveries        []RecoveryRecord       `json:"open_recoveries,omitempty"`
	Integration           *IntegrationRunStatus  `json:"integration,omitempty"`
	ResumePlan            []string               `json:"resume_plan,omitempty"`
	ConfirmationHint      string                 `json:"confirmation_hint,omitempty"`
	GeneratedAt           time.Time              `json:"generated_at"`
}

func (d *Daemon) observeAgentRecovery(agentID, domainID, state, reason string) {
	if d == nil || strings.TrimSpace(agentID) == "" {
		return
	}
	now := time.Now().UTC()
	code := agentFailureCode(state, reason)
	action, maxAttempts := d.recoveryPolicyAction(code)
	d.recoveryMu.Lock()
	if d.recoveryStates == nil {
		d.recoveryStates = make(map[string]AgentRecoveryState)
	}
	recovery := d.recoveryStates[agentID]
	if recovery.AgentID == "" {
		recovery = AgentRecoveryState{AgentID: agentID, DomainID: domainID, Budget: maxAttempts, Epoch: 1}
	}
	recovery.DomainID = firstNonEmpty(domainID, recovery.DomainID)
	recovery.LastError = reason
	recovery.UpdatedAt = now
	enqueue := false
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "crashed":
		recovery.Epoch++
		recovery.Phase = "process_recovering"
		recovery.Owner = "supervisor"
		recovery.FailureCode = code
		recovery.FailureClass = "retry"
		recovery.Cause = reason
		recovery.Attempts++
		recovery.SupervisorRestarts++
		recovery.Budget = maxAttempts
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
		recovery.FailureCode = code
		recovery.Budget = maxAttempts
		recovery.Attempts++
		switch action {
		case "retry", "retry_turn", "reopen_node":
			recovery.FailureClass = "retry"
			if recovery.Attempts >= recovery.Budget {
				recovery.Phase = "awaiting_operator"
				recovery.FailureClass = "attention"
				recovery.FailureCode = "supervisor.restart_budget_exhausted"
			} else {
				recovery.Phase = "work_recovery_pending"
				recovery.Owner = "arbiter"
				enqueue = true
			}
		case "wait":
			recovery.FailureClass = "wait"
			recovery.Phase = "work_recovery_pending"
			recovery.Owner = "arbiter"
		case "none":
			recovery.FailureClass = "none"
			recovery.Phase = "idle"
		default:
			recovery.FailureClass = "attention"
			recovery.Phase = "awaiting_operator"
			recovery.Owner = "arbiter"
			recovery.FailureCode = "supervisor.restart_budget_exhausted"
		}
	}
	d.recoveryStates[agentID] = recovery
	d.recoveryMu.Unlock()
	if enqueue {
		d.enqueueRecovery(RecoveryRequest{
			AgentID: agentID, Action: "revive", Cause: "agent." + strings.ToLower(strings.TrimSpace(state)),
			FailureCode: code, Priority: 20, CreatedAt: now,
		})
	}
}

func agentFailureCode(state, reason string) string {
	text := strings.ToLower(strings.TrimSpace(state + " " + reason))
	switch {
	case strings.Contains(text, "auth") || strings.Contains(text, "401") || strings.Contains(text, "403") || strings.Contains(text, "unauthorized") || strings.Contains(text, "forbidden"):
		return "agent.auth_failed"
	case strings.Contains(text, "context") && (strings.Contains(text, "length") || strings.Contains(text, "window") || strings.Contains(text, "too long")):
		return "agent.context_window_exceeded"
	case strings.Contains(text, "429") || strings.Contains(text, "rate limit"):
		return "agent.provider_rate_limited"
	case strings.Contains(text, "503") || strings.Contains(text, "unavailable") || strings.Contains(text, "overloaded"):
		return "agent.provider_unavailable"
	case strings.Contains(text, "timeout") || strings.Contains(text, "econnreset") || strings.Contains(text, "network"):
		return "agent.transient_network"
	case strings.Contains(text, "loop"):
		return "agent.looping"
	case strings.Contains(text, "boundary") || strings.Contains(text, "ownership"):
		return "agent.domain_boundary_violation"
	case strings.Contains(text, "incomplete"):
		return "agent.incomplete_turn"
	case strings.Contains(text, "crashed") || strings.Contains(text, "exited") || strings.Contains(text, "health timeout"):
		return "agent.process_crashed"
	default:
		return "agent.stopped"
	}
}

func (d *Daemon) recoveryPolicyAction(code string) (string, int) {
	action := "attention"
	maxAttempts := 2
	if d != nil && d.config != nil {
		policy := d.config.Coordination.Recovery
		if strings.TrimSpace(policy.Actions[code]) != "" {
			action = strings.TrimSpace(policy.Actions[code])
		} else if strings.TrimSpace(policy.UnknownFailure) != "" {
			action = strings.TrimSpace(policy.UnknownFailure)
		}
		if policy.MaxAttempts[code] > 0 {
			maxAttempts = policy.MaxAttempts[code]
		}
	}
	return action, maxAttempts
}

func (d *Daemon) integrationFailureAction(class string) string {
	action, _ := d.recoveryPolicyAction("integration." + strings.TrimSpace(class))
	return action
}

func (d *Daemon) enqueueRecovery(request RecoveryRequest) {
	if d == nil || strings.TrimSpace(request.AgentID) == "" || strings.TrimSpace(request.Action) == "" {
		return
	}
	if request.CreatedAt.IsZero() {
		request.CreatedAt = time.Now().UTC()
	}
	request.ID = request.AgentID + "\x00" + request.NodeID + "\x00" + request.Action + "\x00" + request.Cause
	d.recoveryMu.Lock()
	for _, queued := range d.recoveryQueue {
		if queued.ID == request.ID {
			d.recoveryMu.Unlock()
			return
		}
	}
	d.recoveryQueue = append(d.recoveryQueue, request)
	d.recoveryMu.Unlock()
	go d.processRecoveryQueue()
}

func (d *Daemon) processRecoveryQueue() {
	if d == nil {
		return
	}
	d.recoveryMu.Lock()
	if d.recoveryProcessing {
		d.recoveryMu.Unlock()
		return
	}
	d.recoveryProcessing = true
	queue := append([]RecoveryRequest(nil), d.recoveryQueue...)
	d.recoveryMu.Unlock()
	defer func() {
		d.recoveryMu.Lock()
		d.recoveryProcessing = false
		d.recoveryMu.Unlock()
	}()
	var remaining []RecoveryRequest
	for _, request := range queue {
		state := d.recoverySnapshot(request.AgentID)
		if state.Phase == "process_recovering" || state.Phase == "awaiting_operator" {
			remaining = append(remaining, request)
			continue
		}
		switch request.Action {
		case "revive", "retry", "retry_turn":
			info, live := recoveryAgentInfo(d, request.AgentID)
			if live && strings.EqualFold(strings.TrimSpace(info.State), "running") {
				continue
			}
			d.setRecoveryPhase(request.AgentID, "work_recovering", "arbiter")
			if err := d.allocator.ReviveAgent(d.ctx, request.AgentID); err != nil {
				d.setRecoveryPhase(request.AgentID, "awaiting_operator", "arbiter")
				continue
			}
		case "reopen_node":
			if strings.TrimSpace(request.NodeID) == "" {
				continue
			}
			if node, err := d.dagManager.GetNode(request.NodeID); err == nil && node != nil && node.Status == dag.StatusFailed {
				_ = d.dagManager.UpdateNode(request.NodeID, dag.StatusPending)
			}
		}
	}
	d.recoveryMu.Lock()
	d.recoveryQueue = remaining
	d.recoveryMu.Unlock()
	d.persistRecoveryQueue()
}

func recoveryAgentInfo(d *Daemon, agentID string) (AgentInfo, bool) {
	if d == nil || d.allocator == nil {
		return AgentInfo{}, false
	}
	for _, info := range d.allocator.AgentInfos() {
		if info.AgentID == agentID {
			return info, true
		}
	}
	return AgentInfo{}, false
}

func (d *Daemon) setRecoveryPhase(agentID, phase, owner string) {
	d.recoveryMu.Lock()
	defer d.recoveryMu.Unlock()
	state := d.recoveryStates[agentID]
	if state.AgentID == "" {
		return
	}
	state.Phase = phase
	state.Owner = owner
	state.UpdatedAt = time.Now().UTC()
	d.recoveryStates[agentID] = state
}

func (d *Daemon) persistRecoveryQueue() {
	if d == nil || d.runState == nil {
		return
	}
	d.recoveryMu.Lock()
	queue := append([]RecoveryRequest(nil), d.recoveryQueue...)
	d.recoveryMu.Unlock()
	attempt, err := loadBuildSpecAttempt(d.runState.Root)
	if err != nil || attempt == nil {
		return
	}
	attempt.RecoveryQueue = queue
	d.saveBuildSpecAttempt(attempt)
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
	if d.workResumeFn != nil {
		return d.workResumeFn(attempt)
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
				d.enqueueRecovery(RecoveryRequest{
					AgentID: info.AgentID, Action: "revive", Cause: "acceptance_rework", Priority: 30, CreatedAt: time.Now().UTC(),
				})
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
	d.recoveryQueue = append([]RecoveryRequest(nil), attempt.RecoveryQueue...)
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
		preflight.AcceptanceFailureCode = attempt.AcceptanceFailureCode
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
