package coordination

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

func (m *Manager) Request(ctx context.Context, principal Principal, command RequestCommand) (*Contract, error) {
	contract := command.Contract
	normalizeContract(&contract)
	if err := validateRequestedContract(principal, contract); err != nil {
		return nil, err
	}
	if contract.Status == "" {
		contract.Status = StatusRequested
	}
	if contract.Status != StatusRequested || contract.Revision != 0 {
		return nil, fmt.Errorf("coordination: new contract must start at Requested revision 0")
	}
	requestValue := struct {
		Contract Contract `json:"contract"`
	}{Contract: contract}
	commandID, requestHash, err := commandIdentity(command.CommandID, "request", principal, requestValue)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	contract.CreatedAt = now
	contract.UpdatedAt = now
	detailsJSON, err := json.Marshal(contract.Details)
	if err != nil {
		return nil, fmt.Errorf("coordination: encode contract details: %w", err)
	}

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("coordination: begin request: %w", err)
	}
	defer tx.Rollback()
	var prior Contract
	if found, err := loadCommandResult(ctx, tx, commandID, "request", requestHash, &prior); err != nil {
		return nil, err
	} else if found {
		return &prior, nil
	}

	_, err = tx.ExecContext(ctx, `INSERT INTO contracts (
        id, producer_agent_id, producer_node_id, consumer_agent_id, consumer_node_id,
        details_json, status, revision, hard_wait, expires_at, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)`,
		contract.ID, contract.ProducerAgentID, contract.ProducerNodeID,
		contract.ConsumerAgentID, contract.ConsumerNodeID, detailsJSON,
		contract.Status, boolInt(contract.HardWait), formatOptionalTime(contract.ExpiresAt),
		formatTime(now), formatTime(now))
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			return nil, fmt.Errorf("coordination: contract %q already exists", contract.ID)
		}
		return nil, fmt.Errorf("coordination: insert contract: %w", err)
	}
	if err := insertTransition(ctx, tx, contract.ID, "", contract.Status, 0, principalIdentity(principal), "requested", now); err != nil {
		return nil, err
	}
	if err := insertInitialTerms(ctx, tx, contract, principalIdentity(principal), now); err != nil {
		return nil, err
	}
	if err := insertEffect(ctx, tx, effectForContract(commandID, EffectContractRequested, contract, contract.ProducerAgentID, contract.ProducerNodeID, now)); err != nil {
		return nil, err
	}
	if contract.HardWait {
		if err := insertEffect(ctx, tx, effectForContract(commandID+":wait", EffectWaitDeclared, contract, contract.ConsumerAgentID, contract.ConsumerNodeID, now)); err != nil {
			return nil, err
		}
	}
	if err := m.syncCycles(ctx, tx, commandID, now); err != nil {
		return nil, err
	}
	if err := saveCommandResult(ctx, tx, commandID, "request", requestHash, contract, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("coordination: commit request: %w", err)
	}
	return &contract, nil
}

func (m *Manager) Publish(ctx context.Context, principal Principal, command PublishCommand) (*Contract, error) {
	command.ContractID = strings.TrimSpace(command.ContractID)
	if command.ContractID == "" {
		return nil, fmt.Errorf("coordination: contract ID is required")
	}
	if len(command.Content) == 0 {
		return nil, fmt.Errorf("coordination: artifact content is required")
	}
	digest, size := objectIdentity(command.Content)
	requestValue := struct {
		ContractID       string          `json:"contract_id"`
		ExpectedRevision int64           `json:"expected_revision"`
		Digest           string          `json:"digest"`
		MediaType        string          `json:"media_type"`
		Evidence         json.RawMessage `json:"evidence,omitempty"`
	}{command.ContractID, command.ExpectedRevision, digest, strings.TrimSpace(command.MediaType), command.Evidence}
	commandID, requestHash, err := commandIdentity(command.CommandID, "publish", principal, requestValue)
	if err != nil {
		return nil, err
	}

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("coordination: begin publish: %w", err)
	}
	defer tx.Rollback()
	var prior Contract
	if found, err := loadCommandResult(ctx, tx, commandID, "publish", requestHash, &prior); err != nil {
		return nil, err
	} else if found {
		return &prior, nil
	}
	contract, err := getContractTx(ctx, tx, command.ContractID)
	if err != nil {
		return nil, err
	}
	if err := authorizeProducer(principal, contract); err != nil {
		return nil, err
	}
	if contract.Status != StatusRequested && contract.Status != StatusCorrectionRequested {
		return nil, fmt.Errorf("coordination: contract %s cannot publish from %s", contract.ID, contract.Status)
	}
	terms, err := latestTermsTx(ctx, tx, contract.ID)
	if err != nil {
		return nil, err
	}
	if terms.Status != TermsAgreed {
		return nil, fmt.Errorf("coordination: contract %s cannot publish until terms revision %d is agreed", contract.ID, terms.Revision)
	}
	if contract.Revision != command.ExpectedRevision {
		return nil, fmt.Errorf("coordination: contract %s revision conflict: expected %d, current %d", contract.ID, command.ExpectedRevision, contract.Revision)
	}
	storedDigest, storedSize, err := storeObject(m.objectDir, command.Content)
	if err != nil {
		return nil, err
	}
	if storedDigest != digest || storedSize != size {
		return nil, fmt.Errorf("coordination: stored artifact identity changed during publication")
	}
	if err := supersedePendingEffects(ctx, tx, contract.ID, EffectContractRequested, EffectCorrectionRequested); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	previous := contract.Status
	contract.Revision++
	contract.Status = StatusProduced
	contract.Artifact = &Artifact{Digest: digest, Size: size, MediaType: requestValue.MediaType, CreatedAt: now}
	contract.Validation = nil
	contract.CorrectionReason = ""
	contract.UpdatedAt = now
	_, err = tx.ExecContext(ctx, `UPDATE contracts SET status=?, revision=?, artifact_digest=?, artifact_size=?,
        artifact_media_type=?, validation_json=NULL, correction_reason=NULL, updated_at=? WHERE id=?`,
		contract.Status, contract.Revision, digest, size, requestValue.MediaType, formatTime(now), contract.ID)
	if err != nil {
		return nil, fmt.Errorf("coordination: update published contract: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO contract_revisions
        (contract_id, revision, digest, size, media_type, evidence_json, actor_id, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, contract.ID, contract.Revision, digest, size,
		requestValue.MediaType, nullableJSON(command.Evidence), principalIdentity(principal), formatTime(now))
	if err != nil {
		return nil, fmt.Errorf("coordination: record artifact revision: %w", err)
	}
	if err := insertTransition(ctx, tx, contract.ID, previous, contract.Status, contract.Revision, principalIdentity(principal), "artifact published", now); err != nil {
		return nil, err
	}
	if err := insertEffect(ctx, tx, effectForContract(commandID, EffectContractProduced, *contract, contract.ConsumerAgentID, contract.ConsumerNodeID, now)); err != nil {
		return nil, err
	}
	if err := saveCommandResult(ctx, tx, commandID, "publish", requestHash, contract, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("coordination: commit publication: %w", err)
	}
	return contract, nil
}

func (m *Manager) Validate(ctx context.Context, principal Principal, command ValidateCommand) (*Contract, error) {
	return m.resolveProduced(ctx, principal, "validate", command.CommandID, command.ContractID, command.Revision, "", command.Evidence, true)
}

func (m *Manager) Correct(ctx context.Context, principal Principal, command CorrectCommand) (*Contract, error) {
	if strings.TrimSpace(command.Reason) == "" {
		return nil, fmt.Errorf("coordination: correction reason is required")
	}
	return m.resolveProduced(ctx, principal, "correct", command.CommandID, command.ContractID, command.Revision, command.Reason, nil, false)
}

func (m *Manager) resolveProduced(ctx context.Context, principal Principal, operation, suppliedCommandID, contractID string, revision int64, reason string, evidence json.RawMessage, accepted bool) (*Contract, error) {
	contractID = strings.TrimSpace(contractID)
	requestValue := struct {
		ContractID string          `json:"contract_id"`
		Revision   int64           `json:"revision"`
		Reason     string          `json:"reason,omitempty"`
		Evidence   json.RawMessage `json:"evidence,omitempty"`
		Accepted   bool            `json:"accepted"`
	}{contractID, revision, strings.TrimSpace(reason), evidence, accepted}
	commandID, requestHash, err := commandIdentity(suppliedCommandID, operation, principal, requestValue)
	if err != nil {
		return nil, err
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("coordination: begin %s: %w", operation, err)
	}
	defer tx.Rollback()
	var prior Contract
	if found, err := loadCommandResult(ctx, tx, commandID, operation, requestHash, &prior); err != nil {
		return nil, err
	} else if found {
		return &prior, nil
	}
	contract, err := getContractTx(ctx, tx, contractID)
	if err != nil {
		return nil, err
	}
	if err := authorizeConsumer(principal, contract); err != nil {
		return nil, err
	}
	if contract.Status != StatusProduced {
		return nil, fmt.Errorf("coordination: contract %s cannot be %s from %s", contract.ID, operation, contract.Status)
	}
	if revision != contract.Revision {
		return nil, fmt.Errorf("coordination: contract %s revision conflict: requested %d, current %d", contract.ID, revision, contract.Revision)
	}
	now := time.Now().UTC()
	previous := contract.Status
	validation := &Validation{Accepted: accepted, Evidence: evidence, Reason: reason, ActorID: principalIdentity(principal), At: now}
	contract.Validation = validation
	contract.UpdatedAt = now
	validationJSON, _ := json.Marshal(validation)
	if accepted {
		contract.Status = StatusFulfilled
		contract.HardWait = false
		contract.CorrectionReason = ""
	} else {
		contract.Status = StatusCorrectionRequested
		contract.CorrectionReason = reason
	}
	if err := supersedePendingEffects(ctx, tx, contract.ID, EffectContractProduced); err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `UPDATE contracts SET status=?, hard_wait=?, validation_json=?, correction_reason=?, updated_at=? WHERE id=?`,
		contract.Status, boolInt(contract.HardWait), validationJSON, nullableString(contract.CorrectionReason), formatTime(now), contract.ID)
	if err != nil {
		return nil, fmt.Errorf("coordination: update validation: %w", err)
	}
	if err := insertTransition(ctx, tx, contract.ID, previous, contract.Status, contract.Revision, principalIdentity(principal), coalesce(reason, "consumer accepted artifact"), now); err != nil {
		return nil, err
	}
	if accepted {
		if err := insertEffect(ctx, tx, effectForContract(commandID, EffectContractFulfilled, *contract, contract.ProducerAgentID, contract.ProducerNodeID, now)); err != nil {
			return nil, err
		}
		if err := insertEffect(ctx, tx, effectForContract(commandID+":resume", EffectWaitCleared, *contract, contract.ConsumerAgentID, contract.ConsumerNodeID, now)); err != nil {
			return nil, err
		}
		if err := m.maybeCompleteNode(ctx, tx, contract.ProducerNodeID, commandID, now); err != nil {
			return nil, err
		}
	} else {
		if err := insertEffect(ctx, tx, effectForContract(commandID, EffectCorrectionRequested, *contract, contract.ProducerAgentID, contract.ProducerNodeID, now)); err != nil {
			return nil, err
		}
	}
	if err := m.syncCycles(ctx, tx, commandID, now); err != nil {
		return nil, err
	}
	if err := saveCommandResult(ctx, tx, commandID, operation, requestHash, contract, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("coordination: commit %s: %w", operation, err)
	}
	return contract, nil
}

func (m *Manager) SetWait(ctx context.Context, principal Principal, command WaitCommand) (*Contract, error) {
	contractID := strings.TrimSpace(command.ContractID)
	requestValue := struct {
		ContractID string `json:"contract_id"`
		Hard       bool   `json:"hard"`
	}{contractID, command.Hard}
	commandID, requestHash, err := commandIdentity(command.CommandID, "wait", principal, requestValue)
	if err != nil {
		return nil, err
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("coordination: begin wait update: %w", err)
	}
	defer tx.Rollback()
	var prior Contract
	if found, err := loadCommandResult(ctx, tx, commandID, "wait", requestHash, &prior); err != nil {
		return nil, err
	} else if found {
		return &prior, nil
	}
	contract, err := getContractTx(ctx, tx, contractID)
	if err != nil {
		return nil, err
	}
	if err := authorizeConsumer(principal, contract); err != nil {
		return nil, err
	}
	if contract.Status.Terminal() {
		return nil, fmt.Errorf("coordination: terminal contract %s cannot change wait state", contract.ID)
	}
	now := time.Now().UTC()
	contract.HardWait = command.Hard
	contract.UpdatedAt = now
	if _, err := tx.ExecContext(ctx, `UPDATE contracts SET hard_wait=?, updated_at=? WHERE id=?`, boolInt(command.Hard), formatTime(now), contract.ID); err != nil {
		return nil, fmt.Errorf("coordination: update wait state: %w", err)
	}
	effectType := EffectWaitCleared
	if command.Hard {
		effectType = EffectWaitDeclared
	}
	if err := insertEffect(ctx, tx, effectForContract(commandID, effectType, *contract, contract.ConsumerAgentID, contract.ConsumerNodeID, now)); err != nil {
		return nil, err
	}
	if err := m.syncCycles(ctx, tx, commandID, now); err != nil {
		return nil, err
	}
	if err := saveCommandResult(ctx, tx, commandID, "wait", requestHash, contract, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("coordination: commit wait update: %w", err)
	}
	return contract, nil
}

func (m *Manager) RequestNodeCompletion(ctx context.Context, principal Principal, command CompletionCommand) (*CompletionResult, error) {
	command.NodeID = strings.TrimSpace(command.NodeID)
	if principal.Role != RoleAdmin && (principal.AgentID == "" || !principalHasNode(principal, command.NodeID)) {
		return nil, fmt.Errorf("coordination: principal cannot complete node %s", command.NodeID)
	}
	commandID, requestHash, err := commandIdentity(command.CommandID, "complete-node", principal, command)
	if err != nil {
		return nil, err
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("coordination: begin completion request: %w", err)
	}
	defer tx.Rollback()
	var prior CompletionResult
	if found, err := loadCommandResult(ctx, tx, commandID, "complete-node", requestHash, &prior); err != nil {
		return nil, err
	} else if found {
		return &prior, nil
	}
	pending, err := pendingProducerContracts(ctx, tx, command.NodeID)
	if err != nil {
		return nil, err
	}
	result := CompletionResult{Ready: len(pending) == 0, PendingContracts: pending}
	now := time.Now().UTC()
	if !result.Ready {
		_, err = tx.ExecContext(ctx, `INSERT INTO completion_intents(node_id, agent_id, metadata_json, command_id, created_at)
            VALUES (?, ?, ?, ?, ?) ON CONFLICT(node_id) DO UPDATE SET agent_id=excluded.agent_id,
            metadata_json=excluded.metadata_json, command_id=excluded.command_id, created_at=excluded.created_at`,
			command.NodeID, principal.AgentID, nullableJSON(command.Metadata), commandID, formatTime(now))
		if err != nil {
			return nil, fmt.Errorf("coordination: persist completion intent: %w", err)
		}
		_, err = tx.ExecContext(ctx, `UPDATE contracts SET completion_pending=1 WHERE producer_node_id=? AND status NOT IN (?, ?, ?, ?)`,
			command.NodeID, StatusFulfilled, StatusFailed, StatusExpired, StatusCancelled)
		if err != nil {
			return nil, fmt.Errorf("coordination: mark completion pending: %w", err)
		}
	}
	if err := saveCommandResult(ctx, tx, commandID, "complete-node", requestHash, result, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("coordination: commit completion request: %w", err)
	}
	return &result, nil
}

func (m *Manager) Cancel(ctx context.Context, principal Principal, command TerminalCommand) (*Contract, error) {
	return m.terminate(ctx, principal, "cancel", StatusCancelled, command)
}

func (m *Manager) Fail(ctx context.Context, principal Principal, command TerminalCommand) (*Contract, error) {
	return m.terminate(ctx, principal, "fail", StatusFailed, command)
}

func (m *Manager) terminate(ctx context.Context, principal Principal, operation string, status Status, command TerminalCommand) (*Contract, error) {
	command.ContractID = strings.TrimSpace(command.ContractID)
	command.Reason = strings.TrimSpace(command.Reason)
	commandID, requestHash, err := commandIdentity(command.CommandID, operation, principal, command)
	if err != nil {
		return nil, err
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("coordination: begin %s: %w", operation, err)
	}
	defer tx.Rollback()
	var prior Contract
	if found, err := loadCommandResult(ctx, tx, commandID, operation, requestHash, &prior); err != nil {
		return nil, err
	} else if found {
		return &prior, nil
	}
	contract, err := getContractTx(ctx, tx, command.ContractID)
	if err != nil {
		return nil, err
	}
	if principal.Role != RoleAdmin {
		if status == StatusCancelled {
			err = authorizeConsumer(principal, contract)
		} else {
			err = authorizeProducer(principal, contract)
		}
		if err != nil {
			return nil, err
		}
	}
	if contract.Status.Terminal() {
		return nil, fmt.Errorf("coordination: contract %s is already terminal", contract.ID)
	}
	now := time.Now().UTC()
	previous := contract.Status
	contract.Status = status
	contract.HardWait = false
	contract.TerminalReason = command.Reason
	contract.UpdatedAt = now
	if err := supersedePendingEffects(ctx, tx, contract.ID,
		EffectContractRequested, EffectContractProduced, EffectCorrectionRequested,
		EffectWaitDeclared, EffectWaitCleared); err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `UPDATE contracts SET status=?, hard_wait=0, terminal_reason=?, updated_at=? WHERE id=?`,
		status, nullableString(command.Reason), formatTime(now), contract.ID)
	if err != nil {
		return nil, fmt.Errorf("coordination: terminate contract: %w", err)
	}
	if err := insertTransition(ctx, tx, contract.ID, previous, status, contract.Revision, principalIdentity(principal), command.Reason, now); err != nil {
		return nil, err
	}
	if err := insertEffect(ctx, tx, effectForContract(commandID, EffectContractTerminal, *contract, contract.ConsumerAgentID, contract.ConsumerNodeID, now)); err != nil {
		return nil, err
	}
	if err := m.maybeCompleteNode(ctx, tx, contract.ProducerNodeID, commandID, now); err != nil {
		return nil, err
	}
	if err := m.syncCycles(ctx, tx, commandID, now); err != nil {
		return nil, err
	}
	if err := saveCommandResult(ctx, tx, commandID, operation, requestHash, contract, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("coordination: commit %s: %w", operation, err)
	}
	return contract, nil
}

func (m *Manager) Fetch(ctx context.Context, principal Principal, contractID string, revision int64) (*Contract, []byte, error) {
	contract, err := m.Get(ctx, principal, contractID)
	if err != nil {
		return nil, nil, err
	}
	if revision == 0 {
		revision = contract.Revision
	}
	var digest string
	err = m.db.QueryRowContext(ctx, `SELECT digest FROM contract_revisions WHERE contract_id=? AND revision=?`, contract.ID, revision).Scan(&digest)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, fmt.Errorf("coordination: artifact revision %d not found for %s", revision, contract.ID)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("coordination: resolve artifact: %w", err)
	}
	content, err := readObject(m.objectDir, digest)
	if err != nil {
		return nil, nil, err
	}
	return contract, content, nil
}

func (m *Manager) Get(ctx context.Context, principal Principal, contractID string) (*Contract, error) {
	contract, err := getContractDB(ctx, m.db, strings.TrimSpace(contractID))
	if err != nil {
		return nil, err
	}
	if err := authorizeRead(principal, contract); err != nil {
		return nil, err
	}
	return contract, nil
}

func (m *Manager) List(ctx context.Context, principal Principal) ([]Contract, error) {
	query := `SELECT ` + contractColumns + ` FROM contracts`
	var args []any
	if principal.Role == RoleAgent {
		query += ` WHERE producer_agent_id=? OR consumer_agent_id=?`
		args = append(args, principal.AgentID, principal.AgentID)
	} else if principal.Role != RoleAdmin && principal.Role != RoleRunner {
		return nil, fmt.Errorf("coordination: invalid principal role")
	}
	query += ` ORDER BY created_at, id`
	rows, err := m.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("coordination: list contracts: %w", err)
	}
	defer rows.Close()
	var contracts []Contract
	for rows.Next() {
		contract, err := scanContract(rows)
		if err != nil {
			return nil, err
		}
		contracts = append(contracts, *contract)
	}
	return contracts, rows.Err()
}

func (m *Manager) PendingEffects(ctx context.Context, limit int) ([]Effect, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := m.db.QueryContext(ctx, `SELECT effect_id, contract_id, effect_type, target_agent_id,
        target_node_id, payload_json, attempts, created_at, next_attempt_at FROM effects
        WHERE state='pending' AND next_attempt_at<=? ORDER BY created_at, effect_id LIMIT ?`, formatTime(time.Now().UTC()), limit)
	if err != nil {
		return nil, fmt.Errorf("coordination: list pending effects: %w", err)
	}
	defer rows.Close()
	var effects []Effect
	for rows.Next() {
		var effect Effect
		var contractID, targetAgent, targetNode, payload sql.NullString
		var createdAt, nextAttempt string
		if err := rows.Scan(&effect.ID, &contractID, &effect.Type, &targetAgent, &targetNode, &payload, &effect.Attempts, &createdAt, &nextAttempt); err != nil {
			return nil, fmt.Errorf("coordination: scan effect: %w", err)
		}
		effect.ContractID = contractID.String
		effect.TargetAgentID = targetAgent.String
		effect.TargetNodeID = targetNode.String
		if payload.Valid {
			effect.Payload = json.RawMessage(payload.String)
		}
		effect.CreatedAt, _ = parseTime(createdAt)
		effect.NextAttemptAt, _ = parseTime(nextAttempt)
		effects = append(effects, effect)
	}
	return effects, rows.Err()
}

// AcknowledgedNodeCompletionEffects returns durable completion receipts that
// a runtime has already acknowledged after applying them. A composing runtime
// can use these receipts to repair a regressed local task projection on restart.
func (m *Manager) AcknowledgedNodeCompletionEffects(ctx context.Context) ([]Effect, error) {
	rows, err := m.db.QueryContext(ctx, `SELECT effect_id, contract_id, effect_type, target_agent_id,
        target_node_id, payload_json, attempts, created_at, next_attempt_at FROM effects
        WHERE state='acknowledged' AND effect_type=? ORDER BY created_at, effect_id`, EffectNodeCompletionReady)
	if err != nil {
		return nil, fmt.Errorf("coordination: list acknowledged node completions: %w", err)
	}
	defer rows.Close()
	var effects []Effect
	for rows.Next() {
		var effect Effect
		var contractID, targetAgent, targetNode, payload sql.NullString
		var createdAt, nextAttempt string
		if err := rows.Scan(&effect.ID, &contractID, &effect.Type, &targetAgent, &targetNode, &payload, &effect.Attempts, &createdAt, &nextAttempt); err != nil {
			return nil, fmt.Errorf("coordination: scan acknowledged node completion: %w", err)
		}
		effect.ContractID = contractID.String
		effect.TargetAgentID = targetAgent.String
		effect.TargetNodeID = targetNode.String
		if payload.Valid {
			effect.Payload = json.RawMessage(payload.String)
		}
		effect.CreatedAt, _ = parseTime(createdAt)
		effect.NextAttemptAt, _ = parseTime(nextAttempt)
		effects = append(effects, effect)
	}
	return effects, rows.Err()
}

func (m *Manager) AckEffect(ctx context.Context, effectID string) error {
	result, err := m.db.ExecContext(ctx, `UPDATE effects SET state='acknowledged', acknowledged_at=? WHERE effect_id=? AND state='pending'`, formatTime(time.Now().UTC()), strings.TrimSpace(effectID))
	if err != nil {
		return fmt.Errorf("coordination: acknowledge effect: %w", err)
	}
	if count, _ := result.RowsAffected(); count == 0 {
		return fmt.Errorf("coordination: pending effect %q not found", effectID)
	}
	return nil
}

func (m *Manager) FailEffect(ctx context.Context, effectID string, nextAttempt time.Time, deliveryErr error) error {
	message := "delivery failed"
	if deliveryErr != nil {
		message = deliveryErr.Error()
	}
	_, err := m.db.ExecContext(ctx, `UPDATE effects SET attempts=attempts+1, last_error=?, next_attempt_at=?
        WHERE effect_id=? AND state='pending'`, message, formatTime(nextAttempt.UTC()), strings.TrimSpace(effectID))
	if err != nil {
		return fmt.Errorf("coordination: defer effect: %w", err)
	}
	return nil
}

func (m *Manager) Snapshot(ctx context.Context) (*Snapshot, error) {
	contracts, err := m.List(ctx, Principal{Role: RoleAdmin})
	if err != nil {
		return nil, err
	}
	effects, err := m.PendingEffects(ctx, 500)
	if err != nil {
		return nil, err
	}
	cycles, err := m.activeCycles(ctx)
	if err != nil {
		return nil, err
	}
	var terms []ContractTerms
	for _, contract := range contracts {
		contractTerms, listErr := m.listTerms(ctx, contract.ID)
		if listErr != nil {
			return nil, listErr
		}
		terms = append(terms, contractTerms...)
	}
	resolutions, err := m.ListResolutions(ctx)
	if err != nil {
		return nil, err
	}
	return &Snapshot{Contracts: contracts, Terms: terms, Resolutions: resolutions, PendingEffects: effects, Cycles: cycles}, nil
}

func (m *Manager) ExpireDue(ctx context.Context, now time.Time) (int, error) {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("coordination: begin expiry: %w", err)
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id FROM contracts WHERE expires_at IS NOT NULL AND expires_at<=?
        AND status NOT IN (?, ?, ?, ?)`, formatTime(now.UTC()), StatusFulfilled, StatusFailed, StatusExpired, StatusCancelled)
	if err != nil {
		return 0, fmt.Errorf("coordination: query expired contracts: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		contract, err := getContractTx(ctx, tx, id)
		if err != nil {
			return 0, err
		}
		previous := contract.Status
		contract.Status = StatusExpired
		contract.HardWait = false
		contract.TerminalReason = "contract expired"
		contract.UpdatedAt = now.UTC()
		if _, err := tx.ExecContext(ctx, `UPDATE contracts SET status=?, hard_wait=0, terminal_reason=?, updated_at=? WHERE id=?`,
			StatusExpired, contract.TerminalReason, formatTime(now.UTC()), id); err != nil {
			return 0, err
		}
		if err := insertTransition(ctx, tx, id, previous, StatusExpired, contract.Revision, "kernel", contract.TerminalReason, now.UTC()); err != nil {
			return 0, err
		}
		if err := insertEffect(ctx, tx, effectForContract("expiry:"+id, EffectContractTerminal, *contract, contract.ConsumerAgentID, contract.ConsumerNodeID, now.UTC())); err != nil {
			return 0, err
		}
		if err := m.maybeCompleteNode(ctx, tx, contract.ProducerNodeID, "expiry:"+id, now.UTC()); err != nil {
			return 0, err
		}
	}
	if err := m.syncCycles(ctx, tx, "expiry", now.UTC()); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("coordination: commit expiry: %w", err)
	}
	return len(ids), nil
}

func (m *Manager) maybeCompleteNode(ctx context.Context, tx *sql.Tx, nodeID, commandID string, now time.Time) error {
	pending, err := pendingProducerContracts(ctx, tx, nodeID)
	if err != nil || len(pending) > 0 {
		return err
	}
	var agentID, metadataJSON, intentCommand string
	err = tx.QueryRowContext(ctx, `SELECT agent_id, COALESCE(metadata_json, ''), command_id FROM completion_intents WHERE node_id=?`, nodeID).Scan(&agentID, &metadataJSON, &intentCommand)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("coordination: read completion intent: %w", err)
	}
	payload, _ := json.Marshal(map[string]any{"node_id": nodeID, "agent_id": agentID, "metadata": json.RawMessage(metadataJSON)})
	effect := Effect{
		ID:            effectID(commandID+":"+intentCommand, EffectNodeCompletionReady, nodeID),
		Type:          EffectNodeCompletionReady,
		TargetAgentID: agentID,
		TargetNodeID:  nodeID,
		Payload:       payload,
		CreatedAt:     now,
		NextAttemptAt: now,
	}
	if err := insertEffect(ctx, tx, effect); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM completion_intents WHERE node_id=?`, nodeID); err != nil {
		return fmt.Errorf("coordination: clear completion intent: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE contracts SET completion_pending=0 WHERE producer_node_id=?`, nodeID); err != nil {
		return fmt.Errorf("coordination: clear completion markers: %w", err)
	}
	return nil
}

func pendingProducerContracts(ctx context.Context, tx *sql.Tx, nodeID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id FROM contracts WHERE producer_node_id=? AND status NOT IN (?, ?, ?, ?) ORDER BY id`,
		nodeID, StatusFulfilled, StatusFailed, StatusExpired, StatusCancelled)
	if err != nil {
		return nil, fmt.Errorf("coordination: query pending producer contracts: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (m *Manager) syncCycles(ctx context.Context, tx *sql.Tx, commandID string, now time.Time) error {
	rows, err := tx.QueryContext(ctx, `SELECT id, consumer_node_id, producer_node_id FROM contracts WHERE hard_wait=1
        AND status NOT IN (?, ?, ?, ?)`, StatusFulfilled, StatusFailed, StatusExpired, StatusCancelled)
	if err != nil {
		return fmt.Errorf("coordination: load wait graph: %w", err)
	}
	var edges []waitEdge
	for rows.Next() {
		var edge waitEdge
		if err := rows.Scan(&edge.contractID, &edge.from, &edge.to); err != nil {
			rows.Close()
			return err
		}
		edges = append(edges, edge)
	}
	rows.Close()
	cycles := detectCycles(edges)
	active := make(map[string]bool)
	existing := make(map[string]bool)
	existingRows, err := tx.QueryContext(ctx, `SELECT cycle_id FROM cycles WHERE active=1`)
	if err != nil {
		return fmt.Errorf("coordination: read active cycles: %w", err)
	}
	for existingRows.Next() {
		var id string
		_ = existingRows.Scan(&id)
		existing[id] = true
	}
	existingRows.Close()
	if _, err := tx.ExecContext(ctx, `UPDATE cycles SET active=0, resolved_at=? WHERE active=1`, formatTime(now)); err != nil {
		return fmt.Errorf("coordination: reset cycle set: %w", err)
	}
	for _, cycle := range cycles {
		active[cycle.ID] = true
		cycle.DetectedAt = now
		nodesJSON, _ := json.Marshal(cycle.NodeIDs)
		contractsJSON, _ := json.Marshal(cycle.ContractIDs)
		_, err := tx.ExecContext(ctx, `INSERT INTO cycles(cycle_id, node_ids_json, contract_ids_json, active, detected_at, resolved_at)
            VALUES (?, ?, ?, 1, ?, NULL) ON CONFLICT(cycle_id) DO UPDATE SET node_ids_json=excluded.node_ids_json,
            contract_ids_json=excluded.contract_ids_json, active=1, resolved_at=NULL`, cycle.ID, nodesJSON, contractsJSON, formatTime(now))
		if err != nil {
			return fmt.Errorf("coordination: persist cycle: %w", err)
		}
		if !existing[cycle.ID] {
			payload, _ := json.Marshal(cycle)
			effect := Effect{ID: effectID(commandID, EffectCycleDetected, cycle.ID), Type: EffectCycleDetected, Payload: payload, CreatedAt: now, NextAttemptAt: now}
			if err := insertEffect(ctx, tx, effect); err != nil {
				return err
			}
		}
	}
	_ = active
	return nil
}

func (m *Manager) activeCycles(ctx context.Context) ([]Cycle, error) {
	rows, err := m.db.QueryContext(ctx, `SELECT cycle_id, node_ids_json, contract_ids_json, detected_at FROM cycles WHERE active=1 ORDER BY cycle_id`)
	if err != nil {
		return nil, fmt.Errorf("coordination: list cycles: %w", err)
	}
	defer rows.Close()
	var cycles []Cycle
	for rows.Next() {
		var cycle Cycle
		var nodesJSON, contractsJSON []byte
		var detectedAt string
		if err := rows.Scan(&cycle.ID, &nodesJSON, &contractsJSON, &detectedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(nodesJSON, &cycle.NodeIDs)
		_ = json.Unmarshal(contractsJSON, &cycle.ContractIDs)
		cycle.DetectedAt, _ = parseTime(detectedAt)
		cycles = append(cycles, cycle)
	}
	return cycles, rows.Err()
}

const contractColumns = `id, producer_agent_id, producer_node_id, consumer_agent_id, consumer_node_id,
details_json, status, revision, hard_wait, artifact_digest, artifact_size, artifact_media_type,
validation_json, correction_reason, terminal_reason, completion_pending, expires_at, created_at, updated_at`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanContract(row rowScanner) (*Contract, error) {
	var contract Contract
	var detailsJSON []byte
	var hardWait, completionPending int
	var artifactDigest, artifactMedia, validationJSON, correctionReason, terminalReason, expiresAt sql.NullString
	var artifactSize sql.NullInt64
	var createdAt, updatedAt string
	if err := row.Scan(&contract.ID, &contract.ProducerAgentID, &contract.ProducerNodeID,
		&contract.ConsumerAgentID, &contract.ConsumerNodeID, &detailsJSON, &contract.Status,
		&contract.Revision, &hardWait, &artifactDigest, &artifactSize, &artifactMedia,
		&validationJSON, &correctionReason, &terminalReason, &completionPending,
		&expiresAt, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("coordination: contract not found")
		}
		return nil, fmt.Errorf("coordination: scan contract: %w", err)
	}
	if err := json.Unmarshal(detailsJSON, &contract.Details); err != nil {
		return nil, fmt.Errorf("coordination: decode contract details: %w", err)
	}
	contract.HardWait = hardWait != 0
	contract.CompletionPending = completionPending != 0
	contract.CorrectionReason = correctionReason.String
	contract.TerminalReason = terminalReason.String
	contract.CreatedAt, _ = parseTime(createdAt)
	contract.UpdatedAt, _ = parseTime(updatedAt)
	if expiresAt.Valid {
		value, err := parseTime(expiresAt.String)
		if err == nil {
			contract.ExpiresAt = &value
		}
	}
	if artifactDigest.Valid {
		contract.Artifact = &Artifact{Digest: artifactDigest.String, Size: artifactSize.Int64, MediaType: artifactMedia.String, CreatedAt: contract.UpdatedAt}
	}
	if validationJSON.Valid && validationJSON.String != "" {
		var validation Validation
		if err := json.Unmarshal([]byte(validationJSON.String), &validation); err == nil {
			contract.Validation = &validation
		}
	}
	return &contract, nil
}

func getContractTx(ctx context.Context, tx *sql.Tx, id string) (*Contract, error) {
	return scanContract(tx.QueryRowContext(ctx, `SELECT `+contractColumns+` FROM contracts WHERE id=?`, id))
}

func getContractDB(ctx context.Context, db *sql.DB, id string) (*Contract, error) {
	return scanContract(db.QueryRowContext(ctx, `SELECT `+contractColumns+` FROM contracts WHERE id=?`, id))
}

func normalizeContract(contract *Contract) {
	contract.ID = strings.TrimSpace(contract.ID)
	contract.ProducerAgentID = strings.TrimSpace(contract.ProducerAgentID)
	contract.ProducerNodeID = strings.TrimSpace(contract.ProducerNodeID)
	contract.ConsumerAgentID = strings.TrimSpace(contract.ConsumerAgentID)
	contract.ConsumerNodeID = strings.TrimSpace(contract.ConsumerNodeID)
	contract.Details.Name = strings.TrimSpace(contract.Details.Name)
	contract.Details.Kind = strings.TrimSpace(contract.Details.Kind)
}

func validateRequestedContract(principal Principal, contract Contract) error {
	if contract.ID == "" || contract.ProducerAgentID == "" || contract.ProducerNodeID == "" || contract.ConsumerAgentID == "" || contract.ConsumerNodeID == "" {
		return fmt.Errorf("coordination: contract and producer/consumer agent/node identities are required")
	}
	if contract.Details.Name == "" || contract.Details.Kind == "" {
		return fmt.Errorf("coordination: contract name and kind are required")
	}
	if contract.ProducerNodeID == contract.ConsumerNodeID {
		return fmt.Errorf("coordination: producer and consumer nodes must differ")
	}
	if principal.Role != RoleAdmin {
		if principal.Role != RoleAgent || principal.AgentID != contract.ConsumerAgentID || !principalHasNode(principal, contract.ConsumerNodeID) {
			return fmt.Errorf("coordination: only the assigned consumer can request this contract")
		}
	}
	return nil
}

func authorizeProducer(principal Principal, contract *Contract) error {
	if principal.Role == RoleAdmin {
		return nil
	}
	if principal.Role != RoleAgent || principal.AgentID != contract.ProducerAgentID || !principalHasNode(principal, contract.ProducerNodeID) {
		return fmt.Errorf("coordination: principal is not the assigned producer for %s", contract.ID)
	}
	return nil
}

func authorizeConsumer(principal Principal, contract *Contract) error {
	if principal.Role == RoleAdmin || principal.Role == RoleRunner {
		return nil
	}
	if principal.Role != RoleAgent || principal.AgentID != contract.ConsumerAgentID || !principalHasNode(principal, contract.ConsumerNodeID) {
		return fmt.Errorf("coordination: principal is not the assigned consumer for %s", contract.ID)
	}
	return nil
}

func authorizeRead(principal Principal, contract *Contract) error {
	if principal.Role == RoleAdmin || principal.Role == RoleRunner {
		return nil
	}
	if principal.Role == RoleAgent && (principal.AgentID == contract.ProducerAgentID || principal.AgentID == contract.ConsumerAgentID) {
		return nil
	}
	return fmt.Errorf("coordination: principal cannot read contract %s", contract.ID)
}

func principalHasNode(principal Principal, nodeID string) bool {
	for _, candidate := range principal.NodeIDs {
		if strings.TrimSpace(candidate) == nodeID {
			return true
		}
	}
	return false
}

func principalIdentity(principal Principal) string {
	if principal.AgentID != "" {
		return principal.AgentID
	}
	return string(principal.Role)
}

func commandIdentity(supplied, operation string, principal Principal, request any) (string, string, error) {
	data, err := json.Marshal(struct {
		Operation string    `json:"operation"`
		Principal Principal `json:"principal"`
		Request   any       `json:"request"`
	}{operation, principal, request})
	if err != nil {
		return "", "", fmt.Errorf("coordination: encode command identity: %w", err)
	}
	sum := sha256.Sum256(data)
	requestHash := hex.EncodeToString(sum[:])
	supplied = strings.TrimSpace(supplied)
	if supplied == "" {
		supplied = "cmd_" + requestHash[:24]
	}
	return supplied, requestHash, nil
}

func loadCommandResult(ctx context.Context, tx *sql.Tx, commandID, operation, requestHash string, destination any) (bool, error) {
	var storedOperation, storedHash string
	var resultJSON []byte
	err := tx.QueryRowContext(ctx, `SELECT operation, request_hash, result_json FROM commands WHERE command_id=?`, commandID).Scan(&storedOperation, &storedHash, &resultJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("coordination: read command record: %w", err)
	}
	if storedOperation != operation || storedHash != requestHash {
		return false, fmt.Errorf("coordination: command ID %s was already used with different input", commandID)
	}
	if err := json.Unmarshal(resultJSON, destination); err != nil {
		return false, fmt.Errorf("coordination: decode prior command result: %w", err)
	}
	return true, nil
}

func saveCommandResult(ctx context.Context, tx *sql.Tx, commandID, operation, requestHash string, result any, now time.Time) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("coordination: encode command result: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO commands(command_id, operation, request_hash, result_json, created_at) VALUES (?, ?, ?, ?, ?)`,
		commandID, operation, requestHash, data, formatTime(now)); err != nil {
		return fmt.Errorf("coordination: persist command result: %w", err)
	}
	return nil
}

func insertTransition(ctx context.Context, tx *sql.Tx, contractID string, from, to Status, revision int64, actor, reason string, now time.Time) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO transitions(contract_id, from_status, to_status, revision, actor_id, reason, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)`, contractID, nullableString(string(from)), to, revision, actor, nullableString(reason), formatTime(now))
	if err != nil {
		return fmt.Errorf("coordination: persist transition: %w", err)
	}
	return nil
}

func effectForContract(seed string, effectType EffectType, contract Contract, targetAgentID, targetNodeID string, now time.Time) Effect {
	payload, _ := json.Marshal(contract)
	return Effect{
		ID:            effectID(seed, effectType, contract.ID),
		ContractID:    contract.ID,
		Type:          effectType,
		TargetAgentID: targetAgentID,
		TargetNodeID:  targetNodeID,
		Payload:       payload,
		CreatedAt:     now,
		NextAttemptAt: now,
	}
}

func effectID(seed string, effectType EffectType, subject string) string {
	sum := sha256.Sum256([]byte(seed + "\x00" + string(effectType) + "\x00" + subject))
	return "eff_" + hex.EncodeToString(sum[:12])
}

func insertEffect(ctx context.Context, tx *sql.Tx, effect Effect) error {
	_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO effects(effect_id, contract_id, effect_type, target_agent_id,
        target_node_id, payload_json, state, attempts, next_attempt_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?)`, effect.ID, nullableString(effect.ContractID), effect.Type,
		nullableString(effect.TargetAgentID), nullableString(effect.TargetNodeID), nullableJSON(effect.Payload),
		formatTime(effect.NextAttemptAt), formatTime(effect.CreatedAt))
	if err != nil {
		return fmt.Errorf("coordination: persist effect: %w", err)
	}
	return nil
}

func supersedePendingEffects(ctx context.Context, tx *sql.Tx, contractID string, effectTypes ...EffectType) error {
	if len(effectTypes) == 0 {
		return nil
	}
	placeholders := make([]string, len(effectTypes))
	args := make([]any, 0, len(effectTypes)+2)
	args = append(args, formatTime(time.Now().UTC()), contractID)
	for i, effectType := range effectTypes {
		placeholders[i] = "?"
		args = append(args, effectType)
	}
	query := `UPDATE effects SET state='acknowledged', acknowledged_at=? WHERE contract_id=? AND state='pending' AND effect_type IN (` + strings.Join(placeholders, ",") + `)`
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("coordination: supersede pending effects: %w", err)
	}
	return nil
}

func formatTime(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }

func formatOptionalTime(value *time.Time) any {
	if value == nil || value.IsZero() {
		return nil
	}
	return formatTime(*value)
}

func parseTime(value string) (time.Time, error) { return time.Parse(time.RFC3339Nano, value) }

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func nullableString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func nullableJSON(value json.RawMessage) any {
	if len(value) == 0 {
		return nil
	}
	return []byte(value)
}

func coalesce(value, fallback string) string {
	if strings.TrimSpace(value) != "" {
		return value
	}
	return fallback
}

func sortedUnique(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			set[value] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
