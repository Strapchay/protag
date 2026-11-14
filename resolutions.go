package coordination

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

func (m *Manager) DeclareStandstill(ctx context.Context, principal Principal, command StandstillCommand) (*Resolution, error) {
	command.ContractID = strings.TrimSpace(command.ContractID)
	command.Reason = strings.TrimSpace(command.Reason)
	command.Evidence = cleanStrings(command.Evidence)
	if command.ContractID == "" || command.Reason == "" {
		return nil, fmt.Errorf("coordination: contract ID and standstill reason are required")
	}
	commandID, requestHash, err := commandIdentity(command.CommandID, "declare-standstill", principal, command)
	if err != nil {
		return nil, err
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("coordination: begin standstill: %w", err)
	}
	defer tx.Rollback()
	var prior Resolution
	if found, err := loadCommandResult(ctx, tx, commandID, "declare-standstill", requestHash, &prior); err != nil {
		return nil, err
	} else if found {
		return &prior, nil
	}
	contract, err := getContractTx(ctx, tx, command.ContractID)
	if err != nil {
		return nil, err
	}
	if err := authorizeParticipant(principal, contract); err != nil {
		return nil, err
	}
	if contract.Status.Terminal() {
		return nil, fmt.Errorf("coordination: terminal contract %s cannot enter reconciliation", contract.ID)
	}
	terms, err := latestTermsTx(ctx, tx, contract.ID)
	if err != nil {
		return nil, err
	}
	if terms.Revision != command.TermsRevision {
		return nil, fmt.Errorf("coordination: standstill terms revision conflict: requested %d, current %d", command.TermsRevision, terms.Revision)
	}
	if terms.Status == TermsAgreed {
		return nil, fmt.Errorf("coordination: terms revision %d is already agreed", terms.Revision)
	}
	if !command.TerminalIncompatibility && int(terms.Revision) < m.options.MaxNegotiationRounds {
		return nil, fmt.Errorf("coordination: negotiation has not reached the %d-round limit; counter or accept terms", m.options.MaxNegotiationRounds)
	}
	resolutionID := resolutionIdentity(contract.ID, terms.Revision)
	var existing Resolution
	scanErr := scanResolution(tx.QueryRowContext(ctx, `SELECT `+resolutionColumns+` FROM resolutions WHERE resolution_id=?`, resolutionID), &existing)
	if scanErr == nil {
		if err := saveCommandResult(ctx, tx, commandID, "declare-standstill", requestHash, existing, time.Now().UTC()); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return &existing, nil
	}
	if !errors.Is(scanErr, sql.ErrNoRows) {
		return nil, scanErr
	}
	now := time.Now().UTC()
	resolution := Resolution{
		ID: resolutionID, ContractID: contract.ID, TermsRevision: terms.Revision, Status: ResolutionPending,
		Reason: command.Reason, Evidence: command.Evidence, TerminalIncompatibility: command.TerminalIncompatibility,
		AffectedNodeIDs: cleanStrings([]string{contract.ProducerNodeID, contract.ConsumerNodeID}), SemanticAttempts: 1,
		MaxProcessAttempts: m.options.MaxProcessAttempts, MaxRepairAttempts: m.options.MaxRepairAttempts,
		MaxContextExpansions: m.options.MaxContextExpansions, CreatedAt: now, UpdatedAt: now,
	}
	if err := insertResolution(ctx, tx, resolution); err != nil {
		return nil, err
	}
	payload, _ := json.Marshal(resolution)
	if err := insertEffect(ctx, tx, Effect{ID: effectID(commandID, EffectResolutionRequested, resolution.ID),
		ContractID: contract.ID, Type: EffectResolutionRequested, Payload: payload, CreatedAt: now, NextAttemptAt: now}); err != nil {
		return nil, err
	}
	if err := saveCommandResult(ctx, tx, commandID, "declare-standstill", requestHash, resolution, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("coordination: commit standstill: %w", err)
	}
	return &resolution, nil
}

func (m *Manager) ClaimNextResolution(ctx context.Context, leaseFor time.Duration) (*Resolution, error) {
	if leaseFor <= 0 {
		leaseFor = 2 * time.Minute
	}
	if err := m.ExpireResolutionLeases(ctx, time.Now().UTC()); err != nil {
		return nil, err
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var resolution Resolution
	err = scanResolution(tx.QueryRowContext(ctx, `SELECT `+resolutionColumns+` FROM resolutions
        WHERE status IN (?, ?, ?) AND (lease_expires_at IS NULL OR lease_expires_at<=?)
        ORDER BY created_at, resolution_id LIMIT 1`, ResolutionPending, ResolutionValidated, ResolutionApplying, formatTime(time.Now().UTC())), &resolution)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if resolution.ProcessAttempts >= resolution.MaxProcessAttempts && resolution.Status == ResolutionPending {
		if err := setAwaitingUserTx(ctx, tx, &resolution, "coordinator process retry budget exhausted", time.Now().UTC()); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	now := time.Now().UTC()
	token, err := randomToken()
	if err != nil {
		return nil, err
	}
	expires := now.Add(leaseFor)
	if resolution.Status == ResolutionPending {
		resolution.Status = ResolutionRunning
		resolution.ProcessAttempts++
	}
	resolution.LeaseToken = token
	resolution.LeaseExpiresAt = &expires
	resolution.UpdatedAt = now
	if _, err := tx.ExecContext(ctx, `UPDATE resolutions SET status=?, process_attempts=?, lease_token=?,
        lease_expires_at=?, updated_at=? WHERE resolution_id=?`, resolution.Status, resolution.ProcessAttempts,
		token, formatTime(expires), formatTime(now), resolution.ID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &resolution, nil
}

func (m *Manager) RenewResolution(ctx context.Context, resolutionID, leaseToken string, leaseFor time.Duration) error {
	if leaseFor <= 0 {
		leaseFor = 2 * time.Minute
	}
	now := time.Now().UTC()
	result, err := m.db.ExecContext(ctx, `UPDATE resolutions SET lease_expires_at=?, updated_at=?
        WHERE resolution_id=? AND lease_token=? AND status IN (?, ?)`, formatTime(now.Add(leaseFor)), formatTime(now),
		strings.TrimSpace(resolutionID), strings.TrimSpace(leaseToken), ResolutionRunning, ResolutionApplying)
	if err != nil {
		return fmt.Errorf("coordination: renew resolution lease: %w", err)
	}
	if count, _ := result.RowsAffected(); count == 0 {
		return fmt.Errorf("coordination: resolution lease is not active")
	}
	return nil
}

func (m *Manager) RecordResolutionInput(ctx context.Context, resolutionID, leaseToken string, content []byte) (*Resolution, error) {
	return m.recordResolutionArtifact(ctx, resolutionID, leaseToken, content, true, "")
}

func (m *Manager) StageResolutionOutput(ctx context.Context, resolutionID, leaseToken string, content []byte, rationale string) (*Resolution, error) {
	return m.recordResolutionArtifact(ctx, resolutionID, leaseToken, content, false, strings.TrimSpace(rationale))
}

func (m *Manager) recordResolutionArtifact(ctx context.Context, resolutionID, leaseToken string, content []byte, input bool, rationale string) (*Resolution, error) {
	if len(content) == 0 {
		return nil, fmt.Errorf("coordination: resolution artifact is empty")
	}
	digest, size, err := storeObject(m.objectDir, content)
	if err != nil {
		return nil, err
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	resolution, err := resolutionForLease(ctx, tx, resolutionID, leaseToken)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if input {
		resolution.Input = &Artifact{Digest: digest, Size: size, MediaType: "application/json", CreatedAt: now}
		_, err = tx.ExecContext(ctx, `UPDATE resolutions SET input_digest=?, input_size=?, updated_at=? WHERE resolution_id=?`, digest, size, formatTime(now), resolution.ID)
	} else {
		resolution.Output = &Artifact{Digest: digest, Size: size, MediaType: "application/json", CreatedAt: now}
		resolution.Status = ResolutionValidated
		resolution.Rationale = rationale
		resolution.ValidationError = ""
		_, err = tx.ExecContext(ctx, `UPDATE resolutions SET output_digest=?, output_size=?, status=?, rationale=?,
            validation_error=NULL, updated_at=? WHERE resolution_id=?`, digest, size, ResolutionValidated, nullableString(rationale), formatTime(now), resolution.ID)
	}
	if err != nil {
		return nil, fmt.Errorf("coordination: record resolution artifact: %w", err)
	}
	resolution.UpdatedAt = now
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return resolution, nil
}

func (m *Manager) RecordResolutionValidationFailure(ctx context.Context, resolutionID, leaseToken string, validationErr error) (*Resolution, error) {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	resolution, err := resolutionForLease(ctx, tx, resolutionID, leaseToken)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	message := "resolution output failed validation"
	if validationErr != nil {
		message = validationErr.Error()
	}
	resolution.ValidationError = message
	if resolution.RepairAttempts >= resolution.MaxRepairAttempts {
		if err := setAwaitingUserTx(ctx, tx, resolution, "resolution artifact remained invalid after bounded repair", now); err != nil {
			return nil, err
		}
	} else {
		resolution.RepairAttempts++
		resolution.UpdatedAt = now
		if _, err := tx.ExecContext(ctx, `UPDATE resolutions SET repair_attempts=?, validation_error=?, updated_at=? WHERE resolution_id=?`,
			resolution.RepairAttempts, message, formatTime(now), resolution.ID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return resolution, nil
}

func (m *Manager) RecordContextExpansion(ctx context.Context, resolutionID, leaseToken string) (*Resolution, error) {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	resolution, err := resolutionForLease(ctx, tx, resolutionID, leaseToken)
	if err != nil {
		return nil, err
	}
	if resolution.ContextExpansions >= resolution.MaxContextExpansions {
		return nil, fmt.Errorf("coordination: resolution context expansion budget exhausted")
	}
	resolution.ContextExpansions++
	resolution.UpdatedAt = time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `UPDATE resolutions SET context_expansions=?, updated_at=? WHERE resolution_id=?`,
		resolution.ContextExpansions, formatTime(resolution.UpdatedAt), resolution.ID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return resolution, nil
}

func (m *Manager) BeginResolutionApply(ctx context.Context, resolutionID, leaseToken string) (*Resolution, error) {
	return m.setResolutionStatusForLease(ctx, resolutionID, leaseToken, ResolutionValidated, ResolutionApplying)
}

func (m *Manager) FinalizeResolution(ctx context.Context, resolutionID, leaseToken string, terms ContractTerms) (*Resolution, error) {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	resolution, err := resolutionForLease(ctx, tx, resolutionID, leaseToken)
	if err != nil {
		return nil, err
	}
	if resolution.Status != ResolutionApplying {
		return nil, fmt.Errorf("coordination: resolution %s is not applying", resolution.ID)
	}
	contract, err := getContractTx(ctx, tx, resolution.ContractID)
	if err != nil {
		return nil, err
	}
	latest, err := latestTermsTx(ctx, tx, contract.ID)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	terms.ContractID = contract.ID
	terms.Revision = latest.Revision + 1
	terms.ParentRevision = latest.Revision
	terms.ProposerAgentID = "coordinator"
	terms.Status = TermsAgreed
	terms.AcceptedBy = []string{contract.ConsumerAgentID, contract.ProducerAgentID}
	terms.CreatedAt = now
	if terms.Details.Name == "" || terms.Details.Kind == "" {
		return nil, fmt.Errorf("coordination: reconciled terms name and kind are required")
	}
	if err := insertTerms(ctx, tx, terms); err != nil {
		return nil, err
	}
	for _, agentID := range terms.AcceptedBy {
		if _, err := tx.ExecContext(ctx, `INSERT INTO term_acceptances(contract_id, revision, agent_id, accepted_at) VALUES (?, ?, ?, ?)`,
			contract.ID, terms.Revision, agentID, formatTime(now)); err != nil {
			return nil, err
		}
	}
	details, _ := json.Marshal(terms.Details)
	if _, err := tx.ExecContext(ctx, `UPDATE contracts SET details_json=?, hard_wait=0, updated_at=? WHERE id=?`, details, formatTime(now), contract.ID); err != nil {
		return nil, err
	}
	resolution.Status = ResolutionResolved
	resolution.LeaseToken = ""
	resolution.LeaseExpiresAt = nil
	resolution.UpdatedAt = now
	if _, err := tx.ExecContext(ctx, `UPDATE resolutions SET status=?, lease_token=NULL, lease_expires_at=NULL,
        validation_error=NULL, terminal_reason=NULL, updated_at=? WHERE resolution_id=?`, ResolutionResolved, formatTime(now), resolution.ID); err != nil {
		return nil, err
	}
	payload, _ := json.Marshal(resolution)
	if err := insertEffect(ctx, tx, Effect{ID: effectID(resolution.ID, EffectResolutionResolved, resolution.ID), ContractID: contract.ID,
		Type: EffectResolutionResolved, Payload: payload, CreatedAt: now, NextAttemptAt: now}); err != nil {
		return nil, err
	}
	for _, pair := range [][2]string{{contract.ProducerAgentID, contract.ProducerNodeID}, {contract.ConsumerAgentID, contract.ConsumerNodeID}} {
		if err := insertEffect(ctx, tx, effectForTerms(resolution.ID+":"+pair[0], EffectTermsAgreed, terms, pair[0], pair[1], now)); err != nil {
			return nil, err
		}
		if err := insertEffect(ctx, tx, effectForContract(resolution.ID+":resume:"+pair[0], EffectWaitCleared, *contract, pair[0], pair[1], now)); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return resolution, nil
}

func (m *Manager) AwaitResolutionUser(ctx context.Context, resolutionID, leaseToken, reason string) (*Resolution, error) {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	resolution, err := resolutionForLease(ctx, tx, resolutionID, leaseToken)
	if err != nil {
		return nil, err
	}
	if err := setAwaitingUserTx(ctx, tx, resolution, strings.TrimSpace(reason), time.Now().UTC()); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return resolution, nil
}

// FailResolutionAttempt releases a failed coordinator process. A retry is
// made pending only while its persisted process budget remains; otherwise the
// resolution moves to explicit user review.
func (m *Manager) FailResolutionAttempt(ctx context.Context, resolutionID, leaseToken, reason string) (*Resolution, error) {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	resolution, err := resolutionForLease(ctx, tx, resolutionID, leaseToken)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	reason = strings.TrimSpace(reason)
	if resolution.ProcessAttempts >= resolution.MaxProcessAttempts {
		if err := setAwaitingUserTx(ctx, tx, resolution, reason, now); err != nil {
			return nil, err
		}
	} else {
		resolution.Status = ResolutionPending
		resolution.TerminalReason = reason
		resolution.LeaseToken = ""
		resolution.LeaseExpiresAt = nil
		resolution.UpdatedAt = now
		if _, err := tx.ExecContext(ctx, `UPDATE resolutions SET status=?, terminal_reason=?, lease_token=NULL,
            lease_expires_at=NULL, updated_at=? WHERE resolution_id=?`, ResolutionPending, nullableString(reason), formatTime(now), resolution.ID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return resolution, nil
}

func (m *Manager) ExpireResolutionLeases(ctx context.Context, now time.Time) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT `+resolutionColumns+` FROM resolutions WHERE status IN (?, ?, ?)
		AND lease_expires_at IS NOT NULL AND lease_expires_at<=?`, ResolutionRunning, ResolutionValidated, ResolutionApplying, formatTime(now.UTC()))
	if err != nil {
		return err
	}
	var resolutions []Resolution
	for rows.Next() {
		var resolution Resolution
		if err := scanResolution(rows, &resolution); err != nil {
			rows.Close()
			return err
		}
		resolutions = append(resolutions, resolution)
	}
	rows.Close()
	for i := range resolutions {
		resolution := &resolutions[i]
		if resolution.Status == ResolutionValidated || resolution.Status == ResolutionApplying {
			_, err = tx.ExecContext(ctx, `UPDATE resolutions SET lease_token=NULL, lease_expires_at=NULL, updated_at=? WHERE resolution_id=?`, formatTime(now), resolution.ID)
		} else if resolution.ProcessAttempts < resolution.MaxProcessAttempts {
			resolution.Status = ResolutionPending
			_, err = tx.ExecContext(ctx, `UPDATE resolutions SET status=?, lease_token=NULL, lease_expires_at=NULL, updated_at=? WHERE resolution_id=?`, ResolutionPending, formatTime(now), resolution.ID)
		} else {
			err = setAwaitingUserTx(ctx, tx, resolution, "coordinator lease expired after bounded process retries", now)
		}
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// RecoverResolutionLeases releases work owned by a prior kernel process. The
// kernel is the sole runtime owner and calls this once during startup before a
// reconciliation worker can be started.
func (m *Manager) RecoverResolutionLeases(ctx context.Context, now time.Time) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT `+resolutionColumns+` FROM resolutions WHERE status IN (?, ?, ?)
		AND lease_token IS NOT NULL`, ResolutionRunning, ResolutionValidated, ResolutionApplying)
	if err != nil {
		return err
	}
	var resolutions []Resolution
	for rows.Next() {
		var resolution Resolution
		if err := scanResolution(rows, &resolution); err != nil {
			rows.Close()
			return err
		}
		resolutions = append(resolutions, resolution)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	now = now.UTC()
	for i := range resolutions {
		resolution := &resolutions[i]
		switch resolution.Status {
		case ResolutionValidated, ResolutionApplying:
			_, err = tx.ExecContext(ctx, `UPDATE resolutions SET lease_token=NULL, lease_expires_at=NULL, updated_at=? WHERE resolution_id=?`, formatTime(now), resolution.ID)
		case ResolutionRunning:
			if resolution.ProcessAttempts < resolution.MaxProcessAttempts {
				_, err = tx.ExecContext(ctx, `UPDATE resolutions SET status=?, lease_token=NULL, lease_expires_at=NULL, updated_at=? WHERE resolution_id=?`, ResolutionPending, formatTime(now), resolution.ID)
			} else {
				err = setAwaitingUserTx(ctx, tx, resolution, "coordinator process ended before completing its bounded attempt", now)
			}
		}
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (m *Manager) GetResolution(ctx context.Context, resolutionID string) (*Resolution, error) {
	var resolution Resolution
	if err := scanResolution(m.db.QueryRowContext(ctx, `SELECT `+resolutionColumns+` FROM resolutions WHERE resolution_id=?`, strings.TrimSpace(resolutionID)), &resolution); err != nil {
		return nil, err
	}
	return &resolution, nil
}

func (m *Manager) ListResolutions(ctx context.Context) ([]Resolution, error) {
	rows, err := m.db.QueryContext(ctx, `SELECT `+resolutionColumns+` FROM resolutions ORDER BY created_at, resolution_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Resolution
	for rows.Next() {
		var resolution Resolution
		if err := scanResolution(rows, &resolution); err != nil {
			return nil, err
		}
		out = append(out, resolution)
	}
	return out, rows.Err()
}

func (m *Manager) ReadResolutionArtifact(resolution *Resolution, output bool) ([]byte, error) {
	if resolution == nil {
		return nil, fmt.Errorf("coordination: resolution is required")
	}
	artifact := resolution.Input
	if output {
		artifact = resolution.Output
	}
	if artifact == nil {
		return nil, fmt.Errorf("coordination: resolution artifact is unavailable")
	}
	return readObject(m.objectDir, artifact.Digest)
}

const resolutionColumns = `resolution_id, contract_id, terms_revision, status, reason, evidence_json,
terminal_incompatibility, affected_node_ids_json, semantic_attempts, process_attempts, repair_attempts,
context_expansions, max_process_attempts, max_repair_attempts, max_context_expansions, lease_token,
lease_expires_at, input_digest, input_size, output_digest, output_size, validation_error, rationale,
terminal_reason, created_at, updated_at`

func scanResolution(row rowScanner, resolution *Resolution) error {
	var evidence, affected []byte
	var terminal int
	var leaseToken, leaseExpires, inputDigest, outputDigest, validationErr, rationale, terminalReason sql.NullString
	var inputSize, outputSize sql.NullInt64
	var created, updated string
	err := row.Scan(&resolution.ID, &resolution.ContractID, &resolution.TermsRevision, &resolution.Status,
		&resolution.Reason, &evidence, &terminal, &affected, &resolution.SemanticAttempts,
		&resolution.ProcessAttempts, &resolution.RepairAttempts, &resolution.ContextExpansions,
		&resolution.MaxProcessAttempts, &resolution.MaxRepairAttempts, &resolution.MaxContextExpansions,
		&leaseToken, &leaseExpires, &inputDigest, &inputSize, &outputDigest, &outputSize,
		&validationErr, &rationale, &terminalReason, &created, &updated)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return sql.ErrNoRows
		}
		return fmt.Errorf("coordination: scan resolution: %w", err)
	}
	_ = json.Unmarshal(evidence, &resolution.Evidence)
	_ = json.Unmarshal(affected, &resolution.AffectedNodeIDs)
	resolution.TerminalIncompatibility = terminal != 0
	resolution.LeaseToken = leaseToken.String
	resolution.ValidationError = validationErr.String
	resolution.Rationale = rationale.String
	resolution.TerminalReason = terminalReason.String
	resolution.CreatedAt, _ = parseTime(created)
	resolution.UpdatedAt, _ = parseTime(updated)
	if leaseExpires.Valid {
		value, _ := parseTime(leaseExpires.String)
		resolution.LeaseExpiresAt = &value
	}
	if inputDigest.Valid {
		resolution.Input = &Artifact{Digest: inputDigest.String, Size: inputSize.Int64, MediaType: "application/json", CreatedAt: resolution.UpdatedAt}
	}
	if outputDigest.Valid {
		resolution.Output = &Artifact{Digest: outputDigest.String, Size: outputSize.Int64, MediaType: "application/json", CreatedAt: resolution.UpdatedAt}
	}
	return nil
}

func insertResolution(ctx context.Context, tx *sql.Tx, resolution Resolution) error {
	evidence, _ := json.Marshal(resolution.Evidence)
	affected, _ := json.Marshal(resolution.AffectedNodeIDs)
	_, err := tx.ExecContext(ctx, `INSERT INTO resolutions(resolution_id, contract_id, terms_revision, status,
        reason, evidence_json, terminal_incompatibility, affected_node_ids_json, semantic_attempts,
        process_attempts, repair_attempts, context_expansions, max_process_attempts, max_repair_attempts,
        max_context_expansions, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		resolution.ID, resolution.ContractID, resolution.TermsRevision, resolution.Status, resolution.Reason,
		evidence, boolInt(resolution.TerminalIncompatibility), affected, resolution.SemanticAttempts,
		resolution.ProcessAttempts, resolution.RepairAttempts, resolution.ContextExpansions,
		resolution.MaxProcessAttempts, resolution.MaxRepairAttempts, resolution.MaxContextExpansions,
		formatTime(resolution.CreatedAt), formatTime(resolution.UpdatedAt))
	if err != nil {
		return fmt.Errorf("coordination: persist resolution: %w", err)
	}
	return nil
}

func resolutionForLease(ctx context.Context, tx *sql.Tx, resolutionID, leaseToken string) (*Resolution, error) {
	var resolution Resolution
	if err := scanResolution(tx.QueryRowContext(ctx, `SELECT `+resolutionColumns+` FROM resolutions WHERE resolution_id=?`, strings.TrimSpace(resolutionID)), &resolution); err != nil {
		return nil, err
	}
	if resolution.LeaseToken == "" || resolution.LeaseToken != strings.TrimSpace(leaseToken) {
		return nil, fmt.Errorf("coordination: resolution lease token does not match")
	}
	if resolution.LeaseExpiresAt != nil && time.Now().UTC().After(*resolution.LeaseExpiresAt) {
		return nil, fmt.Errorf("coordination: resolution lease expired")
	}
	return &resolution, nil
}

func (m *Manager) setResolutionStatusForLease(ctx context.Context, resolutionID, leaseToken string, from, to ResolutionStatus) (*Resolution, error) {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	resolution, err := resolutionForLease(ctx, tx, resolutionID, leaseToken)
	if err != nil {
		return nil, err
	}
	if resolution.Status != from {
		return nil, fmt.Errorf("coordination: resolution %s cannot transition from %s to %s", resolution.ID, resolution.Status, to)
	}
	resolution.Status = to
	resolution.UpdatedAt = time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `UPDATE resolutions SET status=?, updated_at=? WHERE resolution_id=?`, to, formatTime(resolution.UpdatedAt), resolution.ID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return resolution, nil
}

func setAwaitingUserTx(ctx context.Context, tx *sql.Tx, resolution *Resolution, reason string, now time.Time) error {
	if reason == "" {
		reason = "bounded reconciliation could not produce a safe resolution"
	}
	resolution.Status = ResolutionAwaitingUser
	resolution.TerminalReason = reason
	resolution.LeaseToken = ""
	resolution.LeaseExpiresAt = nil
	resolution.UpdatedAt = now
	if _, err := tx.ExecContext(ctx, `UPDATE resolutions SET status=?, terminal_reason=?, lease_token=NULL,
        lease_expires_at=NULL, updated_at=? WHERE resolution_id=?`, ResolutionAwaitingUser, reason, formatTime(now), resolution.ID); err != nil {
		return err
	}
	payload, _ := json.Marshal(resolution)
	return insertEffect(ctx, tx, Effect{ID: effectID(resolution.ID+":"+reason, EffectResolutionReview, resolution.ID),
		ContractID: resolution.ContractID, Type: EffectResolutionReview, Payload: payload, CreatedAt: now, NextAttemptAt: now})
}

func resolutionIdentity(contractID string, revision int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d", contractID, revision)))
	return "res_" + hex.EncodeToString(sum[:12])
}

func randomToken() (string, error) {
	buffer := make([]byte, 24)
	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf("coordination: create lease token: %w", err)
	}
	return hex.EncodeToString(buffer), nil
}
