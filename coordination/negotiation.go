package coordination

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

func insertInitialTerms(ctx context.Context, tx *sql.Tx, contract Contract, proposer string, now time.Time) error {
	details, err := json.Marshal(contract.Details)
	if err != nil {
		return fmt.Errorf("coordination: encode initial terms: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO contract_terms
        (contract_id, revision, parent_revision, proposer_agent_id, details_json, status, created_at)
        VALUES (?, 1, 0, ?, ?, ?, ?)`, contract.ID, proposer, details, TermsAgreed, formatTime(now)); err != nil {
		return fmt.Errorf("coordination: persist initial terms: %w", err)
	}
	for _, agentID := range []string{contract.ProducerAgentID, contract.ConsumerAgentID} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO term_acceptances(contract_id, revision, agent_id, accepted_at)
            VALUES (?, 1, ?, ?)`, contract.ID, agentID, formatTime(now)); err != nil {
			return fmt.Errorf("coordination: accept initial terms: %w", err)
		}
	}
	return nil
}

func (m *Manager) ProposeTerms(ctx context.Context, principal Principal, command ProposeTermsCommand) (*ContractTerms, error) {
	command.ContractID = strings.TrimSpace(command.ContractID)
	command.Rationale = strings.TrimSpace(command.Rationale)
	command.Details.Name = strings.TrimSpace(command.Details.Name)
	command.Details.Kind = strings.TrimSpace(command.Details.Kind)
	if command.ContractID == "" || command.Details.Name == "" || command.Details.Kind == "" || command.Rationale == "" {
		return nil, fmt.Errorf("coordination: contract ID, terms name/kind, and rationale are required")
	}
	commandID, requestHash, err := commandIdentity(command.CommandID, "propose-terms", principal, command)
	if err != nil {
		return nil, err
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("coordination: begin terms proposal: %w", err)
	}
	defer tx.Rollback()
	var prior ContractTerms
	if found, err := loadCommandResult(ctx, tx, commandID, "propose-terms", requestHash, &prior); err != nil {
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
	if contract.Status != StatusRequested && contract.Status != StatusCorrectionRequested {
		return nil, fmt.Errorf("coordination: contract %s cannot negotiate terms from %s", contract.ID, contract.Status)
	}
	latest, err := latestTermsTx(ctx, tx, contract.ID)
	if err != nil {
		return nil, err
	}
	if latest.Revision != command.ExpectedRevision {
		return nil, fmt.Errorf("coordination: terms revision conflict: expected %d, current %d", command.ExpectedRevision, latest.Revision)
	}
	if int(latest.Revision) >= m.options.MaxNegotiationRounds {
		return nil, fmt.Errorf("coordination: negotiation round limit %d reached; declare standstill", m.options.MaxNegotiationRounds)
	}
	now := time.Now().UTC()
	terms := ContractTerms{
		ContractID: contract.ID, Revision: latest.Revision + 1, ParentRevision: latest.Revision,
		ProposerAgentID: principal.AgentID, Details: command.Details,
		ProducerObligations: cleanStrings(command.ProducerObligations), ConsumerObligations: cleanStrings(command.ConsumerObligations),
		Tradeoffs: cleanStrings(command.Tradeoffs), Rationale: command.Rationale,
		Status: TermsProposed, AcceptedBy: []string{principal.AgentID}, CreatedAt: now,
	}
	if err := insertTerms(ctx, tx, terms); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO term_acceptances(contract_id, revision, agent_id, accepted_at)
        VALUES (?, ?, ?, ?)`, contract.ID, terms.Revision, principal.AgentID, formatTime(now)); err != nil {
		return nil, fmt.Errorf("coordination: accept proposed terms: %w", err)
	}
	targetAgent, targetNode := otherParticipant(contract, principal.AgentID)
	if err := insertEffect(ctx, tx, effectForTerms(commandID, EffectTermsProposed, terms, targetAgent, targetNode, now)); err != nil {
		return nil, err
	}
	if err := saveCommandResult(ctx, tx, commandID, "propose-terms", requestHash, terms, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("coordination: commit terms proposal: %w", err)
	}
	return &terms, nil
}

func (m *Manager) AcceptTerms(ctx context.Context, principal Principal, command AcceptTermsCommand) (*ContractTerms, error) {
	command.ContractID = strings.TrimSpace(command.ContractID)
	commandID, requestHash, err := commandIdentity(command.CommandID, "accept-terms", principal, command)
	if err != nil {
		return nil, err
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("coordination: begin terms acceptance: %w", err)
	}
	defer tx.Rollback()
	var prior ContractTerms
	if found, err := loadCommandResult(ctx, tx, commandID, "accept-terms", requestHash, &prior); err != nil {
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
	terms, err := latestTermsTx(ctx, tx, contract.ID)
	if err != nil {
		return nil, err
	}
	if terms.Revision != command.Revision {
		return nil, fmt.Errorf("coordination: terms revision conflict: requested %d, current %d", command.Revision, terms.Revision)
	}
	if terms.Status == TermsAgreed {
		return terms, nil
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO term_acceptances(contract_id, revision, agent_id, accepted_at)
        VALUES (?, ?, ?, ?)`, contract.ID, terms.Revision, principal.AgentID, formatTime(now)); err != nil {
		return nil, fmt.Errorf("coordination: persist terms acceptance: %w", err)
	}
	accepted, err := termAcceptanceIDs(ctx, tx, contract.ID, terms.Revision)
	if err != nil {
		return nil, err
	}
	terms.AcceptedBy = accepted
	if containsString(accepted, contract.ProducerAgentID) && containsString(accepted, contract.ConsumerAgentID) {
		terms.Status = TermsAgreed
		details, _ := json.Marshal(terms.Details)
		if _, err := tx.ExecContext(ctx, `UPDATE contract_terms SET status=? WHERE contract_id=? AND revision=?`, TermsAgreed, contract.ID, terms.Revision); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE contracts SET details_json=?, updated_at=? WHERE id=?`, details, formatTime(now), contract.ID); err != nil {
			return nil, err
		}
		for _, pair := range [][2]string{{contract.ProducerAgentID, contract.ProducerNodeID}, {contract.ConsumerAgentID, contract.ConsumerNodeID}} {
			if err := insertEffect(ctx, tx, effectForTerms(commandID+":"+pair[0], EffectTermsAgreed, *terms, pair[0], pair[1], now)); err != nil {
				return nil, err
			}
		}
	}
	if err := saveCommandResult(ctx, tx, commandID, "accept-terms", requestHash, terms, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("coordination: commit terms acceptance: %w", err)
	}
	return terms, nil
}

func (m *Manager) Terms(ctx context.Context, principal Principal, contractID string) ([]ContractTerms, error) {
	contract, err := getContractDB(ctx, m.db, strings.TrimSpace(contractID))
	if err != nil {
		return nil, err
	}
	if err := authorizeRead(principal, contract); err != nil {
		return nil, err
	}
	return m.listTerms(ctx, contract.ID)
}

func (m *Manager) listTerms(ctx context.Context, contractID string) ([]ContractTerms, error) {
	rows, err := m.db.QueryContext(ctx, `SELECT contract_id, revision, parent_revision, proposer_agent_id,
        details_json, producer_obligations_json, consumer_obligations_json, tradeoffs_json,
        COALESCE(rationale,''), status, created_at FROM contract_terms WHERE contract_id=? ORDER BY revision`, contractID)
	if err != nil {
		return nil, fmt.Errorf("coordination: list terms: %w", err)
	}
	defer rows.Close()
	var out []ContractTerms
	for rows.Next() {
		terms, err := scanTerms(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *terms)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for i := range out {
		accepted, err := m.termAcceptanceIDsDB(ctx, out[i].ContractID, out[i].Revision)
		if err != nil {
			return nil, err
		}
		out[i].AcceptedBy = accepted
	}
	return out, nil
}

func latestTermsTx(ctx context.Context, tx *sql.Tx, contractID string) (*ContractTerms, error) {
	terms, err := scanTerms(tx.QueryRowContext(ctx, `SELECT contract_id, revision, parent_revision, proposer_agent_id,
        details_json, producer_obligations_json, consumer_obligations_json, tradeoffs_json,
        COALESCE(rationale,''), status, created_at FROM contract_terms WHERE contract_id=? ORDER BY revision DESC LIMIT 1`, contractID))
	if err != nil {
		return nil, err
	}
	accepted, err := termAcceptanceIDs(ctx, tx, contractID, terms.Revision)
	if err != nil {
		return nil, err
	}
	terms.AcceptedBy = accepted
	return terms, nil
}

func scanTerms(row rowScanner) (*ContractTerms, error) {
	var terms ContractTerms
	var details, producer, consumer, tradeoffs []byte
	var created string
	if err := row.Scan(&terms.ContractID, &terms.Revision, &terms.ParentRevision, &terms.ProposerAgentID,
		&details, &producer, &consumer, &tradeoffs, &terms.Rationale, &terms.Status, &created); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("coordination: contract terms not found")
		}
		return nil, fmt.Errorf("coordination: scan terms: %w", err)
	}
	_ = json.Unmarshal(details, &terms.Details)
	_ = json.Unmarshal(producer, &terms.ProducerObligations)
	_ = json.Unmarshal(consumer, &terms.ConsumerObligations)
	_ = json.Unmarshal(tradeoffs, &terms.Tradeoffs)
	terms.CreatedAt, _ = parseTime(created)
	return &terms, nil
}

func insertTerms(ctx context.Context, tx *sql.Tx, terms ContractTerms) error {
	details, _ := json.Marshal(terms.Details)
	producer, _ := json.Marshal(terms.ProducerObligations)
	consumer, _ := json.Marshal(terms.ConsumerObligations)
	tradeoffs, _ := json.Marshal(terms.Tradeoffs)
	_, err := tx.ExecContext(ctx, `INSERT INTO contract_terms(contract_id, revision, parent_revision,
        proposer_agent_id, details_json, producer_obligations_json, consumer_obligations_json,
        tradeoffs_json, rationale, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		terms.ContractID, terms.Revision, terms.ParentRevision, terms.ProposerAgentID, details,
		producer, consumer, tradeoffs, nullableString(terms.Rationale), terms.Status, formatTime(terms.CreatedAt))
	if err != nil {
		return fmt.Errorf("coordination: persist terms: %w", err)
	}
	return nil
}

func termAcceptanceIDs(ctx context.Context, tx *sql.Tx, contractID string, revision int64) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT agent_id FROM term_acceptances WHERE contract_id=? AND revision=? ORDER BY agent_id`, contractID, revision)
	if err != nil {
		return nil, err
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

func (m *Manager) termAcceptanceIDsDB(ctx context.Context, contractID string, revision int64) ([]string, error) {
	rows, err := m.db.QueryContext(ctx, `SELECT agent_id FROM term_acceptances WHERE contract_id=? AND revision=? ORDER BY agent_id`, contractID, revision)
	if err != nil {
		return nil, err
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

func authorizeParticipant(principal Principal, contract *Contract) error {
	if principal.Role == RoleAdmin {
		return nil
	}
	if principal.Role != RoleAgent {
		return fmt.Errorf("coordination: only contract participants may negotiate terms")
	}
	if principal.AgentID == contract.ProducerAgentID && principalHasNode(principal, contract.ProducerNodeID) {
		return nil
	}
	if principal.AgentID == contract.ConsumerAgentID && principalHasNode(principal, contract.ConsumerNodeID) {
		return nil
	}
	return fmt.Errorf("coordination: principal is not a participant in %s", contract.ID)
}

func otherParticipant(contract *Contract, agentID string) (string, string) {
	if agentID == contract.ProducerAgentID {
		return contract.ConsumerAgentID, contract.ConsumerNodeID
	}
	return contract.ProducerAgentID, contract.ProducerNodeID
}

func effectForTerms(seed string, effectType EffectType, terms ContractTerms, targetAgent, targetNode string, now time.Time) Effect {
	payload, _ := json.Marshal(terms)
	return Effect{ID: effectID(seed, effectType, terms.ContractID), ContractID: terms.ContractID, Type: effectType,
		TargetAgentID: targetAgent, TargetNodeID: targetNode, Payload: payload, CreatedAt: now, NextAttemptAt: now}
}

func cleanStrings(values []string) []string {
	seen := make(map[string]bool)
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" && !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	sort.Strings(out)
	return out
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
