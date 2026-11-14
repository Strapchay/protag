package coordination

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type Options struct {
	StateDir             string
	MaxNegotiationRounds int
	MaxProcessAttempts   int
	MaxRepairAttempts    int
	MaxContextExpansions int
}

type Manager struct {
	db        *sql.DB
	stateDir  string
	objectDir string
	options   Options
}

func Open(options Options) (*Manager, error) {
	if options.StateDir == "" {
		return nil, fmt.Errorf("coordination: state directory is required")
	}
	stateDir, err := filepath.Abs(options.StateDir)
	if err != nil {
		return nil, fmt.Errorf("coordination: resolve state directory: %w", err)
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return nil, fmt.Errorf("coordination: create state directory: %w", err)
	}
	objectDir := filepath.Join(stateDir, "objects")
	if err := os.MkdirAll(objectDir, 0o700); err != nil {
		return nil, fmt.Errorf("coordination: create object store: %w", err)
	}

	db, err := sql.Open("sqlite", filepath.Join(stateDir, "contracts.db"))
	if err != nil {
		return nil, fmt.Errorf("coordination: open store: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if options.MaxNegotiationRounds <= 0 {
		options.MaxNegotiationRounds = 3
	}
	if options.MaxProcessAttempts <= 0 {
		options.MaxProcessAttempts = 2
	}
	if options.MaxRepairAttempts < 0 {
		options.MaxRepairAttempts = 0
	} else if options.MaxRepairAttempts == 0 {
		options.MaxRepairAttempts = 1
	}
	if options.MaxContextExpansions < 0 {
		options.MaxContextExpansions = 0
	} else if options.MaxContextExpansions == 0 {
		options.MaxContextExpansions = 1
	}
	manager := &Manager{db: db, stateDir: stateDir, objectDir: objectDir, options: options}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := manager.initialize(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return manager, nil
}

func (m *Manager) initialize(ctx context.Context) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
	}
	for _, statement := range pragmas {
		if _, err := m.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("coordination: configure store: %w", err)
		}
	}

	const schema = `
CREATE TABLE IF NOT EXISTS contracts (
    id TEXT PRIMARY KEY,
    producer_agent_id TEXT NOT NULL,
    producer_node_id TEXT NOT NULL,
    consumer_agent_id TEXT NOT NULL,
    consumer_node_id TEXT NOT NULL,
    details_json BLOB NOT NULL,
    status TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0,
    hard_wait INTEGER NOT NULL DEFAULT 0,
    artifact_digest TEXT,
    artifact_size INTEGER,
    artifact_media_type TEXT,
    validation_json BLOB,
    correction_reason TEXT,
    terminal_reason TEXT,
    completion_pending INTEGER NOT NULL DEFAULT 0,
    expires_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS contracts_producer_idx ON contracts(producer_agent_id, producer_node_id, status);
CREATE INDEX IF NOT EXISTS contracts_consumer_idx ON contracts(consumer_agent_id, consumer_node_id, status);

CREATE TABLE IF NOT EXISTS contract_revisions (
    contract_id TEXT NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
    revision INTEGER NOT NULL,
    digest TEXT NOT NULL,
    size INTEGER NOT NULL,
    media_type TEXT,
    evidence_json BLOB,
    actor_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(contract_id, revision)
);

CREATE TABLE IF NOT EXISTS contract_terms (
    contract_id TEXT NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
    revision INTEGER NOT NULL,
    parent_revision INTEGER NOT NULL DEFAULT 0,
    proposer_agent_id TEXT NOT NULL,
    details_json BLOB NOT NULL,
    producer_obligations_json BLOB,
    consumer_obligations_json BLOB,
    tradeoffs_json BLOB,
    rationale TEXT,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(contract_id, revision)
);

CREATE TABLE IF NOT EXISTS term_acceptances (
    contract_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    agent_id TEXT NOT NULL,
    accepted_at TEXT NOT NULL,
    PRIMARY KEY(contract_id, revision, agent_id),
    FOREIGN KEY(contract_id, revision) REFERENCES contract_terms(contract_id, revision) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS resolutions (
    resolution_id TEXT PRIMARY KEY,
    contract_id TEXT NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
    terms_revision INTEGER NOT NULL,
    status TEXT NOT NULL,
    reason TEXT NOT NULL,
    evidence_json BLOB,
    terminal_incompatibility INTEGER NOT NULL DEFAULT 0,
    affected_node_ids_json BLOB NOT NULL,
    semantic_attempts INTEGER NOT NULL DEFAULT 1,
    process_attempts INTEGER NOT NULL DEFAULT 0,
    repair_attempts INTEGER NOT NULL DEFAULT 0,
    context_expansions INTEGER NOT NULL DEFAULT 0,
    max_process_attempts INTEGER NOT NULL,
    max_repair_attempts INTEGER NOT NULL,
    max_context_expansions INTEGER NOT NULL,
    lease_token TEXT,
    lease_expires_at TEXT,
    input_digest TEXT,
    input_size INTEGER,
    output_digest TEXT,
    output_size INTEGER,
    validation_error TEXT,
    rationale TEXT,
    terminal_reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS resolutions_status_idx ON resolutions(status, updated_at);

CREATE TABLE IF NOT EXISTS transitions (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    contract_id TEXT NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
    from_status TEXT,
    to_status TEXT NOT NULL,
    revision INTEGER NOT NULL,
    actor_id TEXT NOT NULL,
    reason TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS commands (
    command_id TEXT PRIMARY KEY,
    operation TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    result_json BLOB NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS effects (
    effect_id TEXT PRIMARY KEY,
    contract_id TEXT,
    effect_type TEXT NOT NULL,
    target_agent_id TEXT,
    target_node_id TEXT,
    payload_json BLOB,
    state TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    next_attempt_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    acknowledged_at TEXT
);
CREATE INDEX IF NOT EXISTS effects_pending_idx ON effects(state, next_attempt_at, created_at);

CREATE TABLE IF NOT EXISTS completion_intents (
    node_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    metadata_json BLOB,
    command_id TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cycles (
    cycle_id TEXT PRIMARY KEY,
    node_ids_json BLOB NOT NULL,
    contract_ids_json BLOB NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    detected_at TEXT NOT NULL,
    resolved_at TEXT
);`
	if _, err := m.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("coordination: create schema: %w", err)
	}
	return nil
}

func (m *Manager) Close() error {
	if m == nil || m.db == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := m.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		_ = m.db.Close()
		return fmt.Errorf("coordination: checkpoint store: %w", err)
	}
	return m.db.Close()
}

func (m *Manager) StateDir() string {
	if m == nil {
		return ""
	}
	return m.stateDir
}
