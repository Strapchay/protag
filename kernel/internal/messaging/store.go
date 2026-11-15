package messaging

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const (
	KindNotify  = "notify"
	KindRequest = "request"
	KindAck     = "ack"

	TransportQueued     = "queued"
	TransportDelivered  = "delivered"
	TransportDeadLetter = "dead_letter"

	ActorNotRequired = "not_required"
	ActorAwaitingAck = "awaiting_ack"
	ActorAcked       = "acknowledged"
	ActorInProgress  = "in_progress"
	ActorCompleted   = "completed"
	ActorFailed      = "failed"

	maxNoteBytes = 1024
)

// Message is one durable peer message. The registry stores only peer
// communication, not hub chatter or streaming events.
type Message struct {
	ID             string    `json:"id"`
	FromAgent      string    `json:"from_agent"`
	ToAgent        string    `json:"to_agent"`
	Kind           string    `json:"kind"`
	ReplyTo        string    `json:"reply_to,omitempty"`
	Payload        string    `json:"payload,omitempty"`
	TransportState string    `json:"transport_state"`
	ActorState     string    `json:"actor_state"`
	Attempts       int       `json:"attempts"`
	NextAttemptAt  time.Time `json:"next_attempt_at,omitempty"`
	LastError      string    `json:"last_error,omitempty"`
	Note           string    `json:"note,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	DeliveredAt    time.Time `json:"delivered_at,omitempty"`
	AcknowledgedAt time.Time `json:"acknowledged_at,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
	ExpiresAt      time.Time `json:"expires_at,omitempty"`
}

// Store is a run-scoped SQLite message registry.
type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("messaging: database path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("messaging: create state dir: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("messaging: open store: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &Store{db: db}
	if err := store.initialize(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) initialize(ctx context.Context) error {
	for _, statement := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("messaging: configure store: %w", err)
		}
	}
	const schema = `
CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    from_agent TEXT NOT NULL,
    to_agent TEXT NOT NULL,
    kind TEXT NOT NULL,
    reply_to TEXT,
    payload TEXT,
    transport_state TEXT NOT NULL,
    actor_state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    last_error TEXT,
    note TEXT,
    created_at TEXT NOT NULL,
    delivered_at TEXT,
    acknowledged_at TEXT,
    updated_at TEXT NOT NULL,
    expires_at TEXT
);
CREATE INDEX IF NOT EXISTS messages_pending_idx ON messages(transport_state, next_attempt_at);
CREATE INDEX IF NOT EXISTS messages_target_idx ON messages(to_agent, actor_state, created_at);
CREATE INDEX IF NOT EXISTS messages_sender_idx ON messages(from_agent, created_at);
`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("messaging: create schema: %w", err)
	}
	return nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) Submit(ctx context.Context, message Message) error {
	now := time.Now().UTC()
	if strings.TrimSpace(message.ID) == "" {
		return fmt.Errorf("messaging: message id is required")
	}
	if strings.TrimSpace(message.FromAgent) == "" || strings.TrimSpace(message.ToAgent) == "" {
		return fmt.Errorf("messaging: sender and target are required")
	}
	kind := strings.TrimSpace(message.Kind)
	if kind == "" {
		kind = KindNotify
	}
	switch kind {
	case KindNotify, KindRequest, KindAck:
	default:
		return fmt.Errorf("messaging: invalid kind %q", kind)
	}
	actorState := ActorNotRequired
	if kind == KindRequest {
		actorState = ActorAwaitingAck
	}
	if message.CreatedAt.IsZero() {
		message.CreatedAt = now
	}
	if message.UpdatedAt.IsZero() {
		message.UpdatedAt = now
	}
	if message.NextAttemptAt.IsZero() {
		message.NextAttemptAt = now
	}
	_, err := s.db.ExecContext(ctx, `
        INSERT INTO messages (
            id, from_agent, to_agent, kind, reply_to, payload,
            transport_state, actor_state, attempts, next_attempt_at,
            created_at, updated_at, expires_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)`,
		message.ID, message.FromAgent, message.ToAgent, kind, message.ReplyTo, message.Payload,
		TransportQueued, actorState, formatTime(message.NextAttemptAt),
		formatTime(message.CreatedAt), formatTime(message.UpdatedAt), nullableTime(message.ExpiresAt),
	)
	if err != nil {
		return fmt.Errorf("messaging: submit: %w", err)
	}
	return nil
}

func (s *Store) Pending(ctx context.Context, limit int) ([]Message, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
        SELECT id, from_agent, to_agent, kind, reply_to, payload,
               transport_state, actor_state, attempts, next_attempt_at,
               last_error, note, created_at, delivered_at, acknowledged_at, updated_at, expires_at
        FROM messages
        WHERE transport_state=? AND next_attempt_at<=?
        ORDER BY created_at, id LIMIT ?`,
		TransportQueued, formatTime(time.Now().UTC()), limit)
	if err != nil {
		return nil, fmt.Errorf("messaging: pending: %w", err)
	}
	defer rows.Close()
	return scanMessages(rows)
}

func (s *Store) MarkDelivered(ctx context.Context, id string) error {
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `
        UPDATE messages SET transport_state=?, delivered_at=?, updated_at=?
        WHERE id=? AND transport_state=?`,
		TransportDelivered, formatTime(now), formatTime(now), strings.TrimSpace(id), TransportQueued)
	if err != nil {
		return fmt.Errorf("messaging: mark delivered: %w", err)
	}
	return nil
}

func (s *Store) MarkRetry(ctx context.Context, id string, next time.Time, cause error) error {
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	_, err := s.db.ExecContext(ctx, `
        UPDATE messages SET attempts=attempts+1, next_attempt_at=?, last_error=?, updated_at=?
        WHERE id=? AND transport_state=?`,
		formatTime(next.UTC()), truncate(message, maxNoteBytes), formatTime(time.Now().UTC()),
		strings.TrimSpace(id), TransportQueued)
	if err != nil {
		return fmt.Errorf("messaging: mark retry: %w", err)
	}
	return nil
}

func (s *Store) DeadLetter(ctx context.Context, id string, cause error) error {
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `
        UPDATE messages SET transport_state=?, last_error=?, updated_at=?
        WHERE id=?`,
		TransportDeadLetter, truncate(message, maxNoteBytes), formatTime(now), strings.TrimSpace(id))
	if err != nil {
		return fmt.Errorf("messaging: dead letter: %w", err)
	}
	return nil
}

func (s *Store) Ack(ctx context.Context, id, state, note string) error {
	state = strings.TrimSpace(state)
	switch state {
	case ActorAcked, ActorInProgress, ActorCompleted, ActorFailed:
	default:
		return fmt.Errorf("messaging: invalid actor state %q", state)
	}
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `
        UPDATE messages
        SET actor_state=?, note=?, acknowledged_at=COALESCE(acknowledged_at, ?), updated_at=?,
            payload=CASE WHEN ? IN (?, ?) THEN '' ELSE payload END
        WHERE id=? AND actor_state<>?`,
		state, truncate(strings.TrimSpace(note), maxNoteBytes), formatTime(now), formatTime(now),
		state, ActorCompleted, ActorFailed, strings.TrimSpace(id), ActorNotRequired)
	if err != nil {
		return fmt.Errorf("messaging: ack: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("messaging: ack rows: %w", err)
	}
	if affected == 0 {
		return fmt.Errorf("messaging: message %q is not awaiting an actor update", strings.TrimSpace(id))
	}
	return nil
}

func (s *Store) Status(ctx context.Context, id string) (Message, error) {
	row := s.db.QueryRowContext(ctx, `
        SELECT id, from_agent, to_agent, kind, reply_to, payload,
               transport_state, actor_state, attempts, next_attempt_at,
               last_error, note, created_at, delivered_at, acknowledged_at, updated_at, expires_at
        FROM messages WHERE id=?`, strings.TrimSpace(id))
	return scanMessage(row)
}

func (s *Store) List(ctx context.Context, filter ListFilter) ([]Message, error) {
	limit := filter.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := `
        SELECT id, from_agent, to_agent, kind, reply_to, payload,
               transport_state, actor_state, attempts, next_attempt_at,
               last_error, note, created_at, delivered_at, acknowledged_at, updated_at, expires_at
        FROM messages WHERE 1=1`
	args := []any{}
	if value := strings.TrimSpace(filter.FromAgent); value != "" {
		query += " AND from_agent=?"
		args = append(args, value)
	}
	if value := strings.TrimSpace(filter.ToAgent); value != "" {
		query += " AND to_agent=?"
		args = append(args, value)
	}
	if value := strings.TrimSpace(filter.ActorState); value != "" {
		query += " AND actor_state=?"
		args = append(args, value)
	}
	if value := strings.TrimSpace(filter.TransportState); value != "" {
		query += " AND transport_state=?"
		args = append(args, value)
	}
	query += " ORDER BY created_at DESC, id DESC LIMIT ?"
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("messaging: list: %w", err)
	}
	defer rows.Close()
	return scanMessages(rows)
}

type ListFilter struct {
	FromAgent      string
	ToAgent        string
	ActorState     string
	TransportState string
	Limit          int
}

// PruneTerminal bounds run state without a wall-clock TTL. It keeps the most
// recent terminal rows and leaves open requests untouched.
func (s *Store) PruneTerminal(ctx context.Context, keep int) error {
	if keep <= 0 {
		keep = 1000
	}
	_, err := s.db.ExecContext(ctx, `
        DELETE FROM messages
        WHERE transport_state IN (?, ?)
          AND actor_state IN (?, ?, ?)
          AND id NOT IN (
              SELECT id FROM messages
              WHERE transport_state IN (?, ?)
                AND actor_state IN (?, ?, ?)
              ORDER BY created_at DESC LIMIT ?
          )`,
		TransportDelivered, TransportDeadLetter, ActorNotRequired, ActorCompleted, ActorFailed,
		TransportDelivered, TransportDeadLetter, ActorNotRequired, ActorCompleted, ActorFailed,
		keep)
	if err != nil {
		return fmt.Errorf("messaging: prune terminal: %w", err)
	}
	return nil
}

func scanMessages(rows *sql.Rows) ([]Message, error) {
	var messages []Message
	for rows.Next() {
		message, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	return messages, rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanMessage(scanner rowScanner) (Message, error) {
	var message Message
	var nextAttempt, createdAt, deliveredAt, acknowledgedAt, updatedAt, expiresAt sql.NullString
	var lastError, replyTo, payload, note sql.NullString
	if err := scanner.Scan(
		&message.ID, &message.FromAgent, &message.ToAgent, &message.Kind, &replyTo, &payload,
		&message.TransportState, &message.ActorState, &message.Attempts, &nextAttempt,
		&lastError, &note, &createdAt, &deliveredAt, &acknowledgedAt, &updatedAt, &expiresAt,
	); err != nil {
		return Message{}, fmt.Errorf("messaging: scan: %w", err)
	}
	message.ReplyTo = replyTo.String
	message.Payload = payload.String
	message.LastError = lastError.String
	message.Note = note.String
	message.NextAttemptAt = parseTime(nextAttempt.String)
	message.CreatedAt = parseTime(createdAt.String)
	message.DeliveredAt = parseTime(deliveredAt.String)
	message.AcknowledgedAt = parseTime(acknowledgedAt.String)
	message.UpdatedAt = parseTime(updatedAt.String)
	message.ExpiresAt = parseTime(expiresAt.String)
	return message, nil
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func parseTime(value string) time.Time {
	if strings.TrimSpace(value) == "" {
		return time.Time{}
	}
	parsed, _ := time.Parse(time.RFC3339Nano, value)
	return parsed
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func truncate(value string, limit int) string {
	if limit <= 0 || len(value) <= limit {
		return value
	}
	return value[:limit]
}
