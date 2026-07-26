// Package mailbox provides a durable message envelope store for the OMNI
// orchestration system. Messages are persisted in SQLite with at-least-once
// delivery semantics and durable idempotent consumption.
//
// Core guarantees:
//   - At-least-once delivery: messages survive process restart
//   - Durable idempotent consumption: ACK marks consumed; re-ACK is safe
//   - Duplicate message_id → INSERT OR IGNORE (idempotent enqueue)
//   - Not exactly-once: consumers must tolerate replays
//
// API:
//   - Enqueue(msg) → durable INSERT
//   - Dequeue(recipient) → oldest unacked
//   - Ack(message_id) → mark consumed
//   - Redeliver() → unacked older than timeout
package mailbox

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// ── Message Types ──

// Type enumerates the 9 message types in the OMNI protocol.
type Type string

const (
	TypeTaskAssigned       Type = "TASK_ASSIGNED"
	TypeProgressReported   Type = "PROGRESS_REPORTED"
	TypeWorkerBlocked      Type = "WORKER_BLOCKED"
	TypeAttemptCompleted   Type = "ATTEMPT_COMPLETED"
	TypeValidationAccepted Type = "VALIDATION_ACCEPTED"
	TypeValidationRejected Type = "VALIDATION_REJECTED"
	TypeRetryRequested     Type = "RETRY_REQUESTED"
	TypeHandoffReady       Type = "HANDOFF_READY"
	TypeCancelled          Type = "CANCELLED"
)

// ValidTypes is the set of recognized message types.
var ValidTypes = map[Type]bool{
	TypeTaskAssigned:       true,
	TypeProgressReported:   true,
	TypeWorkerBlocked:      true,
	TypeAttemptCompleted:   true,
	TypeValidationAccepted: true,
	TypeValidationRejected: true,
	TypeRetryRequested:     true,
	TypeHandoffReady:       true,
	TypeCancelled:          true,
}

// ── Envelope ──

// Envelope is a durable message with routing metadata and a JSON payload.
type Envelope struct {
	ID                int64           `json:"id"`
	MessageID         string          `json:"message_id"`
	RunID             int64           `json:"run_id"`
	RunEpoch          int64           `json:"run_epoch"`
	TaskID            int64           `json:"task_id"`
	TaskGeneration    int64           `json:"task_generation"`
	AttemptID         int64           `json:"attempt_id"`
	AttemptGeneration int64           `json:"attempt_generation"`
	Sender            string          `json:"sender"`
	Recipient         string          `json:"recipient"`
	Type              Type            `json:"type"`
	ArtifactRefs      string          `json:"artifact_refs"`
	Payload           json.RawMessage `json:"payload"`
	AckRequired       int             `json:"ack_required"`
	Acked             int             `json:"acked"`
	AckedAt           *time.Time      `json:"acked_at,omitempty"`
	CreatedAt         time.Time       `json:"created_at"`
}

// ── Store ──

// Store is a durable SQLite-backed message mailbox.
type Store struct {
	db *sql.DB
	mu sync.Mutex
}

// New opens (or creates) the mailbox store at the given path.
func New(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("mailbox open: %w", err)
	}
	db.SetMaxOpenConns(1)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("mailbox ping: %w", err)
	}

	if path != ":memory:" {
		if err := os.Chmod(path, 0600); err != nil {
			db.Close()
			return nil, fmt.Errorf("mailbox chmod: %w", err)
		}
	}

	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("mailbox migrate: %w", err)
	}
	return s, nil
}

// NewInMemory creates an in-memory mailbox store for tests.
func NewInMemory() (*Store, error) {
	db, err := sql.Open("sqlite", ":memory:?_journal_mode=WAL")
	if err != nil {
		return nil, fmt.Errorf("mailbox open memory: %w", err)
	}
	db.SetMaxOpenConns(1)

	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("mailbox migrate: %w", err)
	}
	return s, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	ddl := `
	CREATE TABLE IF NOT EXISTS envelopes (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		message_id TEXT NOT NULL UNIQUE,
		run_id INTEGER NOT NULL,
		run_epoch INTEGER NOT NULL DEFAULT 1,
		task_id INTEGER NOT NULL,
		task_generation INTEGER NOT NULL DEFAULT 1,
		attempt_id INTEGER NOT NULL,
		attempt_generation INTEGER NOT NULL DEFAULT 1,
		sender TEXT NOT NULL,
		recipient TEXT NOT NULL,
		type TEXT NOT NULL,
		artifact_refs TEXT NOT NULL DEFAULT '',
		payload TEXT NOT NULL DEFAULT '{}',
		ack_required INTEGER NOT NULL DEFAULT 0,
		acked INTEGER NOT NULL DEFAULT 0,
		acked_at TEXT,
		created_at TEXT NOT NULL DEFAULT (datetime('now'))
	);
	CREATE INDEX IF NOT EXISTS idx_envelopes_recipient ON envelopes(recipient, acked, created_at);
	CREATE INDEX IF NOT EXISTS idx_envelopes_message_id ON envelopes(message_id);
	CREATE INDEX IF NOT EXISTS idx_envelopes_run_id ON envelopes(run_id);
	`
	_, err := s.db.Exec(ddl)
	return err
}

// ── API ──

// Enqueue inserts a message. Duplicate message_id → idempotent no-op (returns nil).
// Returns an error if the type is invalid or required fields are missing.
func (s *Store) Enqueue(msg *Envelope) error {
	if msg == nil {
		return fmt.Errorf("mailbox: nil message")
	}
	if msg.MessageID == "" {
		return fmt.Errorf("mailbox: message_id is required")
	}
	if !ValidTypes[msg.Type] {
		return fmt.Errorf("mailbox: invalid message type %q", msg.Type)
	}
	if msg.Sender == "" || msg.Recipient == "" {
		return fmt.Errorf("mailbox: sender and recipient are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	payload := string(msg.Payload)
	if payload == "" {
		payload = "{}"
	}
	ackReq := msg.AckRequired

	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO envelopes
		 (message_id, run_id, run_epoch, task_id, task_generation,
		  attempt_id, attempt_generation, sender, recipient, type,
		  artifact_refs, payload, ack_required)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		msg.MessageID, msg.RunID, msg.RunEpoch, msg.TaskID, msg.TaskGeneration,
		msg.AttemptID, msg.AttemptGeneration, msg.Sender, msg.Recipient, string(msg.Type),
		msg.ArtifactRefs, payload, ackReq,
	)
	return err
}

// Dequeue returns the oldest unacked message for the given recipient.
// Atomically marks the message as in-flight (acked=-1) so concurrent
// consumers cannot dequeue the same message. Returns nil if no messages
// are waiting.
func (s *Store) Dequeue(recipient string) (*Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Find the oldest unacked message.
	row := s.db.QueryRow(
		`SELECT message_id FROM envelopes
		 WHERE recipient=? AND acked=0
		 ORDER BY created_at ASC LIMIT 1`,
		recipient,
	)
	var msgID string
	if err := row.Scan(&msgID); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	// Atomically mark in-flight.
	_, err := s.db.Exec(
		`UPDATE envelopes SET acked=-1 WHERE message_id=? AND acked=0`,
		msgID,
	)
	if err != nil {
		return nil, err
	}

	// Re-read to return full envelope.
	row2 := s.db.QueryRow(
		`SELECT id, message_id, run_id, run_epoch, task_id, task_generation,
		        attempt_id, attempt_generation, sender, recipient, type,
		        artifact_refs, payload, ack_required, acked, acked_at, created_at
		 FROM envelopes WHERE message_id=?`,
		msgID,
	)
	return scanEnvelope(row2)
}

// Ack marks a message as consumed. Idempotent — re-acking is safe.
func (s *Store) Ack(messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Exec(
		`UPDATE envelopes SET acked=1, acked_at=datetime('now') WHERE message_id=? AND acked<=0`,
		messageID,
	)
	return err
}

// Redeliver returns unacked messages older than the given timeout.
// These are candidates for re-delivery after a consumer restart.
func (s *Store) Redeliver(timeout time.Duration) ([]*Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-timeout).UTC().Format("2006-01-02 15:04:05")
	rows, err := s.db.Query(
		`SELECT id, message_id, run_id, run_epoch, task_id, task_generation,
		        attempt_id, attempt_generation, sender, recipient, type,
		        artifact_refs, payload, ack_required, acked, acked_at, created_at
		 FROM envelopes
		 WHERE acked<=0 AND created_at <= ?
		 ORDER BY created_at ASC`,
		cutoff,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEnvelopes(rows)
}

// GetByMessageID looks up a single message by its message_id.
func (s *Store) GetByMessageID(messageID string) (*Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	row := s.db.QueryRow(
		`SELECT id, message_id, run_id, run_epoch, task_id, task_generation,
		        attempt_id, attempt_generation, sender, recipient, type,
		        artifact_refs, payload, ack_required, acked, acked_at, created_at
		 FROM envelopes WHERE message_id=?`,
		messageID,
	)
	return scanEnvelope(row)
}

// ── Internal helpers ──

func scanEnvelope(row *sql.Row) (*Envelope, error) {
	var e Envelope
	var artifactRefs, payload, typeStr, ackedAt sql.NullString
	var acked int
	var ackReq int
	var createdAt string
	err := row.Scan(&e.ID, &e.MessageID, &e.RunID, &e.RunEpoch,
		&e.TaskID, &e.TaskGeneration, &e.AttemptID, &e.AttemptGeneration,
		&e.Sender, &e.Recipient, &typeStr, &artifactRefs, &payload,
		&ackReq, &acked, &ackedAt, &createdAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	e.Type = Type(typeStr.String)
	e.ArtifactRefs = artifactRefs.String
	e.AckRequired = ackReq
	e.Acked = acked
	if payload.Valid && payload.String != "" {
		e.Payload = json.RawMessage(payload.String)
	} else {
		e.Payload = json.RawMessage("{}")
	}
	if ackedAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", ackedAt.String)
		e.AckedAt = &t
	}
	e.CreatedAt, _ = time.Parse("2006-01-02 15:04:05", createdAt)
	return &e, nil
}

func scanEnvelopes(rows *sql.Rows) ([]*Envelope, error) {
	var out []*Envelope
	for rows.Next() {
		var e Envelope
		var artifactRefs, payload, typeStr, ackedAt sql.NullString
		var acked int
		var ackReq int
		var createdAt string
		if err := rows.Scan(&e.ID, &e.MessageID, &e.RunID, &e.RunEpoch,
			&e.TaskID, &e.TaskGeneration, &e.AttemptID, &e.AttemptGeneration,
			&e.Sender, &e.Recipient, &typeStr, &artifactRefs, &payload,
			&ackReq, &acked, &ackedAt, &createdAt); err != nil {
			return nil, err
		}
		e.Type = Type(typeStr.String)
		e.ArtifactRefs = artifactRefs.String
		e.AckRequired = ackReq
		e.Acked = acked
		if payload.Valid && payload.String != "" {
			e.Payload = json.RawMessage(payload.String)
		} else {
			e.Payload = json.RawMessage("{}")
		}
		if ackedAt.Valid {
			t, _ := time.Parse("2006-01-02 15:04:05", ackedAt.String)
			e.AckedAt = &t
		}
		e.CreatedAt, _ = time.Parse("2006-01-02 15:04:05", createdAt)
		out = append(out, &e)
	}
	return out, rows.Err()
}

// Ensure fmt is used.
var _ = fmt.Sprintf

// Ensure strings is used.
var _ = strings.TrimSpace
