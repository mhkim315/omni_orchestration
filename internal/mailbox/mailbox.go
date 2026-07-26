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
	LeasedAt          *time.Time      `json:"leased_at,omitempty"`
	DeliveryAttempt   int             `json:"delivery_attempt"`
	LeaseOwner        string          `json:"lease_owner"`
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
	if _, err := s.db.Exec(ddl); err != nil {
		return err
	}
	// v1.2.1 migration: add lease columns for existing databases.
	for _, col := range []string{"leased_at", "delivery_attempt", "lease_owner"} {
		def := "TEXT"
		if col == "delivery_attempt" {
			def = "INTEGER NOT NULL DEFAULT 0"
		} else if col == "lease_owner" {
			def = "TEXT NOT NULL DEFAULT ''"
		}
		s.db.Exec("ALTER TABLE envelopes ADD COLUMN " + col + " " + def)
	}
	return nil
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
// Dequeue atomically claims the oldest unacked message for a recipient.
// v1.2.1: lease_owner parameter. CAS UPDATE WHERE acked=0 — multi-process safe.
// Returns nil if no messages or claim lost to another consumer.
func (s *Store) Dequeue(recipient, leaseOwner string) (*Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Find the oldest unacked message.
	row := s.db.QueryRow(
		`SELECT message_id FROM envelopes
		 WHERE recipient=? AND acked=0
		 ORDER BY id ASC LIMIT 1`,
		recipient,
	)
	var msgID string
	if err := row.Scan(&msgID); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	// v1.2.1 Fix 6: Atomic claim via CAS UPDATE.
	// Multi-process safe: only one UPDATE succeeds (WHERE acked=0).
	res, err := s.db.Exec(
		`UPDATE envelopes SET acked=-1, leased_at=datetime('now'),
		 delivery_attempt=delivery_attempt+1, lease_owner=?
		 WHERE message_id=? AND acked=0`,
		leaseOwner, msgID,
	)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, nil // claimed by another consumer
	}

	// Re-read full envelope.
	return scanEnvelope(s.db.QueryRow(
		`SELECT id, message_id, run_id, run_epoch, task_id, task_generation,
		        attempt_id, attempt_generation, sender, recipient, type,
		        artifact_refs, payload, ack_required, acked, acked_at,
		        leased_at, delivery_attempt, lease_owner, created_at
		 FROM envelopes WHERE message_id=?`,
		msgID,
	))
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

// Redeliver returns unacked/in-flight messages whose lease has expired.
// v1.2.1 Fix 7: Uses leased_at + timeout, not created_at.
func (s *Store) Redeliver(leaseTimeout time.Duration) ([]*Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-leaseTimeout).UTC().Format("2006-01-02 15:04:05")
	rows, err := s.db.Query(
		`SELECT id, message_id, run_id, run_epoch, task_id, task_generation,
		        attempt_id, attempt_generation, sender, recipient, type,
		        artifact_refs, payload, ack_required, acked, acked_at,
		        leased_at, delivery_attempt, lease_owner, created_at
		 FROM envelopes
		 WHERE acked<=0 AND (leased_at IS NULL OR leased_at <= ?)
		 ORDER BY id ASC`,
		cutoff,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEnvelopes(rows)
}

// ── Consumer ──

// Authority validates that an envelope's run/task/attempt are still current.
// Returns an error if the message should be rejected (stale, cancelled, etc.).
// v1.2.1 Fix 2+3: Production validation, not test-simulated.
type Authority func(msg *Envelope) error

// Consumer runs a loop: Dequeue → validate authority → apply effect → ACK.
// v1.2.1 Fix 4: ACK only AFTER effect success.
type Consumer struct {
	store      *Store
	recipient  string
	leaseOwner string
	authority  Authority
	onEffect   func(*Envelope) error
}

// NewConsumer creates a consumer that processes messages for a recipient.
func NewConsumer(store *Store, recipient, leaseOwner string, authority Authority, onEffect func(*Envelope) error) *Consumer {
	return &Consumer{
		store: store, recipient: recipient, leaseOwner: leaseOwner,
		authority: authority, onEffect: onEffect,
	}
}

// ProcessOne dequeues and processes a single message.
// Returns (true, nil) if processed, (false, nil) if queue empty.
func (c *Consumer) ProcessOne() (bool, error) {
	msg, err := c.store.Dequeue(c.recipient, c.leaseOwner)
	if err != nil {
		return false, err
	}
	if msg == nil {
		return false, nil
	}

	// 1. Validate authority.
	if c.authority != nil {
		if err := c.authority(msg); err != nil {
			c.store.Ack(msg.MessageID) // reject stale/cancelled
			return true, nil
		}
	}

	// 2. Apply effect (must be idempotent for replay safety).
	if c.onEffect != nil {
		if err := c.onEffect(msg); err != nil {
			return false, fmt.Errorf("effect failed for %s: %w", msg.MessageID, err)
		}
	}

	// 3. v1.2.1 Fix 4+5: ACK only AFTER effect success.
	// If ACK fails, effect was already applied (idempotent replay safe).
	if err := c.store.Ack(msg.MessageID); err != nil {
		return true, fmt.Errorf("ack failed (effect applied, replay safe): %w", err)
	}
	return true, nil
}

// GetByMessageID looks up a single message by its message_id.
func (s *Store) GetByMessageID(messageID string) (*Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	row := s.db.QueryRow(
		`SELECT id, message_id, run_id, run_epoch, task_id, task_generation,
		        attempt_id, attempt_generation, sender, recipient, type,
		        artifact_refs, payload, ack_required, acked, acked_at, leased_at, delivery_attempt, lease_owner, created_at
		 FROM envelopes WHERE message_id=?`,
		messageID,
	)
	return scanEnvelope(row)
}

// ── Internal helpers ──

func scanEnvelope(row *sql.Row) (*Envelope, error) {
	var e Envelope
	var artifactRefs, payload, typeStr, ackedAt, leasedAt sql.NullString
	var acked, ackReq, deliveryAttempt int
	var createdAt string
	err := row.Scan(&e.ID, &e.MessageID, &e.RunID, &e.RunEpoch,
		&e.TaskID, &e.TaskGeneration, &e.AttemptID, &e.AttemptGeneration,
		&e.Sender, &e.Recipient, &typeStr, &artifactRefs, &payload,
		&ackReq, &acked, &ackedAt, &leasedAt, &deliveryAttempt, &e.LeaseOwner, &createdAt)
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
	e.DeliveryAttempt = deliveryAttempt
	if payload.Valid && payload.String != "" {
		e.Payload = json.RawMessage(payload.String)
	} else {
		e.Payload = json.RawMessage("{}")
	}
	if ackedAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", ackedAt.String)
		e.AckedAt = &t
	}
	if leasedAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", leasedAt.String)
		e.LeasedAt = &t
	}
	e.CreatedAt, _ = time.Parse("2006-01-02 15:04:05", createdAt)
	return &e, nil
}

func scanEnvelopes(rows *sql.Rows) ([]*Envelope, error) {
	var out []*Envelope
	for rows.Next() {
		var e Envelope
		var artifactRefs, payload, typeStr, ackedAt, leasedAt sql.NullString
		var acked, ackReq, deliveryAttempt int
		var createdAt string
		if err := rows.Scan(&e.ID, &e.MessageID, &e.RunID, &e.RunEpoch,
			&e.TaskID, &e.TaskGeneration, &e.AttemptID, &e.AttemptGeneration,
			&e.Sender, &e.Recipient, &typeStr, &artifactRefs, &payload,
			&ackReq, &acked, &ackedAt, &leasedAt, &deliveryAttempt, &e.LeaseOwner, &createdAt); err != nil {
			return nil, err
		}
		e.Type = Type(typeStr.String)
		e.ArtifactRefs = artifactRefs.String
		e.AckRequired = ackReq
		e.Acked = acked
		e.DeliveryAttempt = deliveryAttempt
		if payload.Valid && payload.String != "" {
			e.Payload = json.RawMessage(payload.String)
		} else {
			e.Payload = json.RawMessage("{}")
		}
		if ackedAt.Valid {
			t, _ := time.Parse("2006-01-02 15:04:05", ackedAt.String)
			e.AckedAt = &t
		}
		if leasedAt.Valid {
			t, _ := time.Parse("2006-01-02 15:04:05", leasedAt.String)
			e.LeasedAt = &t
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
