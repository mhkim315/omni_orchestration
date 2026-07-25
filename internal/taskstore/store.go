// Package taskstore provides a durable SQLite-backed event store for
// orchestrator runs, tasks, attempts, workers, and events.
//
// WAL mode is enabled for concurrent read/write safety.
package taskstore

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Status values.
const (
	StatusPending   = "pending"
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusCancelled = "cancelled"
)

// Store is a durable SQLite-backed event store.
type Store struct {
	db *sql.DB
	mu sync.Mutex
}

// Run represents an orchestrator run.
type Run struct {
	ID        int64     `json:"id"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// Task is a unit of work within a run.
type Task struct {
	ID     int64  `json:"id"`
	RunID  int64  `json:"run_id"`
	Title  string `json:"title"`
	Status string `json:"status"`
}

// Attempt is one execution attempt for a task.
type Attempt struct {
	ID               int64  `json:"id"`
	TaskID           int64  `json:"task_id"`
	Number           int    `json:"number"`
	WorkerID         string `json:"worker_id"`
	Branch           string `json:"branch"`
	BaseCommit       string `json:"base_commit"`
	CheckpointCommit string `json:"checkpoint_commit"`
	Status           string `json:"status"`
}

// WorkerRecord tracks a worker process within an attempt.
type WorkerRecord struct {
	ID         int64  `json:"id"`
	AttemptID  int64  `json:"attempt_id"`
	Command    string `json:"command"`
	CWD        string `json:"cwd"`
	Role       string `json:"role"`
	Generation int64  `json:"generation"`
	Status     string `json:"status"`
}

// Event is an emitted event tied to an orchestrator entity.
type Event struct {
	ID        int64           `json:"id"`
	RunID     int64           `json:"run_id"`
	TaskID    int64           `json:"task_id"`
	AttemptID int64           `json:"attempt_id"`
	WorkerID  int64           `json:"worker_id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

// EventAck records that an event was acknowledged.
type EventAck struct {
	ID      int64     `json:"id"`
	EventID int64     `json:"event_id"`
	AckedAt time.Time `json:"acked_at"`
}

// New opens (or creates) the SQLite store at the given path.
func New(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	db.SetMaxOpenConns(1) // SQLite serializes writes

	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

// NewInMemory creates an in-memory store for tests.
func NewInMemory() (*Store, error) {
	db, err := sql.Open("sqlite", ":memory:?_journal_mode=WAL")
	if err != nil {
		return nil, fmt.Errorf("open memory: %w", err)
	}
	db.SetMaxOpenConns(1)

	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	ddl := `
	CREATE TABLE IF NOT EXISTS runs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		status TEXT NOT NULL DEFAULT 'pending',
		created_at TEXT NOT NULL DEFAULT (datetime('now'))
	);
	CREATE TABLE IF NOT EXISTS tasks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		run_id INTEGER NOT NULL REFERENCES runs(id),
		title TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL DEFAULT 'pending'
	);
	CREATE TABLE IF NOT EXISTS attempts (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		task_id INTEGER NOT NULL REFERENCES tasks(id),
		number INTEGER NOT NULL DEFAULT 1,
		worker_id TEXT NOT NULL DEFAULT '',
		branch TEXT NOT NULL DEFAULT '',
		base_commit TEXT NOT NULL DEFAULT '',
		checkpoint_commit TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL DEFAULT 'pending'
	);
	CREATE TABLE IF NOT EXISTS workers (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		attempt_id INTEGER NOT NULL REFERENCES attempts(id),
		command TEXT NOT NULL DEFAULT '',
		cwd TEXT NOT NULL DEFAULT '',
		role TEXT NOT NULL DEFAULT '',
		generation INTEGER NOT NULL DEFAULT 1,
		status TEXT NOT NULL DEFAULT 'pending'
	);
	CREATE TABLE IF NOT EXISTS events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		run_id INTEGER NOT NULL DEFAULT 0,
		task_id INTEGER NOT NULL DEFAULT 0,
		attempt_id INTEGER NOT NULL DEFAULT 0,
		worker_id INTEGER NOT NULL DEFAULT 0,
		type TEXT NOT NULL DEFAULT '',
		payload TEXT NOT NULL DEFAULT '{}',
		created_at TEXT NOT NULL DEFAULT (datetime('now'))
	);
	CREATE TABLE IF NOT EXISTS event_acks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		event_id INTEGER NOT NULL REFERENCES events(id),
		acked_at TEXT NOT NULL DEFAULT (datetime('now'))
	);
	`
	_, err := s.db.Exec(ddl)
	return err
}

// ── Runs ──

func (s *Store) CreateRun() (*Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.Exec("INSERT INTO runs (status) VALUES ('pending')")
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()
	return s.getRun(id)
}

func (s *Store) getRun(id int64) (*Run, error) {
	r := &Run{}
	var createdAt string
	err := s.db.QueryRow("SELECT id, status, created_at FROM runs WHERE id=?", id).
		Scan(&r.ID, &r.Status, &createdAt)
	if err != nil {
		return nil, err
	}
	r.CreatedAt, _ = time.Parse("2006-01-02 15:04:05", createdAt)
	return r, nil
}

func (s *Store) UpdateRunStatus(id int64, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec("UPDATE runs SET status=? WHERE id=?", status, id)
	return err
}

// ── Tasks ──

func (s *Store) CreateTask(runID int64, title string) (*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.Exec("INSERT INTO tasks (run_id, title, status) VALUES (?, ?, 'pending')", runID, title)
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()
	return &Task{ID: id, RunID: runID, Title: title, Status: StatusPending}, nil
}

func (s *Store) UpdateTaskStatus(id int64, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec("UPDATE tasks SET status=? WHERE id=?", status, id)
	return err
}

func (s *Store) GetTask(id int64) (*Task, error) {
	t := &Task{}
	err := s.db.QueryRow("SELECT id, run_id, title, status FROM tasks WHERE id=?", id).
		Scan(&t.ID, &t.RunID, &t.Title, &t.Status)
	if err != nil {
		return nil, err
	}
	return t, nil
}

// ── Attempts ──

func (s *Store) CreateAttempt(taskID int64, number int, workerID, branch, baseCommit string) (*Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.Exec(
		"INSERT INTO attempts (task_id, number, worker_id, branch, base_commit, status) VALUES (?,?,?,?,?, 'pending')",
		taskID, number, workerID, branch, baseCommit,
	)
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()
	return &Attempt{ID: id, TaskID: taskID, Number: number, WorkerID: workerID, Branch: branch, BaseCommit: baseCommit, Status: StatusPending}, nil
}

func (s *Store) UpdateAttemptStatus(id int64, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec("UPDATE attempts SET status=? WHERE id=?", status, id)
	return err
}

func (s *Store) UpdateAttemptCheckpoint(id int64, sha string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec("UPDATE attempts SET checkpoint_commit=? WHERE id=?", sha, id)
	return err
}

func (s *Store) GetActiveAttempts() ([]*Attempt, error) {
	rows, err := s.db.Query("SELECT id, task_id, number, worker_id, branch, base_commit, checkpoint_commit, status FROM attempts WHERE status IN ('pending','running')")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Attempt
	for rows.Next() {
		a := &Attempt{}
		if err := rows.Scan(&a.ID, &a.TaskID, &a.Number, &a.WorkerID, &a.Branch, &a.BaseCommit, &a.CheckpointCommit, &a.Status); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ── Workers ──

func (s *Store) RecordWorker(attemptID int64, command, cwd, role string, generation int64) (*WorkerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.Exec(
		"INSERT INTO workers (attempt_id, command, cwd, role, generation, status) VALUES (?,?,?,?,?, 'running')",
		attemptID, command, cwd, role, generation,
	)
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()
	return &WorkerRecord{ID: id, AttemptID: attemptID, Command: command, CWD: cwd, Role: role, Generation: generation, Status: StatusRunning}, nil
}

func (s *Store) UpdateWorkerStatus(id int64, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec("UPDATE workers SET status=? WHERE id=?", status, id)
	return err
}

// ── Events ──

func (s *Store) EmitEvent(runID, taskID, attemptID, workerID int64, typ string, payload json.RawMessage) (*Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if payload == nil {
		payload = json.RawMessage("{}")
	}
	res, err := s.db.Exec(
		"INSERT INTO events (run_id, task_id, attempt_id, worker_id, type, payload) VALUES (?,?,?,?,?,?)",
		runID, taskID, attemptID, workerID, typ, string(payload),
	)
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()
	return &Event{
		ID: id, RunID: runID, TaskID: taskID, AttemptID: attemptID,
		WorkerID: workerID, Type: typ, Payload: payload, CreatedAt: time.Now(),
	}, nil
}

// GetUnackedEvents returns events that have not been acknowledged.
func (s *Store) GetUnackedEvents() ([]*Event, error) {
	rows, err := s.db.Query(`
		SELECT e.id, e.run_id, e.task_id, e.attempt_id, e.worker_id, e.type, e.payload, e.created_at
		FROM events e
		LEFT JOIN event_acks a ON e.id = a.event_id
		WHERE a.id IS NULL
		ORDER BY e.id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEvents(rows)
}

// AckEvent records an acknowledgement for an event.
// Dedup: re-acking the same event is idempotent (INSERT OR IGNORE).
func (s *Store) AckEvent(eventID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec("INSERT OR IGNORE INTO event_acks (event_id) VALUES (?)", eventID)
	return err
}

// ── helpers ──

func scanEvents(rows *sql.Rows) ([]*Event, error) {
	var out []*Event
	for rows.Next() {
		e := &Event{}
		var payload, createdAt string
		if err := rows.Scan(&e.ID, &e.RunID, &e.TaskID, &e.AttemptID, &e.WorkerID, &e.Type, &payload, &createdAt); err != nil {
			return nil, err
		}
		e.Payload = json.RawMessage(payload)
		e.CreatedAt, _ = time.Parse("2006-01-02 15:04:05", createdAt)
		out = append(out, e)
	}
	return out, rows.Err()
}
