// Package taskstore provides a durable SQLite-backed event store for
// orchestrator runs, tasks, attempts, workers, and events.
//
// WAL mode is enabled for concurrent read/write safety.
package taskstore

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
	PID        int    `json:"pid"`
	PGID       int    `json:"pgid"`
	StartTime  int64  `json:"start_time"`
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

	// Verify the database is reachable before setting permissions.
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}

	// HIGH: SQLite store must be owner-only. Chmod AFTER the file exists.
	if path != ":memory:" {
		if err := os.Chmod(path, 0600); err != nil {
			db.Close()
			return nil, fmt.Errorf("chmod: %w", err)
		}
	}

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
	CREATE TABLE IF NOT EXISTS run_records (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		run_id INTEGER NOT NULL REFERENCES runs(id),
		provider TEXT NOT NULL DEFAULT '',
		model TEXT NOT NULL DEFAULT '',
		role TEXT NOT NULL DEFAULT '',
		task_category TEXT NOT NULL DEFAULT '',
		repo TEXT NOT NULL DEFAULT '',
		attempt_count INTEGER NOT NULL DEFAULT 0,
		validator_reject_count INTEGER NOT NULL DEFAULT 0,
		replacement_count INTEGER NOT NULL DEFAULT 0,
		duration_ms INTEGER NOT NULL DEFAULT 0,
		final_adopted_attempt INTEGER NOT NULL DEFAULT 0,
		created_at TEXT NOT NULL DEFAULT (datetime('now'))
	);
	CREATE TABLE IF NOT EXISTS event_acks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		event_id INTEGER NOT NULL UNIQUE REFERENCES events(id),
		acked_at TEXT NOT NULL DEFAULT (datetime('now'))
	);
	CREATE TABLE IF NOT EXISTS effect_keys (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		run_id INTEGER NOT NULL,
		effect_key TEXT NOT NULL,
		created_at TEXT NOT NULL DEFAULT (datetime('now')),
		UNIQUE(run_id, effect_key)
	);
	`
	_, err := s.db.Exec(ddl)
	if err != nil {
		return err
	}
	// Migration: add pid/pgid/start_time_ms. Ignore "duplicate column" errors.
	for _, col := range []string{"pid", "pgid", "start_time_ms"} {
		if _, err := s.db.Exec("ALTER TABLE workers ADD COLUMN " + col + " INTEGER NOT NULL DEFAULT 0"); err != nil {
			// SQLite error "duplicate column name" is expected on re-run.
			if !strings.Contains(err.Error(), "duplicate") {
				return fmt.Errorf("alter workers add %s: %w", col, err)
			}
		}
	}
	return nil
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

// GetTasksByRun returns all tasks for a run.
func (s *Store) GetTasksByRun(runID int64) ([]*Task, error) {
	rows, err := s.db.Query("SELECT id, run_id, title, status FROM tasks WHERE run_id=?", runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Task
	for rows.Next() {
		t := &Task{}
		if err := rows.Scan(&t.ID, &t.RunID, &t.Title, &t.Status); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
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

// ── Result queries ──

// GetAttemptsByTask returns all attempts for a task.
func (s *Store) GetAttemptsByTask(taskID int64) ([]*Attempt, error) {
	rows, err := s.db.Query(
		"SELECT id, task_id, number, worker_id, branch, base_commit, checkpoint_commit, status FROM attempts WHERE task_id=? ORDER BY number",
		taskID,
	)
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

// GetAttempt returns a single attempt by ID.
func (s *Store) GetAttempt(id int64) (*Attempt, error) {
	a := &Attempt{}
	err := s.db.QueryRow(
		"SELECT id, task_id, number, worker_id, branch, base_commit, checkpoint_commit, status FROM attempts WHERE id=?",
		id,
	).Scan(&a.ID, &a.TaskID, &a.Number, &a.WorkerID, &a.Branch, &a.BaseCommit, &a.CheckpointCommit, &a.Status)
	if err != nil {
		return nil, err
	}
	return a, nil
}

// GetRunRecord returns the run_records row for a run.
func (s *Store) GetRunRecord(runID int64) (*RunRecord, error) {
	r := &RunRecord{}
	err := s.db.QueryRow(
		"SELECT id, run_id, provider, model, role, task_category, repo, attempt_count, validator_reject_count, replacement_count, duration_ms, final_adopted_attempt, created_at FROM run_records WHERE run_id=?",
		runID,
	).Scan(&r.ID, &r.RunID, &r.Provider, &r.Model, &r.Role, &r.TaskCategory, &r.Repo, &r.AttemptCount, &r.ValidatorRejectCount, &r.ReplacementCount, &r.DurationMs, &r.FinalAdoptedAttempt, &r.CreatedAt)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// GetRun returns a run by ID.
func (s *Store) GetRun(id int64) (*Run, error) { return s.getRun(id) }

// ── Workers ──

func (s *Store) RecordWorker(attemptID int64, command, cwd, role string, generation int64) (*WorkerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nowMs := time.Now().UnixMilli()
	res, err := s.db.Exec(
		"INSERT INTO workers (attempt_id, command, cwd, role, generation, status, pid, pgid, start_time_ms) VALUES (?,?,?,?,?, 'running', ?, ?, ?)",
		attemptID, command, cwd, role, generation, 0, 0, nowMs,
	)
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()
	return &WorkerRecord{ID: id, AttemptID: attemptID, Command: command, CWD: cwd, Role: role, Generation: generation, Status: StatusRunning, StartTime: nowMs}, nil
}

// RecordWorkerPID stores a worker with process identity for recovery.
func (s *Store) RecordWorkerPID(attemptID int64, command, cwd, role string, generation int64, pid, pgid int) (*WorkerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nowMs := time.Now().UnixMilli()
	res, err := s.db.Exec(
		"INSERT INTO workers (attempt_id, command, cwd, role, generation, status, pid, pgid, start_time_ms) VALUES (?,?,?,?,?, 'running', ?, ?, ?)",
		attemptID, command, cwd, role, generation, pid, pgid, nowMs,
	)
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()
	return &WorkerRecord{ID: id, AttemptID: attemptID, Command: command, CWD: cwd, Role: role, Generation: generation, Status: StatusRunning, PID: pid, PGID: pgid, StartTime: nowMs}, nil
}

// GetWorker reads a worker record from the database.
func (s *Store) GetWorker(id int64) (*WorkerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row := s.db.QueryRow("SELECT id, attempt_id, command, cwd, role, generation, status, pid, pgid, start_time_ms FROM workers WHERE id=?", id)
	var w WorkerRecord
	err := row.Scan(&w.ID, &w.AttemptID, &w.Command, &w.CWD, &w.Role, &w.Generation, &w.Status, &w.PID, &w.PGID, &w.StartTime)
	if err != nil {
		return nil, err
	}
	return &w, nil
}

// GetWorkerByAttempt reads the first worker for a given attempt ID.
func (s *Store) GetWorkerByAttempt(attemptID int64) (*WorkerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row := s.db.QueryRow("SELECT id, attempt_id, command, cwd, role, generation, status, pid, pgid, start_time_ms FROM workers WHERE attempt_id=? ORDER BY id DESC LIMIT 1", attemptID)
	var w WorkerRecord
	err := row.Scan(&w.ID, &w.AttemptID, &w.Command, &w.CWD, &w.Role, &w.Generation, &w.Status, &w.PID, &w.PGID, &w.StartTime)
	if err != nil {
		return nil, err
	}
	return &w, nil
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

// ── Performance Data Collection (D) ──

// RunRecord captures the outcome of one orchestrator run for provider stats.
type RunRecord struct {
	ID                   int64  `json:"id"`
	RunID                int64  `json:"run_id"`
	Provider             string `json:"provider"`
	Model                string `json:"model"`
	Role                 string `json:"role"`
	TaskCategory         string `json:"task_category"`
	Repo                 string `json:"repo"`
	AttemptCount         int    `json:"attempt_count"`
	ValidatorRejectCount int    `json:"validator_reject_count"`
	ReplacementCount     int    `json:"replacement_count"`
	DurationMs           int64  `json:"duration_ms"`
	FinalAdoptedAttempt  int    `json:"final_adopted_attempt"`
	CreatedAt            string `json:"created_at"`
}

// ProviderStats summarizes performance for one provider+category.
type ProviderStats struct {
	Provider      string
	TaskCategory  string
	TotalRuns     int
	SuccessRate   float64 // 0.0 - 1.0
	AvgAttempts   float64
	AvgDurationMs int64
	Confidence    string // "high", "low", "insufficient"
}

// RecordRun inserts a run record. Called once per orchestrator run.
func (s *Store) RecordRun(runID int64, provider, model, role, taskCategory, repo string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`INSERT INTO run_records (run_id, provider, model, role, task_category, repo)
		 VALUES (?,?,?,?,?,?)`,
		runID, provider, model, role, taskCategory, repo,
	)
	return err
}

// RecordAttemptOutcome updates the run record with attempt-level stats.
func (s *Store) RecordAttemptOutcome(runID int64, attemptNum int, validatorPassed bool, durationMs int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if validatorPassed {
		_, err := s.db.Exec(
			`UPDATE run_records SET attempt_count=MAX(attempt_count,?),
			 validator_reject_count=validator_reject_count,
			 duration_ms=duration_ms+? WHERE run_id=?`,
			attemptNum, durationMs, runID,
		)
		return err
	}
	_, err := s.db.Exec(
		`UPDATE run_records SET attempt_count=MAX(attempt_count,?),
		 validator_reject_count=validator_reject_count+1,
		 duration_ms=duration_ms+? WHERE run_id=?`,
		attemptNum, durationMs, runID,
	)
	return err
}

// StatsFor returns aggregate provider stats (R2 Fix 1: local type, no coordinator import).
func (s *Store) StatsFor(provider string) ProviderStatsRow {
	var total, successes, rejects int
	var totalMs int64
	row := s.db.QueryRow(
		"SELECT COUNT(*), COALESCE(SUM(CASE WHEN final_adopted_attempt>0 THEN 1 ELSE 0 END),0), COALESCE(SUM(validator_reject_count),0), COALESCE(SUM(duration_ms),0) FROM run_records WHERE provider=?",
		provider,
	)
	row.Scan(&total, &successes, &rejects, &totalMs)
	return ProviderStatsRow{
		TotalAttempts: total, Successes: successes,
		TotalRejects: rejects, TotalTimeMs: totalMs,
	}
}

// ProviderStatsRow holds aggregate provider stats.
type ProviderStatsRow struct {
	TotalAttempts int
	Successes     int
	TotalRejects  int
	TotalTimeMs   int64
}

// RecordAdoption marks the final adopted attempt for a run.
func (s *Store) RecordAdoption(runID int64, attemptNum int, adopted bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if adopted {
		_, err := s.db.Exec(
			`UPDATE run_records SET final_adopted_attempt=? WHERE run_id=?`,
			attemptNum, runID,
		)
		return err
	}
	return nil
}

// RecordEffect records a durable effect key for a run. Returns false if the
// effect key already exists (duplicate), true if it was newly inserted.
// R2 Fix 4: Decision Gateway durable effect keys in SQLite.
func (s *Store) RecordEffect(runID int64, key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.Exec(
		"INSERT OR IGNORE INTO effect_keys (run_id, effect_key) VALUES (?, ?)",
		runID, key,
	)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// HasEffect returns true if the effect key has already been recorded for the run.
// R2 Fix 4: Decision Gateway durable effect keys in SQLite.
func (s *Store) HasEffect(runID int64, key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	var count int
	s.db.QueryRow(
		"SELECT COUNT(*) FROM effect_keys WHERE run_id=? AND effect_key=?",
		runID, key,
	).Scan(&count)
	return count > 0
}

// GetProviderStats returns aggregated stats for a provider + task category.
// Confidence: "high" (≥10 runs), "low" (3-9 runs), "insufficient" (<3 runs).
func (s *Store) GetProviderStats(provider, taskCategory string) (*ProviderStats, error) {
	st := &ProviderStats{Provider: provider, TaskCategory: taskCategory}

	rows, err := s.db.Query(
		`SELECT COUNT(*), AVG(CAST(validator_reject_count=0 AND final_adopted_attempt>0 AS FLOAT)),
		 AVG(CAST(attempt_count AS FLOAT)), AVG(CAST(duration_ms AS FLOAT))
		 FROM run_records
		 WHERE provider=? AND task_category=? AND final_adopted_attempt>0`,
		provider, taskCategory,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		st.Confidence = "insufficient"
		return st, nil
	}

	var count int
	var successRate, avgAttempts, avgDuration sql.NullFloat64
	rows.Scan(&count, &successRate, &avgAttempts, &avgDuration)
	st.TotalRuns = count
	if count > 0 {
		st.SuccessRate = successRate.Float64
		st.AvgAttempts = avgAttempts.Float64
		st.AvgDurationMs = int64(avgDuration.Float64)
	}

	switch {
	case count >= 10:
		st.Confidence = "high"
	case count >= 3:
		st.Confidence = "low"
	default:
		st.Confidence = "insufficient"
	}
	return st, nil
}

// GetBestProvider returns the provider with the highest recent success rate
// for a given task category + repo. Returns empty result if insufficient data.
func (s *Store) GetBestProvider(taskCategory, repo string) (string, *ProviderStats, error) {

	rows, err := s.db.Query(
		`SELECT provider, COUNT(*) as cnt,
		 AVG(CAST(validator_reject_count=0 AND final_adopted_attempt>0 AS FLOAT)) as rate
		 FROM run_records
		 WHERE task_category=? AND repo=? AND final_adopted_attempt>0
		 GROUP BY provider HAVING cnt >= 3
		 ORDER BY rate DESC, cnt DESC LIMIT 1`,
		taskCategory, repo,
	)
	if err != nil {
		return "", nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return "", &ProviderStats{Confidence: "insufficient"}, nil
	}

	var provider string
	var cnt int
	var rate float64
	rows.Scan(&provider, &cnt, &rate)

	stats, _ := s.GetProviderStats(provider, taskCategory)
	return provider, stats, nil
}

// ── Durable Effect Keys (R3 Fix 4) ──

// HasEffectKey returns true if an effect key was already recorded (dedup).
func (s *Store) HasEffectKey(key string) bool {
	var count int
	s.db.QueryRow("SELECT COUNT(*) FROM event_acks WHERE event_id=(SELECT id FROM events WHERE type='effect' AND payload LIKE ?)", "%"+key+"%").Scan(&count)
	return count > 0
}

// RecordEffectKey stores a durable effect key for replay protection.
func (s *Store) RecordEffectKey(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec("INSERT INTO events (type, payload) VALUES ('effect', ?)", key)
	return err
}
