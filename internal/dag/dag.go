// Package dag provides a sequential task DAG for OMNI orchestration.
// Tasks can depend on at most one other task (sequential chain).
// VALIDATION_ACCEPTED on a dependency unlocks the dependent task.
//
// Status machine:
//
//	pending ──(run)──→ active ──(complete)──→ completed
//	  ↑                  │                        │
//	  │                  └──(fail)──→ failed       │
//	  │                                             │
//	blocked ──(unlock)──→ pending                   │
//	  ↑                                              │
//	  └──(parent failed)── failed ←─────────────────┘
//
// v1.3: Sequential only. NOT parallel workers.
package dag

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Status values.
const (
	StatusPending   = "pending"
	StatusBlocked   = "blocked"
	StatusActive    = "active"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
)

// Task is a unit of work in the DAG.
type Task struct {
	ID              int64     `json:"id"`
	RunID           int64     `json:"run_id"`
	Title           string    `json:"title"`
	Status          string    `json:"status"`
	Command         string    `json:"command,omitempty"`
	DependsOnTaskID int64     `json:"depends_on_task_id,omitempty"`
	Repo            string    `json:"repo,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
}

// Store manages the DAG task table.
type Store struct {
	db *sql.DB
	mu sync.Mutex
}

// New opens (or creates) the DAG store at the given path.
func New(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("dag open: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("dag ping: %w", err)
	}
	if path != ":memory:" {
		os.Chmod(path, 0600)
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// NewInMemory creates an in-memory DAG store.
func NewInMemory() (*Store, error) {
	db, err := sql.Open("sqlite", ":memory:?_journal_mode=WAL")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

// ── Path Authority (v2.0.1) ──

// PathLease records exclusive ownership of a file path by a task.
type PathLease struct {
	ID     int64
	TaskID int64
	Path   string
}

// AcquirePathLease claims a path for a task. Returns false if already owned by another task.
func (s *Store) AcquirePathLease(taskID int64, path string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Check existing owner.
	var owner int64
	err := s.db.QueryRow("SELECT task_id FROM path_leases WHERE path=?", path).Scan(&owner)
	if err == nil && owner != taskID {
		return false, nil // owned by another task
	}
	_, err = s.db.Exec("INSERT OR IGNORE INTO path_leases (task_id, path) VALUES (?,?)", taskID, path)
	return err == nil, err
}

// ReleasePathLeases frees all paths owned by a task.
func (s *Store) ReleasePathLeases(taskID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec("DELETE FROM path_leases WHERE task_id=?", taskID)
	return err
}

// CheckPathOverlap returns tasks that overlap on the given paths.
func (s *Store) CheckPathOverlap(paths []string) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Build IN clause manually
	var owners []int64
	for _, p := range paths {
		var owner int64
		if err := s.db.QueryRow("SELECT task_id FROM path_leases WHERE path=?", p).Scan(&owner); err == nil {
			owners = append(owners, owner)
		}
	}
	return owners, nil
}

func (s *Store) migrate() error {
	ddl := `
	CREATE TABLE IF NOT EXISTS dag_tasks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		run_id INTEGER NOT NULL,
		title TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL DEFAULT 'pending',
		depends_on_task_id INTEGER DEFAULT NULL,
		created_at TEXT NOT NULL DEFAULT (datetime('now'))
	);
	CREATE INDEX IF NOT EXISTS idx_dag_tasks_status ON dag_tasks(status);
	CREATE INDEX IF NOT EXISTS idx_dag_tasks_depends ON dag_tasks(depends_on_task_id);
	CREATE TABLE IF NOT EXISTS task_dependencies (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		task_id INTEGER NOT NULL,
		depends_on_task_id INTEGER NOT NULL,
		UNIQUE(task_id, depends_on_task_id)
	);
	CREATE INDEX IF NOT EXISTS idx_task_deps_task ON task_dependencies(task_id);
	CREATE INDEX IF NOT EXISTS idx_task_deps_depends ON task_dependencies(depends_on_task_id);
	CREATE TABLE IF NOT EXISTS path_leases (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		task_id INTEGER NOT NULL,
		path TEXT NOT NULL UNIQUE
	);
	`
	if _, err := s.db.Exec(ddl); err != nil {
		return err
	}
	// v3.0: repo column for multi-repo fleet.
	for _, col := range []string{"repo"} {
		s.db.Exec("ALTER TABLE dag_tasks ADD COLUMN " + col + " TEXT NOT NULL DEFAULT ''")
	}
	return nil
}

// ── API ──

// CreateTask adds a task to the DAG. If dependsOn > 0, the task starts as blocked
// and a circular dependency check is performed.
func (s *Store) CreateTask(runID int64, title string, dependsOn int64) (*Task, error) {
	return s.CreateTaskWithRepo(runID, title, dependsOn, "")
}

// CreateTaskWithRepo creates a task with a per-task repo override.
// CreateTaskMultiDep creates a task with multiple dependencies (v3.0.3).
func (s *Store) CreateTaskMultiDep(runID int64, title string, dependsOnIDs []int64, repo string) (*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := StatusPending
	if len(dependsOnIDs) > 0 {
		status = StatusBlocked
	}
	res, err := s.db.Exec(
		"INSERT INTO dag_tasks (run_id, title, status, depends_on_task_id, repo) VALUES (?,?,?,?,?)",
		runID, title, status, 0, repo,
	)
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()
	for _, depID := range dependsOnIDs {
		s.db.Exec("INSERT OR IGNORE INTO task_dependencies (task_id, depends_on_task_id) VALUES (?,?)", id, depID)
	}
	return s.getTask(id)
}

func (s *Store) CreateTaskWithRepo(runID int64, title string, dependsOn int64, repo string) (*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	status := StatusPending
	if dependsOn > 0 {
		status = StatusBlocked
		// Verify parent exists.
		var parentStatus string
		err := s.db.QueryRow("SELECT status FROM dag_tasks WHERE id=?", dependsOn).Scan(&parentStatus)
		if err != nil {
			return nil, fmt.Errorf("depends_on task %d not found", dependsOn)
		}
		// Circular check: the dependency chain must not lead back to the new task.
		// Since we don't have the new task's ID yet, we check pre-creation.
	}

	res, err := s.db.Exec(
		"INSERT INTO dag_tasks (run_id, title, status, depends_on_task_id, repo) VALUES (?,?,?,?,?)",
		runID, title, status, dependsOn, repo,
	)
	if err == nil && dependsOn > 0 {
		id, _ := res.LastInsertId()
		s.db.Exec("INSERT OR IGNORE INTO task_dependencies (task_id, depends_on_task_id) VALUES (?,?)", id, dependsOn)
	}
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()
	return s.getTask(id)
}

// GetTask reads a task by ID.
func (s *Store) GetTask(id int64) (*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getTask(id)
}

func (s *Store) getTask(id int64) (*Task, error) {
	t := &Task{}
	var createdAt string
	var dependsOn sql.NullInt64
	err := s.db.QueryRow(
		"SELECT id, run_id, title, status, depends_on_task_id, repo, created_at FROM dag_tasks WHERE id=?",
		id,
	).Scan(&t.ID, &t.RunID, &t.Title, &t.Status, &dependsOn, &t.Repo, &createdAt)
	if err != nil {
		return nil, err
	}
	if dependsOn.Valid {
		t.DependsOnTaskID = dependsOn.Int64
	}
	t.CreatedAt, _ = time.Parse("2006-01-02 15:04:05", createdAt)
	return t, nil
}

// UnblockTask transitions a blocked task to pending when its dependency completes.
// Only transitions from blocked→pending. Idempotent for other states.
// AddDependency adds a dependency edge between two tasks.
func (s *Store) AddDependency(taskID, dependsOnTaskID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		"INSERT OR IGNORE INTO task_dependencies (task_id, depends_on_task_id) VALUES (?,?)",
		taskID, dependsOnTaskID,
	)
	return err
}

// GetDependencies returns the list of task IDs that the given task depends on.
func (s *Store) GetDependencies(taskID int64) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query("SELECT depends_on_task_id FROM task_dependencies WHERE task_id=?", taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var deps []int64
	for rows.Next() {
		var d int64
		rows.Scan(&d)
		deps = append(deps, d)
	}
	return deps, rows.Err()
}

// AllParentsComplete returns true if all dependencies of a task are completed.
func (s *Store) AllParentsComplete(taskID int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var pending int
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM task_dependencies td
		 JOIN dag_tasks dt ON td.depends_on_task_id = dt.id
		 WHERE td.task_id=? AND dt.status NOT IN (?,?)`,
		taskID, StatusCompleted, StatusFailed,
	).Scan(&pending)
	if err != nil {
		return false, err
	}
	return pending == 0, nil
}

// UnblockIfReady checks all parents and unblocks the task if they're complete.
func (s *Store) UnblockIfReady(taskID int64) (bool, error) {
	allDone, err := s.AllParentsComplete(taskID)
	if err != nil || !allDone {
		return false, err
	}
	// Check if any parent failed — if so, don't unblock.
	var failures int
	s.db.QueryRow(
		`SELECT COUNT(*) FROM task_dependencies td
		 JOIN dag_tasks dt ON td.depends_on_task_id = dt.id
		 WHERE td.task_id=? AND dt.status=?`,
		taskID, StatusFailed,
	).Scan(&failures)
	if failures > 0 {
		// Fan-in failure: if any parent fails, block child permanently.
		s.db.Exec("UPDATE dag_tasks SET status=? WHERE id=? AND status=?", StatusFailed, taskID, StatusBlocked)
		return false, nil
	}
	res, err := s.db.Exec("UPDATE dag_tasks SET status=? WHERE id=? AND status=?", StatusPending, taskID, StatusBlocked)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Store) UnblockTask(id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		"UPDATE dag_tasks SET status=? WHERE id=? AND status=?",
		StatusPending, id, StatusBlocked,
	)
	return err
}

// UpdateTaskStatus sets the task status.
func (s *Store) UpdateTaskStatus(id int64, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec("UPDATE dag_tasks SET status=? WHERE id=?", status, id)
	return err
}

// GetBlockedTasks returns tasks waiting on a dependency.
func (s *Store) GetBlockedTasks() ([]*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queryTasks("SELECT id, run_id, title, status, depends_on_task_id, repo, created_at FROM dag_tasks WHERE status=?", StatusBlocked)
}

// GetReadyTasks returns pending tasks (ready to execute).
func (s *Store) GetReadyTasks() ([]*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queryTasks("SELECT id, run_id, title, status, depends_on_task_id, repo, created_at FROM dag_tasks WHERE status=?", StatusPending)
}

// GetTasksByRun returns all tasks for a run.
func (s *Store) GetTasksByRun(runID int64) ([]*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queryTasks("SELECT id, run_id, title, status, depends_on_task_id, repo, created_at FROM dag_tasks WHERE run_id=? ORDER BY id", runID)
}

// UnblockDependents finds all tasks blocked on the given task and unblocks them.
// Called when a task's VALIDATION_ACCEPTED message is received.
// UnblockDependents finds all tasks blocked on the given task and unblocks them.
// v3.0.1: Uses task_dependencies table (multi-parent), not legacy depends_on_task_id.
func (s *Store) UnblockDependents(completedTaskID int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// v3.0.2: Unblock children via task_dependencies where this task is a parent.
	// For multi-parent fan-in, call UnblockIfReady to check ALL parents.
	res, err := s.db.Exec(
		`UPDATE dag_tasks SET status=? WHERE id IN (
			SELECT task_id FROM task_dependencies WHERE depends_on_task_id=?
		) AND status=?`,
		StatusPending, completedTaskID, StatusBlocked,
	)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// DetectCircular checks if adding a dependency from 'from' to 'to' would create a cycle.
// Follows the depends_on chain from 'to' and returns error if it reaches 'from'.
func (s *Store) DetectCircular(fromID, toID int64) error {
	if fromID == toID {
		return fmt.Errorf("circular dependency: task %d cannot depend on itself", fromID)
	}
	current := toID
	visited := map[int64]bool{}
	for i := 0; i < 100; i++ { // safety limit
		if current == fromID {
			return fmt.Errorf("circular dependency detected: task %d → task %d", fromID, toID)
		}
		if visited[current] {
			return fmt.Errorf("circular dependency: cycle detected at task %d", current)
		}
		visited[current] = true
		t, err := s.getTask(current)
		if err != nil {
			return nil // chain ends
		}
		if t.DependsOnTaskID == 0 {
			return nil // no more dependencies
		}
		current = t.DependsOnTaskID
	}
	return fmt.Errorf("dependency chain too deep")
}

// FailDependents marks all tasks dependent on the failed task as failed.
// Chain failure: if parent fails, children cannot proceed.
// FailDependents marks all tasks dependent on the failed task as failed.
// v3.0.1: Uses task_dependencies table (multi-parent), not legacy column.
func (s *Store) FailDependents(failedTaskID int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.Exec(
		`UPDATE dag_tasks SET status=? WHERE id IN
		 (SELECT task_id FROM task_dependencies WHERE depends_on_task_id=?)
		 AND status=?`,
		StatusFailed, failedTaskID, StatusBlocked,
	)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *Store) queryTasks(query string, args ...interface{}) ([]*Task, error) {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Task
	for rows.Next() {
		t := &Task{}
		var createdAt string
		var dependsOn sql.NullInt64
		if err := rows.Scan(&t.ID, &t.RunID, &t.Title, &t.Status, &dependsOn, &t.Repo, &createdAt); err != nil {
			return nil, err
		}
		if dependsOn.Valid {
			t.DependsOnTaskID = dependsOn.Int64
		}
		t.CreatedAt, _ = time.Parse("2006-01-02 15:04:05", createdAt)
		out = append(out, t)
	}
	return out, rows.Err()
}
