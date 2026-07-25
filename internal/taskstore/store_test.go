package taskstore

import (
	"encoding/json"
	"sync"
	"testing"
)

func TestStore_CreateRun(t *testing.T) {
	s, err := NewInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	run, err := s.CreateRun()
	if err != nil {
		t.Fatal(err)
	}
	if run.ID != 1 {
		t.Errorf("id = %d, want 1", run.ID)
	}
	if run.Status != StatusPending {
		t.Errorf("status = %s, want pending", run.Status)
	}

	if err := s.UpdateRunStatus(run.ID, StatusRunning); err != nil {
		t.Fatal(err)
	}
}

func TestStore_CreateTask(t *testing.T) {
	s, _ := NewInMemory()
	defer s.Close()

	run, _ := s.CreateRun()
	task, err := s.CreateTask(run.ID, "test-task")
	if err != nil {
		t.Fatal(err)
	}
	if task.Title != "test-task" {
		t.Errorf("title = %s", task.Title)
	}
	if task.RunID != run.ID {
		t.Errorf("runID = %d, want %d", task.RunID, run.ID)
	}

	got, err := s.GetTask(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Title != "test-task" {
		t.Errorf("get title = %s", got.Title)
	}
}

func TestStore_CreateAttempt(t *testing.T) {
	s, _ := NewInMemory()
	defer s.Close()

	run, _ := s.CreateRun()
	task, _ := s.CreateTask(run.ID, "task")

	a, err := s.CreateAttempt(task.ID, 1, "worker-1", "task/task/attempt-1", "abc123")
	if err != nil {
		t.Fatal(err)
	}
	if a.Number != 1 {
		t.Errorf("number = %d", a.Number)
	}
	if a.Branch != "task/task/attempt-1" {
		t.Errorf("branch = %s", a.Branch)
	}

	if err := s.UpdateAttemptStatus(a.ID, StatusRunning); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateAttemptCheckpoint(a.ID, "def456"); err != nil {
		t.Fatal(err)
	}

	active, err := s.GetActiveAttempts()
	if err != nil {
		t.Fatal(err)
	}
	if len(active) == 0 {
		t.Error("expected active attempt")
	}
}

func TestStore_RecordWorker(t *testing.T) {
	s, _ := NewInMemory()
	defer s.Close()

	run, _ := s.CreateRun()
	task, _ := s.CreateTask(run.ID, "task")
	a, _ := s.CreateAttempt(task.ID, 1, "w1", "b", "c1")

	w, err := s.RecordWorker(a.ID, "claude", "/tmp", "primary", 1)
	if err != nil {
		t.Fatal(err)
	}
	if w.Command != "claude" {
		t.Errorf("command = %s", w.Command)
	}
	if w.Role != "primary" {
		t.Errorf("role = %s", w.Role)
	}
	if w.Generation != 1 {
		t.Errorf("generation = %d", w.Generation)
	}

	if err := s.UpdateWorkerStatus(w.ID, StatusCompleted); err != nil {
		t.Fatal(err)
	}
}

func TestStore_EmitAndAckEvent(t *testing.T) {
	s, _ := NewInMemory()
	defer s.Close()

	run, _ := s.CreateRun()
	task, _ := s.CreateTask(run.ID, "task")
	a, _ := s.CreateAttempt(task.ID, 1, "w1", "b", "c1")
	w, _ := s.RecordWorker(a.ID, "cmd", "/", "primary", 1)

	payload := json.RawMessage(`{"key":"value"}`)
	ev, err := s.EmitEvent(run.ID, task.ID, a.ID, w.ID, "worker_started", payload)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Type != "worker_started" {
		t.Errorf("type = %s", ev.Type)
	}
	if string(ev.Payload) != `{"key":"value"}` {
		t.Errorf("payload = %s", ev.Payload)
	}

	// Get unacked — should include our event.
	unacked, err := s.GetUnackedEvents()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, u := range unacked {
		if u.ID == ev.ID {
			found = true
			break
		}
	}
	if !found {
		t.Error("event not found in unacked")
	}

	// Ack it.
	if err := s.AckEvent(ev.ID); err != nil {
		t.Fatal(err)
	}

	// Should no longer be unacked.
	unacked, _ = s.GetUnackedEvents()
	for _, u := range unacked {
		if u.ID == ev.ID {
			t.Error("event still unacked after ack")
		}
	}
}

func TestStore_EventAckDedup(t *testing.T) {
	s, _ := NewInMemory()
	defer s.Close()

	run, _ := s.CreateRun()
	task, _ := s.CreateTask(run.ID, "task")
	a, _ := s.CreateAttempt(task.ID, 1, "w1", "b", "c1")
	w, _ := s.RecordWorker(a.ID, "cmd", "/", "p", 1)

	ev, _ := s.EmitEvent(run.ID, task.ID, a.ID, w.ID, "test", nil)

	// Ack twice — second should be idempotent (INSERT OR IGNORE).
	if err := s.AckEvent(ev.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.AckEvent(ev.ID); err != nil {
		t.Fatalf("second ack should be idempotent: %v", err)
	}
}

func TestStore_ConcurrentWrites(t *testing.T) {
	s, _ := NewInMemory()
	defer s.Close()

	run, _ := s.CreateRun()
	task, _ := s.CreateTask(run.ID, "concurrent")

	var wg sync.WaitGroup
	errs := make(chan error, 20)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			a, err := s.CreateAttempt(task.ID, n, "worker", "branch", "base")
			if err != nil {
				errs <- err
				return
			}
			w, err := s.RecordWorker(a.ID, "cmd", "/tmp", "role", int64(n))
			if err != nil {
				errs <- err
				return
			}
			_, err = s.EmitEvent(run.ID, task.ID, a.ID, w.ID, "attempt_created", nil)
			if err != nil {
				errs <- err
			}
		}(i + 1)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent error: %v", err)
	}

	// All 20 attempts should be active.
	active, err := s.GetActiveAttempts()
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 20 {
		t.Errorf("active attempts = %d, want 20", len(active))
	}
}

func TestStore_ZeroValueForeignKeys(t *testing.T) {
	s, _ := NewInMemory()
	defer s.Close()

	// Events can have zero foreign keys (not tied to specific entity).
	ev, err := s.EmitEvent(0, 0, 0, 0, "system_event", nil)
	if err != nil {
		t.Fatal(err)
	}
	if ev.ID == 0 {
		t.Error("expected event ID > 0")
	}
}

func TestStore_UnackedEmpty(t *testing.T) {
	s, _ := NewInMemory()
	defer s.Close()

	unacked, err := s.GetUnackedEvents()
	if err != nil {
		t.Fatal(err)
	}
	if len(unacked) != 0 {
		t.Errorf("expected 0 unacked, got %d", len(unacked))
	}
}

func TestStore_AttemptCheckpoint(t *testing.T) {
	s, _ := NewInMemory()
	defer s.Close()

	run, _ := s.CreateRun()
	task, _ := s.CreateTask(run.ID, "ckpt-task")
	a, _ := s.CreateAttempt(task.ID, 1, "w1", "branch", "base")

	if err := s.UpdateAttemptCheckpoint(a.ID, "sha-abc123"); err != nil {
		t.Fatal(err)
	}

	active, _ := s.GetActiveAttempts()
	if len(active) == 0 {
		t.Fatal("no active attempts")
	}
	// checkpoint_commit column. Query manually.
	var ckpt string
	s.db.QueryRow("SELECT checkpoint_commit FROM attempts WHERE id=?", a.ID).Scan(&ckpt)
	if ckpt != "sha-abc123" {
		t.Errorf("checkpoint_commit = %s", ckpt)
	}
}
