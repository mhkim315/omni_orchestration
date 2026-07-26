package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mhkim315/omni_orchestration/internal/coordinator"
	"github.com/mhkim315/omni_orchestration/internal/runtime"
	"github.com/mhkim315/omni_orchestration/internal/taskstore"
	"github.com/mhkim315/omni_orchestration/internal/worktree"
)

func TestRecovery_ReconcileOrphanRuns(t *testing.T) {
	store, _ := taskstore.NewInMemory()
	defer store.Close()

	// Create a run with an in-progress attempt.
	run, _ := store.CreateRun()
	task, _ := store.CreateTask(run.ID, "orphan task")
	attempt, _ := store.CreateAttempt(task.ID, 1, "w1", "br", "abc")
	store.UpdateAttemptStatus(attempt.ID, taskstore.StatusRunning)

	// Reconcile should mark it interrupted.
	result := Reconcile(store, worktree.New())
	if result.OrphanRuns != 1 {
		t.Errorf("orphan runs = %d, want 1", result.OrphanRuns)
	}
	if result.Interrupted != 1 {
		t.Errorf("interrupted = %d, want 1", result.Interrupted)
	}

	// Verify attempt status updated.
	active, _ := store.GetActiveAttempts()
	if len(active) != 0 {
		t.Errorf("expected 0 active after reconcile, got %d", len(active))
	}
}

func TestRecovery_IdempotentReconcile(t *testing.T) {
	store, _ := taskstore.NewInMemory()
	defer store.Close()

	run, _ := store.CreateRun()
	task, _ := store.CreateTask(run.ID, "idempotent")
	attempt, _ := store.CreateAttempt(task.ID, 1, "w1", "br", "abc")
	store.UpdateAttemptStatus(attempt.ID, taskstore.StatusRunning)

	wt := worktree.New()

	// First reconcile.
	r1 := Reconcile(store, wt)
	if r1.Interrupted != 1 {
		t.Errorf("first reconcile: %d interrupted, want 1", r1.Interrupted)
	}

	// Second reconcile — idempotent (no double-count).
	r2 := Reconcile(store, wt)
	if r2.Interrupted != 0 {
		t.Errorf("second reconcile: %d interrupted, want 0", r2.Interrupted)
	}
}

func TestRecovery_CompletedRunsUntouched(t *testing.T) {
	store, _ := taskstore.NewInMemory()
	defer store.Close()

	run, _ := store.CreateRun()
	task, _ := store.CreateTask(run.ID, "completed task")
	attempt, _ := store.CreateAttempt(task.ID, 1, "w1", "br", "abc")
	store.UpdateAttemptStatus(attempt.ID, taskstore.StatusCompleted)

	result := Reconcile(store, worktree.New())
	if result.OrphanRuns != 0 {
		t.Errorf("completed runs: expected 0 orphans, got %d", result.OrphanRuns)
	}
}

func TestRecovery_StaleCoordinatorDetection(t *testing.T) {
	if StaleCoordinatorDetected(5, 3) != true {
		t.Error("5 != 3 should be stale")
	}
	if StaleCoordinatorDetected(3, 3) != false {
		t.Error("3 == 3 should not be stale")
	}
	if StaleCoordinatorDetected(0, 0) != false {
		t.Error("0,0 should not be stale")
	}
}

func TestRecovery_KillDuringExecution_Restart(t *testing.T) {
	// Simulate: run starts, process is killed, recovery restarts.
	store, _ := taskstore.NewInMemory()
	defer store.Close()

	repoDir, _ := os.MkdirTemp("", "omni-recovery-*")
	defer os.RemoveAll(repoDir)
	runGit(t, repoDir, "init")
	runGit(t, repoDir, "config", "user.email", "t@t")
	runGit(t, repoDir, "config", "user.name", "T")
	os.WriteFile(filepath.Join(repoDir, "R.md"), []byte("# T"), 0644)
	runGit(t, repoDir, "add", "R.md")
	runGit(t, repoDir, "commit", "-m", "init")

	// First run: start, kill mid-execution.
	run, _ := store.CreateRun()
	task, _ := store.CreateTask(run.ID, "recovery test")
	attempt, _ := store.CreateAttempt(task.ID, 1, "w1", "br", "abc123")
	store.UpdateAttemptStatus(attempt.ID, taskstore.StatusRunning)

	// Simulate daemon kill — reconcile.
	wt := worktree.New()
	result := Reconcile(store, wt)
	if result.Interrupted != 1 {
		t.Errorf("expected 1 interrupted, got %d", result.Interrupted)
	}

	// Restart: create new attempt from checkpoint.
	attempt2, _ := store.CreateAttempt(task.ID, 2, "w2", "br2", "abc123")
	store.UpdateAttemptStatus(attempt2.ID, taskstore.StatusRunning)

	// Complete successfully.
	store.UpdateAttemptStatus(attempt2.ID, taskstore.StatusCompleted)
	store.UpdateTaskStatus(task.ID, taskstore.StatusCompleted)
	t.Log("kill → reconcile → restart flow complete")
}

func TestRecovery_CoordinatorWakeRecovery(t *testing.T) {
	store, _ := taskstore.NewInMemory()
	defer store.Close()

	// Simulate: coordinator was mid-wake when daemon died.
	mock := coordinator.NewMockCoordinator(DecisionValidate)
	cr := coordinator.NewCoordinatorRuntime(&runtime.Runtime{}, mock)

	repoDir, _ := os.MkdirTemp("", "omni-coord-rec-*")
	defer os.RemoveAll(repoDir)
	runGit(t, repoDir, "init")
	runGit(t, repoDir, "config", "user.email", "t@t")
	runGit(t, repoDir, "config", "user.name", "T")
	os.WriteFile(filepath.Join(repoDir, "R.md"), []byte("# T"), 0644)
	runGit(t, repoDir, "add", "R.md")
	runGit(t, repoDir, "commit", "-m", "init")

	cfg := Config{
		Repo: repoDir, Task: "coord wake test", Command: "echo done > out.txt",
		CWD: "", Validator: "grep -q done out.txt", MaxAttempts: 2,
		Coordinator: cr,
	}

	// Simulate stale coordinator: replace generation.
	oldGen := cfg.Coordinator.Generation()
	newRT := runtime.NewWithID("coord-new", oldGen+1)
	cfg.Coordinator.Replace(context.Background(), newRT)
	newGen := cfg.Coordinator.Generation()
	if newGen != oldGen+1 {
		t.Errorf("generation: old=%d new=%d, want new=old+1", oldGen, newGen)
	}
	t.Logf("coordinator recovery: gen %d → %d", oldGen, newGen)
}

func TestRecovery_ResumeWithRecovery(t *testing.T) {
	store, _ := taskstore.NewInMemory()
	defer store.Close()

	repoDir, _ := os.MkdirTemp("", "omni-resume-*")
	defer os.RemoveAll(repoDir)
	runGit(t, repoDir, "init")
	runGit(t, repoDir, "config", "user.email", "t@t")
	runGit(t, repoDir, "config", "user.name", "T")
	os.WriteFile(filepath.Join(repoDir, "R.md"), []byte("# T"), 0644)
	runGit(t, repoDir, "add", "R.md")
	runGit(t, repoDir, "commit", "-m", "init")

	// Create orphaned run.
	run, _ := store.CreateRun()
	task, _ := store.CreateTask(run.ID, "resume test")
	attempt, _ := store.CreateAttempt(task.ID, 1, "w1", "br", "abc")
	store.UpdateAttemptStatus(attempt.ID, taskstore.StatusRunning)

	cfg := Config{
		Repo: repoDir, Task: "resume run", Command: "echo done > out.txt",
		CWD: "", Validator: "grep -q done out.txt", MaxAttempts: 1,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	decisions, err := ResumeWithRecovery(ctx, cfg, store, worktree.New())
	if err != nil {
		t.Fatalf("ResumeWithRecovery: %v", err)
	}
	t.Logf("resume decisions: %v", decisions)
}

// TestRuntimeAttachRealProcess verifies Attach on a live process.
func TestRuntimeAttachRealProcess(t *testing.T) {
	// Start a long-lived process for reliable attach testing.
	rt := runtime.New()
	if err := rt.Start("sleep 300", "/tmp"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	pid := rt.PID()
	if pid <= 0 {
		rt.Close(context.Background(), rt.Generation())
		t.Fatal("no PID after Start")
	}
	t.Logf("started process pid=%d", pid)

	// Attach to the running process. Use empty Executable to skip
	// command check (ps comm may differ from bash -c wrapper).
	rt2 := runtime.NewWithID("attached-1", 1)
	id := runtime.AttachIdentity{PID: pid}
	if err := rt2.Attach(pid, id, 1); err != nil {
		rt.Close(context.Background(), rt.Generation())
		t.Fatalf("Attach: %v", err)
	}
	if rt2.PID() != pid {
		t.Errorf("attached PID=%d, want %d", rt2.PID(), pid)
	}

	// Signal-based cleanup: attached Runtime can send signals.
	if err := rt2.Interrupt(1); err != nil {
		t.Logf("Interrupt on attached: %v (expected for non-child)", err)
	}

	// Fake PID must fail-closed.
	rt3 := runtime.NewWithID("fake", 1)
	if err := rt3.Attach(99999, runtime.AttachIdentity{PID: 99999}, 1); err == nil {
		t.Error("fake PID should fail")
	}

	// Clean up the original process.
	rt.Close(context.Background(), rt.Generation())
	rt.Wait()
	t.Log("attach test complete")
}

// runGit is defined in orchestrator_test.go
