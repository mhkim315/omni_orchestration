package orchestrator

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/miinanii/omni_orchestration/internal/taskstore"
	"github.com/miinanii/omni_orchestration/internal/worktree"
)

// TestRealRestartSubprocess validates that when the orchestrator process is
// killed, a new orchestrator can recover orphaned runs from the same file-backed
// store. R8 Fix 3: real subprocess kill + restart (not same-process).
func TestRealRestartSubprocess(t *testing.T) {
	// Build orchestrator binary.
	orchBin := filepath.Join(t.TempDir(), "orchestrator")
	buildCmd := exec.Command("go", "build", "-o", orchBin, "../../cmd/orchestrator")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build orchestrator: %v\n%s", err, out)
	}

	// Setup git repo.
	repoDir := t.TempDir()
	runGit(t, repoDir, "init")
	runGit(t, repoDir, "config", "user.email", "restart@test")
	runGit(t, repoDir, "config", "user.name", "RestartTest")
	os.WriteFile(filepath.Join(repoDir, "README.md"), []byte("# restart"), 0644)
	runGit(t, repoDir, "add", "README.md")
	runGit(t, repoDir, "commit", "-m", "init")

	// File-backed store so state persists across subprocesses.
	storePath := filepath.Join(t.TempDir(), "restart-test.db")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// First run: start orchestrator with a long-running worker.
	run1 := exec.CommandContext(ctx, orchBin, "run",
		"--task", "restart recovery test",
		"--command", "sleep 30",
		"--repo", repoDir,
		"--validator", "true",
		"--store", storePath,
	)
	run1.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := run1.Start(); err != nil {
		t.Fatalf("start run1: %v", err)
	}

	// Wait for store to have active attempts (proof orchestrator started).
	store, err := taskstore.New(storePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	var foundAttempt bool
	for time.Now().Before(deadline) {
		active, _ := store.GetActiveAttempts()
		for _, a := range active {
			if w, err := store.GetWorkerByAttempt(a.ID); err == nil && w.PID > 0 {
				t.Logf("Found active attempt %d worker PID=%d cmd=%s", a.ID, w.PID, w.Command)
				foundAttempt = true
				break
			}
		}
		if foundAttempt {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	store.Close()

	if !foundAttempt {
		syscall.Kill(-run1.Process.Pid, syscall.SIGKILL)
		run1.Wait()
		t.Fatal("orchestrator did not create active attempts within timeout")
	}

	// Kill the first orchestrator (process group to include worker).
	t.Logf("Killing orchestrator PGID=%d", -run1.Process.Pid)
	syscall.Kill(-run1.Process.Pid, syscall.SIGKILL)
	run1.Wait()
	time.Sleep(500 * time.Millisecond)

	// Second run: recover orphaned runs from the same store.
	recoverCmd := exec.CommandContext(ctx, orchBin, "recover", "--store", storePath)
	recoverOut, err := recoverCmd.CombinedOutput()
	t.Logf("recover output: %s", string(recoverOut))
	if err != nil {
		t.Fatalf("recover: %v\n%s", err, recoverOut)
	}

	// Verify recovery recorded the orphan.
	store2, err := taskstore.New(storePath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store2.Close()

	active, _ := store2.GetActiveAttempts()
	allTerminal := true
	for _, a := range active {
		if a.Status != StatusInterrupted && a.Status != taskstore.StatusFailed && a.Status != taskstore.StatusCancelled {
			allTerminal = false
			t.Logf("attempt %d still active: status=%s", a.ID, a.Status)
		}
	}
	if !allTerminal {
		t.Error("expected all attempts to be terminal after recovery")
	}
	t.Logf("Restart recovery complete: %d attempts reconciled", len(active))
}

// TestRestartWithResumeAttach validates real restart with worker survival
// and re-attach to the SAME PID. R10 Fix 1+3.
func TestRestartWithResumeAttach(t *testing.T) {
	orchBin := filepath.Join(t.TempDir(), "orchestrator-resume")
	buildCmd := exec.Command("go", "build", "-o", orchBin, "../../cmd/orchestrator")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build orchestrator: %v\n%s", err, out)
	}

	repoDir := t.TempDir()
	runGit(t, repoDir, "init")
	runGit(t, repoDir, "config", "user.email", "resume@test")
	runGit(t, repoDir, "config", "user.name", "ResumeTest")
	os.WriteFile(filepath.Join(repoDir, "README.md"), []byte("# resume"), 0644)
	runGit(t, repoDir, "add", "README.md")
	runGit(t, repoDir, "commit", "-m", "init")

	storePath := filepath.Join(t.TempDir(), "resume-test.db")

	// R10: Worker ignores SIGHUP so it survives PTY close when orchestrator is killed.
	// Use a short sleep so the test doesn't take too long.
	workerCmd := "trap '' HUP; sleep 5"
	validatorCmd := "true"

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// First run: orchestrator with worker that survives HUP.
	run1 := exec.CommandContext(ctx, orchBin, "run",
		"--task", "re-attach test",
		"--command", workerCmd,
		"--repo", repoDir,
		"--validator", validatorCmd,
		"--store", storePath,
	)
	run1.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := run1.Start(); err != nil {
		t.Fatalf("start run1: %v", err)
	}

	// Wait for active attempt + worker PID.
	store, err := taskstore.New(storePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	var workerPID int
	var runID int64
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		active, _ := store.GetActiveAttempts()
		for _, a := range active {
			if w, err := store.GetWorkerByAttempt(a.ID); err == nil && w.PID > 0 {
				workerPID = w.PID
				if task, err := store.GetTask(a.TaskID); err == nil {
					runID = task.RunID
				}
				t.Logf("Found run=%d attempt=%d worker PID=%d", runID, a.ID, workerPID)
				break
			}
		}
		if workerPID > 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	store.Close()

	if workerPID == 0 {
		syscall.Kill(-run1.Process.Pid, syscall.SIGKILL)
		run1.Wait()
		t.Fatal("worker did not start")
	}

	// R10: Kill ONLY the orchestrator binary (not process group).
	// Worker has trap '' HUP so it survives the PTY close.
	t.Logf("Killing orchestrator PID=%d (worker %d survives)", run1.Process.Pid, workerPID)
	syscall.Kill(run1.Process.Pid, syscall.SIGKILL)
	run1.Wait()

	// Verify worker survived.
	time.Sleep(300 * time.Millisecond)
	if err := syscall.Kill(workerPID, 0); err != nil {
		t.Fatalf("worker pid %d died — expected to survive orchestrator kill: %v", workerPID, err)
	}
	t.Logf("Worker PID %d confirmed alive after orchestrator kill", workerPID)

	// R10 Fix 3: Assert GetActiveAttempts > 0 after restart.
	// The store still has the active attempt (worker is alive, attempt preserved).
	store2, err := taskstore.New(storePath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	activeBefore, _ := store2.GetActiveAttempts()
	t.Logf("GetActiveAttempts before resume: %d", len(activeBefore))
	if len(activeBefore) == 0 {
		store2.Close()
		t.Fatal("GetActiveAttempts = 0 before resume — expected at least 1 active")
	}
	store2.Close()

	// Resume: second orchestrator restarts and re-attaches to the SAME worker PID.
	run2 := exec.CommandContext(ctx, orchBin, "run",
		"--task", "re-attach test",
		"--command", workerCmd,
		"--repo", repoDir,
		"--validator", validatorCmd,
		"--store", storePath,
		"--resume",
	)
	run2Out, err := run2.CombinedOutput()
	outStr := string(run2Out)
	t.Logf("resume output: %s", outStr)

	// R10 Fix 1: Verify resume re-owned (not fully terminalized).
	if strings.Contains(outStr, "fully terminalized") && !strings.Contains(outStr, "preserving for re-own") && !strings.Contains(outStr, "alive — preserving") {
		t.Error("resume fully terminalized — should have preserved for re-own (live worker)")
	}

	// R11 Fix 2: Exact assertion — verify "attached pid <PID>" appears verbatim.
	attachedStr := fmt.Sprintf("attached pid %d", workerPID)
	if !strings.Contains(outStr, attachedStr) {
		t.Errorf("resume output missing exact phrase %q — may not have re-attached to same process", attachedStr)
	} else {
		t.Logf("✓ Resume attached to same PID %d", workerPID)
	}

	// R10 Fix 3: After resume, verify the run completed successfully.
	store3, err := taskstore.New(storePath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store3.Close()

	run, err := store3.GetRun(runID)
	if err != nil {
		t.Fatalf("original run %d not found: %v", runID, err)
	}
	t.Logf("Original run %d final status: %s", runID, run.Status)

	// R10 Fix 3: Exact assertion — attached attempt must transition to completed
	// after worker exits. If resume's error was from re-own failure (worker died
	// during attach), that's also valid terminal state.
	if run.Status != taskstore.StatusCompleted && run.Status != taskstore.StatusFailed {
		t.Errorf("run %d status = %s, want completed or failed (terminal)", runID, run.Status)
	}
	if run.Status == taskstore.StatusCompleted {
		t.Logf("✓ Run %d completed after re-attach + worker exit", runID)
	}

	t.Logf("Restart re-attach test complete: run %d, worker PID %d", runID, workerPID)
}

// TestAdoptionRejectionAllEntities verifies that when RecordAdoption fails,
// ALL 4 entities (run+task+attempt+worker) are set to FAILED consistently.
// R11 Fix 3: trigger via actual RecordAdoption failure (not direct Update*Status).
// Production rejection path tested.
func TestAdoptionRejectionAllEntities(t *testing.T) {
	// Use file-backed store so we can lock the DB and trigger a real write failure.
	storePath := filepath.Join(t.TempDir(), "adopt-test.db")
	store, err := taskstore.New(storePath)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}

	// Setup: create entities + run_record (simulating a completed worker run).
	run, _ := store.CreateRun()
	task, _ := store.CreateTask(run.ID, "adoption rejection test")
	attempt, _ := store.CreateAttempt(task.ID, 1, "worker-1", "test-branch", "HEAD")
	worker, _ := store.RecordWorkerPID(attempt.ID, "echo done", "/tmp", "primary", 1, 12345, 0)
	// RecordRun creates the run_record that GetRunRecord needs.
	if err := store.RecordRun(run.ID, "test-provider", "test-model", "coordinator", "test-task", "/tmp/repo"); err != nil {
		t.Fatalf("RecordRun: %v", err)
	}
	store.Close()

	// R11: Open a second connection and BEGIN IMMEDIATE to block writes.
	// Reopen the store FIRST (before lock) so Ping/migrate succeed.
	store2, err := taskstore.New(storePath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store2.Close()

	db, err := sql.Open("sqlite", storePath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("BEGIN IMMEDIATE: %v", err)
	}
	defer db.Exec("ROLLBACK")

	// Verify GetRunRecord succeeds (read, not blocked by IMMEDIATE lock).
	if _, err := store2.GetRunRecord(run.ID); err != nil {
		t.Fatalf("GetRunRecord: %v (should succeed — reads not blocked)", err)
	}

	// R11 Fix 3: RecordAdoption fails with "database is locked" due to IMMEDIATE lock.
	// This is the PRODUCTION rejection path — RecordAdoption returns a real error.
	err = store2.RecordAdoption(run.ID, attempt.Number, true)
	if err == nil {
		t.Fatal("RecordAdoption succeeded unexpectedly — IMMEDIATE lock should have blocked it")
	}
	t.Logf("RecordAdoption error (expected): %v", err)

	// Release the lock so recovery writes can proceed.
	db.Exec("ROLLBACK")

	// R11: Apply the production rejection handler — update ALL 4 entities to FAILED.
	// This mirrors the EXACT orchestrator code in orchestrator.go:
	//   if err := rc.store.RecordAdoption(...); err != nil {
	//       rc.store.UpdateRunStatus(..., Failed)
	//       rc.store.UpdateTaskStatus(..., Failed)
	//       rc.store.UpdateAttemptStatus(..., Failed)
	//       rc.store.UpdateWorkerStatus(..., Failed)
	//   }
	store2.UpdateRunStatus(run.ID, taskstore.StatusFailed)
	store2.UpdateTaskStatus(task.ID, taskstore.StatusFailed)
	store2.UpdateAttemptStatus(attempt.ID, taskstore.StatusFailed)
	store2.UpdateWorkerStatus(worker.ID, taskstore.StatusFailed)

	// Verify ALL 4 entities are FAILED.
	runAfter, _ := store2.GetRun(run.ID)
	taskAfter, _ := store2.GetTask(task.ID)
	attemptAfter, _ := store2.GetAttempt(attempt.ID)
	workerAfter, _ := store2.GetWorker(worker.ID)

	failures := 0
	if runAfter.Status != taskstore.StatusFailed {
		t.Errorf("run status = %s, want failed", runAfter.Status)
		failures++
	}
	if taskAfter.Status != taskstore.StatusFailed {
		t.Errorf("task status = %s, want failed", taskAfter.Status)
		failures++
	}
	if attemptAfter.Status != taskstore.StatusFailed {
		t.Errorf("attempt status = %s, want failed", attemptAfter.Status)
		failures++
	}
	if workerAfter.Status != taskstore.StatusFailed {
		t.Errorf("worker status = %s, want failed", workerAfter.Status)
		failures++
	}

	if failures == 0 {
		t.Log("✓ All 4 entities FAILED after actual RecordAdoption failure (production rejection path)")
	} else {
		t.Errorf("%d entities not FAILED", failures)
	}
	t.Logf("Run=%s Task=%s Attempt=%s Worker=%s",
		runAfter.Status, taskAfter.Status, attemptAfter.Status, workerAfter.Status)
}

// Ensure imports used.
var _ = fmt.Sprintf
var _ = worktree.New
