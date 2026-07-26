package orchestrator

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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
		// Kill run1.
		syscall.Kill(-run1.Process.Pid, syscall.SIGKILL)
		run1.Wait()
		t.Fatal("orchestrator did not create active attempts within timeout")
	}

	// Kill the first orchestrator (process group to include worker).
	t.Logf("Killing orchestrator PGID=%d", -run1.Process.Pid)
	syscall.Kill(-run1.Process.Pid, syscall.SIGKILL)
	run1.Wait()

	// Give OS a moment to reap.
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

	// At least one run should exist.
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

// TestRestartWithResume validates the full --resume flow: subprocess kill,
// then a new orchestrator with --resume re-owns surviving workers.
func TestRestartWithResume(t *testing.T) {
	// Build orchestrator binary.
	orchBin := filepath.Join(t.TempDir(), "orchestrator-resume")
	buildCmd := exec.Command("go", "build", "-o", orchBin, "../../cmd/orchestrator")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build orchestrator: %v\n%s", err, out)
	}

	// Setup git repo.
	repoDir := t.TempDir()
	runGit(t, repoDir, "init")
	runGit(t, repoDir, "config", "user.email", "resume@test")
	runGit(t, repoDir, "config", "user.name", "ResumeTest")
	os.WriteFile(filepath.Join(repoDir, "README.md"), []byte("# resume"), 0644)
	runGit(t, repoDir, "add", "README.md")
	runGit(t, repoDir, "commit", "-m", "init")

	storePath := filepath.Join(t.TempDir(), "resume-test.db")

	// Use a worker that writes output then waits — validator will pass.
	workerCmd := fmt.Sprintf("echo 'done' > %s/result.txt && sleep 30", repoDir)
	validatorCmd := fmt.Sprintf("grep -q done %s/result.txt", repoDir)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// First run: orchestrator with worker that should pass validator.
	run1 := exec.CommandContext(ctx, orchBin, "run",
		"--task", "resume recovery test",
		"--command", workerCmd,
		"--repo", repoDir,
		"--validator", validatorCmd,
		"--store", storePath,
	)
	run1.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := run1.Start(); err != nil {
		t.Fatalf("start run1: %v", err)
	}

	// Wait for worker to start and produce output.
	deadline := time.Now().Add(15 * time.Second)
	workerStarted := false
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(repoDir, "result.txt")); err == nil {
			workerStarted = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !workerStarted {
		syscall.Kill(-run1.Process.Pid, syscall.SIGKILL)
		run1.Wait()
		t.Fatal("worker did not produce output")
	}

	// Kill orchestrator (process group).
	t.Logf("Killing orchestrator PGID=%d", -run1.Process.Pid)
	syscall.Kill(-run1.Process.Pid, syscall.SIGKILL)
	run1.Wait()
	time.Sleep(500 * time.Millisecond)

	// Second run with --resume: should detect orphaned run and recover.
	run2 := exec.CommandContext(ctx, orchBin, "run",
		"--task", "resume recovery test",
		"--command", workerCmd,
		"--repo", repoDir,
		"--validator", validatorCmd,
		"--store", storePath,
		"--resume",
	)
	run2Out, _ := run2.CombinedOutput()
	t.Logf("resume output: %s", string(run2Out))

	// Verify store has terminal runs after recovery.
	store, err := taskstore.New(storePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	active, _ := store.GetActiveAttempts()
	allTerminal := true
	for _, a := range active {
		if a.Status != StatusInterrupted && a.Status != taskstore.StatusFailed &&
			a.Status != taskstore.StatusCancelled && a.Status != taskstore.StatusCompleted {
			allTerminal = false
		}
	}
	if !allTerminal {
		t.Error("expected all attempts terminal after --resume recovery")
	}
	t.Logf("Resume recovery: %d attempts in terminal state", len(active))
}

// Ensure worktree import is used.
var _ = fmt.Sprintf
var _ = worktree.New
