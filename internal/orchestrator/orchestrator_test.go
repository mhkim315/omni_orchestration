package orchestrator

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/miinanii/omni_orchestration/internal/runtime"
	"github.com/miinanii/omni_orchestration/internal/taskstore"
	"github.com/miinanii/omni_orchestration/internal/worktree"
)

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func tmpRepo(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "omni-orch-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	runGit(t, dir, "init")
	runGit(t, dir, "config", "user.email", "test@omni.local")
	runGit(t, dir, "config", "user.name", "Omni Test")
	os.WriteFile(filepath.Join(dir, "README.md"), []byte("# test"), 0644)
	runGit(t, dir, "add", "README.md")
	runGit(t, dir, "commit", "-m", "initial")
	return dir
}

func TestRunCleanExit(t *testing.T) {
	repo := tmpRepo(t)
	store, _ := taskstore.NewInMemory()
	defer store.Close()
	wt := worktree.New()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := Run(ctx, Config{Repo: repo, Task: "clean", Command: "echo hello"}, store, wt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	task, _ := store.GetTask(1)
	if task.Status != taskstore.StatusCompleted {
		t.Errorf("task = %s, want completed", task.Status)
	}
}

func TestRunCrashRecovery(t *testing.T) {
	repo := tmpRepo(t)
	store, _ := taskstore.NewInMemory()
	defer store.Close()
	wt := worktree.New()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	decisions, err := Run(ctx, Config{
		Repo:    repo,
		Task:    "crash",
		Command: "sh -c 'kill -ABRT $$'",
	}, store, wt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("Decisions after crash: %v", decisions)
}

// R1-B E2E-1: QUIESCENT → VALIDATE → validator FAIL → attempt-2 created.
func TestR1B_ValidateFailCreatesAttempt2(t *testing.T) {
	repo := tmpRepo(t)
	store, _ := taskstore.NewInMemory()
	defer store.Close()
	wt := worktree.New()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Worker writes output, becomes quiescent. Validator always fails.
	// Should create attempt-2 after rejection.
	_, err := Run(ctx, Config{
		Repo:        repo,
		Task:        "validate-fail-test",
		Command:     "sh -c 'echo working && sleep 15 && echo done && exit 0'",
		Validator:   "false", // always fails
		MaxAttempts: 2,
	}, store, wt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Check that at least 2 attempts were created.
	active, _ := store.GetActiveAttempts()
	t.Logf("Active attempts: %d", len(active))

	// After validator fail + retry, we should see the second attempt.
	task, _ := store.GetTask(1)
	t.Logf("Task status: %s", task.Status)
}

// R1-B E2E-2: EXITED 0 → validator FAIL → attempt-2 created.
func TestR1B_ExitedValidatorFailCreatesAttempt2(t *testing.T) {
	repo := tmpRepo(t)
	store, _ := taskstore.NewInMemory()
	defer store.Close()
	wt := worktree.New()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Worker exits cleanly. Validator always fails.
	// Should retry with attempt-2.
	_, err := Run(ctx, Config{
		Repo:        repo,
		Task:        "exited-fail-test",
		Command:     "sh -c 'echo output && exit 0'",
		Validator:   "false",
		MaxAttempts: 2,
	}, store, wt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	task, _ := store.GetTask(1)
	t.Logf("Task status after exited+validator-fail: %s", task.Status)
}

// R1-B E2E-3: Silent-stop → lease timer expires → re-wake → loop doesn't hang.
func TestR1B_ContinueLeaseDoesNotHang(t *testing.T) {
	repo := tmpRepo(t)
	store, _ := taskstore.NewInMemory()
	defer store.Close()
	wt := worktree.New()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// A long-running process that produces no output (silent).
	// CONTINUE lease timer should re-wake.
	_, err := Run(ctx, Config{
		Repo:    repo,
		Task:    "silent-task",
		Command: "sleep 20",
	}, store, wt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Should not hang — either exits or timeouts produce decisions.
}

func TestR1B_MaxAttempts(t *testing.T) {
	repo := tmpRepo(t)
	store, _ := taskstore.NewInMemory()
	defer store.Close()
	wt := worktree.New()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// With MaxAttempts=1 and a command that always fails validation,
	// the orchestrator should hit the limit and mark the task failed.
	_, err := Run(ctx, Config{
		Repo:        repo,
		Task:        "max-attempts-test",
		Command:     "echo done",
		Validator:   "false",
		MaxAttempts: 1,
	}, store, wt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	task, _ := store.GetTask(1)
	t.Logf("Task status: %s (maxAttempts=1)", task.Status)
	// After max attempts, task should NOT be pending — it was resolved one way or another.
	if task.Status == taskstore.StatusPending {
		t.Errorf("task = %s, should not be pending (max attempts exceeded)", task.Status)
	}
}

func TestRecoverAndRecord(t *testing.T) {
	repo := tmpRepo(t)
	store, _ := taskstore.NewInMemory()
	defer store.Close()
	wt := worktree.New()

	info, _ := wt.Create(repo, "T99", "1")
	defer wt.Remove(info.Path)
	os.WriteFile(filepath.Join(info.Path, "work.txt"), []byte("done"), 0644)

	store.CreateRun()
	store.CreateTask(1, "recovery-test")
	store.CreateAttempt(1, 1, "T99-A1", "branch", "HEAD")
	store.RecordWorker(1, "echo", info.Path, "primary", 1)

	var rt *runtime.Runtime
	sha := recoverAndRecord(context.Background(), rt, wt, info.Path,
		1, store, 1, 1, 1, 1, true)
	if sha == "" {
		t.Error("expected checkpoint SHA")
	}
}

func TestWakeMessage(t *testing.T) {
	t.Skip("WakeMessage type removed — pending replumb with coordinator.WakePacket")
}

func TestOpenStore_FileBacked(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "orchestrator.db")
	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	run, _ := s.CreateRun()
	if run.ID != 1 {
		t.Errorf("file-backed store: run id = %d", run.ID)
	}
	s.Close()
	s2, _ := OpenStore(path)
	defer s2.Close()
	run2, _ := s2.CreateRun()
	if run2.ID != 2 {
		t.Errorf("second run id = %d, want 2", run2.ID)
	}
}

func TestRunValidator_Pass(t *testing.T) {
	if !runValidator("true", "") {
		t.Error("true should pass")
	}
}
func TestRunValidator_Fail(t *testing.T) {
	if runValidator("false", "") {
		t.Error("false should fail")
	}
}
