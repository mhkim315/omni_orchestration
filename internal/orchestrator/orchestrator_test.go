package orchestrator

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

	cfg := Config{
		Repo:    repo,
		Task:    "clean exit",
		Command: "echo hello",
	}

	decisions, err := Run(ctx, cfg, store, wt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("Decisions: %v", decisions)

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

	cfg := Config{
		Repo:    repo,
		Task:    "crash",
		Command: "sh -c 'kill -ABRT $$'",
	}

	decisions, err := Run(ctx, cfg, store, wt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("Decisions after crash: %v", decisions)
}

func TestRunWithValidator(t *testing.T) {
	repo := tmpRepo(t)
	store, _ := taskstore.NewInMemory()
	defer store.Close()
	wt := worktree.New()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Use validator that always passes (true). The validator runs
	// in the worktree directory. This proves the validator flow works.
	cfg := Config{
		Repo:      repo,
		Task:      "validated task",
		Command:   "sh -c 'echo result > output.txt && exit 0'",
		Validator: "true",
	}

	_, err := Run(ctx, cfg, store, wt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	task, _ := store.GetTask(1)
	if task.Status != taskstore.StatusCompleted {
		t.Errorf("task = %s, want completed (validator should pass)", task.Status)
	}
}

func TestRunWithValidatorReject(t *testing.T) {
	repo := tmpRepo(t)
	store, _ := taskstore.NewInMemory()
	defer store.Close()
	wt := worktree.New()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := Config{
		Repo:      repo,
		Task:      "failing task",
		Command:   "sh -c 'echo nothing && exit 0'",
		Validator: "false", // always fails
	}

	_, err := Run(ctx, cfg, store, wt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	task, _ := store.GetTask(1)
	// After validator rejection, a new attempt happens with cleaned state.
	// The task may be completed or cancelled depending on loop.
	t.Logf("Task status after reject: %s", task.Status)
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

	var rt *runtime.Runtime // no live runtime
	sha := recoverAndRecord(context.Background(), rt, wt, info.Path, "T99-A1",
		store, 1, 1, 1, 1, true)

	if sha == "" {
		t.Error("expected checkpoint SHA")
	}
}

func TestWakeMessage(t *testing.T) {
	w := WakeMessage{
		AttemptID:  "T1-A1",
		State:      "QUIESCENT_CANDIDATE",
		Checkpoint: "abc123",
		Dirty:      true,
		Timestamp:  time.Now(),
	}
	msg := w.String()
	if !strings.Contains(msg, "T1-A1") {
		t.Error("missing attempt ID")
	}
	if !strings.Contains(msg, "VALIDATE") {
		t.Error("missing options")
	}
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

	// Re-open should persist data.
	s.Close()
	s2, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	run2, err := s2.CreateRun()
	if err != nil {
		t.Fatal(err)
	}
	if run2.ID != 2 {
		t.Errorf("file-backed store: second run id = %d, want 2", run2.ID)
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
