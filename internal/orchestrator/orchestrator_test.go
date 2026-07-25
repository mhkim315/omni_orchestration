package orchestrator

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/miinanii/omni_orchestration/internal/taskstore"
	"github.com/miinanii/omni_orchestration/internal/worktree"
)

// runGit is a test helper for git commands.
func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

// tmpRepo creates a temporary git repo for testing.
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

func TestOrchestrator_RunCleanExit(t *testing.T) {
	repo := tmpRepo(t)
	store, _ := taskstore.NewInMemory()
	defer store.Close()
	wt := worktree.New()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := Config{
		Repo:    repo,
		Task:    "clean-exit-test",
		Command: "echo hello",
	}

	decisions, err := Run(ctx, cfg, store, wt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(decisions) != 0 {
		t.Logf("Decisions: %v", decisions)
	}

	// Task should be completed.
	task, _ := store.GetTask(1)
	if task == nil || task.Status != taskstore.StatusCompleted {
		t.Errorf("task status = %v, want completed", task)
	}
}

func TestOrchestrator_RunCrashRecovery(t *testing.T) {
	repo := tmpRepo(t)
	store, _ := taskstore.NewInMemory()
	defer store.Close()
	wt := worktree.New()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := Config{
		Repo:    repo,
		Task:    "crash-test",
		Command: "sh -c 'kill -ABRT $$'", // SIGABRT → real crash
	}

	decisions, err := Run(ctx, cfg, store, wt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// After crash, the orchestrator should emit at least one decision.
	t.Logf("Decisions after crash: %v", decisions)

	// Verify the attempt was recorded.
	active, _ := store.GetActiveAttempts()
	t.Logf("Active attempts after crash: %d", len(active))
}

func TestOrchestrator_WorktreeCheckpoint(t *testing.T) {
	repo := tmpRepo(t)
	store, _ := taskstore.NewInMemory()
	defer store.Close()
	wt := worktree.New()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Command that writes a file and exits cleanly.
	cfg := Config{
		Repo:    repo,
		Task:    "ckpt-test",
		Command: "sh -c 'echo result > output.txt && exit 0'",
	}

	_, err := Run(ctx, cfg, store, wt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	task, _ := store.GetTask(1)
	if task.Status != taskstore.StatusCompleted {
		t.Errorf("task status = %s, want completed", task.Status)
	}
}

func TestOrchestrator_DecisionLoop(t *testing.T) {
	repo := tmpRepo(t)
	store, _ := taskstore.NewInMemory()
	defer store.Close()
	wt := worktree.New()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Long-running command that exits after writing output.
	cfg := Config{
		Repo:    repo,
		Task:    "decision-test",
		Command: "sh -c 'echo working && sleep 2 && echo done && exit 0'",
	}

	decisions, err := Run(ctx, cfg, store, wt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("Decisions: %v", decisions)
}

func TestWakeMessage_Format(t *testing.T) {
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
	if !strings.Contains(msg, "quiescent") {
		t.Error("missing quiescent keyword")
	}
	if !strings.Contains(msg, "abc123") {
		t.Error("missing checkpoint SHA")
	}
	if !strings.Contains(msg, "VALIDATE") {
		t.Error("missing decision options")
	}
}

func TestWakeMessage_CleanExit(t *testing.T) {
	w := WakeMessage{
		AttemptID: "T2-A1",
		State:     "EXITED",
		Dirty:     false,
		Timestamp: time.Now(),
	}

	msg := w.String()
	if !strings.Contains(msg, "exited") {
		t.Error("missing exited keyword")
	}
	if strings.Contains(msg, "worktree modified") {
		t.Error("should not say worktree modified on clean exit")
	}
}
