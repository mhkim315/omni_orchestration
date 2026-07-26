package daemon

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/mhkim315/omni_orchestration/internal/dag"
	"github.com/mhkim315/omni_orchestration/internal/orchestrator"
	"github.com/mhkim315/omni_orchestration/internal/taskstore"
	"github.com/mhkim315/omni_orchestration/internal/worktree"
)

// setupTracker creates a tracker with in-memory stores and a temp repo.
func setupTracker(t *testing.T) (*Tracker, *dag.Store, func()) {
	t.Helper()
	store, _ := taskstore.NewInMemory()
	dagStore, _ := dag.NewInMemory()

	repoDir := t.TempDir()
	runGit(t, repoDir, "init", ".")
	runGit(t, repoDir, "config", "user.email", "tracker@test")
	runGit(t, repoDir, "config", "user.name", "Tracker")
	os.WriteFile(filepath.Join(repoDir, "README.md"), []byte("# t"), 0644)
	runGit(t, repoDir, "add", "README.md")
	runGit(t, repoDir, "commit", "-m", "init")

	cfg := orchestrator.Config{
		Repo:      repoDir,
		Command:   "echo 'tracker task'",
		Validator: "true",
	}
	tr := NewTracker(store, dagStore, worktree.New(), cfg)

	cleanup := func() {
		tr.Close()
		store.Close()
		dagStore.Close()
	}
	return tr, dagStore, cleanup
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

// 1. Tracker polls and creates attempt when unblocked
func TestTrackerPollsAndCreatesAttempt(t *testing.T) {
	tr, dagStore, cleanup := setupTracker(t)
	defer cleanup()

	// Create a pending DAG task.
	task, err := dagStore.CreateTask(1, "test-task", 0)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if task.Status != dag.StatusPending {
		t.Fatalf("task status = %s, want pending", task.Status)
	}

	// Start tracker.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.Start(ctx)

	// Wait for tracker to pick up task.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if tr.ActiveCount() > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if tr.ActiveCount() == 0 {
		t.Fatal("tracker did not pick up task within timeout")
	}
	t.Logf("tracker picked up task — active count: %d", tr.ActiveCount())

	// Wait for task to complete.
	time.Sleep(2 * time.Second)
	taskAfter, _ := dagStore.GetTask(task.ID)
	t.Logf("task %d final status: %s", task.ID, taskAfter.Status)
	if taskAfter.Status == dag.StatusCompleted {
		t.Log("✓ Tracker completed task successfully")
	}
}

// 2. Tracker resumes active tasks after restart
func TestTrackerRespawnAfterKill(t *testing.T) {
	// Use file-backed stores.
	storePath := filepath.Join(t.TempDir(), "tracker-store.db")
	dagPath := filepath.Join(t.TempDir(), "tracker-dag.db")

	store, _ := taskstore.New(storePath)
	dagStore, _ := dag.New(dagPath)

	repoDir := t.TempDir()
	os.WriteFile(filepath.Join(repoDir, "README.md"), []byte("# t"), 0644)

	cfg := orchestrator.Config{Repo: repoDir, Command: "echo done", Validator: "true"}
	tr := NewTracker(store, dagStore, worktree.New(), cfg)

	// Create pending task.
	task, _ := dagStore.CreateTask(1, "resume-task", 0)

	ctx, cancel := context.WithCancel(context.Background())
	tr.Start(ctx)

	// Let it start executing.
	time.Sleep(500 * time.Millisecond)

	// Kill tracker (simulate crash).
	cancel()
	tr.Close()
	store.Close()
	dagStore.Close()

	// Reopen stores.
	store2, _ := taskstore.New(storePath)
	dagStore2, _ := dag.New(dagPath)
	defer store2.Close()
	defer dagStore2.Close()

	// New tracker resumes.
	tr2 := NewTracker(store2, dagStore2, worktree.New(), cfg)
	defer tr2.Close()
	tr2.ResumeActiveTasks()

	// Task should still exist.
	taskAfter, _ := dagStore2.GetTask(task.ID)
	t.Logf("task %d after restart: %s", task.ID, taskAfter.Status)
	if taskAfter.Status == "" {
		t.Error("task not found after restart")
	}
}

// 3. Graceful shutdown mid-run
func TestGracefulShutdownMidRun(t *testing.T) {
	tr, dagStore, cleanup := setupTracker(t)
	defer cleanup()

	// Create a task.
	task, _ := dagStore.CreateTask(1, "graceful-task", 0)

	ctx, cancel := context.WithCancel(context.Background())
	tr.Start(ctx)

	// Let tracker pick it up.
	time.Sleep(500 * time.Millisecond)

	// Graceful shutdown.
	cancel()
	tr.Close()

	// Task should be terminal (completed or failed).
	taskAfter, _ := dagStore.GetTask(task.ID)
	t.Logf("task %d after graceful shutdown: %s", task.ID, taskAfter.Status)
	if taskAfter.Status == dag.StatusActive {
		t.Error("task still active after graceful shutdown")
	}
}

var _ = os.TempDir
