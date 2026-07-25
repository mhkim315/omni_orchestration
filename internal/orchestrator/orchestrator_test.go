package orchestrator

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/miinanii/omni_orchestration/internal/coordinator"
	"github.com/miinanii/omni_orchestration/internal/runtime"
	"github.com/miinanii/omni_orchestration/internal/taskstore"
	"github.com/miinanii/omni_orchestration/internal/worktree"
)

// TestOrchestratorWithMockCoordinator validates the full flow with a mock.
func TestOrchestratorWithMockCoordinator(t *testing.T) {
	store, _ := taskstore.NewInMemory()
	defer store.Close()

	repoDir, err := os.MkdirTemp("", "omni-orch-e2e-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(repoDir)

	runGit(t, repoDir, "init")
	runGit(t, repoDir, "config", "user.email", "test@omni.local")
	runGit(t, repoDir, "config", "user.name", "Test")
	os.WriteFile(filepath.Join(repoDir, "README.md"), []byte("# test"), 0644)
	runGit(t, repoDir, "add", "README.md")
	runGit(t, repoDir, "commit", "-m", "initial")

	wm := worktree.New()

	// Mock coordinator: VALIDATE → RETRY_CLEAN → RETRY_CLEAN → COMPLETE
	mock := coordinator.NewMockCoordinator(
		DecisionValidate,
		DecisionRetryClean,
		DecisionRetryClean,
		DecisionComplete,
	)
	cr := coordinator.NewCoordinatorRuntime(&runtime.Runtime{}, mock)

	cfg := Config{
		Repo:        repoDir,
		Task:        "write a test file and validate it",
		Command:     "echo 'hello from worker' > output.txt",
		CWD:         "", // empty = worktree path
		Validator:   "grep -q 'hello from worker' output.txt",
		MaxAttempts: 3,
		Coordinator: cr,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	decisions, err := Run(ctx, cfg, store, wm)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	t.Logf("decisions: %v", decisions)
	if len(decisions) < 2 {
		t.Fatalf("expected at least 2 decisions, got %d", len(decisions))
	}
	if decisions[len(decisions)-1] != DecisionComplete {
		t.Errorf("final decision = %s, want COMPLETE", decisions[len(decisions)-1])
	}
}

// TestOrchestrator_NoCoordinatorDefaultsToValidate verifies that when no
// coordinator is configured, the orchestrator defaults to VALIDATE.
func TestOrchestrator_NoCoordinatorDefaultsToValidate(t *testing.T) {
	store, _ := taskstore.NewInMemory()
	defer store.Close()

	repoDir, _ := os.MkdirTemp("", "omni-no-coord-*")
	defer os.RemoveAll(repoDir)

	runGit(t, repoDir, "init")
	runGit(t, repoDir, "config", "user.email", "t@t")
	runGit(t, repoDir, "config", "user.name", "T")
	os.WriteFile(filepath.Join(repoDir, "README.md"), []byte("# t"), 0644)
	runGit(t, repoDir, "add", "README.md")
	runGit(t, repoDir, "commit", "-m", "init")

	cfg := Config{
		Repo: repoDir, Task: "simple", Command: "echo done > out.txt",
		CWD: "/tmp", Validator: "grep -q done out.txt", MaxAttempts: 1,
		// No Coordinator — defaults to VALIDATE.
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	decisions, err := Run(ctx, cfg, store, worktree.New())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("no-coordinator decisions: %v", decisions)
}

// ── Helpers ──

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}
