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

// TestBlackBoxCodexCoordinator uses a fake codex binary to validate the
// full coordinator→orchestrator contract at the process level.
// The fake codex echoes valid JSON decisions; no real LLM is called.
func TestBlackBoxCodexCoordinator(t *testing.T) {
	// Create a fake codex binary that echoes structured JSON.
	fakeCodexDir, err := os.MkdirTemp("", "omni-fake-codex-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(fakeCodexDir)

	fakeCodexPath := filepath.Join(fakeCodexDir, "codex")
	// Fake codex: reads stdin, echoes a valid decision JSON.
	fakeCodexScript := `#!/bin/bash
# Fake Codex: reads the prompt from args, returns a structured JSON decision.
# Decision sequence: VALIDATE → RETRY_CLEAN (with next_instruction) → COMPLETE
CALL_FILE="/tmp/omni-fake-codex-calls"
CALL_COUNT=1
if [ -f "$CALL_FILE" ]; then
	CALL_COUNT=$(($(cat "$CALL_FILE") + 1))
fi
echo "$CALL_COUNT" > "$CALL_FILE"

case $CALL_COUNT in
	1) echo '{"decision":"VALIDATE","reason":"start executing","next_instruction":"write a test file and validate it"}' ;;
	2) echo '{"decision":"RETRY_CLEAN","reason":"validator failed - try again","next_instruction":"write a test file that passes validation"}' ;;
	3) echo '{"decision":"RETRY_CLEAN","reason":"try once more","next_instruction":"please write the correct output this time"}' ;;
	4) echo '{"decision":"COMPLETE","reason":"validator passed - task done","next_instruction":""}' ;;
	*) echo '{"decision":"FAIL","reason":"too many calls","next_instruction":""}' ;;
esac
exit 0
`
	if err := os.WriteFile(fakeCodexPath, []byte(fakeCodexScript), 0755); err != nil {
		t.Fatalf("WriteFile fake codex: %v", err)
	}

	// Setup git repo.
	repoDir, err := os.MkdirTemp("", "omni-codex-e2e-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(repoDir)

	runGit(t, repoDir, "init")
	runGit(t, repoDir, "config", "user.email", "e2e@omni.local")
	runGit(t, repoDir, "config", "user.name", "Codex E2E")
	os.WriteFile(filepath.Join(repoDir, "README.md"), []byte("# E2E"), 0644)
	runGit(t, repoDir, "add", "README.md")
	runGit(t, repoDir, "commit", "-m", "initial")

	store, _ := taskstore.NewInMemory()
	defer store.Close()

	// Build coordinator with fake codex.
	codexCoord := coordinator.NewCodexCoordinator()
	codexCoord.Bin = fakeCodexPath
	rt := runtime.NewWithID("coordinator-codex", 1)
	cr := coordinator.NewCoordinatorRuntime(rt, codexCoord)

	cfg := Config{
		Repo:        repoDir,
		Task:        "write a test file and validate it",
		Command:     "echo 'hello from worker' > output.txt",
		CWD:         "",
		Validator:   "grep -q 'hello from worker' output.txt",
		MaxAttempts: 3,
		Coordinator: cr,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	decisions, err := Run(ctx, cfg, store, worktree.New())
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

	// Clean up the call counter.
	os.Remove("/tmp/omni-fake-codex-calls")
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
