package worktree

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestManager_Create(t *testing.T) {
	dir := t.TempDir()
	repo, err := gitInit(dir)
	if err != nil {
		t.Fatal(err)
	}

	m := New()
	info, err := m.Create(repo, "task-42", "1")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer m.Remove(info.Path)

	if info.Branch != "task/task-42/attempt-1" {
		t.Errorf("branch = %s, want task/task-42/attempt-1", info.Branch)
	}
	if info.TaskID != "task-42" {
		t.Errorf("taskID = %s", info.TaskID)
	}
	if info.AttemptID != "1" {
		t.Errorf("attemptID = %s", info.AttemptID)
	}

	// Verify worktree exists and is a git repo.
	if _, err := os.Stat(info.Path); err != nil {
		t.Errorf("worktree path missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(info.Path, ".git")); err != nil {
		t.Errorf("worktree .git missing: %v", err)
	}
}

func TestManager_Checkpoint(t *testing.T) {
	dir := t.TempDir()
	repo, err := gitInit(dir)
	if err != nil {
		t.Fatal(err)
	}

	m := New()
	info, err := m.Create(repo, "task-1", "1")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Remove(info.Path)

	// Write a file.
	if err := writeFile(info.Path, "output.txt", "hello world"); err != nil {
		t.Fatal(err)
	}

	// Checkpoint.
	sha, err := m.Checkpoint(info.Path, "checkpoint: task-1 output")
	if err != nil {
		t.Fatalf("Checkpoint: %v\n%s", err, debugLog(info.Path))
	}
	if sha == "" {
		t.Error("expected commit SHA, got empty")
	}
	t.Logf("Checkpoint SHA: %s", sha)

	// Verify the file was committed (status should be clean after checkpoint).
	report := m.Status(info.Path)
	if !report.Clean {
		t.Errorf("worktree not clean after checkpoint: %v", report.ChangedFiles)
	}
}

func TestManager_StatusDirty(t *testing.T) {
	dir := t.TempDir()
	repo, err := gitInit(dir)
	if err != nil {
		t.Fatal(err)
	}

	m := New()
	info, err := m.Create(repo, "task-2", "1")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Remove(info.Path)

	// Write a file without committing.
	if err := writeFile(info.Path, "dirty.txt", "uncommitted"); err != nil {
		t.Fatal(err)
	}

	report := m.Status(info.Path)
	if report.Clean {
		t.Error("expected dirty status after uncommitted write")
	}
	if len(report.ChangedFiles) == 0 {
		t.Error("expected changed files in report")
	}
	t.Logf("Dirty: %v", report.ChangedFiles)
}

func TestManager_StatusClean(t *testing.T) {
	dir := t.TempDir()
	repo, err := gitInit(dir)
	if err != nil {
		t.Fatal(err)
	}

	m := New()
	info, err := m.Create(repo, "task-3", "1")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Remove(info.Path)

	report := m.Status(info.Path)
	if !report.Clean {
		t.Errorf("expected clean worktree, got: %v", report.ChangedFiles)
	}
}

func TestManager_Remove(t *testing.T) {
	dir := t.TempDir()
	repo, err := gitInit(dir)
	if err != nil {
		t.Fatal(err)
	}

	m := New()
	info, err := m.Create(repo, "task-4", "1")
	if err != nil {
		t.Fatal(err)
	}

	// Verify it exists.
	if _, err := os.Stat(info.Path); err != nil {
		t.Fatal("worktree should exist after create")
	}

	// Remove.
	if err := m.Remove(info.Path); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	// Verify it's gone.
	if _, err := os.Stat(info.Path); !os.IsNotExist(err) {
		t.Error("worktree should not exist after remove")
	}
}

func TestManager_ConcurrentCheckpoints(t *testing.T) {
	dir := t.TempDir()
	repo, err := gitInit(dir)
	if err != nil {
		t.Fatal(err)
	}

	m := New()

	// Create multiple worktrees.
	var infos []*WorktreeInfo
	for i := 0; i < 4; i++ {
		info, err := m.Create(repo, "task-concurrent", string(rune('0'+i)))
		if err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
		defer m.Remove(info.Path)
		infos = append(infos, info)
	}

	// Write and checkpoint concurrently.
	var wg sync.WaitGroup
	errs := make(chan error, len(infos))
	for i, info := range infos {
		wg.Add(1)
		go func(idx int, wt *WorktreeInfo) {
			defer wg.Done()
			if err := writeFile(wt.Path, "result.txt", "from goroutine"); err != nil {
				errs <- err
				return
			}
			if _, err := m.Checkpoint(wt.Path, "concurrent checkpoint"); err != nil {
				errs <- err
			}
		}(i, info)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent error: %v", err)
	}

	// All worktrees should be clean after checkpoints.
	for _, info := range infos {
		report := m.Status(info.Path)
		if !report.Clean {
			t.Errorf("worktree %s not clean after concurrent checkpoint: %v",
				info.Branch, report.ChangedFiles)
		}
	}
}

func TestManager_CreateDuplicateBranch(t *testing.T) {
	dir := t.TempDir()
	repo, err := gitInit(dir)
	if err != nil {
		t.Fatal(err)
	}

	m := New()

	// First creation should succeed.
	info1, err := m.Create(repo, "task-dup", "1")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Remove(info1.Path)

	// Second creation with same task+attempt should fail (branch exists).
	_, err = m.Create(repo, "task-dup", "1")
	if err == nil {
		t.Error("expected error for duplicate branch creation")
	}
	t.Logf("Duplicate branch error: %v", err)
}

func TestManager_CreateInvalidRepo(t *testing.T) {
	m := New()
	dir := t.TempDir()

	_, err := m.Create(dir, "task-bad", "1")
	if err == nil {
		t.Error("expected error for non-repo path")
	}
}

func TestManager_StatusInvalidPath(t *testing.T) {
	m := New()
	report := m.Status("/nonexistent/path")
	if report.Error == "" {
		t.Error("expected error for nonexistent path")
	}
}

func TestManager_CheckpointClean(t *testing.T) {
	dir := t.TempDir()
	repo, err := gitInit(dir)
	if err != nil {
		t.Fatal(err)
	}

	m := New()
	info, err := m.Create(repo, "task-clean", "1")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Remove(info.Path)

	// Checkpoint with no changes (--allow-empty).
	sha, err := m.Checkpoint(info.Path, "empty checkpoint")
	if err != nil {
		t.Fatalf("Checkpoint on clean worktree: %v\n%s", err, debugLog(info.Path))
	}
	if sha == "" {
		t.Error("expected commit SHA even for empty checkpoint")
	}
}

func TestManager_CreateMultipleWorktrees(t *testing.T) {
	dir := t.TempDir()
	repo, err := gitInit(dir)
	if err != nil {
		t.Fatal(err)
	}

	m := New()

	var infos []*WorktreeInfo
	for i := 0; i < 3; i++ {
		info, err := m.Create(repo, "multi", string(rune('A'+i)))
		if err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
		infos = append(infos, info)
	}

	// Each should have a unique branch and path.
	seen := make(map[string]bool)
	for _, info := range infos {
		if seen[info.Branch] {
			t.Errorf("duplicate branch: %s", info.Branch)
		}
		seen[info.Branch] = true

		// Write unique content to each.
		writeFile(info.Path, "id.txt", info.Branch)
		m.Checkpoint(info.Path, "checkpoint "+info.Branch)

		// Remove.
		if err := m.Remove(info.Path); err != nil {
			t.Errorf("Remove %s: %v", info.Branch, err)
		}
	}
}
