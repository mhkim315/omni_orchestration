// Package worktree provides a Git worktree manager for task-isolated
// workspaces. Each task gets its own worktree on a unique attempt branch,
// with checkpoint commits and cleanup on completion.
//
// All operations use direct git command execution (no libgit2).
package worktree

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

// Manager creates and manages git worktrees for task isolation.
type Manager struct {
	mu sync.Mutex
}

// New returns a new worktree Manager.
func New() *Manager {
	return &Manager{}
}

// WorktreeInfo describes a managed worktree.
type WorktreeInfo struct {
	Path      string // filesystem path of the worktree
	Branch    string // full branch name (e.g. task/task-42/attempt-3)
	BaseRepo  string // original repo path
	TaskID    string
	AttemptID string
}

// StatusReport summarizes the state of a worktree.
type StatusReport struct {
	Path         string
	Branch       string
	Clean        bool
	ChangedFiles []string
	DiffStat     string
	Error        string
}

// Create creates a new worktree from baseRepo for the given task+attempt.
// The branch is named task/{taskID}/attempt-{N} where N is derived from
// the attemptID. Returns the worktree path and branch name.
func (m *Manager) Create(baseRepo, taskID, attemptID string) (*WorktreeInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := validateRepo(baseRepo); err != nil {
		return nil, err
	}

	branch := fmt.Sprintf("task/%s/attempt-%s", taskID, attemptID)

	// Derive worktree path: baseRepo's parent + .worktrees/ + taskID-attemptID.
	repoDir := filepath.Base(baseRepo)
	wtName := fmt.Sprintf("%s-%s", taskID, attemptID)
	wtPath := filepath.Join(filepath.Dir(baseRepo), ".worktrees", repoDir, wtName)

	// Create the branch from HEAD of baseRepo.
	createBranch := exec.Command("git", "-C", baseRepo, "branch", branch, "HEAD")
	if out, err := createBranch.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("create branch %s: %w\n%s", branch, err, out)
	}

	// Create the worktree on that branch.
	addWT := exec.Command("git", "-C", baseRepo, "worktree", "add", wtPath, branch)
	if out, err := addWT.CombinedOutput(); err != nil {
		// Clean up the branch on failure.
		exec.Command("git", "-C", baseRepo, "branch", "-D", branch).Run()
		return nil, fmt.Errorf("create worktree at %s: %w\n%s", wtPath, err, out)
	}

	return &WorktreeInfo{
		Path:      wtPath,
		Branch:    branch,
		BaseRepo:  baseRepo,
		TaskID:    taskID,
		AttemptID: attemptID,
	}, nil
}

// Checkpoint stages all changes in the worktree, commits them with the
// given message, and pushes the branch to origin. Returns the commit SHA.
func (m *Manager) Checkpoint(worktreePath, message string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := validateWorktree(worktreePath); err != nil {
		return "", err
	}

	// git add -A.
	add := exec.Command("git", "-C", worktreePath, "add", "-A")
	if out, err := add.CombinedOutput(); err != nil {
		return "", fmt.Errorf("add: %w\n%s", err, out)
	}

	// git commit -m <message> (allow empty commits — no-op checkpoint).
	commit := exec.Command("git", "-C", worktreePath, "commit",
		"-m", message, "--allow-empty")
	if out, err := commit.CombinedOutput(); err != nil {
		return "", fmt.Errorf("commit: %w\n%s", err, out)
	}

	// Get the commit SHA.
	revParse := exec.Command("git", "-C", worktreePath, "rev-parse", "--short", "HEAD")
	out, err := revParse.Output()
	if err != nil {
		return "", fmt.Errorf("rev-parse: %w", err)
	}
	sha := strings.TrimSpace(string(out))

	// Push to origin (best-effort: not all repos have a remote).
	push := exec.Command("git", "-C", worktreePath, "push", "origin", "HEAD")
	push.Run() // ignore errors — no origin in test repos

	return sha, nil
}

// Status returns the current state of a worktree.
func (m *Manager) Status(worktreePath string) *StatusReport {
	report := &StatusReport{Path: worktreePath}

	if err := validateWorktree(worktreePath); err != nil {
		report.Error = err.Error()
		return report
	}

	// Get current branch.
	branch := exec.Command("git", "-C", worktreePath, "rev-parse", "--abbrev-ref", "HEAD")
	if out, err := branch.Output(); err == nil {
		report.Branch = strings.TrimSpace(string(out))
	}

	// Short status: changed + untracked files.
	status := exec.Command("git", "-C", worktreePath, "status", "--short")
	if out, err := status.Output(); err == nil {
		lines := strings.Split(strings.TrimSpace(string(out)), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line != "" {
				// Format: "XY filename" — keep the filename part.
				report.ChangedFiles = append(report.ChangedFiles, line)
			}
		}
	}

	report.Clean = len(report.ChangedFiles) == 0

	// Diff stat.
	diff := exec.Command("git", "-C", worktreePath, "diff", "--stat", "HEAD")
	if out, err := diff.Output(); err == nil {
		report.DiffStat = strings.TrimSpace(string(out))
	}

	return report
}

// Remove deletes the worktree directory and its associated branch.
// The base repo is inferred from the worktree's git metadata.
func (m *Manager) Remove(worktreePath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := validateWorktree(worktreePath); err != nil {
		return err
	}

	// Get the branch name before removing the worktree.
	branchCmd := exec.Command("git", "-C", worktreePath, "rev-parse", "--abbrev-ref", "HEAD")
	branchOut, err := branchCmd.Output()
	if err != nil {
		return fmt.Errorf("get branch: %w", err)
	}
	branch := strings.TrimSpace(string(branchOut))

	// Find the main repo from the worktree's git metadata.
	mainRepo, err := getMainRepo(worktreePath)
	if err != nil {
		return err
	}

	// Remove the worktree (prunes the administrative entry).
	removeWT := exec.Command("git", "-C", mainRepo, "worktree", "remove", worktreePath, "--force")
	if out, err := removeWT.CombinedOutput(); err != nil {
		return fmt.Errorf("remove worktree %s: %w\n%s", worktreePath, err, out)
	}

	// Delete the branch from the main repo.
	deleteBranch := exec.Command("git", "-C", mainRepo, "branch", "-D", branch)
	out, err := deleteBranch.CombinedOutput()
	if err != nil {
		// Branch may already be deleted or not exist — non-fatal.
		_ = out
	}

	// Remove the worktree directory if it still exists.
	os.RemoveAll(worktreePath)

	return nil
}

// ── helpers ──

func validateRepo(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("repo path %s: %w", path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("repo path %s: not a directory", path)
	}
	gitDir := filepath.Join(path, ".git")
	if _, err := os.Stat(gitDir); err != nil {
		return fmt.Errorf("repo path %s: no .git directory", path)
	}
	return nil
}

func validateWorktree(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("worktree path %s: %w", path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("worktree path %s: not a directory", path)
	}
	gitFile := filepath.Join(path, ".git")
	if _, err := os.Stat(gitFile); err != nil {
		return fmt.Errorf("worktree path %s: no .git file", path)
	}
	return nil
}

// getMainRepo reads the .git file in a worktree to find the main repo path.
func getMainRepo(wtPath string) (string, error) {
	gitFile := filepath.Join(wtPath, ".git")
	data, err := os.ReadFile(gitFile)
	if err != nil {
		return "", fmt.Errorf("read .git: %w", err)
	}
	// The .git file contains: "gitdir: /path/to/main/.git/worktrees/name"
	content := strings.TrimSpace(string(data))
	if !strings.HasPrefix(content, "gitdir: ") {
		return "", fmt.Errorf("unexpected .git format: %s", content)
	}
	gitDir := strings.TrimPrefix(content, "gitdir: ")
	// gitDir is something like /path/to/main/.git/worktrees/name
	// We need /path/to/main
	// Walk up: .git/worktrees/name → .git → main repo
	worktreesDir := filepath.Dir(gitDir)     // .../.git/worktrees
	gitMainDir := filepath.Dir(worktreesDir) // .../.git
	mainRepo := filepath.Dir(gitMainDir)     // ...
	return mainRepo, nil
}

// ── testing helpers ──

// gitInit creates a temporary git repo and returns its path.
func gitInit(dir string) (string, error) {
	repo := filepath.Join(dir, "test-repo")
	if err := os.MkdirAll(repo, 0755); err != nil {
		return "", err
	}
	cmds := [][]string{
		{"init", "-b", "main"},
		{"config", "user.email", "test@test.local"},
		{"config", "user.name", "Test"},
	}
	for _, args := range cmds {
		cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			return "", fmt.Errorf("git %s: %w\n%s", args[0], err, out)
		}
	}
	// Create an initial commit so HEAD exists.
	initialFile := filepath.Join(repo, "README.md")
	os.WriteFile(initialFile, []byte("# test\n"), 0644)
	add := exec.Command("git", "-C", repo, "add", "README.md")
	add.Run()
	commit := exec.Command("git", "-C", repo, "commit", "-m", "initial")
	if out, err := commit.CombinedOutput(); err != nil {
		return "", fmt.Errorf("initial commit: %w\n%s", err, out)
	}
	return repo, nil
}

// writeFile creates a file in the worktree.
func writeFile(wtPath, name, content string) error {
	return os.WriteFile(filepath.Join(wtPath, name), []byte(content), 0644)
}

// readFile reads a file from the worktree.
func readFile(wtPath, name string) (string, error) {
	data, err := os.ReadFile(filepath.Join(wtPath, name))
	return strings.TrimSpace(string(data)), err
}

// debugLog writes debug info about the worktree git state.
func debugLog(wtPath string) string {
	var buf bytes.Buffer
	cmds := []string{
		"status --short",
		"log --oneline -3",
		"branch -a",
		"rev-parse --abbrev-ref HEAD",
	}
	for _, c := range cmds {
		parts := strings.Fields(c)
		cmd := exec.Command("git", append([]string{"-C", wtPath}, parts...)...)
		out, _ := cmd.CombinedOutput()
		buf.WriteString(fmt.Sprintf("git %s:\n%s\n", c, out))
	}
	return buf.String()
}
