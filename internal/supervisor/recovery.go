package supervisor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/miinanii/omni_orchestration/internal/runtime"
	"github.com/miinanii/omni_orchestration/internal/worktree"
)

// secretPatterns matched during pre-checkpoint scanning.
// Fail-closed: any match blocks the checkpoint.
var secretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`sk-ant-api-[A-Za-z0-9_-]+`),
	regexp.MustCompile(`sk-orca-[A-Za-z0-9_-]+`),
	regexp.MustCompile(`ghp_[A-Za-z0-9]{36}`),
	regexp.MustCompile(`-----BEGIN (RSA|OPENSSH|EC) PRIVATE KEY-----`),
	regexp.MustCompile(`xox[baprs]-[A-Za-z0-9-]+`),
}

// RecoveryConfig controls recovery behavior.
type RecoveryConfig struct {
	// WorktreePath is the filesystem path to the task's git worktree.
	// Empty means no worktree checkpointing (worker output only).
	WorktreePath string

	// AttemptID is the current attempt identifier for the checkpoint message.
	AttemptID string

	// SecretScanEnabled enables pre-checkpoint secret scanning.
	// When true, any matched secret pattern blocks the checkpoint.
	SecretScanEnabled bool
}

// RecoveryResult describes the outcome of a recovery attempt.
type RecoveryResult struct {
	Checkpointed  bool   // true if a checkpoint was committed
	CommitSHA     string // the checkpoint commit SHA (empty if not checkpointed)
	SkippedClean  bool   // worktree was clean, nothing to checkpoint
	BlockedSecret bool   // checkpoint blocked by secret scan
	Error         string // any error encountered
}

// Recover performs checkpoint recovery on a terminated worker.
//
// Flow:
//  1. Stop the worker (SIGKILL if still alive)
//  2. If worktree exists: check dirty status
//  3. If dirty: run secret scan
//  4. If clean of secrets: git add -A && git commit
//  5. Return checkpoint SHA
func Recover(ctx context.Context, worker *runtime.Runtime, cfg RecoveryConfig, wm *worktree.Manager) RecoveryResult {
	result := RecoveryResult{}

	// 1. Ensure worker is stopped.
	if worker != nil {
		closeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := worker.Stop(closeCtx); err != nil {
			result.Error = fmt.Sprintf("worker close: %v", err)
			return result
		}
	}

	// 2. No worktree → nothing to checkpoint.
	if cfg.WorktreePath == "" || wm == nil {
		return result
	}

	// Verify the worktree path exists.
	if _, err := os.Stat(cfg.WorktreePath); os.IsNotExist(err) {
		result.Error = fmt.Sprintf("worktree path does not exist: %s", cfg.WorktreePath)
		return result
	}

	// 3. Check worktree status.
	report := wm.Status(cfg.WorktreePath)
	if !report.Clean {
		// 4. Secret scan on changed files.
		if cfg.SecretScanEnabled {
			if blocked, matched := scanSecrets(cfg.WorktreePath, report.ChangedFiles); blocked {
				result.BlockedSecret = true
				result.Error = fmt.Sprintf("secret scan blocked checkpoint: matched pattern in %s", matched)
				return result
			}
		}

		// 5. Create checkpoint commit.
		msg := fmt.Sprintf("recovery: %s checkpoint", cfg.AttemptID)
		sha, err := wm.Checkpoint(cfg.WorktreePath, msg)
		if err != nil {
			result.Error = fmt.Sprintf("checkpoint commit: %v", err)
			return result
		}
		result.Checkpointed = true
		result.CommitSHA = sha
	} else {
		result.SkippedClean = true
	}

	return result
}

// scanSecrets checks changed files for secret patterns.
// Returns (blocked, matchedFile) — blocked is true if any pattern matches.
func scanSecrets(worktreePath string, changedFiles []string) (bool, string) {
	for _, entry := range changedFiles {
		// git status --short lines are "XY filename". Strip the 3-character
		// prefix if present (two status chars + space).
		relPath := entry
		if len(entry) >= 3 && entry[2] == ' ' {
			relPath = strings.TrimSpace(entry[3:])
		}
		// Handle rename lines ("R  old -> new") — take the new name.
		if idx := strings.Index(relPath, " -> "); idx >= 0 {
			relPath = relPath[idx+4:]
		}
		absPath := filepath.Join(worktreePath, relPath)
		// Skip directories and non-existent files.
		fi, err := os.Stat(absPath)
		if err != nil {
			return true, relPath // HIGH: fail-closed on stat error
		}
		if fi.IsDir() {
			continue
		}
		data, err := os.ReadFile(absPath)
		if err != nil {
			return true, relPath // HIGH: fail-closed on read error
		}
		for _, pat := range secretPatterns {
			if pat.Match(data) {
				return true, relPath
			}
		}
	}
	return false, ""
}

// Checkpointed is a convenience check for a successful recovery result.
func (r RecoveryResult) CheckpointedSuccessfully() bool {
	return r.Error == "" && !r.BlockedSecret
}

// ShouldCheckpoint reports whether the worktree state warrants a checkpoint.
// Returns true when the worktree is dirty and secret scan is passed or disabled.
func ShouldCheckpoint(report worktree.StatusReport, secretScanEnabled bool) (bool, string) {
	if report.Clean {
		return false, "worktree is clean"
	}
	// The caller is responsible for running the actual scan.
	// This function only checks the precondition.
	return true, ""
}

// CleanupWorktree removes the worktree directory after a successful checkpoint.
// Only call this after verifying the checkpoint SHA is recorded in the store.
func CleanupWorktree(ctx context.Context, wm *worktree.Manager, worktreePath string) error {
	if wm == nil {
		return nil
	}
	return wm.Remove(worktreePath)
}

// stripANSI removes basic ANSI escape sequences for cleaner error messages.
func stripANSI(s string) string {
	ansiRE := regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)
	return strings.TrimSpace(ansiRE.ReplaceAllString(s, ""))
}
