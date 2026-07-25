// Package orchestrator integrates worktree, runtime, supervisor, and taskstore
// into a coordinated task-execution loop with quiescence-driven wake messages.
package orchestrator

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/miinanii/omni_orchestration/internal/runtime"
	"github.com/miinanii/omni_orchestration/internal/supervisor"
	"github.com/miinanii/omni_orchestration/internal/taskstore"
	"github.com/miinanii/omni_orchestration/internal/worktree"
)

// Config is the orchestrator run configuration.
type Config struct {
	Repo    string // path to git repository
	Task    string // task title
	Command string // worker command (e.g. "claude")
	CWD     string // working directory override (defaults to worktree path)
}

// WakeMessage is emitted when the worker reaches a decision point.
type WakeMessage struct {
	AttemptID  string    `json:"attempt_id"`
	State      string    `json:"state"`
	Message    string    `json:"message"`
	Checkpoint string    `json:"checkpoint,omitempty"`
	Dirty      bool      `json:"dirty"`
	Timestamp  time.Time `json:"timestamp"`
}

// String formats the wake message as human-readable text.
func (w WakeMessage) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Executor attempt %s became quiescent.\n", w.AttemptID))
	sb.WriteString(fmt.Sprintf("Evidence: runtime %s", strings.ToLower(w.State)))
	if w.Dirty {
		sb.WriteString(", worktree modified")
	}
	if w.Checkpoint != "" {
		sb.WriteString(fmt.Sprintf(", checkpoint %s", w.Checkpoint))
	}
	sb.WriteString(".\n")
	sb.WriteString("Choose: VALIDATE / CONTINUE / RETRY_CLEAN / REPLACE / FAIL\n")
	return sb.String()
}

// Decision is a coordinator response to a wake message.
type Decision string

const (
	DecisionValidate   Decision = "VALIDATE"
	DecisionContinue   Decision = "CONTINUE"
	DecisionRetryClean Decision = "RETRY_CLEAN"
	DecisionReplace    Decision = "REPLACE"
	DecisionFail       Decision = "FAIL"
)

// Run executes the full orchestrator flow for one task.
// Returns the set of decisions made (for testing) and any error.
func Run(ctx context.Context, cfg Config, store *taskstore.Store, wt *worktree.Manager) ([]Decision, error) {
	if cfg.Repo == "" || cfg.Task == "" || cfg.Command == "" {
		return nil, fmt.Errorf("repo, task, and command are required")
	}

	// 1. CreateRun → CreateTask
	run, err := store.CreateRun()
	if err != nil {
		return nil, fmt.Errorf("create run: %w", err)
	}
	task, err := store.CreateTask(run.ID, cfg.Task)
	if err != nil {
		return nil, fmt.Errorf("create task: %w", err)
	}

	var decisions []Decision
	attemptNum := 1
	baseCommit := "HEAD"

	for {
		select {
		case <-ctx.Done():
			return decisions, ctx.Err()
		default:
		}

		attemptID := fmt.Sprintf("T%d-A%d", task.ID, attemptNum)
		branch := fmt.Sprintf("task/T%d/attempt-%d", task.ID, attemptNum)

		// 2. Worktree.Create → isolated branch.
		info, err := wt.Create(cfg.Repo, fmt.Sprintf("T%d", task.ID), fmt.Sprintf("%d", attemptNum))
		if err != nil {
			return decisions, fmt.Errorf("create worktree: %w", err)
		}
		cwd := cfg.CWD
		if cwd == "" {
			cwd = info.Path
		}

		// 3. Store the attempt.
		attempt, err := store.CreateAttempt(task.ID, attemptNum, attemptID, branch, baseCommit)
		if err != nil {
			wt.Remove(info.Path)
			return decisions, fmt.Errorf("create attempt: %w", err)
		}

		// 4. Record worker.
		worker, err := store.RecordWorker(attempt.ID, cfg.Command, cwd, "primary", 1)
		if err != nil {
			wt.Remove(info.Path)
			return decisions, fmt.Errorf("record worker: %w", err)
		}

		// 5. Runtime.Start.
		rt := runtime.New()
		if err := rt.Start(cfg.Command, cwd); err != nil {
			store.EmitEvent(run.ID, task.ID, attempt.ID, worker.ID, "start_failed", nil)
			wt.Remove(info.Path)
			return decisions, fmt.Errorf("start runtime: %w", err)
		}
		store.EmitEvent(run.ID, task.ID, attempt.ID, worker.ID, "worker_started", nil)

		// 6. Supervisor.Observe.
		sup := supervisor.New(supervisor.DefaultConfig())
		stateCh := sup.Observe(ctx, rt)

		// 7. Wait for quiescence or exit.
		var checkpointSHA string
		var finalState supervisor.State
		dirty := false

	loop:
		for sc := range stateCh {
			finalState = sc.To
			switch sc.To {
			case supervisor.StateActive:
				// Reset dirty on new output (worker is active again).
			case supervisor.StateQuiescentCandidate:
				// Worker went silent — checkpoint and emit wake message.
				dirty = wtIsDirty(wt, info.Path)
				if dirty {
					sha, err := wt.Checkpoint(info.Path, fmt.Sprintf("auto-checkpoint: %s quiescent", attemptID))
					if err != nil {
						log.Printf("checkpoint error: %v", err)
					} else {
						checkpointSHA = sha
						store.UpdateAttemptCheckpoint(attempt.ID, sha)
					}
				}
				store.EmitEvent(run.ID, task.ID, attempt.ID, worker.ID, "quiescent", nil)

				wake := WakeMessage{
					AttemptID:  attemptID,
					State:      string(finalState),
					Message:    "Worker became quiescent — no output for quiescence timeout",
					Checkpoint: checkpointSHA,
					Dirty:      dirty,
					Timestamp:  time.Now(),
				}
				fmt.Fprint(os.Stderr, wake.String())

				// Wait for coordinator decision (simulated).
				decision := awaitDecision(ctx, attemptNum)
				decisions = append(decisions, decision)

				switch decision {
				case DecisionValidate, DecisionContinue:
					// Keep running — reset quiescence detection.
					continue loop
				case DecisionRetryClean:
					// New attempt with clean worktree.
					rt.Stop(ctx)
					wt.Remove(info.Path)
					store.UpdateAttemptStatus(attempt.ID, taskstore.StatusCancelled)
					store.EmitEvent(run.ID, task.ID, attempt.ID, worker.ID, "attempt_retry", nil)
					attemptNum++
					baseCommit = "HEAD"
					break loop
				case DecisionReplace:
					// New attempt preserving dirty worktree.
					rt.Stop(ctx)
					store.UpdateAttemptStatus(attempt.ID, taskstore.StatusCancelled)
					attemptNum++
					baseCommit = branch // branch from current work
					wt.Remove(info.Path)
					break loop
				case DecisionFail:
					rt.Stop(ctx)
					store.UpdateAttemptStatus(attempt.ID, taskstore.StatusFailed)
					store.UpdateTaskStatus(task.ID, taskstore.StatusFailed)
					store.EmitEvent(run.ID, task.ID, attempt.ID, worker.ID, "attempt_failed", nil)
					return decisions, nil
				}

			case supervisor.StateExited:
				// Clean exit — checkpoint if dirty, then finish.
				dirty = wtIsDirty(wt, info.Path)
				if dirty {
					sha, _ := wt.Checkpoint(info.Path, fmt.Sprintf("final-checkpoint: %s exited", attemptID))
					checkpointSHA = sha
					store.UpdateAttemptCheckpoint(attempt.ID, sha)
				}
				store.UpdateAttemptStatus(attempt.ID, taskstore.StatusCompleted)
				store.UpdateTaskStatus(task.ID, taskstore.StatusCompleted)
				store.EmitEvent(run.ID, task.ID, attempt.ID, worker.ID, "attempt_completed", nil)
				wt.Remove(info.Path)
				return decisions, nil

			case supervisor.StateCrashed:
				// Worker crashed — checkpoint dirty changes, then recover.
				dirty = wtIsDirty(wt, info.Path)
				if dirty {
					sha, _ := wt.Checkpoint(info.Path, fmt.Sprintf("crash-recovery: %s", attemptID))
					checkpointSHA = sha
					store.UpdateAttemptCheckpoint(attempt.ID, sha)
				}
				store.UpdateAttemptStatus(attempt.ID, taskstore.StatusFailed)
				store.EmitEvent(run.ID, task.ID, attempt.ID, worker.ID, "worker_crashed", nil)

				wake := WakeMessage{
					AttemptID:  attemptID,
					State:      "CRASHED",
					Message:    "Worker crashed — dirty changes checkpointed",
					Checkpoint: checkpointSHA,
					Dirty:      dirty,
					Timestamp:  time.Now(),
				}
				fmt.Fprint(os.Stderr, wake.String())

				// New attempt with crash recovery.
				decision := awaitDecision(ctx, attemptNum+1)
				decisions = append(decisions, decision)
				if decision == DecisionFail {
					return decisions, nil
				}
				attemptNum++
				baseCommit = "HEAD"
				break loop
			}
		}

		// Stop runtime if still running after state loop exit.
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		rt.Stop(stopCtx)
		cancel()
	}
}

// wtIsDirty checks if the worktree has uncommitted changes.
func wtIsDirty(wt *worktree.Manager, path string) bool {
	return !wt.Status(path).Clean
}

// awaitDecision simulates a coordinator decision.
// In production this would block on an external message.
// For MVP, it auto-advances: first quiescence → VALIDATE, crashes → RETRY_CLEAN.
func awaitDecision(ctx context.Context, attemptNum int) Decision {
	// Simulated: first quiescence validates, subsequent continues.
	select {
	case <-ctx.Done():
		return DecisionFail
	case <-time.After(100 * time.Millisecond):
	}
	return DecisionValidate
}
