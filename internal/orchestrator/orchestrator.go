// Package orchestrator integrates worktree, runtime, supervisor, and taskstore
// into a coordinated task-execution loop with quiescence-driven wake messages.
//
// Gate 2: real validator, file-backed store, stdin prompt delivery.
// Gate 3: supervisor.Recover() with secret scan, branch cleanup, restart recovery.
package orchestrator

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/miinanii/omni_orchestration/internal/runtime"
	"github.com/miinanii/omni_orchestration/internal/supervisor"
	"github.com/miinanii/omni_orchestration/internal/taskstore"
	"github.com/miinanii/omni_orchestration/internal/worktree"
)

// Config is the orchestrator run configuration.
type Config struct {
	Repo      string // path to git repository
	Task      string // task title (+ prompt content for worker stdin)
	Command   string // worker command (e.g. "claude")
	CWD       string // working directory override (defaults to worktree path)
	Validator string // external shell command for validation (empty = skip)
	StorePath string // SQLite file path (empty = in-memory)
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

// Run executes the full orchestrator flow.
func Run(ctx context.Context, cfg Config, store *taskstore.Store, wt *worktree.Manager) ([]Decision, error) {
	if cfg.Repo == "" || cfg.Task == "" || cfg.Command == "" {
		return nil, fmt.Errorf("repo, task, and command are required")
	}

	// Gate 3: recover in-progress run from a previous daemon instance.
	existing, _ := store.GetActiveAttempts()
	if len(existing) > 0 {
		log.Printf("Gate-3 recovery: %d in-progress attempts found", len(existing))
		for _, a := range existing {
			store.UpdateAttemptStatus(a.ID, taskstore.StatusCancelled)
		}
	}

	// 1. CreateRun → CreateTask.
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

		// 2. Worktree.Create.
		info, err := wt.Create(cfg.Repo, fmt.Sprintf("T%d", task.ID), fmt.Sprintf("%d", attemptNum))
		if err != nil {
			return decisions, fmt.Errorf("create worktree: %w", err)
		}
		cwd := cfg.CWD
		if cwd == "" {
			cwd = info.Path
		}

		// 3. Store attempt.
		attempt, err := store.CreateAttempt(task.ID, attemptNum, attemptID, branch, baseCommit)
		if err != nil {
			wt.Remove(info.Path)
			return decisions, fmt.Errorf("create attempt: %w", err)
		}
		worker, err := store.RecordWorker(attempt.ID, cfg.Command, cwd, "primary", 1)
		if err != nil {
			wt.Remove(info.Path)
			return decisions, fmt.Errorf("record worker: %w", err)
		}

		// 4. Runtime.Start.
		rt := runtime.New()
		if err := rt.Start(cfg.Command, cwd); err != nil {
			store.EmitEvent(run.ID, task.ID, attempt.ID, worker.ID, "start_failed", nil)
			wt.Remove(info.Path)
			return decisions, fmt.Errorf("start runtime: %w", err)
		}
		store.EmitEvent(run.ID, task.ID, attempt.ID, worker.ID, "worker_started", nil)

		// Gate 2: Send task prompt to worker stdin.
		if cfg.Task != "" {
			prompt := cfg.Task + "\n"
			if _, err := rt.Write(1, []byte(prompt)); err != nil {
				log.Printf("stdin write: %v", err)
			}
		}

		// 5. Supervisor.Observe.
		supCfg := supervisor.DefaultConfig()
		supCfg.QuiescenceTimeout = 10 * time.Second // shorter for MVP
		supCfg.PollInterval = 2 * time.Second
		sup := supervisor.New(supCfg)
		stateCh := sup.Observe(ctx, rt)

		// 6. State loop.
		var checkpointSHA string
		dirty := false

	loop:
		for sc := range stateCh {
			switch sc.To {
			case supervisor.StateActive:
				// Worker is producing output.

			case supervisor.StateQuiescentCandidate:
				dirty = wtIsDirty(wt, info.Path)
				checkpointSHA = recoverAndRecord(ctx, rt, wt, info.Path, attemptID, store,
					run.ID, task.ID, attempt.ID, worker.ID, dirty)

				// Gate 2: run validator if configured.
				if cfg.Validator != "" && dirty {
					valid := runValidator(cfg.Validator, info.Path)
					if valid {
						store.UpdateTaskStatus(task.ID, taskstore.StatusCompleted)
						store.UpdateAttemptStatus(attempt.ID, taskstore.StatusCompleted)
						store.EmitEvent(run.ID, task.ID, attempt.ID, worker.ID, "validated", nil)
						wt.Remove(info.Path)
						return decisions, nil
					}
					// REJECT → retry.
					store.EmitEvent(run.ID, task.ID, attempt.ID, worker.ID, "validation_rejected", nil)
					wt.Remove(info.Path)
					attemptNum++
					baseCommit = "HEAD"
					break loop
				}

				wake := WakeMessage{
					AttemptID:  attemptID,
					State:      string(sc.To),
					Checkpoint: checkpointSHA,
					Dirty:      dirty,
					Timestamp:  time.Now(),
				}
				fmt.Fprint(os.Stderr, wake.String())

				// Gate 2: real coordinator input via stdin.
				decision := awaitDecision(ctx)
				decisions = append(decisions, decision)

				switch decision {
				case DecisionValidate, DecisionContinue:
					continue loop
				case DecisionRetryClean:
					rt.Stop(context.Background())
					wt.Remove(info.Path)
					store.UpdateAttemptStatus(attempt.ID, taskstore.StatusCancelled)
					attemptNum++
					baseCommit = "HEAD"
					break loop
				case DecisionReplace:
					rt.Stop(context.Background())
					store.UpdateAttemptStatus(attempt.ID, taskstore.StatusCancelled)
					attemptNum++
					baseCommit = branch
					wt.Remove(info.Path)
					break loop
				case DecisionFail:
					rt.Stop(context.Background())
					store.UpdateAttemptStatus(attempt.ID, taskstore.StatusFailed)
					store.UpdateTaskStatus(task.ID, taskstore.StatusFailed)
					return decisions, nil
				}

			case supervisor.StateExited:
				dirty = wtIsDirty(wt, info.Path)
				checkpointSHA = recoverAndRecord(ctx, rt, wt, info.Path, attemptID, store,
					run.ID, task.ID, attempt.ID, worker.ID, dirty)

				// Gate 2: validate on exit.
				if cfg.Validator != "" {
					if runValidator(cfg.Validator, info.Path) {
						store.UpdateTaskStatus(task.ID, taskstore.StatusCompleted)
						store.UpdateAttemptStatus(attempt.ID, taskstore.StatusCompleted)
						store.EmitEvent(run.ID, task.ID, attempt.ID, worker.ID, "validated", nil)
					} else {
						store.UpdateAttemptStatus(attempt.ID, taskstore.StatusFailed)
						store.EmitEvent(run.ID, task.ID, attempt.ID, worker.ID, "validation_rejected", nil)
					}
				} else {
					store.UpdateAttemptStatus(attempt.ID, taskstore.StatusCompleted)
					store.UpdateTaskStatus(task.ID, taskstore.StatusCompleted)
				}
				store.EmitEvent(run.ID, task.ID, attempt.ID, worker.ID, "attempt_completed", nil)

				// Gate 3: clean up worktree after checkpoint recorded.
				if checkpointSHA != "" {
					supervisor.CleanupWorktree(ctx, wt, info.Path)
				}
				return decisions, nil

			case supervisor.StateCrashed:
				dirty = wtIsDirty(wt, info.Path)
				checkpointSHA = recoverAndRecord(ctx, rt, wt, info.Path, attemptID, store,
					run.ID, task.ID, attempt.ID, worker.ID, dirty)
				store.UpdateAttemptStatus(attempt.ID, taskstore.StatusFailed)
				store.EmitEvent(run.ID, task.ID, attempt.ID, worker.ID, "worker_crashed", nil)
				supervisor.CleanupWorktree(ctx, wt, info.Path)

				wake := WakeMessage{
					AttemptID:  attemptID,
					State:      "CRASHED",
					Checkpoint: checkpointSHA,
					Dirty:      dirty,
					Timestamp:  time.Now(),
				}
				fmt.Fprint(os.Stderr, wake.String())

				decision := awaitDecision(ctx)
				decisions = append(decisions, decision)
				if decision == DecisionFail {
					return decisions, nil
				}
				attemptNum++
				baseCommit = "HEAD"
				break loop
			}
		}

		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		rt.Stop(stopCtx)
		cancel()
	}
}

// recoverAndRecord uses supervisor.Recover() for secret-scanned checkpoint,
// then records the SHA in SQLite. Gate 3.
func recoverAndRecord(ctx context.Context, rt *runtime.Runtime, wt *worktree.Manager,
	worktreePath, attemptID string, store *taskstore.Store,
	runID, taskID, attemptIDVal, workerID int64, dirty bool) string {

	if !dirty {
		return ""
	}

	result := supervisor.Recover(ctx, rt, supervisor.RecoveryConfig{
		WorktreePath:      worktreePath,
		AttemptID:         attemptID,
		SecretScanEnabled: true,
	}, wt)

	if result.BlockedSecret {
		log.Printf("SECRET BLOCKED: checkpoint blocked for %s", attemptID)
		return ""
	}
	if result.CommitSHA != "" {
		store.UpdateAttemptCheckpoint(attemptIDVal, result.CommitSHA)
		log.Printf("RECOVERED: attempt %s checkpoint %s", attemptID, result.CommitSHA)
	}
	return result.CommitSHA
}

// runValidator executes an external shell command and returns true if exit 0.
func runValidator(command, cwd string) bool {
	if command == "" {
		return true
	}
	cmd := exec.Command("sh", "-c", command)
	if cwd != "" {
		cmd.Dir = cwd
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("VALIDATOR FAIL: %v\n%s", err, out)
		return false
	}
	log.Printf("VALIDATOR PASS: %s", strings.TrimSpace(string(out)))
	return true
}

// wtIsDirty checks if the worktree has uncommitted changes.
func wtIsDirty(wt *worktree.Manager, path string) bool {
	if path == "" {
		return false
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return !wt.Status(path).Clean
}

// awaitDecision reads a coordinator decision from stdin.
// Gate 2: real input instead of simulated.
func awaitDecision(ctx context.Context) Decision {
	reader := bufio.NewReader(os.Stdin)
	for {
		select {
		case <-ctx.Done():
			return DecisionFail
		default:
		}
		// Non-blocking read with timeout via goroutine.
		type result struct {
			line string
			err  error
		}
		ch := make(chan result, 1)
		go func() {
			line, err := reader.ReadString('\n')
			ch <- result{line, err}
		}()
		select {
		case r := <-ch:
			if r.err == io.EOF {
				return DecisionFail
			}
			if r.err != nil {
				continue
			}
			d := Decision(strings.TrimSpace(r.line))
			switch d {
			case DecisionValidate, DecisionContinue, DecisionRetryClean, DecisionReplace, DecisionFail:
				return d
			default:
				fmt.Fprintf(os.Stderr, "Unknown decision: %q. Choose: VALIDATE / CONTINUE / RETRY_CLEAN / REPLACE / FAIL\n", d)
				continue
			}
		case <-time.After(5 * time.Second):
			// No input within 5s → return CONTINUE (non-blocking default).
			return DecisionContinue
		case <-ctx.Done():
			return DecisionFail
		}
	}
}

// ── Store helpers ──

// OpenStore opens a file-backed or in-memory SQLite store.
func OpenStore(path string) (*taskstore.Store, error) {
	if path == "" {
		return taskstore.NewInMemory()
	}
	// Ensure parent directory exists.
	dir := filepath.Dir(path)
	if dir != "." {
		os.MkdirAll(dir, 0755)
	}
	return taskstore.New(path)
}
