package orchestrator

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/mhkim315/omni_orchestration/internal/runtime"
	"github.com/mhkim315/omni_orchestration/internal/supervisor"
	"github.com/mhkim315/omni_orchestration/internal/taskstore"
	"github.com/mhkim315/omni_orchestration/internal/worktree"
)

// StatusInterrupted marks an attempt that was in-flight when the daemon stopped.
const StatusInterrupted = "interrupted"

// ReconcileResult summarizes recovery actions.
type ReconcileResult struct {
	OrphanRuns    int
	RecoveredRuns int
	Interrupted   int
	StaleCoords   int
	Errors        []string
}

// Reconcile scans the store for orphaned runs (status != completed/failed)
// and transitions them to a recoverable state. Active attempts are marked
// interrupted; coordinator generations are invalidated.
//
// Reconcile is idempotent — repeated calls on the same store produce the
// same result.
func Reconcile(store *taskstore.Store, wt *worktree.Manager) ReconcileResult {
	var r ReconcileResult

	active, err := store.GetActiveAttempts()
	if err != nil {
		r.Errors = append(r.Errors, fmt.Sprintf("get active: %v", err))
		return r
	}
	r.OrphanRuns = len(active)

	for _, a := range active {
		if a.Status != taskstore.StatusRunning && a.Status != taskstore.StatusPending {
			continue
		}
		// R10: Preserve for re-own if checkpoint exists OR worker is still alive.
		// Checkpoint is definitive evidence. Live worker (kill(pid,0) OK) is also
		// valid — we can re-attach and observe it to completion.
		liveWorker := false
		if a.CheckpointCommit == "" {
			if w, wErr := store.GetWorkerByAttempt(a.ID); wErr == nil && w.PID > 0 {
				if err := syscall.Kill(w.PID, 0); err == nil {
					liveWorker = true
					log.Printf("RECOVERY: attempt %d pid %d alive — preserving for re-own (no checkpoint)", a.ID, w.PID)
				}
			}
		}
		if a.CheckpointCommit == "" && !liveWorker {
			// Resolve run/task/worker IDs for full terminalization.
			taskID := a.TaskID
			var runID int64
			if t, err := store.GetTask(taskID); err == nil {
				runID = t.RunID
			}
			var wPID int
			if w, err := store.GetWorkerByAttempt(a.ID); err == nil {
				wPID = w.PID
				store.UpdateWorkerStatus(w.ID, StatusInterrupted)
			}
			store.UpdateAttemptStatus(a.ID, StatusInterrupted)
			store.UpdateRunStatus(runID, StatusInterrupted)
			store.UpdateTaskStatus(taskID, StatusInterrupted)
			r.Interrupted++
			log.Printf("RECOVERY: attempt %d pid %d (no checkpoint, worker dead) fully terminalized", a.ID, wPID)
		} else if a.CheckpointCommit != "" {
			cpshort := a.CheckpointCommit
			if len(cpshort) > 8 {
				cpshort = cpshort[:8]
			}
			log.Printf("RECOVERY: attempt %d checkpoint %s — preserving for re-own", a.ID, cpshort)
		}
	}

	r.RecoveredRuns = len(active)
	return r
}

// ResumeRun restarts a previously-interrupted run from the last checkpoint.
// It creates a new coordinator with fresh generation and re-enters the
// orchestrator loop.
func ResumeRun(ctx context.Context, cfg Config, store *taskstore.Store, wt *worktree.Manager) ([]Decision, error) {
	// Check for active attempts — reconcile first if needed.
	active, _ := store.GetActiveAttempts()
	if len(active) > 0 {
		Reconcile(store, wt)
	}

	// Re-read after reconcile.
	active, _ = store.GetActiveAttempts()
	if len(active) > 0 {
		log.Printf("RESUME: %d active attempts still pending after reconcile", len(active))
	}

	// Create a fresh coordinator if configured.
	if cfg.Coordinator != nil {
		// Replace stale coordinator generation.
		oldGen := cfg.Coordinator.Generation()
		newRT := runtime.NewWithID("coordinator-"+cfg.Coordinator.ID()+"-resume", oldGen+1)
		cfg.Coordinator.Replace(ctx, newRT)
		log.Printf("RESUME: coordinator generation %d → %d", oldGen, cfg.Coordinator.Generation())
	}

	// Re-enter the main orchestrator loop.
	return Run(ctx, cfg, store, wt)
}

// ResumeWithRecovery reconciles orphan runs. If active attempts exist
// after reconcile, returns their state without creating a new run.
// If no active attempts remain, falls through to create a new run.
func ResumeWithRecovery(ctx context.Context, cfg Config, store *taskstore.Store, wt *worktree.Manager) ([]Decision, error) {
	result := Reconcile(store, wt)
	if len(result.Errors) > 0 {
		log.Printf("RECOVERY: %d errors during reconcile", len(result.Errors))
		for _, e := range result.Errors {
			log.Printf("  - %s", e)
		}
	}
	if result.Interrupted > 0 {
		log.Printf("RECOVERY: %d attempts interrupted, %d orphan runs reconciled",
			result.Interrupted, result.OrphanRuns)
	}

	// If active runs exist after reconcile, re-own their workers
	// instead of creating a new run.
	active, _ := store.GetActiveAttempts()
	if len(active) > 0 {
		log.Printf("RESUME: %d active attempts after reconcile — re-owning", len(active))
		var decisions []Decision
		var wg sync.WaitGroup
		for _, a := range active {
			// Fix 1: resolve actual runID+taskID from attempt.
			var runID, taskID int64
			taskID = a.TaskID
			if t, err := store.GetTask(taskID); err == nil {
				runID = t.RunID
			}
			// Look up worker by attempt ID (worker primary key, not attempt ID).
			w, err := store.GetWorkerByAttempt(a.ID)
			if err != nil {
				log.Printf("RESUME: attempt %d no worker record: %v", a.ID, err)
				store.UpdateAttemptStatus(a.ID, taskstore.StatusFailed)
				store.UpdateRunStatus(runID, taskstore.StatusFailed)
				store.UpdateTaskStatus(taskID, taskstore.StatusFailed)
				decisions = append(decisions, DecisionFail)
				continue
			}
			log.Printf("RESUME: attempt %d worker pid=%d pgid=%d start=%d cmd=%s gen=%d",
				a.ID, w.PID, w.PGID, w.StartTime, w.Command, w.Generation)

			// kill(pid, 0) verifies process is alive.
			if err := syscall.Kill(w.PID, 0); err != nil {
				log.Printf("RESUME: attempt %d pid %d not alive: %v — fail closed", a.ID, w.PID, err)
				store.UpdateAttemptStatus(a.ID, taskstore.StatusFailed)
				store.UpdateWorkerStatus(w.ID, taskstore.StatusFailed)
				store.UpdateRunStatus(runID, taskstore.StatusFailed)
				store.UpdateTaskStatus(taskID, taskstore.StatusFailed)
				decisions = append(decisions, DecisionFail)
				continue
			}

			// Attach with identity verification + stored generation.
			rt := runtime.NewWithID(a.WorkerID, w.Generation)
			id := runtime.AttachIdentity{
				PID: w.PID, Executable: w.Command,
				CWD: w.CWD, StartTimeMs: w.StartTime, PGID: w.PGID,
			}
			if err := rt.Attach(w.PID, id, w.Generation); err != nil {
				log.Printf("RESUME: attempt %d attach failed: %v", a.ID, err)
				store.UpdateAttemptStatus(a.ID, taskstore.StatusFailed)
				store.UpdateWorkerStatus(w.ID, taskstore.StatusFailed)
				store.UpdateRunStatus(runID, taskstore.StatusFailed)
				store.UpdateTaskStatus(taskID, taskstore.StatusFailed)
				decisions = append(decisions, DecisionFail)
				continue
			}

			// Fix 2+3: start supervisor loop + validator on attached runtime.
			wg.Add(1)
			go func(attemptID int64, workerCWD string, rID, tID int64) {
				defer wg.Done()
				supCfg := supervisor.Config{QuiescenceTimeout: 30 * time.Second, PollInterval: 5 * time.Second}
				sup := supervisor.New(supCfg)
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
				defer cancel()
				for sc := range sup.Observe(ctx, rt) {
					log.Printf("RESUME: attempt %d supervisor: %s→%s", attemptID, sc.From, sc.To)
					if sc.To == supervisor.StateExited || sc.To == supervisor.StateCrashed {
						ev := rt.Wait()
						status := taskstore.StatusFailed
						// Fix 3: CRASHED (exitCode=-1) → skip validator, mark failed.
						if sc.To == supervisor.StateExited && ev.ExitCode >= 0 {
							if cfg.Validator != "" {
								// Fix 2: validator runs in stored worker CWD.
								if runValidatorOnPath(workerCWD, cfg.Validator) {
									status = taskstore.StatusCompleted
								}
							} else {
								status = taskstore.StatusCompleted
							}
						}
						// Fix 2: finalizeTerminal on ALL paths.
						store.UpdateWorkerStatus(w.ID, status)
						store.UpdateAttemptStatus(attemptID, status)
						store.UpdateRunStatus(rID, status)
						store.UpdateTaskStatus(tID, status)
						return
					}
				}
			}(a.ID, w.CWD, runID, taskID)

			decisions = append(decisions, DecisionRetryClean)
			store.UpdateAttemptStatus(a.ID, taskstore.StatusRunning)
			log.Printf("RESUME: attempt %d attached pid %d cmd=%s gen=%d", a.ID, w.PID, w.Command, w.Generation)
		}
		wg.Wait() // Fix 5: Resume blocks until recovery completes.
		return decisions, nil
	}

	return Run(ctx, cfg, store, wt)
}

// RecoverOnly performs reconciliation without starting a new run.
// Used by `orchestrator recover`.
func RecoverOnly(store *taskstore.Store, wt *worktree.Manager) ReconcileResult {
	result := Reconcile(store, wt)

	if len(result.Errors) > 0 {
		for _, e := range result.Errors {
			log.Printf("RECOVERY ERROR: %s", e)
		}
	}

	log.Printf("RECOVERY: %d orphan runs, %d attempts interrupted, %d stale coordinators",
		result.OrphanRuns, result.Interrupted, result.StaleCoords)

	return result
}

// StaleCoordinatorDetected returns true if the coordinator generation
// is likely stale (process died between wake and decision).
func StaleCoordinatorDetected(coordGen int64, lastKnownGen int64) bool {
	return coordGen > 0 && lastKnownGen > 0 && coordGen != lastKnownGen
}

// runValidatorOnPath runs the validator in the given directory.
func runValidatorOnPath(path, validatorCmd string) bool {
	cmd := exec.Command("bash", "-c", validatorCmd)
	cmd.Dir = path
	return cmd.Run() == nil
}

// Ensure fmt is used.
var _ = fmt.Sprintf
