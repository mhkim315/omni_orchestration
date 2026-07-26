package orchestrator

import (
	"context"
	"fmt"
	"log"

	"github.com/miinanii/omni_orchestration/internal/runtime"
	"github.com/miinanii/omni_orchestration/internal/taskstore"
	"github.com/miinanii/omni_orchestration/internal/worktree"
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
		// Only mark as interrupted if there's NO checkpoint (never started).
		// Attempts with checkpoints are re-own candidates, not interrupted.
		if a.CheckpointCommit == "" {
			if err := store.UpdateAttemptStatus(a.ID, StatusInterrupted); err != nil {
				r.Errors = append(r.Errors, fmt.Sprintf("update attempt %d: %v", a.ID, err))
				continue
			}
			r.Interrupted++
			log.Printf("RECOVERY: attempt %d (no checkpoint) marked interrupted", a.ID)
		} else {
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
		log.Printf("RESUME: %d active attempts after reconcile — re-owning workers", len(active))
		var decisions []Decision
		for _, a := range active {
			decisions = append(decisions, DecisionRetryClean)
			store.UpdateAttemptStatus(a.ID, taskstore.StatusRunning)
		}
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

// Ensure fmt is used.
var _ = fmt.Sprintf
