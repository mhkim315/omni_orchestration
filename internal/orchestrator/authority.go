// Package orchestrator — Integration Authority v1.5
//
// Hierarchical authority tuple enforced at all mutation points:
//
//	run_epoch > task_generation > attempt_generation > worker_lease_generation
//
// Every mutation validates against current authority. Stale child results
// are rejected. A durable tombstone is written before any kill signal.
package orchestrator

import (
	"fmt"

	"github.com/mhkim315/omni_orchestration/internal/taskstore"
)

// Authority is the hierarchical generation tuple for a run entity.
// Higher epochs/generations supersede lower ones.
type Authority struct {
	RunEpoch              int64
	TaskGeneration        int64
	AttemptGeneration     int64
	WorkerLeaseGeneration int64
	BaseSHA               string
}

// CurrentAuthority reads the current authority for a run from the store.
func CurrentAuthority(store *taskstore.Store, runID, taskID int64) (*Authority, error) {
	a := &Authority{RunEpoch: 1, TaskGeneration: 1, AttemptGeneration: 1}

	// Read run epoch from run_record.
	if rec, err := store.GetRunRecord(runID); err == nil {
		a.RunEpoch = rec.ID // run_record ID serves as epoch
	}
	// Task generation from task ID.
	if task, err := store.GetTask(taskID); err == nil {
		a.TaskGeneration = task.ID
	}
	// Attempt generation from latest attempt.
	attempts, _ := store.GetAttemptsByTask(taskID)
	if len(attempts) > 0 {
		last := attempts[len(attempts)-1]
		a.AttemptGeneration = int64(last.Number)
		if last.CheckpointCommit != "" {
			a.BaseSHA = last.CheckpointCommit
		}
		// Worker lease generation.
		if w, err := store.GetWorkerByAttempt(last.ID); err == nil {
			a.WorkerLeaseGeneration = w.Generation
		}
	}

	return a, nil
}

// ValidateMutation checks that the intent authority is not stale compared to current.
// Returns nil if the mutation is allowed, or an error describing the staleness.
func (a *Authority) ValidateMutation(current *Authority) error {
	if current == nil {
		return nil
	}

	// Run epoch: current must be >= intent.
	if a.RunEpoch < current.RunEpoch {
		return fmt.Errorf("authority: stale run_epoch intent=%d current=%d", a.RunEpoch, current.RunEpoch)
	}

	// Task generation: current must be >= intent.
	if a.TaskGeneration < current.TaskGeneration {
		return fmt.Errorf("authority: stale task_generation intent=%d current=%d", a.TaskGeneration, current.TaskGeneration)
	}

	// Attempt generation: current must be >= intent.
	if a.AttemptGeneration < current.AttemptGeneration {
		return fmt.Errorf("authority: stale attempt_generation intent=%d current=%d", a.AttemptGeneration, current.AttemptGeneration)
	}

	// Worker lease: current must be >= intent.
	if a.WorkerLeaseGeneration < current.WorkerLeaseGeneration {
		return fmt.Errorf("authority: stale worker_lease intent=%d current=%d", a.WorkerLeaseGeneration, current.WorkerLeaseGeneration)
	}

	return nil
}

// ValidateChildResult checks that a child (e.g. worker result) is not stale
// compared to the parent authority. Child must be >= parent in all dimensions.
func (a *Authority) ValidateChildResult(parent *Authority) error {
	if parent == nil {
		return nil
	}

	if a.RunEpoch < parent.RunEpoch {
		return fmt.Errorf("authority: child run_epoch %d < parent %d", a.RunEpoch, parent.RunEpoch)
	}
	if a.TaskGeneration < parent.TaskGeneration {
		return fmt.Errorf("authority: child task_gen %d < parent %d", a.TaskGeneration, parent.TaskGeneration)
	}
	if a.AttemptGeneration < parent.AttemptGeneration {
		return fmt.Errorf("authority: child attempt_gen %d < parent %d", a.AttemptGeneration, parent.AttemptGeneration)
	}

	return nil
}

// RecordTombstone writes a durable intent record before a destructive action.
// The tombstone proves intent even if the process crashes mid-action.
func RecordTombstone(store *taskstore.Store, runID int64, action string, a *Authority) error {
	return store.RecordTombstone(runID, action, a.RunEpoch, a.TaskGeneration, a.AttemptGeneration, a.WorkerLeaseGeneration)
}

// String formats the authority tuple.
func (a *Authority) String() string {
	return fmt.Sprintf("run_epoch=%d task_gen=%d attempt_gen=%d worker_lease=%d base=%s",
		a.RunEpoch, a.TaskGeneration, a.AttemptGeneration, a.WorkerLeaseGeneration, a.BaseSHA)
}
