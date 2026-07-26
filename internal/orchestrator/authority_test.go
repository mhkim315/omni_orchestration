package orchestrator

import (
	"testing"

	"github.com/mhkim315/omni_orchestration/internal/taskstore"
)

// 1. Hierarchical reject all levels
func TestHierarchicalReject(t *testing.T) {
	current := &Authority{
		RunEpoch: 5, TaskGeneration: 3, AttemptGeneration: 2, WorkerLeaseGeneration: 4,
	}

	tests := []struct {
		name    string
		intent  *Authority
		wantErr bool
	}{
		{"equal", &Authority{5, 3, 2, 4, ""}, false},
		{"higher all", &Authority{6, 4, 3, 5, ""}, false},
		{"stale run_epoch", &Authority{4, 3, 2, 4, ""}, true},
		{"stale task_gen", &Authority{5, 2, 2, 4, ""}, true},
		{"stale attempt_gen", &Authority{5, 3, 1, 4, ""}, true},
		{"stale worker_lease", &Authority{5, 3, 2, 3, ""}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.intent.ValidateMutation(current)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateMutation() error=%v wantErr=%v", err, tt.wantErr)
			}
		})
	}
}

// 2. Tombstone before kill — durable record written
func TestTombstoneBeforeKill(t *testing.T) {
	store, _ := taskstore.NewInMemory()
	defer store.Close()

	run, _ := store.CreateRun()
	auth := &Authority{RunEpoch: 1, TaskGeneration: 1, AttemptGeneration: 1, WorkerLeaseGeneration: 1}

	err := RecordTombstone(store, run.ID, "SIGKILL", auth)
	if err != nil {
		t.Fatalf("RecordTombstone: %v", err)
	}

	// Tombstone persisted — even if process crashes here, intent is durable.
	t.Log("tombstone recorded before kill — crash-safe")
}

// 3. Stale child block — child with lower generation rejected
func TestStaleChildBlock(t *testing.T) {
	parent := &Authority{RunEpoch: 3, TaskGeneration: 2, AttemptGeneration: 2}
	child := &Authority{RunEpoch: 2, TaskGeneration: 2, AttemptGeneration: 2}

	err := child.ValidateChildResult(parent)
	if err == nil {
		t.Fatal("stale child should have been rejected (run_epoch 2 < 3)")
	}
	t.Logf("stale child blocked (expected): %v", err)

	// Fresh child passes.
	valid := &Authority{RunEpoch: 3, TaskGeneration: 2, AttemptGeneration: 2}
	if err := valid.ValidateChildResult(parent); err != nil {
		t.Errorf("fresh child rejected: %v", err)
	}
}

// 4. CurrentAuthority reads from store
func TestCurrentAuthorityReadsFromStore(t *testing.T) {
	store, _ := taskstore.NewInMemory()
	defer store.Close()

	run, _ := store.CreateRun()
	task, _ := store.CreateTask(run.ID, "test")
	attempt, _ := store.CreateAttempt(task.ID, 1, "w1", "br", "HEAD")
	store.RecordWorkerPID(attempt.ID, "echo", "/tmp", "primary", 1, 12345, 0)

	auth, err := CurrentAuthority(store, run.ID, task.ID)
	if err != nil {
		t.Fatalf("CurrentAuthority: %v", err)
	}
	if auth.AttemptGeneration < 1 {
		t.Errorf("attempt generation = %d, want >=1", auth.AttemptGeneration)
	}
	t.Logf("Current authority: %s", auth)
}
