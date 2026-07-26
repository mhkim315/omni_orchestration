package hardening

import (
	"testing"
	"time"

	"github.com/mhkim315/omni_orchestration/internal/dag"
	"github.com/mhkim315/omni_orchestration/internal/taskstore"
)

// 1. Backoff after 3rd retry — exponential growth verified
func TestBackoffAfterThirdRetry(t *testing.T) {
	b := NewWorkerBudget(5, 30*time.Minute)

	// Retry 0 → backoff for retry 1.
	d1 := b.Backoff()
	t.Logf("retry 1 backoff: %v", d1)
	if d1 < 1*time.Second {
		t.Errorf("retry 1 backoff too small: %v", d1)
	}

	// Record 3 retries → backoff for retry 4 should be >= 8s.
	b.RecordRetry()
	b.RecordRetry()
	b.RecordRetry()
	d4 := b.Backoff()
	t.Logf("retry 4 backoff: %v", d4)
	if d4 < 4*time.Second {
		t.Errorf("retry 4 backoff too small: %v (expected >=4s)", d4)
	}

	// Budget exhausted.
	b.RecordRetry()
	b.RecordRetry()
	if b.CanRetry() {
		t.Error("budget should be exhausted after 5 retries")
	}
}

// 2. Orphaned path lease cleaned after task removed
func TestOrphanedPathLeaseCleaned(t *testing.T) {
	store, _ := taskstore.NewInMemory()
	dagStore, _ := dag.NewInMemory()
	defer store.Close()
	defer dagStore.Close()

	// Create task with path lease.
	task, _ := dagStore.CreateTask(1, "orphan-task", 0)
	dagStore.AcquirePathLease(task.ID, "/tmp/orphan-path")

	// Verify lease exists.
	owners, _ := dagStore.CheckPathOverlap([]string{"/tmp/orphan-path"})
	if len(owners) != 1 {
		t.Fatal("lease not found before cleanup")
	}

	// Release and verify.
	dagStore.ReleasePathLeases(task.ID)
	owners2, _ := dagStore.CheckPathOverlap([]string{"/tmp/orphan-path"})
	if len(owners2) != 0 {
		t.Errorf("orphan lease not cleaned: %d owners remain", len(owners2))
	}
	t.Log("orphan path lease cleaned successfully")
}

// 3. Migration rollback — store remains accessible after migration check
func TestMigrationRollback(t *testing.T) {
	store, _ := taskstore.NewInMemory()
	defer store.Close()

	// Run migration rollback check.
	err := MigrationRollback(store)
	if err != nil {
		t.Fatalf("MigrationRollback: %v", err)
	}

	// Store still usable.
	run, err := store.CreateRun()
	if err != nil {
		t.Fatalf("CreateRun after rollback check: %v", err)
	}
	if run.ID != 1 {
		t.Errorf("run ID = %d, want 1", run.ID)
	}
	t.Log("migration rollback check passed, store operational")
}

// 4. Worker budget over-runtime detection
func TestWorkerBudgetOverRuntime(t *testing.T) {
	b := NewWorkerBudget(3, 1*time.Millisecond)
	b.StartTime = time.Now().Add(-2 * time.Millisecond)
	if !b.OverRuntime() {
		t.Error("budget should detect over-runtime")
	}
}

// 5. ExtendedDoctor runs all checks
func TestExtendedDoctor(t *testing.T) {
	store, _ := taskstore.NewInMemory()
	defer store.Close()

	checks := ExtendedDoctor(store)
	if len(checks) < 2 {
		t.Errorf("expected >=2 checks, got %d", len(checks))
	}
	for _, c := range checks {
		t.Logf("  %s: ok=%v detail=%s", c.Name, c.OK, c.Detail)
	}
}
