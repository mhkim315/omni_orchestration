package dag

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// 1. Single task — no dependency, starts pending
func TestSingleTask(t *testing.T) {
	s, _ := NewInMemory()
	defer s.Close()

	task, err := s.CreateTask(1, "lone task", 0)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if task.Status != StatusPending {
		t.Errorf("status = %s, want pending", task.Status)
	}
	if task.DependsOnTaskID != 0 {
		t.Error("single task should have no dependency")
	}
}

// 2. Chain — A→B→C, each unlocks next
func TestChainUnlock(t *testing.T) {
	s, _ := NewInMemory()
	defer s.Close()

	// Create chain: task 1 (pending) → task 2 (blocked) → task 3 (blocked)
	t1, _ := s.CreateTask(1, "task A", 0)
	t2, _ := s.CreateTask(1, "task B", t1.ID)
	t3, _ := s.CreateTask(1, "task C", t2.ID)

	if t1.Status != StatusPending {
		t.Errorf("t1 status = %s, want pending", t1.Status)
	}
	if t2.Status != StatusBlocked {
		t.Errorf("t2 status = %s, want blocked", t2.Status)
	}
	if t3.Status != StatusBlocked {
		t.Errorf("t3 status = %s, want blocked", t3.Status)
	}

	// Complete t1 → unblocks t2.
	n, err := s.UnblockDependents(t1.ID)
	if err != nil {
		t.Fatalf("UnblockDependents t1: %v", err)
	}
	if n != 1 {
		t.Errorf("unblocked %d tasks, want 1", n)
	}
	t2after, _ := s.GetTask(t2.ID)
	if t2after.Status != StatusPending {
		t.Errorf("t2 status after unblock = %s, want pending", t2after.Status)
	}

	// Complete t2 → unblocks t3.
	s.UnblockDependents(t2.ID)
	t3after, _ := s.GetTask(t3.ID)
	if t3after.Status != StatusPending {
		t.Errorf("t3 status after unblock = %s, want pending", t3after.Status)
	}

	// t1 still pending (unchanged).
	t1after, _ := s.GetTask(t1.ID)
	if t1after.Status != StatusPending {
		t.Errorf("t1 status after = %s, want pending (unchanged)", t1after.Status)
	}
}

// 3. Fail-blocked — if parent fails, child stays blocked
func TestFailBlocked(t *testing.T) {
	s, _ := NewInMemory()
	defer s.Close()

	t1, _ := s.CreateTask(1, "parent", 0)
	t2, _ := s.CreateTask(1, "child", t1.ID)

	s.UpdateTaskStatus(t1.ID, StatusFailed)

	// Child stays blocked.
	t2after, _ := s.GetTask(t2.ID)
	if t2after.Status != StatusBlocked {
		t.Errorf("child status = %s, want blocked after parent failed", t2after.Status)
	}

	// FailDependents marks blocked children as failed.
	n, err := s.FailDependents(t1.ID)
	if err != nil {
		t.Fatalf("FailDependents: %v", err)
	}
	if n != 1 {
		t.Errorf("failed %d dependents, want 1", n)
	}
	t2final, _ := s.GetTask(t2.ID)
	if t2final.Status != StatusFailed {
		t.Errorf("child final status = %s, want failed", t2final.Status)
	}
}

// 4. Circular error — detect and reject
func TestCircularError(t *testing.T) {
	s, _ := NewInMemory()
	defer s.Close()

	// Create chain: 1 → 2 → 3
	t1, _ := s.CreateTask(1, "task 1", 0)
	t2, _ := s.CreateTask(1, "task 2", t1.ID)
	t3, _ := s.CreateTask(1, "task 3", t2.ID)

	// Try to make t1 depend on t3 (creates cycle).
	err := s.DetectCircular(t1.ID, t3.ID)
	if err == nil {
		t.Error("circular dependency not detected: 1 → 3 would create 1→2→3→1")
	}
	t.Logf("circular error (expected): %v", err)

	// Self-dependency.
	err2 := s.DetectCircular(t1.ID, t1.ID)
	if err2 == nil {
		t.Error("self-dependency not detected")
	}
}

// 5. Restart resume — DAG state survives kill/restart
func TestRestartResume(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dag-restart.db")

	// Create store, add chain.
	s1, _ := New(path)
	t1, _ := s1.CreateTask(1, "task A", 0)
	t2, _ := s1.CreateTask(1, "task B", t1.ID)
	s1.Close()

	// Simulate restart.
	s2, _ := New(path)
	defer s2.Close()

	// Tasks survive.
	tt1, err := s2.GetTask(t1.ID)
	if err != nil {
		t.Fatalf("t1 not found after restart: %v", err)
	}
	if tt1.Status != StatusPending {
		t.Errorf("t1 status = %s, want pending", tt1.Status)
	}

	tt2, err := s2.GetTask(t2.ID)
	if err != nil {
		t.Fatalf("t2 not found after restart: %v", err)
	}
	if tt2.Status != StatusBlocked {
		t.Errorf("t2 status = %s, want blocked", tt2.Status)
	}

	// Chain relationship survived.
	if tt2.DependsOnTaskID != t1.ID {
		t.Errorf("t2 depends_on = %d, want %d", tt2.DependsOnTaskID, t1.ID)
	}

	// Unblock still works after restart.
	s2.UnblockDependents(t1.ID)
	tt2after, _ := s2.GetTask(t2.ID)
	if tt2after.Status != StatusPending {
		t.Errorf("t2 after restart unblock = %s, want pending", tt2after.Status)
	}
}

var _ = fmt.Sprintf
var _ = os.TempDir

// 6. Multi-repo: per-task repo override, cross-repo dependency, partial failure
func TestMultiRepoCrossRepoPartialFailure(t *testing.T) {
	s, _ := NewInMemory()
	defer s.Close()

	// Task A in repo-1.
	a, _ := s.CreateTaskWithRepo(1, "repo-1 task", 0, "/repos/repo-1")
	// Task B in repo-2, depends on A (cross-repo).
	b, _ := s.CreateTaskWithRepo(1, "repo-2 task", a.ID, "/repos/repo-2")
	// Task C in repo-1, no dependency.
	c, _ := s.CreateTaskWithRepo(1, "repo-1 task 2", 0, "/repos/repo-1")

	if a.Repo != "/repos/repo-1" {
		t.Errorf("A repo = %s, want /repos/repo-1", a.Repo)
	}
	if b.Repo != "/repos/repo-2" {
		t.Errorf("B repo = %s, want /repos/repo-2", b.Repo)
	}
	if b.DependsOnTaskID != a.ID {
		t.Errorf("B depends_on = %d, want %d", b.DependsOnTaskID, a.ID)
	}

	// Partial failure: fail A → B stays blocked.
	s.UpdateTaskStatus(a.ID, StatusFailed)
	bAfter, _ := s.GetTask(b.ID)
	if bAfter.Status != StatusBlocked {
		t.Errorf("B status after A failed = %s, want blocked (cross-repo partial failure)", bAfter.Status)
	}

	// C (independent, same repo as A) completes fine.
	s.UpdateTaskStatus(c.ID, StatusCompleted)
	cAfter, _ := s.GetTask(c.ID)
	if cAfter.Status != StatusCompleted {
		t.Errorf("C status = %s, want completed (independent task)", cAfter.Status)
	}

	t.Logf("multi-repo: A=%s B=%s C=%s (partial failure: A failed blocks B, C independent)",
		a.Repo, b.Repo, c.Repo)
}

// 7. Two tasks same shared/file.go → serialized (path conflict)
func TestTwoTasksSameSharedFileSerialized(t *testing.T) {
	s, _ := NewInMemory()
	defer s.Close()

	// Both tasks claim ownership of shared/file.go
	a, _ := s.CreateTaskFull(1, "task-A", 0, "", "echo A", "true", "shared/file.go")
	b, _ := s.CreateTaskFull(1, "task-B", 0, "", "echo B", "true", "shared/file.go")

	// Acquire path lease for A.
	okA, _ := s.AcquirePathLease(a.ID, "shared/file.go")
	if !okA {
		t.Fatal("A could not acquire shared/file.go")
	}

	// B tries to acquire same path — should fail (already owned).
	okB, _ := s.AcquirePathLease(b.ID, "shared/file.go")
	if okB {
		t.Error("B acquired shared/file.go — should have been blocked by A's lease")
	}
	t.Logf("path conflict serialized: A=%d owns shared/file.go, B=%d blocked", a.ID, b.ID)

	// After A releases, B can acquire.
	s.ReleasePathLeases(a.ID)
	okB2, _ := s.AcquirePathLease(b.ID, "shared/file.go")
	if !okB2 {
		t.Error("B could not acquire after A released")
	}
	t.Log("serialized: A released → B acquired")
}

// 8. 3 tasks multi-parent → join after BOTH done
func TestMultiParentJoin(t *testing.T) {
	s, _ := NewInMemory()
	defer s.Close()

	// Fan-in: B → D, C → D. D requires both B and C.
	b, _ := s.CreateTask(1, "B", 0)
	c, _ := s.CreateTask(1, "C", 0)
	d, _ := s.CreateTask(1, "D", 0)
	s.AddDependency(d.ID, b.ID)
	s.AddDependency(d.ID, c.ID)

	// Complete B only → D still blocked.
	s.UpdateTaskStatus(b.ID, StatusCompleted)
	s.UpdateTaskStatus(d.ID, StatusBlocked)
	unblocked, _ := s.UnblockIfReady(d.ID)
	if unblocked {
		t.Error("D unblocked before C completed")
	}

	// Complete C → D unblocks (both parents done).
	s.UpdateTaskStatus(c.ID, StatusCompleted)
	unblocked2, _ := s.UnblockIfReady(d.ID)
	if !unblocked2 {
		t.Errorf("D not unblocked after both parents completed. D status check")
	}
	dFinal, _ := s.GetTask(d.ID)
	t.Logf("fan-in: B + C → D. D final=%s (want pending)", dFinal.Status)
	if dFinal.Status != StatusPending {
		t.Errorf("D final = %s, want pending", dFinal.Status)
	}
}

// 9. Tracker real command proof — CreateTaskFull stores command + validator
func TestCreateTaskFullStoresAllFields(t *testing.T) {
	s, _ := NewInMemory()
	defer s.Close()

	task, err := s.CreateTaskFull(1, "real-task", 0, "/repo/x", "claude -p 'fix bug'", "grep -q PASS", "src/bug.go,src/fix.go")
	if err != nil {
		t.Fatalf("CreateTaskFull: %v", err)
	}
	if task.Command != "claude -p 'fix bug'" {
		t.Errorf("command = %s", task.Command)
	}
	if task.Validator != "grep -q PASS" {
		t.Errorf("validator = %s", task.Validator)
	}
	if task.OwnedPaths != "src/bug.go,src/fix.go" {
		t.Errorf("owned_paths = %s", task.OwnedPaths)
	}
	if task.Repo != "/repo/x" {
		t.Errorf("repo = %s", task.Repo)
	}
	t.Logf("full task: cmd=%q val=%q paths=%q repo=%q", task.Command, task.Validator, task.OwnedPaths, task.Repo)
}
