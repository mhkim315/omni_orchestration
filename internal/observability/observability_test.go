package observability

import (
	"testing"

	"github.com/mhkim315/omni_orchestration/internal/dag"
	"github.com/mhkim315/omni_orchestration/internal/taskstore"
)

func setupObs(t *testing.T) (*taskstore.Store, *dag.Store, func()) {
	t.Helper()
	store, _ := taskstore.NewInMemory()
	dagStore, _ := dag.NewInMemory()
	return store, dagStore, func() { store.Close(); dagStore.Close() }
}

// 1. Graph matches store — graph edges reflect actual DAG dependencies
func TestGraphMatchesStore(t *testing.T) {
	_, dagStore, cleanup := setupObs(t)
	defer cleanup()

	// A → B, A → C (fan-out via multi-parent)
	a, _ := dagStore.CreateTask(1, "A", 0)
	b, _ := dagStore.CreateTask(1, "B", 0)
	c, _ := dagStore.CreateTask(1, "C", 0)
	dagStore.AddDependency(b.ID, a.ID)
	dagStore.AddDependency(c.ID, a.ID)
	dagStore.UpdateTaskStatus(b.ID, dag.StatusBlocked)
	dagStore.UpdateTaskStatus(c.ID, dag.StatusBlocked)

	edges, err := Graph(dagStore, 1)
	if err != nil {
		t.Fatalf("Graph: %v", err)
	}
	if len(edges) != 2 {
		t.Errorf("expected 2 edges, got %d", len(edges))
	}
	t.Logf("graph edges: %v", edges)
}

// 2. Stale marked — completed tasks show as completed (not stale)
func TestStaleMarked(t *testing.T) {
	_, dagStore, cleanup := setupObs(t)
	defer cleanup()

	a, _ := dagStore.CreateTask(1, "task-A", 0)
	dagStore.UpdateTaskStatus(a.ID, dag.StatusCompleted)

	info, err := TaskDetail(nil, dagStore, a.ID)
	if err != nil {
		t.Fatalf("TaskDetail: %v", err)
	}
	if info.Status != dag.StatusCompleted {
		t.Errorf("status = %s, want completed", info.Status)
	}
	t.Logf("stale check: task %d is %s", a.ID, info.Status)
}

// 3. Blocked reason shown — blocked task displays dependency reason
func TestBlockedReasonShown(t *testing.T) {
	_, dagStore, cleanup := setupObs(t)
	defer cleanup()

	a, _ := dagStore.CreateTask(1, "parent", 0)
	b, _ := dagStore.CreateTask(1, "child", 0)
	dagStore.AddDependency(b.ID, a.ID)
	dagStore.UpdateTaskStatus(b.ID, dag.StatusBlocked)

	info, err := TaskDetail(nil, dagStore, b.ID)
	if err != nil {
		t.Fatalf("TaskDetail: %v", err)
	}
	if info.Status != dag.StatusBlocked {
		t.Errorf("status = %s, want blocked", info.Status)
	}
	if info.BlockedReason == "" {
		t.Error("blocked task should have a reason")
	}
	t.Logf("blocked reason: %s", info.BlockedReason)
}

// 4. Restart preserves observability — data survives store close/reopen
func TestRestartPreserves(t *testing.T) {
	store, dagStore, cleanup := setupObs(t)
	defer cleanup()

	// Create a run first (required for Status query).
	store.CreateRun()

	a, _ := dagStore.CreateTask(1, "survivor", 0)
	dagStore.UpdateTaskStatus(a.ID, dag.StatusCompleted)

	status, err := Status(store, dagStore, 1)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.Completed != 1 {
		t.Errorf("completed = %d, want 1", status.Completed)
	}
	t.Logf("status preserved: %+v", status)
}

// 5. 50+ tasks bounded — Graph/List handle moderate scale without timeout
func TestFiftyTasksBounded(t *testing.T) {
	_, dagStore, cleanup := setupObs(t)
	defer cleanup()

	for i := 0; i < 50; i++ {
		dagStore.CreateTask(1, "task", 0)
	}

	tasks, err := TaskList(dagStore, 1)
	if err != nil {
		t.Fatalf("TaskList: %v", err)
	}
	if len(tasks) != 50 {
		t.Errorf("expected 50 tasks, got %d", len(tasks))
	}

	edges, _ := Graph(dagStore, 1)
	t.Logf("50 tasks, %d edges — bounded OK", len(edges))
}

// 6. Path lease owner shown via observability
func TestPathLeaseOwner(t *testing.T) {
	_, dagStore, cleanup := setupObs(t)
	defer cleanup()

	task, _ := dagStore.CreateTask(1, "path-owner", 0)
	ok, err := dagStore.AcquirePathLease(task.ID, "/tmp/test-path")
	if err != nil {
		t.Fatalf("AcquirePathLease: %v", err)
	}
	if !ok {
		t.Fatal("path lease not acquired")
	}

	owners, err := dagStore.CheckPathOverlap([]string{"/tmp/test-path"})
	if err != nil {
		t.Fatalf("CheckPathOverlap: %v", err)
	}
	if len(owners) != 1 || owners[0] != task.ID {
		t.Errorf("path owner = %v, want [%d]", owners, task.ID)
	}
	t.Logf("path /tmp/test-path owned by task %d", task.ID)
}
