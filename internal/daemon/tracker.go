// Package daemon — Tracker v1.8
//
// Limited parallelism: two independent executors with owned file paths.
// Each DAG leaf task dispatched to available worker. OwnedPathMap prevents
// conflicts. No shared files. Validator after each. Restart recovers both.
package daemon

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/mhkim315/omni_orchestration/internal/dag"
	"github.com/mhkim315/omni_orchestration/internal/orchestrator"
	"github.com/mhkim315/omni_orchestration/internal/taskstore"
	"github.com/mhkim315/omni_orchestration/internal/worktree"
)

const defaultMaxWorkers = 2

// TaskGroup orchestrates a set of related DAG tasks.
type TaskGroup struct {
	RunID int64
	Tasks []*dag.Task
}

// OwnedPathMap tracks which paths are owned by which worker.
type OwnedPathMap struct {
	mu    sync.Mutex
	paths map[string]int64 // path → taskID
}

// TryAcquire attempts to claim a set of paths. Returns true if all available.
func (o *OwnedPathMap) TryAcquire(taskID int64, paths []string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, p := range paths {
		if owner, ok := o.paths[p]; ok && owner != taskID {
			return false // conflict with another task
		}
	}
	for _, p := range paths {
		o.paths[p] = taskID
	}
	return true
}

// Release frees paths owned by a task.
func (o *OwnedPathMap) Release(taskID int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for p, owner := range o.paths {
		if owner == taskID {
			delete(o.paths, p)
		}
	}
}

// ── Tracker ──

type Tracker struct {
	store    *taskstore.Store
	dagStore *dag.Store
	wt       *worktree.Manager
	cfg      orchestrator.Config

	mu         sync.Mutex
	active     map[int64]context.CancelFunc
	maxWorkers int
	paths      *OwnedPathMap
	pollTick   time.Duration
	closed     bool
}

// NewTracker creates a task tracker with the given worker pool size.
func NewTracker(store *taskstore.Store, dagStore *dag.Store, wt *worktree.Manager, cfg orchestrator.Config) *Tracker {
	return NewTrackerWithWorkers(store, dagStore, wt, cfg, defaultMaxWorkers)
}

// NewTrackerWithWorkers creates a tracker with a configurable worker count.
func NewTrackerWithWorkers(store *taskstore.Store, dagStore *dag.Store, wt *worktree.Manager, cfg orchestrator.Config, maxWorkers int) *Tracker {
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	return &Tracker{
		store:      store,
		dagStore:   dagStore,
		wt:         wt,
		cfg:        cfg,
		active:     make(map[int64]context.CancelFunc),
		maxWorkers: maxWorkers,
		paths:      &OwnedPathMap{paths: make(map[string]int64)},
		pollTick:   2 * time.Second,
	}
}

func (t *Tracker) Start(ctx context.Context) {
	go t.loop(ctx)
	log.Printf("tracker: started (workers=%d, poll=%v)", t.maxWorkers, t.pollTick)
}

func (t *Tracker) loop(ctx context.Context) {
	ticker := time.NewTicker(t.pollTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Printf("tracker: stopped")
			return
		case <-ticker.C:
			t.poll(ctx)
		}
	}
}

// poll dispatches ready tasks to available workers.
func (t *Tracker) poll(ctx context.Context) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return
	}

	// Count active workers.
	activeCount := len(t.active)
	if activeCount >= t.maxWorkers {
		return // all workers busy
	}

	ready, err := t.dagStore.GetReadyTasks()
	if err != nil {
		log.Printf("tracker: poll error: %v", err)
		return
	}

	for _, task := range ready {
		if len(t.active) >= t.maxWorkers {
			break
		}
		if _, running := t.active[task.ID]; running {
			continue
		}

		// v3.0.1: Persistent path leases via DAG store (not in-memory).
		taskPaths := taskOwnedPaths(task)
		acquiredAll := true
		for _, p := range taskPaths {
			ok, err := t.dagStore.AcquirePathLease(task.ID, p)
			if err != nil || !ok {
				log.Printf("tracker: task %d path %s conflict — waiting", task.ID, p)
				acquiredAll = false
				break
			}
		}
		if !acquiredAll {
			continue
		}

		t.executeTask(ctx, task)
	}
}

// taskOwnedPaths returns the file paths a task will own.
// Leaf tasks (no dependents) own their worktree path.
func taskOwnedPaths(task *dag.Task) []string {
	return []string{fmt.Sprintf("task-%d", task.ID)}
}

func (t *Tracker) executeTask(ctx context.Context, task *dag.Task) {
	taskCtx, cancel := context.WithCancel(ctx)
	t.active[task.ID] = cancel

	log.Printf("tracker: worker executing task %d (%s)", task.ID, task.Title)

	cfg := t.cfg
	cfg.Task = task.Title
	// v3.0.3: Use real task fields (Command, Repo, OwnedPaths).
	cfg.Command = task.Command
	if cfg.Command == "" {
		cfg.Command = fmt.Sprintf("echo \"task %d executing\"", task.ID)
	}
	if task.Repo != "" {
		cfg.Repo = task.Repo
	}

	go func() {
		defer func() {
			t.mu.Lock()
			delete(t.active, task.ID)
			cancel()
			t.mu.Unlock()
			// v3.0.2: Release persistent path leases on completion.
			t.dagStore.ReleasePathLeases(task.ID)
		}()

		run, _ := t.store.CreateRun()
		taskRec, _ := t.store.CreateTask(run.ID, task.Title)
		attempt, err := t.store.CreateAttempt(taskRec.ID, 1, fmt.Sprintf("tracker-%d", task.ID), "HEAD", "HEAD")
		if err != nil {
			t.dagStore.UpdateTaskStatus(task.ID, dag.StatusFailed)
			return
		}
		_ = attempt
		t.dagStore.UpdateTaskStatus(task.ID, dag.StatusActive)

		decisions, err := orchestrator.Run(taskCtx, cfg, t.store, t.wt)
		if err != nil {
			log.Printf("tracker: task %d run error: %v", task.ID, err)
			t.dagStore.UpdateTaskStatus(task.ID, dag.StatusFailed)
			return
		}

		for _, d := range decisions {
			if d == orchestrator.DecisionComplete {
				t.dagStore.UpdateTaskStatus(task.ID, dag.StatusCompleted)
				n, _ := t.dagStore.UnblockDependents(task.ID)
				if n > 0 {
					log.Printf("tracker: task %d completed — unblocked %d dependents", task.ID, n)
				}
				return
			}
			if d == orchestrator.DecisionFail {
				t.dagStore.UpdateTaskStatus(task.ID, dag.StatusFailed)
				t.dagStore.FailDependents(task.ID)
				return
			}
		}
	}()
}

func (t *Tracker) ResumeActiveTasks() {
	t.mu.Lock()
	defer t.mu.Unlock()

	ready, _ := t.dagStore.GetReadyTasks()
	for _, task := range ready {
		log.Printf("tracker: found pending task %d (%s) — will execute", task.ID, task.Title)
	}

	blocked, _ := t.dagStore.GetBlockedTasks()
	for _, task := range blocked {
		if task.DependsOnTaskID > 0 {
			dep, err := t.dagStore.GetTask(task.DependsOnTaskID)
			if err == nil && dep.Status == dag.StatusCompleted {
				t.dagStore.UnblockDependents(task.DependsOnTaskID)
				log.Printf("tracker: unblocked task %d (dependency %d completed)", task.ID, task.DependsOnTaskID)
			}
		}
	}
}

func (t *Tracker) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	for id, cancel := range t.active {
		cancel()
		delete(t.active, id)
	}
	log.Printf("tracker: closed (%d active tasks cancelled)", len(t.active))
}

func (t *Tracker) ActiveCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.active)
}

var _ = time.Now
