// Package daemon — Tracker v1.7
//
// Daemon-powered unattended run tracker. Polls DAG tasks, creates
// attempts for unblocked tasks, monitors completion via mailbox,
// and resumes active work on restart.
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

// Tracker polls the DAG for ready tasks and executes them.
type Tracker struct {
	store    *taskstore.Store
	dagStore *dag.Store
	wt       *worktree.Manager
	cfg      orchestrator.Config // template for new runs

	mu       sync.Mutex
	active   map[int64]context.CancelFunc // taskID → cancel
	pollTick time.Duration
	closed   bool
}

// NewTracker creates a task tracker.
func NewTracker(store *taskstore.Store, dagStore *dag.Store, wt *worktree.Manager, cfg orchestrator.Config) *Tracker {
	return &Tracker{
		store:    store,
		dagStore: dagStore,
		wt:       wt,
		cfg:      cfg,
		active:   make(map[int64]context.CancelFunc),
		pollTick: 2 * time.Second,
	}
}

// Start begins the tracker loop. Returns a cancel function.
func (t *Tracker) Start(ctx context.Context) {
	go t.loop(ctx)
	log.Printf("tracker: started (poll=%v)", t.pollTick)
}

// loop polls for ready tasks and executes them.
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

// poll checks for ready DAG tasks and starts execution.
func (t *Tracker) poll(ctx context.Context) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return
	}

	ready, err := t.dagStore.GetReadyTasks()
	if err != nil {
		log.Printf("tracker: poll error: %v", err)
		return
	}

	for _, task := range ready {
		if _, running := t.active[task.ID]; running {
			continue // already being tracked
		}
		t.executeTask(ctx, task)
	}
}

// executeTask marks a task active and runs it in a goroutine.
func (t *Tracker) executeTask(ctx context.Context, task *dag.Task) {
	taskCtx, cancel := context.WithCancel(ctx)
	t.active[task.ID] = cancel

	log.Printf("tracker: executing task %d (%s)", task.ID, task.Title)

	// Create orchestrator run.
	cfg := t.cfg
	cfg.Task = task.Title
	cfg.Command = fmt.Sprintf("echo 'task %d executing'", task.ID)

	go func() {
		defer func() {
			t.mu.Lock()
			delete(t.active, task.ID)
			cancel()
			t.mu.Unlock()
		}()

		// Create attempt in taskstore.
		run, err := t.store.CreateRun()
		if err != nil {
			log.Printf("tracker: task %d create run: %v", task.ID, err)
			t.dagStore.UpdateTaskStatus(task.ID, dag.StatusFailed)
			return
		}
		taskRec, err := t.store.CreateTask(run.ID, task.Title)
		if err != nil {
			t.dagStore.UpdateTaskStatus(task.ID, dag.StatusFailed)
			return
		}
		attempt, err := t.store.CreateAttempt(taskRec.ID, 1, fmt.Sprintf("tracker-%d", task.ID), "HEAD", "HEAD")
		if err != nil {
			t.dagStore.UpdateTaskStatus(task.ID, dag.StatusFailed)
			return
		}
		_ = attempt

		// Update DAG status to active.
		t.dagStore.UpdateTaskStatus(task.ID, dag.StatusActive)

		// Execute orchestrator run.
		decisions, err := orchestrator.Run(taskCtx, cfg, t.store, t.wt)
		if err != nil {
			log.Printf("tracker: task %d run error: %v", task.ID, err)
			t.dagStore.UpdateTaskStatus(task.ID, dag.StatusFailed)
			return
		}

		// On completion, mark DAG task completed and unblock dependents.
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

		log.Printf("tracker: task %d decisions: %v", task.ID, decisions)
	}()
}

// ResumeActiveTasks finds all active DAG tasks and resumes tracking.
func (t *Tracker) ResumeActiveTasks() {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Find DAG tasks that are active (were running when daemon stopped).
	allTasks, err := t.dagStore.GetReadyTasks() // pending → need execution
	if err != nil {
		log.Printf("tracker: resume error: %v", err)
		return
	}
	for _, task := range allTasks {
		log.Printf("tracker: found pending task %d (%s) — will execute", task.ID, task.Title)
	}

	// Also check for blocked tasks — unblock if their dependency completed.
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

// Close stops the tracker gracefully.
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

// ActiveCount returns how many tasks are currently being tracked.
func (t *Tracker) ActiveCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.active)
}

var _ = time.Now
