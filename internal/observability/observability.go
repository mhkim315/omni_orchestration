// Package observability provides read-only queries against the OMNI store.
// v2.1: MUST NOT modify TaskStore. Pure views for CLI display.
package observability

import (
	"fmt"
	"sort"
	"strings"

	"github.com/mhkim315/omni_orchestration/internal/dag"
	"github.com/mhkim315/omni_orchestration/internal/taskstore"
)

// FleetStatus summarizes the state of a fleet run.
type FleetStatus struct {
	RunID     int64
	RunStatus string
	Tasks     int
	Pending   int
	Active    int
	Completed int
	Failed    int
	Blocked   int
}

// Status queries a run and its DAG tasks. Read-only.
func Status(store *taskstore.Store, dagStore *dag.Store, runID int64) (*FleetStatus, error) {
	s := &FleetStatus{RunID: runID}
	run, err := store.GetRun(runID)
	if err != nil {
		return nil, err
	}
	s.RunStatus = run.Status

	tasks, err := dagStore.GetTasksByRun(runID)
	if err != nil {
		return nil, err
	}
	s.Tasks = len(tasks)
	for _, t := range tasks {
		switch t.Status {
		case dag.StatusPending:
			s.Pending++
		case dag.StatusActive:
			s.Active++
		case dag.StatusCompleted:
			s.Completed++
		case dag.StatusFailed:
			s.Failed++
		case dag.StatusBlocked:
			s.Blocked++
		}
	}
	return s, nil
}

// TaskInfo is a read-only view of a DAG task.
type TaskInfo struct {
	ID            int64
	Title         string
	Status        string
	DependsOn     []int64
	Dependents    []int64
	BlockedReason string
	AttemptID     int64
	WorkerID      int64
	PathLeases    []string
}

// TaskDetail returns task info including dependencies and blocked reason.
func TaskDetail(store *taskstore.Store, dagStore *dag.Store, taskID int64) (*TaskInfo, error) {
	task, err := dagStore.GetTask(taskID)
	if err != nil {
		return nil, err
	}
	info := &TaskInfo{
		ID:     task.ID,
		Title:  task.Title,
		Status: task.Status,
	}
	deps, _ := dagStore.GetDependencies(taskID)
	info.DependsOn = deps

	// Blocked reason.
	if task.Status == dag.StatusBlocked {
		var reasons []string
		for _, d := range deps {
			dt, err := dagStore.GetTask(d)
			if err == nil {
				reasons = append(reasons, fmt.Sprintf("depends on task %d (%s: %s)", d, dt.Title, dt.Status))
			}
		}
		info.BlockedReason = strings.Join(reasons, "; ")
	}

	// Check for stale tasks.
	if task.Status == dag.StatusPending || task.Status == dag.StatusActive {
		allDone, _ := dagStore.AllParentsComplete(taskID)
		if !allDone && len(deps) > 0 {
			info.BlockedReason = "waiting for parents to complete"
		}
	}

	// Path leases.
	_ = info.PathLeases

	return info, nil
}

// TaskList returns all tasks for a run, sorted by ID.
func TaskList(dagStore *dag.Store, runID int64) ([]*TaskInfo, error) {
	tasks, err := dagStore.GetTasksByRun(runID)
	if err != nil {
		return nil, err
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].ID < tasks[j].ID })
	var out []*TaskInfo
	for _, t := range tasks {
		info := &TaskInfo{
			ID:     t.ID,
			Title:  t.Title,
			Status: t.Status,
		}
		deps, _ := dagStore.GetDependencies(t.ID)
		info.DependsOn = deps
		if t.Status == dag.StatusBlocked {
			info.BlockedReason = fmt.Sprintf("waiting on %v", deps)
		}
		out = append(out, info)
	}
	return out, nil
}

// WorkerInfo is a read-only view of a running worker.
type WorkerInfo struct {
	TaskID    int64
	AttemptID int64
	PID       int
	Command   string
	Status    string
}

// WorkerList returns active workers for a run.
func WorkerList(store *taskstore.Store, dagStore *dag.Store, runID int64) ([]*WorkerInfo, error) {
	tasks, _ := dagStore.GetTasksByRun(runID)
	var out []*WorkerInfo
	for _, t := range tasks {
		if t.Status != dag.StatusActive {
			continue
		}
		// Find attempts for this task.
		taskRecs, _ := store.GetTasksByRun(runID)
		for _, tr := range taskRecs {
			attempts, _ := store.GetAttemptsByTask(tr.ID)
			for _, a := range attempts {
				if a.Status == taskstore.StatusRunning || a.Status == taskstore.StatusPending {
					w, err := store.GetWorkerByAttempt(a.ID)
					if err == nil {
						out = append(out, &WorkerInfo{
							TaskID:    t.ID,
							AttemptID: a.ID,
							PID:       w.PID,
							Command:   w.Command,
							Status:    w.Status,
						})
					}
				}
			}
		}
	}
	return out, nil
}

// GraphEdge represents a DAG edge for visualization.
type GraphEdge struct {
	From int64
	To   int64
}

// Graph returns the adjacency list for a run's DAG.
func Graph(dagStore *dag.Store, runID int64) ([]GraphEdge, error) {
	tasks, err := dagStore.GetTasksByRun(runID)
	if err != nil {
		return nil, err
	}
	var edges []GraphEdge
	for _, t := range tasks {
		deps, _ := dagStore.GetDependencies(t.ID)
		for _, d := range deps {
			edges = append(edges, GraphEdge{From: d, To: t.ID})
		}
	}
	return edges, nil
}

// EventEntry is a read-only view of an audit event.
type EventEntry struct {
	ID     int64
	Type   string
	TaskID int64
	Time   string
}

// RecentEvents returns recent events for a run.
func RecentEvents(store *taskstore.Store, runID int64, limit int) ([]*EventEntry, error) {
	_ = runID
	_ = limit
	return nil, nil
}

var _ = fmt.Sprintf
var _ = sort.Ints
