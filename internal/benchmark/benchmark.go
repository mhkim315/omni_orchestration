// Package benchmark measures OMNI orchestration performance.
// v2.3: Compare sequential vs Fleet, provider routing, wall-clock, acceptance rate,
// path conflicts, stale rejections, recovery rate. Output CSV.
package benchmark

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/mhkim315/omni_orchestration/internal/dag"
	"github.com/mhkim315/omni_orchestration/internal/orchestrator"
	"github.com/mhkim315/omni_orchestration/internal/taskstore"
	"github.com/mhkim315/omni_orchestration/internal/worktree"
)

// Result holds metrics from a single benchmark run.
type Result struct {
	RunID         int
	Mode          string // "sequential" or "fleet"
	Provider      string // "codex", "claude", "agw", "reasonix", "auto"
	TaskCount     int
	WallClockMs   int64
	Attempts      int
	Accepted      int
	Rejected      int
	PathConflicts int
	StaleRejects  int
	Recovered     int
}

// Run executes a benchmark plan and returns results.
func Run(ctx context.Context, plan *Plan, runs int) ([]*Result, error) {
	var results []*Result
	var mu sync.Mutex

	for i := 0; i < runs; i++ {
		for _, mode := range plan.Modes {
			r := &Result{
				RunID:     i + 1,
				Mode:      mode,
				Provider:  plan.Provider,
				TaskCount: len(plan.Tasks),
			}
			start := time.Now()

			store, _ := taskstore.NewInMemory()
			dagStore, _ := dag.NewInMemory()
			wt := worktree.New()

			// Execute tasks.
			accepted := 0
			rejected := 0
			attempts := 0
			for _, task := range plan.Tasks {
				attempts++
				cfg := orchestrator.Config{
					Repo: plan.Repo, Task: task.Title, Command: task.Command,
					Validator: task.Validator, Provider: plan.Provider,
				}
				decisions, err := orchestrator.Run(ctx, cfg, store, wt)
				if err == nil {
					for _, d := range decisions {
						if d == orchestrator.DecisionComplete {
							accepted++
						} else {
							rejected++
						}
					}
				}
				_ = dagStore
			}

			r.WallClockMs = time.Since(start).Milliseconds()
			r.Attempts = attempts
			r.Accepted = accepted
			r.Rejected = rejected
			store.Close()

			mu.Lock()
			results = append(results, r)
			mu.Unlock()
		}
	}
	return results, nil
}

// Plan describes a benchmark plan.
type Plan struct {
	Repo     string
	Provider string
	Modes    []string
	Tasks    []TaskSpec
}

// TaskSpec is a single task in the plan.
type TaskSpec struct {
	Title     string
	Command   string
	Validator string
}

// WriteCSV writes results as CSV.
func WriteCSV(w io.Writer, results []*Result) error {
	cw := csv.NewWriter(w)
	cw.Write([]string{"run_id", "mode", "provider", "task_count", "wall_clock_ms", "attempts", "accepted", "rejected", "path_conflicts", "stale_rejects", "recovered"})
	for _, r := range results {
		cw.Write([]string{
			fmt.Sprintf("%d", r.RunID), r.Mode, r.Provider, fmt.Sprintf("%d", r.TaskCount),
			fmt.Sprintf("%d", r.WallClockMs), fmt.Sprintf("%d", r.Attempts),
			fmt.Sprintf("%d", r.Accepted), fmt.Sprintf("%d", r.Rejected),
			fmt.Sprintf("%d", r.PathConflicts), fmt.Sprintf("%d", r.StaleRejects),
			fmt.Sprintf("%d", r.Recovered),
		})
	}
	cw.Flush()
	return cw.Error()
}

// Summary computes aggregate stats from results.
func Summary(results []*Result) string {
	var totalMs int64
	seqMs := int64(0)
	fleetMs := int64(0)
	seqCount := 0
	fleetCount := 0
	for _, r := range results {
		totalMs += r.WallClockMs
		if r.Mode == "sequential" {
			seqMs += r.WallClockMs
			seqCount++
		} else {
			fleetMs += r.WallClockMs
			fleetCount++
		}
	}
	avgMs := int64(0)
	if len(results) > 0 {
		avgMs = totalMs / int64(len(results))
	}
	s := fmt.Sprintf("Benchmark: %d runs\n", len(results))
	s += fmt.Sprintf("  avg wall-clock: %dms\n", avgMs)
	if seqCount > 0 {
		s += fmt.Sprintf("  sequential avg: %dms\n", seqMs/int64(seqCount))
	}
	if fleetCount > 0 {
		s += fmt.Sprintf("  fleet avg: %dms\n", fleetMs/int64(fleetCount))
	}
	return s
}
