// Package coordinator implements the decision layer for the OMNI
// orchestration system.
//
// A Coordinator reads run state and returns a bounded Decision. It
// never directly manipulates workers, git worktrees, or the filesystem.
//
// Two implementations are provided:
//   - CodexCoordinator: delegates to a codex CLI process (codex_coordinator.go)
//   - MockCoordinator: deterministic testing (this file)
//
// CoordinatorRuntime wraps a Runtime with generation-gated wake
// semantics, ACK tracking, timeout, and stale-generation rejection
// (runtime.go).
package coordinator

import (
	"context"
	"fmt"
)

// Decision is a bounded coordinator decision.
type Decision string

const (
	DecisionValidate   Decision = "VALIDATE"
	DecisionContinue   Decision = "CONTINUE"
	DecisionRetryClean Decision = "RETRY_CLEAN"
	DecisionReplace    Decision = "REPLACE"
	DecisionFail       Decision = "FAIL"
)

// Result is a coordinator decision with a machine-readable reason.
type Result struct {
	Decision Decision `json:"decision"`
	Reason   string   `json:"reason"`
}

// RunState is a read-only snapshot of the current run for the coordinator.
type RunState struct {
	RunID           int64  `json:"run_id"`
	TaskID          int64  `json:"task_id"`
	TaskTitle       string `json:"task_title"`
	TaskStatus      string `json:"task_status"`
	AttemptNumber   int    `json:"attempt_number"`
	AttemptStatus   string `json:"attempt_status"`
	CheckpointSHA   string `json:"checkpoint_sha"`
	BaseCommit      string `json:"base_commit"`
	Branch          string `json:"branch"`
	DiffSummary     string `json:"diff_summary"`
	ValidatorOutput string `json:"validator_output"`
}

// Coordinator reads run state and returns a decision.
type Coordinator interface {
	Decide(ctx context.Context, state RunState) (Result, error)
}

// MockCoordinator returns a fixed decision for deterministic testing.
type MockCoordinator struct {
	Decisions []Decision
	pos       int
}

// NewMockCoordinator returns a coordinator that cycles through the given decisions.
func NewMockCoordinator(decisions ...Decision) *MockCoordinator {
	return &MockCoordinator{Decisions: decisions}
}

// Decide returns the next decision in the sequence.
func (m *MockCoordinator) Decide(ctx context.Context, state RunState) (Result, error) {
	if m.pos >= len(m.Decisions) {
		return Result{Decision: DecisionFail, Reason: "mock: no more decisions"}, nil
	}
	d := m.Decisions[m.pos]
	m.pos++
	return Result{Decision: d, Reason: fmt.Sprintf("mock decision #%d: %s", m.pos, d)}, nil
}
