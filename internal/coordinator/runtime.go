package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miinanii/omni_orchestration/internal/runtime"
)

// ErrMalformedResponse is returned when the coordinator produces invalid JSON.
var ErrMalformedResponse = errors.New("coordinator: malformed response")

// ErrCoordinatorTimeout is returned when the coordinator does not respond in time.
var ErrCoordinatorTimeout = errors.New("coordinator: timeout")

// ErrStaleCoordinator is returned when the coordinator generation does not match.
var ErrStaleCoordinator = errors.New("coordinator: stale generation")

// WakePacket is the input delivered to the coordinator on each wake.
type WakePacket struct {
	RunID            int64    `json:"run_id"`
	TaskID           int64    `json:"task_id"`
	TaskTitle        string   `json:"task"`
	AttemptNumber    int      `json:"attempt"`
	AttemptStatus    string   `json:"attempt_status"`
	WorkerState      string   `json:"worker_state"`
	ExitCode         int      `json:"exit_code"`
	CheckpointSHA    string   `json:"checkpoint"`
	ValidatorOutput  string   `json:"validator"`
	DiffSummary      string   `json:"diff_summary"`
	AllowedDecisions []string `json:"allowed_decisions"`
}

// WakeResponse is the structured decision returned by the coordinator.
type WakeResponse struct {
	Decision        Decision `json:"decision"`
	Reason          string   `json:"reason"`
	NextInstruction string   `json:"next_instruction"`
}

// CoordinatorRuntime wraps a Runtime with generation-gated wake semantics.
// Each wake is an input→output cycle with ACK tracking and timeout.
type CoordinatorRuntime struct {
	rt      *runtime.Runtime
	mu      sync.Mutex
	gen     atomic.Int64 // C4: atomic for generation gating
	id      string
	ackSeq  int64 // monotonic ACK sequence
	timeout time.Duration

	// coordinator is the decision engine called on each wake.
	coordinator Coordinator
}

// NewCoordinatorRuntime wraps a Runtime as a generation-gated coordinator.
func NewCoordinatorRuntime(rt *runtime.Runtime, coord Coordinator) *CoordinatorRuntime {
	return &CoordinatorRuntime{
		rt:          rt,
		id:          rt.ID(),
		coordinator: coord,
		timeout:     120 * time.Second,
	}
}

// ID returns the coordinator identity.
func (c *CoordinatorRuntime) ID() string { return c.id }

// Generation returns the current coordinator generation.
func (c *CoordinatorRuntime) Generation() int64 {
	return c.gen.Load()
}

// SetTimeout overrides the default 120s wake timeout.
func (c *CoordinatorRuntime) SetTimeout(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timeout = d
}

// Wake sends a packet to the coordinator and waits for a validated decision.
// The generation must match; stale callers receive ErrStaleCoordinator.
// Malformed responses return ErrMalformedResponse. A coordinator that does
// not respond within the timeout returns ErrCoordinatorTimeout.
func (c *CoordinatorRuntime) Wake(ctx context.Context, generation int64, pkt WakePacket) (WakeResponse, error) {
	if generation != c.gen.Load() {
		return WakeResponse{}, ErrStaleCoordinator
	}

	// Build the coordinator state from the packet.
	state := RunState{
		RunID:           pkt.RunID,
		TaskID:          pkt.TaskID,
		TaskTitle:       pkt.TaskTitle,
		TaskStatus:      pkt.AttemptStatus,
		AttemptNumber:   pkt.AttemptNumber,
		AttemptStatus:   pkt.AttemptStatus,
		CheckpointSHA:   pkt.CheckpointSHA,
		ValidatorOutput: pkt.ValidatorOutput,
		DiffSummary:     pkt.DiffSummary,
	}

	// Input ACK: assign a monotonic sequence number.
	seq := atomic.AddInt64(&c.ackSeq, 1)
	snapshotGen := generation // capture at call time
	_ = seq

	// Call the coordinator with timeout.
	timeoutCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	result, err := c.coordinator.Decide(timeoutCtx, state)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return WakeResponse{}, ErrCoordinatorTimeout
		}
		return WakeResponse{}, fmt.Errorf("coordinator decide: %w", err)
	}

	// C4: reject if generation changed during Decide (old-gen response).
	if c.gen.Load() != snapshotGen {
		return WakeResponse{}, ErrStaleCoordinator
	}

	// Validate the decision against allowed decisions.
	if len(pkt.AllowedDecisions) > 0 {
		if !isAllowed(result.Decision, pkt.AllowedDecisions) {
			return WakeResponse{}, fmt.Errorf("%w: decision %q not in allowed set", ErrMalformedResponse, result.Decision)
		}
	}

	instruction := result.NextInstruction
	if instruction == "" {
		instruction = buildInstruction(result)
	}
	return WakeResponse{
		Decision:        result.Decision,
		Reason:          result.Reason,
		NextInstruction: instruction,
	}, nil
}

// Replace retires the current coordinator runtime and creates a replacement
// with a new generation. The old runtime is stopped. Callers must use the
// new generation for subsequent Wake calls.
func (c *CoordinatorRuntime) Replace(ctx context.Context, newRT *runtime.Runtime) *CoordinatorRuntime {
	c.mu.Lock()
	oldGen := c.gen.Load()
	c.rt.Stop(ctx)
	c.rt = newRT
	c.id = newRT.ID()
	c.gen.Store(oldGen + 1)
	newGen := c.gen.Load()

	_ = newGen
	return c
}

// Close stops the underlying runtime.
func (c *CoordinatorRuntime) Close(ctx context.Context) error {
	return c.rt.Stop(ctx)
}

// isAllowed checks whether a decision is in the allowed set.
func isAllowed(d Decision, allowed []string) bool {
	for _, a := range allowed {
		if string(d) == a {
			return true
		}
	}
	return false
}

// buildInstruction creates a next_instruction string from the decision.
func buildInstruction(result Result) string {
	switch result.Decision {
	case DecisionValidate:
		return "Run the validator against the current checkpoint."
	case DecisionContinue:
		return "Continue executing. No changes needed."
	case DecisionComplete:
		return "Task completed successfully. Stop the run."
	case DecisionRetryClean:
		return "Discard current work. Start a fresh attempt with the same task."
	case DecisionReplace:
		return "Task specification needs human intervention."
	case DecisionFail:
		return "Unrecoverable error. Stop the run."
	default:
		return "Unknown decision."
	}
}

// MarshalPacket serializes a WakePacket to JSON bytes for delivery
// to a provider process via stdin.
func MarshalPacket(pkt WakePacket) ([]byte, error) {
	return json.Marshal(pkt)
}

// UnmarshalResponse parses a WakeResponse from JSON bytes.
// Returns ErrMalformedResponse if the JSON is invalid or the decision
// field is missing/unknown.
func UnmarshalResponse(data []byte) (WakeResponse, error) {
	var resp WakeResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return WakeResponse{}, fmt.Errorf("%w: %v", ErrMalformedResponse, err)
	}
	if resp.Decision == "" {
		return WakeResponse{}, fmt.Errorf("%w: missing decision field", ErrMalformedResponse)
	}
	if normalizeDecision(string(resp.Decision)) == DecisionFail && resp.Decision != DecisionFail {
		return WakeResponse{}, fmt.Errorf("%w: unknown decision %q", ErrMalformedResponse, resp.Decision)
	}
	resp.Decision = normalizeDecision(string(resp.Decision))
	return resp, nil
}
