package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
)

// AGYCoordinator delegates decisions to an AGY CLI process.
// It implements Coordinator by calling `agy --print` with a
// structured prompt and parsing the JSON response.
//
// Same contract as CodexCoordinator and ClaudeCoordinator.
type AGYCoordinator struct {
	Bin            string // path to agy CLI (default: "agy")
	Model          string // model override (optional)
	Effort         string // effort level: low|medium|high (optional)
	requestedModel string // set by ApplyProfile
}

// NewAGYCoordinator returns an AGY-backed coordinator.
func NewAGYCoordinator() *AGYCoordinator {
	return &AGYCoordinator{Bin: "agy"}
}

// Decide calls agy --print with the run state as a prompt and
// parses the structured decision from the response.
func (c *AGYCoordinator) Decide(ctx context.Context, state RunState) (Result, error) {
	bin := c.Bin
	if bin == "" {
		bin = "agy"
	}

	prompt := buildAGYPrompt(state)

	args := []string{"--print", "--print-timeout", "120s", "--dangerously-skip-permissions"}
	if c.Model != "" {
		args = append(args, "--model", c.Model)
	}
	if c.Effort != "" {
		args = append(args, "--effort", c.Effort)
	}
	args = append(args, prompt)

	cmd := exec.CommandContext(ctx, bin, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return Result{Decision: DecisionFail, Reason: fmt.Sprintf("agy exec failed: %v", err)}, nil
	}

	result, err := parseAGYDecision(string(out))
	if err != nil {
		return Result{Decision: DecisionFail, Reason: err.Error()}, err
	}
	return result, nil
}

// buildAGYPrompt constructs a structured prompt from run state.
func buildAGYPrompt(state RunState) string {
	return fmt.Sprintf(`You are an orchestrator coordinator. Decide the next action.

Task: %s (ID: %d)
Attempt: #%d
Status: %s
Checkpoint: %s
Base commit: %s

Validator output: %s
Diff summary: %s

Return EXACTLY one decision as JSON with no other text:
{"decision": "<DECISION>", "reason": "<one-line explanation>", "next_instruction": "<instruction for worker>"}

The next_instruction field is REQUIRED — it will be delivered to the worker process stdin.
Valid decisions:
- VALIDATE: run the validator against the current checkpoint
- CONTINUE: keep the worker running, no action needed
- COMPLETE: validator passed, task is done
- RETRY_CLEAN: discard current work, start a fresh attempt with same task
- REPLACE: task specification is wrong, needs human intervention
- FAIL: unrecoverable error, stop the run

Decision:`,
		state.TaskTitle, state.TaskID,
		state.AttemptNumber, state.AttemptStatus,
		state.CheckpointSHA, state.BaseCommit,
		state.ValidatorOutput, state.DiffSummary,
	)
}

// parseAGYDecision extracts the JSON decision from AGY output.
func parseAGYDecision(output string) (Result, error) {
	decoded := extractJSON(output)
	if decoded == nil {
		return Result{
			Decision: DecisionFail,
			Reason:   "agy response did not contain a valid decision JSON",
		}, nil
	}

	var parsed struct {
		Decision        string `json:"decision"`
		Reason          string `json:"reason"`
		NextInstruction string `json:"next_instruction"`
	}
	if err := json.Unmarshal(decoded, &parsed); err != nil {
		return Result{Decision: DecisionFail, Reason: fmt.Sprintf("parse decision: %v", err)}, nil
	}

	decision := normalizeDecision(parsed.Decision)
	return Result{Decision: decision, Reason: parsed.Reason, NextInstruction: parsed.NextInstruction}, nil
}
