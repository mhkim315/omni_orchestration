package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
)

// ClaudeCoordinator delegates decisions to a Claude Code CLI process.
// It implements Coordinator by calling `claude -p --output-format json`
// with a structured prompt and parsing the JSON response.
//
// The contract is identical to CodexCoordinator — same prompt format,
// same JSON response schema, same decision vocabulary.
type ClaudeCoordinator struct {
	Bin            string // path to claude CLI (default: "claude")
	Model          string // model override (optional)
	Effort         string // effort level: low|medium|high (optional)
	requestedModel string // set by ApplyProfile
}

// NewClaudeCoordinator returns a Claude-backed coordinator.
func NewClaudeCoordinator() *ClaudeCoordinator {
	return &ClaudeCoordinator{Bin: "claude"}
}

// Decide calls claude -p --output-format json with the run state as a
// prompt and parses the structured decision from the response.
func (c *ClaudeCoordinator) Decide(ctx context.Context, state RunState) (Result, error) {
	bin := c.Bin
	if bin == "" {
		bin = "claude"
	}

	prompt := buildClaudePrompt(state)

	args := []string{"-p", "--output-format", "json"}
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
		return Result{Decision: DecisionFail, Reason: fmt.Sprintf("claude exec failed: %v", err)}, nil
	}

	r, err := parseClaudeDecision(string(out))
	return r, err
}

// buildClaudePrompt constructs a structured prompt from run state.
// Same format as Codex — provider-independent contract.
func buildClaudePrompt(state RunState) string {
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

// parseClaudeDecision extracts the JSON decision from Claude's output.
// Claude --output-format json returns a single JSON object.
func parseClaudeDecision(output string) (Result, error) {
	decoded := extractJSON(output)
	if decoded == nil {
		return Result{
			Decision: DecisionFail,
			Reason:   "claude response did not contain a valid decision JSON",
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

// Ensure strings import used.
var _ = strings.TrimSpace
