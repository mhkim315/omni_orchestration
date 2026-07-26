package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
)

// CodexCoordinator delegates decisions to a codex CLI process.
// It implements Coordinator by calling `codex exec --json` with a
// structured prompt and parsing the JSON response.
type CodexCoordinator struct {
	Bin            string // path to codex CLI (default: "codex")
	Model          string // model override (optional)
	requestedModel string // set by ApplyProfile
}

// NewCodexCoordinator returns a Codex-backed coordinator.
func NewCodexCoordinator() *CodexCoordinator {
	return &CodexCoordinator{Bin: "codex"}
}

// Decide calls codex exec --json with the run state as a prompt and
// parses the structured decision from the response.
func (c *CodexCoordinator) Decide(ctx context.Context, state RunState) (Result, error) {
	bin := c.Bin
	if bin == "" {
		bin = "codex"
	}

	prompt := buildCodexPrompt(state)

	args := []string{"exec", "--json", "--sandbox", "read-only"}
	if c.Model != "" {
		args = append(args, "--model", c.Model)
	}
	args = append(args, prompt)

	cmd := exec.CommandContext(ctx, bin, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return Result{Decision: DecisionFail, Reason: fmt.Sprintf("codex exec failed: %v", err)}, nil
	}

	r, err := parseCodexDecision(string(out))
	return r, err
}

// buildCodexPrompt constructs a structured prompt from run state.
func buildCodexPrompt(state RunState) string {
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

// parseCodexDecision extracts the JSON decision from codex output.
func parseCodexDecision(output string) (Result, error) {
	decoded := extractCodexJSONL(output)
	if decoded == nil {
		return Result{
			Decision: DecisionFail,
			Reason:   "codex response did not contain a valid decision JSON",
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

// extractJSON finds the last complete JSON object in a string.
func extractJSON(s string) []byte {
	start := strings.LastIndex(s, "{")
	if start < 0 {
		return nil
	}
	end := strings.LastIndex(s, "}")
	if end <= start {
		return nil
	}
	candidate := s[start : end+1]
	if json.Valid([]byte(candidate)) {
		return []byte(candidate)
	}
	return nil
}

func normalizeDecision(s string) Decision {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "START":
		return DecisionStart
	case "VALIDATE":
		return DecisionValidate
	case "CONTINUE":
		return DecisionContinue
	case "COMPLETE":
		return DecisionComplete
	case "RETRY_CLEAN", "RETRY":
		return DecisionRetryClean
	case "REPLACE":
		return DecisionReplace
	case "FAIL":
		return DecisionFail
	default:
		return DecisionFail
	}
}
