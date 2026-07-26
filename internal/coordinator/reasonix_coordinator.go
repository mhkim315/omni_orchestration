package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// ReasonixCoordinator delegates decisions to a Reasonix CLI process.
// It implements Coordinator by calling `reasonix --max-steps 0 --prompt "..."`.
//
// Reasonix has no built-in timeout and exhibits 14-30s latency. The
// coordinator wraps every call with a 180s context timeout.
type ReasonixCoordinator struct {
	Bin            string // path to reasonix CLI (default: "reasonix")
	Model          string // model override in provider/model format (optional)
	Timeout        time.Duration
	requestedModel string // set by ApplyProfile
}

// NewReasonixCoordinator returns a Reasonix-backed coordinator.
func NewReasonixCoordinator() *ReasonixCoordinator {
	return &ReasonixCoordinator{Bin: "reasonix", Timeout: 180 * time.Second}
}

// Decide calls reasonix --max-steps 0 --prompt "<prompt>" with a
// structured prompt and parses the JSON decision from stdout.
func (c *ReasonixCoordinator) Decide(ctx context.Context, state RunState) (Result, error) {
	bin := c.Bin
	if bin == "" {
		bin = "reasonix"
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 180 * time.Second
	}

	// Reasonix has no built-in timeout. Wrap with our own.
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	prompt := buildReasonixPrompt(state)
	args := []string{"--max-steps", "0"}
	if c.Model != "" {
		args = append(args, "-model", c.Model)
	}
	args = append(args, "--prompt", prompt)
	cmd := exec.CommandContext(ctx, bin, args...)
	out, err := cmd.Output()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return Result{Decision: DecisionFail, Reason: "reasonix: timeout exceeded"}, nil
		}
		return Result{Decision: DecisionFail, Reason: fmt.Sprintf("reasonix: %v", err)}, err
	}

	return parseReasonixResponse(out)
}

// buildReasonixPrompt constructs a structured prompt for Reasonix.
// Reasonix expects a concise decision prompt with the run state.
func buildReasonixPrompt(state RunState) string {
	var sb strings.Builder
	sb.WriteString("You are an OMNI orchestration coordinator. Based on the run state below, ")
	sb.WriteString("choose exactly one decision from the allowed set and explain why.\n\n")
	sb.WriteString(fmt.Sprintf("Run ID: %d\n", state.RunID))
	sb.WriteString(fmt.Sprintf("Task: %s\n", state.TaskTitle))
	sb.WriteString(fmt.Sprintf("Task Status: %s\n", state.TaskStatus))
	sb.WriteString(fmt.Sprintf("Attempt: %d (%s)\n", state.AttemptNumber, state.AttemptStatus))

	if state.CheckpointSHA != "" {
		sb.WriteString(fmt.Sprintf("Checkpoint: %s\n", state.CheckpointSHA))
	}
	if state.BaseCommit != "" {
		sb.WriteString(fmt.Sprintf("Base Commit: %s\n", state.BaseCommit))
	}
	if state.ValidatorOutput != "" {
		sb.WriteString(fmt.Sprintf("Validator: %s\n", state.ValidatorOutput))
	}
	if state.DiffSummary != "" {
		sb.WriteString(fmt.Sprintf("Diff: %s\n", state.DiffSummary))
	}

	sb.WriteString("\nAllowed decisions: VALIDATE, CONTINUE, COMPLETE, RETRY_CLEAN, REPLACE, FAIL\n")
	sb.WriteString("\nRespond with exactly one JSON object: {\"decision\": \"<choice>\", \"reason\": \"<explanation>\"}\n")
	return sb.String()
}

// parseReasonixResponse extracts the JSON decision from Reasonix output.
func parseReasonixResponse(out []byte) (Result, error) {
	text := strings.TrimSpace(string(out))
	if text == "" {
		return Result{Decision: DecisionFail, Reason: "reasonix: empty response"}, nil
	}

	decoded := extractReasonixCommentary(text)
	if decoded == nil {
		return Result{Decision: DecisionFail, Reason: "reasonix: no JSON found in response"}, nil
	}

	jsonStr := string(decoded)
	var parsed struct {
		Decision string `json:"decision"`
		Reason   string `json:"reason"`
	}
	if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
		return Result{Decision: DecisionFail, Reason: fmt.Sprintf("reasonix: parse error: %v", err)}, nil
	}

	decision := parseDecision(parsed.Decision)
	return Result{Decision: decision, Reason: parsed.Reason}, nil
}

// parseDecision maps a raw string to a Decision value (case-insensitive).
func parseDecision(raw string) Decision {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
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
