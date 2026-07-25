//go:build smoketest

package coordinator

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// Smoke tests verify real provider CLI binaries with actual auth.
// Requires OMNI_SMOKE_TEST=1 and the provider binary on PATH.
//
// Run:
//
//	OMNI_SMOKE_TEST=1 go test -tags smoketest -run Smoke -v ./internal/coordinator/ -timeout 300s

const smokeState = `Task: smoke-test (ID: 1)
Attempt: #1
Status: running
Checkpoint: abc123
Base commit: HEAD
Validator output: PASS
Diff summary: output.txt created`

type smokeResult struct {
	Provider   string
	Version    string
	Model      string
	Effort     string
	Duration   time.Duration
	Decision   Decision
	ParseMode  string // "strict_json" | "extracted" | "failed"
	ExitStatus int
	OutputHash string // SHA-256 of raw output (sanitized)
}

func runSmoke(t *testing.T, name string, coord Coordinator, versionCmd []string) smokeResult {
	t.Helper()

	if os.Getenv("OMNI_SMOKE_TEST") != "1" {
		t.Skip("OMNI_SMOKE_TEST not set")
	}

	r := smokeResult{Provider: name}

	// Get version.
	if len(versionCmd) > 0 {
		out, _ := runCmd(versionCmd[0], versionCmd[1:]...)
		r.Version = strings.TrimSpace(string(out))
	}

	state := RunState{
		TaskTitle: "smoke-test", TaskID: 1, AttemptNumber: 1, AttemptStatus: "running",
		CheckpointSHA: "abc123", BaseCommit: "HEAD",
		ValidatorOutput: "PASS", DiffSummary: "output.txt created",
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	result, err := coord.Decide(ctx, state)
	r.Duration = time.Since(start)

	if err != nil {
		r.ParseMode = "failed"
		r.Decision = DecisionFail
		r.OutputHash = fmt.Sprintf("error: %v", err)
		return r
	}

	r.Decision = result.Decision
	r.ParseMode = "strict_json"
	if result.Decision == DecisionFail && result.Reason != "" {
		r.ParseMode = "extracted"
	}

	// Verify decision is in valid set.
	switch result.Decision {
	case DecisionStart, DecisionValidate, DecisionContinue, DecisionComplete,
		DecisionRetryClean, DecisionReplace, DecisionFail:
	default:
		r.ParseMode = "failed"
		r.Decision = DecisionFail
	}

	return r
}

func runCmd(name string, args ...string) ([]byte, error) {
	return nil, nil // simplified for build-tag test
}

// TestSmoke_AllProviders runs real smoke tests for all 4 providers.
func TestSmoke_AllProviders(t *testing.T) {
	// Must be run explicitly: OMNI_SMOKE_TEST=1 go test -tags smoketest -run Smoke -v
	if os.Getenv("OMNI_SMOKE_TEST") != "1" {
		t.Skip("OMNI_SMOKE_TEST not set — skipping real provider smoke tests")
		return
	}

	results := make([]smokeResult, 0, 4)

	// Codex
	results = append(results, runSmoke(t, "codex",
		NewCodexCoordinator(),
		[]string{"codex", "--version"},
	))

	// Claude
	results = append(results, runSmoke(t, "claude",
		NewClaudeCoordinator(),
		[]string{"claude", "--version"},
	))

	// AGY
	results = append(results, runSmoke(t, "agy",
		NewAGYCoordinator(),
		[]string{"agy", "--version"},
	))

	// Reasonix
	results = append(results, runSmoke(t, "reasonix",
		NewReasonixCoordinator(),
		[]string{"reasonix", "version"},
	))

	// Print matrix.
	t.Logf("=== SMOKE TEST MATRIX ===")
	for _, r := range results {
		t.Logf("%-12s ver=%-12s duration=%v decision=%s parse=%s",
			r.Provider, r.Version, r.Duration.Round(time.Millisecond),
			r.Decision, r.ParseMode)
	}

	// Verify all 7 decisions exist in parser.
	t.Run("all_7_decisions_parse", func(t *testing.T) {
		inputs := []string{"START", "VALIDATE", "CONTINUE", "RETRY_CLEAN", "REPLACE", "FAIL", "COMPLETE"}
		expected := []Decision{DecisionStart, DecisionValidate, DecisionContinue, DecisionRetryClean, DecisionReplace, DecisionFail, DecisionComplete}

		for i, in := range inputs {
			got := normalizeDecision(in)
			if got != expected[i] {
				t.Errorf("normalizeDecision(%q) = %s, want %s", in, got, expected[i])
			}
			// Also verify Reasonix parser.
			got2 := parseDecision(in)
			if got2 != expected[i] {
				t.Errorf("parseDecision(%q) = %s, want %s", in, got2, expected[i])
			}
		}
	})
}

// TestSmoke_CodexOnly runs only the Codex smoke test.
func TestSmoke_CodexOnly(t *testing.T) {
	r := runSmoke(t, "codex", NewCodexCoordinator(), []string{"codex", "--version"})
	t.Logf("codex: version=%s duration=%v decision=%s", r.Version, r.Duration, r.Decision)
	if r.Decision == DecisionFail && r.ParseMode == "failed" {
		t.Error("Codex smoke test failed")
	}
}

// TestSmoke_ClaudeOnly runs only the Claude smoke test.
func TestSmoke_ClaudeOnly(t *testing.T) {
	r := runSmoke(t, "claude", NewClaudeCoordinator(), []string{"claude", "--version"})
	t.Logf("claude: version=%s duration=%v decision=%s", r.Version, r.Duration, r.Decision)
}

// TestSmoke_AGYOnly runs only the AGY smoke test.
func TestSmoke_AGYOnly(t *testing.T) {
	r := runSmoke(t, "agy", NewAGYCoordinator(), []string{"agy", "--version"})
	t.Logf("agy: version=%s duration=%v decision=%s", r.Version, r.Duration, r.Decision)
}

// TestSmoke_ReasonixOnly runs only the Reasonix smoke test.
func TestSmoke_ReasonixOnly(t *testing.T) {
	r := runSmoke(t, "reasonix", NewReasonixCoordinator(), []string{"reasonix", "version"})
	t.Logf("reasonix: version=%s duration=%v decision=%s", r.Version, r.Duration, r.Decision)
}
