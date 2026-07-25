package coordinator

import (
	"context"
	"testing"
	"time"
)

// Reasonix conformance suite. Tests the Coordinator contract without
// requiring the reasonix binary.

func TestReasonix_ParseResponse_Validate(t *testing.T) {
	out := []byte(`Some preamble text...
{"decision": "VALIDATE", "reason": "The diff looks correct and validator passed."}
Some trailing text...`)

	result, err := parseReasonixResponse(out)
	if err != nil {
		t.Fatal(err)
	}
	if result.Decision != DecisionValidate {
		t.Errorf("decision = %s, want VALIDATE", result.Decision)
	}
	if result.Reason == "" {
		t.Error("reason is empty")
	}
}

func TestReasonix_ParseResponse_RetryClean(t *testing.T) {
	out := []byte(`{"decision":"RETRY_CLEAN","reason":"validator rejected"}`)
	result, _ := parseReasonixResponse(out)
	if result.Decision != DecisionRetryClean {
		t.Errorf("decision = %s, want RETRY_CLEAN", result.Decision)
	}
}

func TestReasonix_ParseResponse_Complete(t *testing.T) {
	out := []byte(`{"decision":"COMPLETE","reason":"all checks passed"}`)
	result, _ := parseReasonixResponse(out)
	if result.Decision != DecisionComplete {
		t.Errorf("decision = %s, want COMPLETE", result.Decision)
	}
}

func TestReasonix_ParseResponse_Fail(t *testing.T) {
	out := []byte(`{"decision":"FAIL","reason":"unrecoverable error"}`)
	result, _ := parseReasonixResponse(out)
	if result.Decision != DecisionFail {
		t.Errorf("decision = %s, want FAIL", result.Decision)
	}
}

func TestReasonix_ParseResponse_Continue(t *testing.T) {
	out := []byte(`{"decision":"CONTINUE","reason":"more work needed"}`)
	result, _ := parseReasonixResponse(out)
	if result.Decision != DecisionContinue {
		t.Errorf("decision = %s, want CONTINUE", result.Decision)
	}
}

func TestReasonix_ParseResponse_Replace(t *testing.T) {
	out := []byte(`{"decision":"REPLACE","reason":"restart with different approach"}`)
	result, _ := parseReasonixResponse(out)
	if result.Decision != DecisionReplace {
		t.Errorf("decision = %s, want REPLACE", result.Decision)
	}
}

func TestReasonix_ParseResponse_CaseInsensitive(t *testing.T) {
	out := []byte(`{"decision":"validate","reason":"lowercase"}`)
	result, _ := parseReasonixResponse(out)
	if result.Decision != DecisionValidate {
		t.Errorf("case-insensitive: got %s, want VALIDATE", result.Decision)
	}
}

func TestReasonix_ParseResponse_RetryAlias(t *testing.T) {
	out := []byte(`{"decision":"RETRY","reason":"retry alias"}`)
	result, _ := parseReasonixResponse(out)
	if result.Decision != DecisionRetryClean {
		t.Errorf("retry alias: got %s, want RETRY_CLEAN", result.Decision)
	}
}

func TestReasonix_ParseResponse_EmptyOutput(t *testing.T) {
	result, _ := parseReasonixResponse([]byte(""))
	if result.Decision != DecisionFail {
		t.Errorf("empty: got %s, want FAIL", result.Decision)
	}
}

func TestReasonix_ParseResponse_NoJSON(t *testing.T) {
	result, _ := parseReasonixResponse([]byte("just some text without JSON"))
	if result.Decision != DecisionFail {
		t.Errorf("no JSON: got %s, want FAIL", result.Decision)
	}
}

func TestReasonix_ParseResponse_MalformedJSON(t *testing.T) {
	result, _ := parseReasonixResponse([]byte(`{"decision": "VALIDATE", reason: broken}`))
	if result.Decision != DecisionFail {
		t.Errorf("malformed: got %s, want FAIL", result.Decision)
	}
}

func TestReasonix_ParseResponse_UnknownDecision(t *testing.T) {
	out := []byte(`{"decision":"DEPLOY_TO_PRODUCTION","reason":"why not"}`)
	result, _ := parseReasonixResponse(out)
	if result.Decision != DecisionFail {
		t.Errorf("unknown: got %s, want FAIL", result.Decision)
	}
}

func TestReasonix_ParseResponse_NoDecision(t *testing.T) {
	out := []byte(`{"reason":"no decision field"}`)
	result, _ := parseReasonixResponse(out)
	if result.Decision != DecisionFail {
		t.Errorf("no decision: got %s, want FAIL", result.Decision)
	}
}

func TestReasonix_BuildPrompt(t *testing.T) {
	state := RunState{
		RunID:         42,
		TaskID:        7,
		TaskTitle:     "fix critical bug",
		TaskStatus:    "running",
		AttemptNumber: 2,
		AttemptStatus: "completed",
		CheckpointSHA: "abc123",
		BaseCommit:    "def456",
	}

	prompt := buildReasonixPrompt(state)
	if len(prompt) == 0 {
		t.Error("prompt is empty")
	}
	if prompt == "" {
		t.Error("prompt empty")
	}
	// Must contain key fields.
	for _, want := range []string{"42", "fix critical bug", "abc123", "def456", "VALIDATE"} {
		if !contains(prompt, want) {
			t.Errorf("prompt missing %q", want)
		}
	}
}

func TestReasonix_Timeout(t *testing.T) {
	// Test that the coordinator wraps with timeout correctly.
	// The default timeout is 180s; we verify the field is set.
	c := NewReasonixCoordinator()
	if c.Timeout != 180*time.Second {
		t.Errorf("default timeout = %v, want 180s", c.Timeout)
	}

	// Custom timeout.
	c2 := &ReasonixCoordinator{Bin: "reasonix", Timeout: 30 * time.Second}
	if c2.Timeout != 30*time.Second {
		t.Errorf("custom timeout = %v, want 30s", c2.Timeout)
	}
}

func TestReasonix_ContextTimeout(t *testing.T) {
	c := NewReasonixCoordinator()
	c.Timeout = 1 * time.Millisecond // very short for testing

	ctx := context.Background()
	_, err := c.Decide(ctx, RunState{TaskTitle: "test"})
	// Expect timeout — reasonix binary likely not found.
	// The coordinator should handle the error gracefully.
	t.Logf("Decide result: %v (expected: binary not found or timeout)", err)
}

func TestReasonix_ImplementsCoordinator(t *testing.T) {
	var c Coordinator = NewReasonixCoordinator()
	if c == nil {
		t.Fatal("nil coordinator")
	}
	_ = c.(*ReasonixCoordinator)
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
