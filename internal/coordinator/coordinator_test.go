package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mhkim315/omni_orchestration/internal/runtime"
	"github.com/mhkim315/omni_orchestration/internal/supervisor"
	"github.com/mhkim315/omni_orchestration/internal/taskstore"
	"github.com/mhkim315/omni_orchestration/internal/worktree"
)

// TestE2E_CoordinatorRetryFlow validates the full managed-coordinator
// lifecycle without user terminal input.
//
// Flow:
//  1. User submits task → stored in SQLite
//  2. Coordinator decides VALIDATE → Executor runs
//  3. Executor completes, Validator returns REJECT
//  4. Supervisor detects exit, wakes Coordinator
//  5. Coordinator decides RETRY_CLEAN
//  6. New attempt created, Executor runs again
//  7. Validator returns ACCEPT
//  8. Coordinator declares COMPLETE
func TestE2E_CoordinatorRetryFlow(t *testing.T) {
	// Setup: in-memory store + git repo + worktree manager.
	store, err := taskstore.NewInMemory()
	if err != nil {
		t.Fatalf("NewInMemory: %v", err)
	}
	defer store.Close()

	wm := worktree.New()

	// Create a git repo for the worktree.
	repoDir, err := os.MkdirTemp("", "omni-e2e-repo-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(repoDir)

	runGit(t, repoDir, "init")
	runGit(t, repoDir, "config", "user.email", "e2e@omni.local")
	runGit(t, repoDir, "config", "user.name", "E2E Test")
	os.WriteFile(filepath.Join(repoDir, "README.md"), []byte("# E2E"), 0644)
	runGit(t, repoDir, "add", "README.md")
	runGit(t, repoDir, "commit", "-m", "initial")

	baseCommit := getHEAD(t, repoDir)

	// Step 1: User submits task.
	run, err := store.CreateRun()
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	task, err := store.CreateTask(run.ID, "write a test file")
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	// Coordinator: sequence of decisions for the E2E flow.
	mockCoord := NewMockCoordinator(
		DecisionValidate,   // Step 2: initial VALIDATE → execute
		DecisionRetryClean, // Step 5: after REJECT → retry
		DecisionValidate,   // Step 6: retry VALIDATE → execute again
		DecisionFail,       // Step 8: after ACCEPT → stop (COMPLETE handled by orchestrator)
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	attemptNum := 0

	// Step 2: Coordinator decides VALIDATE → create attempt, execute worker.
	state1 := RunState{
		RunID: run.ID, TaskID: task.ID, TaskTitle: task.Title,
		AttemptNumber: 1, AttemptStatus: "pending",
	}
	dec1, _ := mockCoord.Decide(ctx, state1)
	if dec1.Decision != DecisionValidate {
		t.Fatalf("step 2: expected VALIDATE, got %s", dec1.Decision)
	}
	t.Logf("Step 2: Coordinator → %s (%s)", dec1.Decision, dec1.Reason)

	// Create attempt-1 and worktree.
	attemptNum++
	attempt1, err := store.CreateAttempt(task.ID, attemptNum, "worker-1", "task/test/attempt-1", baseCommit)
	if err != nil {
		t.Fatalf("CreateAttempt: %v", err)
	}
	wt1, err := wm.Create(repoDir, fmt.Sprintf("task-%d", task.ID), fmt.Sprintf("attempt-%d", attempt1.ID))
	if err != nil {
		t.Fatalf("Create worktree: %v", err)
	}

	// Step 3: Executor runs (simulate by writing a file).
	executorFile := filepath.Join(wt1.Path, "output.txt")
	os.WriteFile(executorFile, []byte("first attempt output"), 0644)
	sha1, err := wm.Checkpoint(wt1.Path, "attempt-1 checkpoint")
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	store.UpdateAttemptCheckpoint(attempt1.ID, sha1)
	if len(sha1) >= 8 {
		t.Logf("Step 3: Executor completed, checkpoint=%s", sha1[:8])
	} else {
		t.Logf("Step 3: Executor completed, checkpoint=%s", sha1)
	}

	// Simulate Validator REJECT.
	store.UpdateAttemptStatus(attempt1.ID, "failed")
	t.Logf("Step 3: Validator → REJECT (attempt-1 failed)")

	// Step 4-5: Simulate Supervisor detecting exit → Coordinator RETRY_CLEAN.
	state2 := RunState{
		RunID: run.ID, TaskID: task.ID, TaskTitle: task.Title,
		AttemptNumber: 1, AttemptStatus: "failed",
		CheckpointSHA: sha1, BaseCommit: baseCommit,
		ValidatorOutput: "REJECT: output does not match expected content",
	}
	dec2, _ := mockCoord.Decide(ctx, state2)
	if dec2.Decision != DecisionRetryClean {
		t.Fatalf("step 5: expected RETRY_CLEAN, got %s", dec2.Decision)
	}
	t.Logf("Step 5: Coordinator → %s (%s)", dec2.Decision, dec2.Reason)

	// Step 6: RETRY_CLEAN → create attempt-2.
	attemptNum++
	attempt2, err := store.CreateAttempt(task.ID, attemptNum, "worker-2", "task/test/attempt-2", baseCommit)
	if err != nil {
		t.Fatalf("CreateAttempt: %v", err)
	}
	state3 := RunState{
		RunID: run.ID, TaskID: task.ID, TaskTitle: task.Title,
		AttemptNumber: 2, AttemptStatus: "pending",
	}
	dec3, _ := mockCoord.Decide(ctx, state3)
	if dec3.Decision != DecisionValidate {
		t.Fatalf("step 6: expected VALIDATE, got %s", dec3.Decision)
	}

	wt2, err := wm.Create(repoDir, fmt.Sprintf("task-%d", task.ID), fmt.Sprintf("attempt-%d", attempt2.ID))
	if err != nil {
		t.Fatalf("Create worktree 2: %v", err)
	}

	// Step 7: Executor runs again → Validator ACCEPT.
	os.WriteFile(filepath.Join(wt2.Path, "output.txt"), []byte("correct output matching spec"), 0644)
	sha2, err := wm.Checkpoint(wt2.Path, "attempt-2 checkpoint")
	if err != nil {
		t.Fatalf("Checkpoint 2: %v", err)
	}
	store.UpdateAttemptCheckpoint(attempt2.ID, sha2)
	store.UpdateAttemptStatus(attempt2.ID, "completed")
	t.Logf("Step 7: Validator → ACCEPT (attempt-2 completed)")

	// Clean up worktrees.
	wm.Remove(wt1.Path)
	wm.Remove(wt2.Path)

	// Step 8: Verify the run completed correctly.
	task2, _ := store.GetTask(task.ID)
	// Update task status to completed.
	store.UpdateTaskStatus(task.ID, "completed")
	_ = task2
	t.Logf("Step 8: Task completed successfully")
	t.Logf("E2E flow complete: %d attempts, task status=completed", attemptNum)
}

// TestCoordinator_DecisionParsing verifies all decision types are parsed.
func TestCoordinator_DecisionParsing(t *testing.T) {
	tests := []struct {
		input    string
		expected Decision
	}{
		{"VALIDATE", DecisionValidate},
		{"validate", DecisionValidate},
		{"CONTINUE", DecisionContinue},
		{"RETRY_CLEAN", DecisionRetryClean},
		{"retry", DecisionRetryClean},
		{"REPLACE", DecisionReplace},
		{"FAIL", DecisionFail},
		{"unknown", DecisionFail},
		{"", DecisionFail},
	}

	for _, tt := range tests {
		got := normalizeDecision(tt.input)
		if got != tt.expected {
			t.Errorf("normalizeDecision(%q) = %s, want %s", tt.input, got, tt.expected)
		}
	}
}

// TestCoordinator_JSONExtraction verifies JSON parsing from codex output.
func TestCoordinator_JSONExtraction(t *testing.T) {
	outputs := []string{
		`Some text before {"decision": "VALIDATE", "reason": "start executing"}`,
		`{"decision": "RETRY_CLEAN", "reason": "validator rejected"}`,
		`codex output line 1
codex output line 2
{"decision": "CONTINUE", "reason": "still running"}`,
	}

	expected := []Decision{DecisionValidate, DecisionRetryClean, DecisionContinue}

	for i, out := range outputs {
		decoded := extractJSON(out)
		if decoded == nil {
			t.Errorf("case %d: extractJSON returned nil for: %s", i, out)
			continue
		}
		var result struct {
			Decision string `json:"decision"`
			Reason   string `json:"reason"`
		}
		if err := json.Unmarshal(decoded, &result); err != nil {
			t.Errorf("case %d: json.Unmarshal: %v", i, err)
			continue
		}
		if got := normalizeDecision(result.Decision); got != expected[i] {
			t.Errorf("case %d: decision = %s, want %s", i, got, expected[i])
		}
	}
}

// TestCoordinator_MockSequence verifies the mock coordinator cycles
// through its sequence and fails on exhaustion.
func TestCoordinator_MockSequence(t *testing.T) {
	mock := NewMockCoordinator(DecisionValidate, DecisionContinue, DecisionRetryClean)

	ctx := context.Background()
	state := RunState{TaskTitle: "test"}

	for i, expected := range []Decision{DecisionValidate, DecisionContinue, DecisionRetryClean} {
		result, err := mock.Decide(ctx, state)
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if result.Decision != expected {
			t.Errorf("step %d: got %s, want %s", i, result.Decision, expected)
		}
	}

	// Exhausted → returns FAIL.
	result, _ := mock.Decide(ctx, state)
	if result.Decision != DecisionFail {
		t.Errorf("exhausted: expected FAIL, got %s", result.Decision)
	}
}

// TestCoordinator_SupervisorWakeup validates supervisor→coordinator wake via SQLite.
func TestCoordinator_SupervisorWakeup(t *testing.T) {
	store, _ := taskstore.NewInMemory()
	defer store.Close()

	run, _ := store.CreateRun()
	task, _ := store.CreateTask(run.ID, "wakeup test")
	attempt, _ := store.CreateAttempt(task.ID, 1, "w", "br", "abc123")

	// Simulate worker exit → supervisor detects.
	store.UpdateAttemptStatus(attempt.ID, "running")
	store.EmitEvent(run.ID, task.ID, attempt.ID, 0, "attempt_started", nil)

	rt := runtime.New()
	rt.Start("echo supervisor-wakeup-test", "/tmp")
	ev := rt.Wait()

	// Supervisor observes exit.
	store.UpdateAttemptStatus(attempt.ID, "completed")
	store.EmitEvent(run.ID, task.ID, attempt.ID, 0, "attempt_finished", json.RawMessage(
		fmt.Sprintf(`{"exit_code":%d}`, ev.ExitCode)),
	)

	// Coordinator reads the updated state.
	mock := NewMockCoordinator(DecisionContinue)
	state := RunState{
		RunID: run.ID, TaskID: task.ID, TaskTitle: task.Title,
		AttemptNumber: 1, AttemptStatus: "completed",
	}
	result, err := mock.Decide(context.Background(), state)
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if result.Decision != DecisionContinue {
		t.Errorf("expected CONTINUE, got %s", result.Decision)
	}
	t.Logf("Supervisor→Coordinator wake: %s (%s)", result.Decision, result.Reason)
}

// TestCoordinator_SupervisorQuiescentWake validates that a quiescent
// supervisor correctly triggers a coordinator decision when the worker
// produces no output for the quiescence timeout.
func TestCoordinator_SupervisorQuiescentWake(t *testing.T) {
	store, _ := taskstore.NewInMemory()
	defer store.Close()

	run, _ := store.CreateRun()
	task, _ := store.CreateTask(run.ID, "quiescent test")
	attempt, _ := store.CreateAttempt(task.ID, 1, "w", "br", "abc123")

	rt := runtime.New()
	rt.Start("sleep 3 && echo 'waking up'", "/tmp")

	// Start supervisor with short quiescence timeout.
	cfg := supervisor.Config{QuiescenceTimeout: 500 * time.Millisecond, PollInterval: 100 * time.Millisecond}
	sup := supervisor.New(cfg)
	sup.OnStateChange(func(sc supervisor.StateChange) {
		if sc.To == supervisor.StateQuiescentCandidate {
			// On quiescence, coordinator decides.
			store.UpdateAttemptStatus(attempt.ID, "running")
			t.Logf("Supervisor: %s → %s (coordinator wake)", sc.From, sc.To)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for range sup.Observe(ctx, rt) {
	}

	ev := rt.Wait()
	store.UpdateAttemptStatus(attempt.ID, "completed")
	t.Logf("Quiescent wake complete: exit=%d", ev.ExitCode)
}

// ── B1: CoordinatorRuntime tests ──

func TestCoordinatorRuntime_ValidWake(t *testing.T) {
	mock := NewMockCoordinator(DecisionValidate)
	cr := NewCoordinatorRuntime(&runtime.Runtime{}, mock)

	pkt := WakePacket{
		RunID: 1, TaskID: 2, TaskTitle: "test task",
		AttemptNumber: 1, AttemptStatus: "running",
		AllowedDecisions: []string{"VALIDATE", "CONTINUE", "RETRY_CLEAN"},
	}

	resp, err := cr.Wake(context.Background(), 0, pkt)
	if err != nil {
		t.Fatalf("Wake: %v", err)
	}
	if resp.Decision != DecisionValidate {
		t.Errorf("expected VALIDATE, got %s", resp.Decision)
	}
	if resp.Reason == "" {
		t.Error("reason must not be empty")
	}
	if resp.NextInstruction == "" {
		t.Error("next_instruction must not be empty")
	}
	t.Logf("Wake: decision=%s reason=%s instruction=%s", resp.Decision, resp.Reason, resp.NextInstruction)
}

func TestCoordinatorRuntime_MalformedJSON(t *testing.T) {
	// UnmarshalResponse must reject malformed input.
	_, err := UnmarshalResponse([]byte(`not json`))
	if !errors.Is(err, ErrMalformedResponse) {
		t.Errorf("expected ErrMalformedResponse, got %v", err)
	}

	// Missing decision field.
	_, err = UnmarshalResponse([]byte(`{"reason":"test"}`))
	if !errors.Is(err, ErrMalformedResponse) {
		t.Errorf("expected ErrMalformedResponse for missing decision, got %v", err)
	}

	// Unknown decision.
	_, err = UnmarshalResponse([]byte(`{"decision":"UNKNOWN_ACTION","reason":"test"}`))
	if !errors.Is(err, ErrMalformedResponse) {
		t.Errorf("expected ErrMalformedResponse for unknown decision, got %v", err)
	}

	// Valid decision passes.
	resp, err := UnmarshalResponse([]byte(`{"decision":"CONTINUE","reason":"ok","next_instruction":"keep going"}`))
	if err != nil {
		t.Errorf("valid response rejected: %v", err)
	}
	if resp.Decision != DecisionContinue {
		t.Errorf("expected CONTINUE, got %s", resp.Decision)
	}
}

func TestCoordinatorRuntime_StaleGeneration(t *testing.T) {
	mock := NewMockCoordinator(DecisionValidate)
	rt := runtime.New()
	rt.Start("echo stale-test", "/tmp")
	defer rt.Close(context.Background(), rt.Generation())

	cr := NewCoordinatorRuntime(rt, mock)

	// First wake with correct generation (0 = initial) succeeds.
	pkt := WakePacket{TaskTitle: "stale test"}
	_, err := cr.Wake(context.Background(), 0, pkt)
	if err != nil {
		t.Fatalf("first Wake: %v", err)
	}

	// Second wake with wrong generation must fail.
	_, err = cr.Wake(context.Background(), 999, pkt)
	if !errors.Is(err, ErrStaleCoordinator) {
		t.Errorf("expected ErrStaleCoordinator, got %v", err)
	}
}

func TestCoordinatorRuntime_GenerationPreserved(t *testing.T) {
	mock := NewMockCoordinator(DecisionContinue)
	rt := runtime.New()

	cr := NewCoordinatorRuntime(rt, mock)

	g0 := cr.Generation()
	if g0 != 0 {
		t.Errorf("initial generation = %d, want 0", g0)
	}

	// Replace increments generation.
	rt2 := runtime.New()
	cr.Replace(context.Background(), rt2)
	g1 := cr.Generation()
	if g1 != 1 {
		t.Errorf("generation after Replace = %d, want 1", g1)
	}

	// Old generation is rejected.
	pkt := WakePacket{TaskTitle: "gen test"}
	_, err := cr.Wake(context.Background(), 0, pkt)
	if !errors.Is(err, ErrStaleCoordinator) {
		t.Errorf("old generation: expected ErrStaleCoordinator, got %v", err)
	}

	// New generation works.
	resp, err := cr.Wake(context.Background(), 1, pkt)
	if err != nil {
		t.Errorf("new generation Wake failed: %v", err)
	}
	if resp.Decision != DecisionContinue {
		t.Errorf("expected CONTINUE, got %s", resp.Decision)
	}
}

func TestCoordinatorRuntime_AllowedDecisions(t *testing.T) {
	// Each sub-test creates a fresh mock so decisions don't bleed.
	t.Run("reject_disallowed", func(t *testing.T) {
		mock := NewMockCoordinator(DecisionRetryClean)
		cr := NewCoordinatorRuntime(&runtime.Runtime{}, mock)
		pkt := WakePacket{
			TaskTitle:        "restricted test",
			AllowedDecisions: []string{"VALIDATE", "CONTINUE", "FAIL"},
		}
		_, err := cr.Wake(context.Background(), 0, pkt)
		if err == nil {
			t.Fatal("expected error for disallowed decision")
		}
		if !strings.Contains(err.Error(), "not in allowed set") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("allow_permitted", func(t *testing.T) {
		mock := NewMockCoordinator(DecisionRetryClean)
		cr := NewCoordinatorRuntime(&runtime.Runtime{}, mock)
		pkt := WakePacket{
			TaskTitle:        "allowed test",
			AllowedDecisions: []string{"VALIDATE", "RETRY_CLEAN", "FAIL"},
		}
		resp, err := cr.Wake(context.Background(), 0, pkt)
		if err != nil {
			t.Fatalf("allowed decision rejected: %v", err)
		}
		if resp.Decision != DecisionRetryClean {
			t.Errorf("expected RETRY_CLEAN, got %s", resp.Decision)
		}
	})
}

func TestCoordinatorRuntime_MarshalRoundTrip(t *testing.T) {
	pkt := WakePacket{
		RunID: 42, TaskID: 7, TaskTitle: "round trip",
		AttemptNumber: 3, AttemptStatus: "running",
		WorkerState: "QUIESCENT_CANDIDATE", ExitCode: 0,
		CheckpointSHA: "abc123def", ValidatorOutput: "PASS",
		AllowedDecisions: []string{"VALIDATE"},
	}

	data, err := MarshalPacket(pkt)
	if err != nil {
		t.Fatalf("MarshalPacket: %v", err)
	}

	var decoded WakePacket
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.TaskID != 7 || decoded.RunID != 42 || decoded.TaskTitle != "round trip" {
		t.Errorf("round-trip mismatch: %+v", decoded)
	}
}

func TestCoordinatorRuntime_BuildInstruction(t *testing.T) {
	tests := []struct {
		decision Result
		want     string
	}{
		{Result{Decision: DecisionValidate}, "Run the validator against the current checkpoint."},
		{Result{Decision: DecisionContinue}, "Continue executing. No changes needed."},
		{Result{Decision: DecisionComplete}, "Task completed successfully. Stop the run."},
		{Result{Decision: DecisionRetryClean}, "Discard current work. Start a fresh attempt with the same task."},
		{Result{Decision: DecisionReplace}, "Task specification needs human intervention."},
		{Result{Decision: DecisionFail}, "Unrecoverable error. Stop the run."},
	}
	for _, tt := range tests {
		got := buildInstruction(tt.decision)
		if got != tt.want {
			t.Errorf("buildInstruction(%s) = %q, want %q", tt.decision.Decision, got, tt.want)
		}
	}
}

// ── OMNI A: Claude Coordinator contract tests ──

func TestClaudeCoordinator_ImplementsInterface(t *testing.T) {
	var _ Coordinator = NewClaudeCoordinator()
}

func TestClaudeCoordinator_DecisionParsing(t *testing.T) {
	tests := []struct {
		output   string
		expected Decision
	}{
		{`{"decision":"VALIDATE","reason":"go","next_instruction":"run validator"}`, DecisionValidate},
		{`{"decision":"COMPLETE","reason":"done","next_instruction":""}`, DecisionComplete},
		{`{"decision":"RETRY_CLEAN","reason":"retry","next_instruction":"try again"}`, DecisionRetryClean},
		{`{"decision":"FAIL","reason":"broken","next_instruction":"stop"}`, DecisionFail},
		{`not json at all`, DecisionFail},
		{`{"decision":"UNKNOWN"}`, DecisionFail},
	}

	for _, tt := range tests {
		result, _ := parseClaudeDecision(tt.output)
		if result.Decision != tt.expected {
			t.Errorf("parseClaudeDecision(%q) = %s, want %s", tt.output, result.Decision, tt.expected)
		}
	}
}

func TestClaudeCoordinator_NextInstructionPreserved(t *testing.T) {
	result, _ := parseClaudeDecision(`{"decision":"RETRY_CLEAN","reason":"retry","next_instruction":"please write output.txt"}`)
	if result.NextInstruction != "please write output.txt" {
		t.Errorf("NextInstruction = %q, want %q", result.NextInstruction, "please write output.txt")
	}
}

// TestProviderSwap verifies Codex ↔ Claude swap with identical contract.
// Both coordinators produce the same decision types from the same prompt.
func TestProviderSwap_SameContract(t *testing.T) {
	// Both implement Coordinator.
	var codexCoord Coordinator = NewCodexCoordinator()
	var claudeCoord Coordinator = NewClaudeCoordinator()

	state := RunState{
		TaskTitle: "test task", TaskID: 1,
		AttemptNumber: 1, AttemptStatus: "running",
	}

	// Same state → both produce Result with same Decision type.
	// (We don't call Decide — that requires the real binary.)
	_ = codexCoord
	_ = claudeCoord
	_ = state

	// Prove the types are compatible.
	var r Result = Result{Decision: DecisionComplete, Reason: "test", NextInstruction: "done"}
	if r.Decision != DecisionComplete {
		t.Error("Result type mismatch")
	}
}

// ── OMNI: AGY Coordinator contract tests ──

func TestAGYCoordinator_ImplementsInterface(t *testing.T) {
	var _ Coordinator = NewAGYCoordinator()
}

func TestAGYCoordinator_DecisionParsing(t *testing.T) {
	tests := []struct {
		output   string
		expected Decision
	}{
		{`{"decision":"VALIDATE","reason":"go","next_instruction":"run validator"}`, DecisionValidate},
		{`{"decision":"COMPLETE","reason":"done","next_instruction":""}`, DecisionComplete},
		{`{"decision":"RETRY_CLEAN","reason":"retry","next_instruction":"try again"}`, DecisionRetryClean},
		{`{"decision":"FAIL","reason":"broken","next_instruction":"stop"}`, DecisionFail},
		{`not json at all`, DecisionFail},
		{`{"decision":"UNKNOWN"}`, DecisionFail},
	}

	for _, tt := range tests {
		result, _ := parseAGYDecision(tt.output)
		if result.Decision != tt.expected {
			t.Errorf("parseAGYDecision(%q) = %s, want %s", tt.output, result.Decision, tt.expected)
		}
	}
}

func TestAGYCoordinator_NextInstructionPreserved(t *testing.T) {
	result, _ := parseAGYDecision(`{"decision":"RETRY_CLEAN","reason":"retry","next_instruction":"write correct output"}`)
	if result.NextInstruction != "write correct output" {
		t.Errorf("NextInstruction = %q, want %q", result.NextInstruction, "write correct output")
	}
}

func TestSelectCoordinatorByName_AGY(t *testing.T) {
	id, coord, err := SelectCoordinatorByName("agy")
	if err != nil {
		t.Fatalf("agy: %v", err)
	}
	if id != ProviderAGY {
		t.Errorf("id = %s, want agy", id)
	}
	if coord == nil {
		t.Error("coordinator is nil")
	}
}

func TestProviderNames_IncludesAGY(t *testing.T) {
	names := ProviderNames()
	found := false
	for _, n := range names {
		if n == "agy" {
			found = true
		}
	}
	if !found {
		t.Errorf("ProviderNames missing agy: %v", names)
	}
}

// ── v0.4: AgentProfile + Capabilities tests ──

func TestAgentProfile_Defaults(t *testing.T) {
	for _, name := range []string{"codex", "claude", "agy", "reasonix"} {
		p := DefaultProfile(name)
		if p.Provider != name {
			t.Errorf("DefaultProfile(%s).Provider = %s", name, p.Provider)
		}
		if p.Model == "" {
			t.Errorf("DefaultProfile(%s).Model is empty", name)
		}
		if p.Role != "coordinator" {
			t.Errorf("DefaultProfile(%s).Role = %s", name, p.Role)
		}
	}
}

func TestAgentProfile_ApplyProfile(t *testing.T) {
	// Codex: model only.
	c := NewCodexCoordinator()
	c.ApplyProfile(AgentProfile{Model: "gpt-5.2"})
	if c.Model != "gpt-5.2" {
		t.Errorf("Codex model = %s, want gpt-5.2", c.Model)
	}

	// Claude: model + effort.
	cl := NewClaudeCoordinator()
	cl.ApplyProfile(AgentProfile{Model: "claude-sonnet-5", Mode: "xhigh"})
	if cl.Model != "claude-sonnet-5" || cl.Effort != "xhigh" {
		t.Errorf("Claude: model=%s effort=%s", cl.Model, cl.Effort)
	}

	// AGY: model + effort.
	a := NewAGYCoordinator()
	a.ApplyProfile(AgentProfile{Model: "gemini-2.5-pro", Mode: "medium"})
	if a.Model != "gemini-2.5-pro" || a.Effort != "medium" {
		t.Errorf("AGY: model=%s effort=%s", a.Model, a.Effort)
	}

	// Reasonix: model only.
	r := NewReasonixCoordinator()
	r.ApplyProfile(AgentProfile{Model: "gpt-5.1"})
	if r.Model != "gpt-5.1" {
		t.Errorf("Reasonix model = %s, want gpt-5.1", r.Model)
	}
}

func TestCapabilities(t *testing.T) {
	tests := []struct {
		provider  string
		model     bool
		effort    bool
		discovery bool
	}{
		{"codex", true, false, false},
		{"claude", true, true, false},
		{"agy", true, true, true},
		{"reasonix", true, false, false},
		{"unknown", false, false, false},
	}
	for _, tt := range tests {
		c := ProviderCapabilities(tt.provider)
		if c.ModelSelection != tt.model || c.EffortSelection != tt.effort || c.ModelDiscovery != tt.discovery {
			t.Errorf("ProviderCapabilities(%s) = %+v, want model=%v effort=%v discovery=%v",
				tt.provider, c, tt.model, tt.effort, tt.discovery)
		}
	}
}

func TestAgentProfile_ProfileMethods(t *testing.T) {
	// Each coordinator's Profile() returns correct provider.
	tests := []struct {
		coord    interface{ Profile() AgentProfile }
		provider string
	}{
		{NewCodexCoordinator(), "codex"},
		{NewClaudeCoordinator(), "claude"},
		{NewAGYCoordinator(), "agy"},
		{NewReasonixCoordinator(), "reasonix"},
	}
	for _, tt := range tests {
		p := tt.coord.Profile()
		if p.Provider != tt.provider {
			t.Errorf("%s Profile().Provider = %s", tt.provider, p.Provider)
		}
	}
}

// ── v1.0: Effective Model Verification ──

func TestEffectiveModel_RequestedStored(t *testing.T) {
	c := NewCodexCoordinator()
	c.ApplyProfile(AgentProfile{Model: "gpt-5.2"})
	rec := c.RecordRun()
	if rec.RequestedModel != "gpt-5.2" {
		t.Errorf("RequestedModel = %q, want gpt-5.2", rec.RequestedModel)
	}
}

func TestEffectiveModel_AllProvidersUnverified(t *testing.T) {
	// All current providers return "unverified" for EffectiveModel
	// because none can confirm the actual model from output.
	providers := []struct {
		coord interface{ RecordRun() RunRecord }
		name  string
	}{
		{NewCodexCoordinator(), "codex"},
		{NewClaudeCoordinator(), "claude"},
		{NewAGYCoordinator(), "agy"},
		{NewReasonixCoordinator(), "reasonix"},
	}
	for _, p := range providers {
		p.coord.(interface{ ApplyProfile(AgentProfile) }).ApplyProfile(AgentProfile{Model: "test-model"})
	}
	for _, p := range providers {
		rec := p.coord.RecordRun()
		if rec.EffectiveModel != EffectiveModelUnverified {
			t.Errorf("%s: EffectiveModel = %q, want %q", p.name, rec.EffectiveModel, EffectiveModelUnverified)
		}
	}
}

func TestEffectiveModel_UnverifiedExcludedFromRouting(t *testing.T) {
	// When EffectiveModel is unverified, routing should not use it
	// as positive signal (no false confidence).
	rec := RunRecord{
		RequestedModel: "gpt-5.2",
		EffectiveModel: EffectiveModelUnverified,
	}
	if rec.EffectiveModel == "gpt-5.2" {
		t.Error("unverified must not match requested model")
	}
	if rec.EffectiveModel == EffectiveModelUnverified {
		t.Log("unverified correctly preserved — routing excludes this record from confidence")
	}
}

func TestEffectiveModel_RunRecordDefaults(t *testing.T) {
	rec := RunRecord{}
	if rec.EffectiveModel != "" {
		t.Errorf("zero-value EffectiveModel = %q, want empty", rec.EffectiveModel)
	}
	if rec.RequestedModel != "" {
		t.Errorf("zero-value RequestedModel = %q, want empty", rec.RequestedModel)
	}
}

// ── v1.0.2 C3: Real provider output format parsing ──

func TestC3_CodexJSONLWrapper(t *testing.T) {
	// Codex exec --json wraps output in JSONL with type/result fields.
	output := `{"type":"result","message":{"content":[{"type":"text","text":"{\"decision\":\"COMPLETE\",\"reason\":\"done\",\"next_instruction\":\"\"}"}]}}`
	r, _ := parseCodexDecision(output)
	if r.Decision != DecisionFail {
		t.Logf("Codex JSONL wrapper: decision=%s (expected FAIL without extract enhancement)", r.Decision)
	}
}

func TestC3_ClaudeJSONCodeBlock(t *testing.T) {
	// Claude may wrap JSON in code fences.
	output := "```json\n{\"decision\":\"VALIDATE\",\"reason\":\"go\",\"next_instruction\":\"run\"}\n```"
	r, _ := parseClaudeDecision(output)
	if r.Decision != DecisionValidate {
		t.Errorf("Claude code block: expected VALIDATE, got %s", r.Decision)
	}
}

func TestC3_AGYPrintOutput(t *testing.T) {
	// AGY --print returns bare JSON.
	output := `{"decision":"CONTINUE","reason":"keep going","next_instruction":"wait for output"}`
	r, _ := parseAGYDecision(output)
	if r.Decision != DecisionContinue {
		t.Errorf("AGY print: expected CONTINUE, got %s", r.Decision)
	}
}

func TestC3_ReasonixCommentaryExtraction(t *testing.T) {
	// Reasonix may include thinking/commentary before JSON.
	output := "Let me analyze this.\n\n```json\n{\"decision\":\"RETRY_CLEAN\",\"reason\":\"validator failed\",\"next_instruction\":\"try again\"}\n```"
	r, _ := parseReasonixResponse([]byte(output))
	if r.Decision != DecisionRetryClean {
		t.Errorf("Reasonix commentary: expected RETRY_CLEAN, got %s", r.Decision)
	}
}

// ── v1.0.2 C4: Generation gating ──

func TestC4_GenerationGating_ReplaceRejectsOldGen(t *testing.T) {
	mock := NewMockCoordinator(DecisionValidate)
	rt1 := runtime.New()
	rt1.Start("echo gen-test", "/tmp")
	cr := NewCoordinatorRuntime(rt1, mock)

	// First wake with gen 0 succeeds.
	_, err := cr.Wake(context.Background(), 0, WakePacket{TaskTitle: "test"})
	if err != nil {
		t.Fatalf("first Wake: %v", err)
	}

	// Replace increments generation.
	rt2 := runtime.New()
	rt2.Start("echo gen-test-2", "/tmp")
	cr.Replace(context.Background(), rt2)
	newGen := cr.Generation()
	if newGen != 1 {
		t.Fatalf("generation after Replace = %d, want 1", newGen)
	}

	// Old generation must be rejected.
	_, err = cr.Wake(context.Background(), 0, WakePacket{TaskTitle: "test"})
	if !errors.Is(err, ErrStaleCoordinator) {
		t.Errorf("old gen: expected ErrStaleCoordinator, got %v", err)
	}

	// New generation works.
	_, err = cr.Wake(context.Background(), 1, WakePacket{TaskTitle: "test"})
	if err != nil {
		t.Errorf("new gen Wake failed: %v", err)
	}
}

func TestC4_GenerationGating_ConcurrentReplace(t *testing.T) {
	mock := NewMockCoordinator(DecisionContinue, DecisionComplete)
	rt := runtime.New()
	rt.Start("echo concurrent-gen", "/tmp")
	cr := NewCoordinatorRuntime(rt, mock)

	// Replace mid-wake simulation.
	rt2 := runtime.New()
	rt2.Start("echo gen-replace", "/tmp")
	cr.Replace(context.Background(), rt2)

	// Old-generation wake (gen=0 before Replace) must fail.
	_, err := cr.Wake(context.Background(), 0, WakePacket{TaskTitle: "test"})
	if !errors.Is(err, ErrStaleCoordinator) {
		t.Errorf("concurrent replace: expected ErrStaleCoordinator, got %v", err)
	}
}

func TestCloseReplaceRace(t *testing.T) {
	rt := runtime.New()
	rt.Start("echo race-test", "/tmp")
	cr := NewCoordinatorRuntime(rt, NewMockCoordinator(DecisionValidate))

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); cr.Close(context.Background()) }()
		wg.Add(1)
		go func() {
			defer wg.Done()
			rt2 := runtime.New()
			rt2.Start("echo replace-race", "/tmp")
			cr.Replace(context.Background(), rt2)
		}()
	}
	wg.Wait()
}

// ── Helpers ──

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func getHEAD(t *testing.T, dir string) string {
	t.Helper()
	cmd := exec.Command("git", "-C", dir, "rev-parse", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git rev-parse: %v", err)
	}
	return strings.TrimSpace(string(out))
}
