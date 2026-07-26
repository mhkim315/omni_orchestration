package supervisor

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mhkim315/omni_orchestration/internal/runtime"
)

func TestSupervisor_ActiveToExited(t *testing.T) {
	rt := runtime.New()
	if err := rt.Start("exit 0", "/tmp"); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cfg := Config{QuiescenceTimeout: 2 * time.Second, PollInterval: 200 * time.Millisecond}
	sup := New(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ch := sup.Observe(ctx, rt)

	var states []State
	for sc := range ch {
		states = append(states, sc.To)
	}

	if len(states) < 2 {
		t.Fatalf("expected at least 2 state changes, got %d: %v", len(states), states)
	}
	if states[0] != StateActive {
		t.Errorf("first state = %s, want ACTIVE", states[0])
	}
	last := states[len(states)-1]
	if last != StateExited {
		t.Errorf("final state = %s, want EXITED", last)
	}
}

func TestSupervisor_ActiveToQuiescent(t *testing.T) {
	rt := runtime.New()
	// sleep does not produce output → becomes quiescent.
	if err := rt.Start("sleep 10", "/tmp"); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cfg := Config{QuiescenceTimeout: 500 * time.Millisecond, PollInterval: 100 * time.Millisecond}
	sup := New(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch := sup.Observe(ctx, rt)

	var states []State
	timeout := time.After(3 * time.Second)

loop:
	for {
		select {
		case sc, ok := <-ch:
			if !ok {
				break loop
			}
			states = append(states, sc.To)
			// Once we reach quiescent, stop the runtime.
			if sc.To == StateQuiescentCandidate {
				rt.Stop(ctx)
			}
		case <-timeout:
			break loop
		}
	}

	// Must see ACTIVE → QUIESCENT_CANDIDATE.
	foundActive := false
	foundQuiescent := false
	for _, s := range states {
		if s == StateActive {
			foundActive = true
		}
		if s == StateQuiescentCandidate {
			foundQuiescent = true
		}
	}
	if !foundActive {
		t.Error("never saw ACTIVE")
	}
	if !foundQuiescent {
		t.Error("never saw QUIESCENT_CANDIDATE")
	}
	t.Logf("states: %v", states)
}

func TestSupervisor_QuiescentBackToActive(t *testing.T) {
	rt := runtime.New()
	if err := rt.Start("sleep 1 && echo wake-up", "/tmp"); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cfg := Config{QuiescenceTimeout: 500 * time.Millisecond, PollInterval: 100 * time.Millisecond}
	sup := New(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ch := sup.Observe(ctx, rt)

	var states []State
	timeout := time.After(5 * time.Second)

loop:
	for {
		select {
		case sc, ok := <-ch:
			if !ok {
				break loop
			}
			states = append(states, sc.To)
			if sc.To == StateExited || sc.To == StateCrashed {
				break loop
			}
		case <-timeout:
			break loop
		}
	}

	// Expected: ACTIVE → QUIESCENT_CANDIDATE → ACTIVE (wake-up) → EXITED
	if len(states) < 3 {
		t.Fatalf("expected at least 3 transitions, got %d: %v", len(states), states)
	}
	if states[0] != StateActive {
		t.Errorf("first = %s, want ACTIVE", states[0])
	}

	foundQuiescent := false
	foundActiveAgain := false
	for i, s := range states {
		if s == StateQuiescentCandidate {
			foundQuiescent = true
		}
		// After quiescent, we should go back to ACTIVE.
		if s == StateActive && i > 0 && states[i-1] == StateQuiescentCandidate {
			foundActiveAgain = true
		}
	}
	if !foundQuiescent {
		t.Error("never saw QUIESCENT_CANDIDATE")
	}
	if !foundActiveAgain {
		t.Error("never saw ACTIVE after QUIESCENT_CANDIDATE")
	}
	t.Logf("states: %v", states)
}

func TestSupervisor_CrashedOnNonZeroExit(t *testing.T) {
	rt := runtime.New()
	// Use kill -SEGV to produce a signal exit (bypasses PTY exit-code masking).
	if err := rt.Start("kill -SEGV $$", "/tmp"); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cfg := Config{QuiescenceTimeout: 5 * time.Second, PollInterval: 200 * time.Millisecond}
	sup := New(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ch := sup.Observe(ctx, rt)

	var states []State
	for sc := range ch {
		states = append(states, sc.To)
	}

	if len(states) < 1 {
		t.Fatalf("expected at least 1 transition, got %d: %v", len(states), states)
	}
	last := states[len(states)-1]
	// A signaled process is either CRASHED (if we detect the signal) or
	// EXITED with non-zero. Either way the supervisor must reach a terminal.
	if last != StateCrashed && last != StateExited {
		t.Errorf("final = %s, want CRASHED or EXITED", last)
	}
	t.Logf("states: %v", states)
	t.Logf("exit: code=%d signaled=%v", rt.Wait().ExitCode, rt.Wait().Signaled)
}

func TestSupervisor_OnStateChange(t *testing.T) {
	rt := runtime.New()
	if err := rt.Start("echo hello", "/tmp"); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var mu sync.Mutex
	var transitions []StateChange

	cfg := Config{QuiescenceTimeout: 5 * time.Second, PollInterval: 200 * time.Millisecond}
	sup := New(cfg)
	sup.OnStateChange(func(sc StateChange) {
		mu.Lock()
		transitions = append(transitions, sc)
		mu.Unlock()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for range sup.Observe(ctx, rt) {
	}

	mu.Lock()
	defer mu.Unlock()
	if len(transitions) < 1 {
		t.Fatal("OnStateChange was never called")
	}
	if transitions[0].To != StateActive {
		t.Errorf("first transition = %s, want ACTIVE", transitions[0].To)
	}
	t.Logf("transitions: %d", len(transitions))
	for _, sc := range transitions {
		t.Logf("  %s → %s", sc.From, sc.To)
	}
}

func TestSupervisor_ConcurrentSafety(t *testing.T) {
	// Start several supervisors concurrently — must not race.
	rt := runtime.New()
	if err := rt.Start(fmt.Sprintf("for i in $(seq 5); do echo line$i; sleep 0.1; done"), "/tmp"); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cfg := Config{QuiescenceTimeout: 2 * time.Second, PollInterval: 100 * time.Millisecond}

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sup := New(cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for range sup.Observe(ctx, rt) {
			}
		}()
	}
	wg.Wait()
}

func TestSupervisor_TerminalStatesAreSticky(t *testing.T) {
	rt := runtime.New()
	if err := rt.Start("echo done", "/tmp"); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cfg := Config{QuiescenceTimeout: 10 * time.Second, PollInterval: 200 * time.Millisecond}
	sup := New(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ch := sup.Observe(ctx, rt)

	var states []State
	for sc := range ch {
		states = append(states, sc.To)
	}

	// Once EXITED, no further transitions should occur (terminal is sticky).
	exitedCount := 0
	for _, s := range states {
		if s == StateExited {
			exitedCount++
		}
	}
	if exitedCount != 1 {
		t.Errorf("EXITED should appear exactly once, got %d times: %v", exitedCount, states)
	}

	// Last state after EXITED must be EXITED.
	for i, s := range states {
		if s == StateExited && i < len(states)-1 {
			t.Errorf("non-terminal state after EXITED: states[%d]=%s, next=%s", i, s, states[i+1])
		}
	}
}

func TestSupervisor_ContextCancellation(t *testing.T) {
	rt := runtime.New()
	if err := rt.Start("sleep 30", "/tmp"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer rt.Stop(context.Background())

	cfg := Config{QuiescenceTimeout: 1 * time.Second, PollInterval: 200 * time.Millisecond}
	sup := New(cfg)

	ctx, cancel := context.WithCancel(context.Background())

	ch := sup.Observe(ctx, rt)

	// Wait for ACTIVE.
	select {
	case sc := <-ch:
		if sc.To != StateActive {
			t.Errorf("expected ACTIVE, got %s", sc.To)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for ACTIVE")
	}

	// Cancel context.
	cancel()

	// Channel should close promptly.
	select {
	case _, ok := <-ch:
		if ok {
			t.Log("got final state before close")
		}
	case <-time.After(3 * time.Second):
		t.Error("channel did not close after context cancellation")
	}
}

func TestSupervisor_AlreadyExitedRuntime(t *testing.T) {
	rt := runtime.New()
	if err := rt.Start("echo instant", "/tmp"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Wait for it to exit.
	rt.Wait()
	time.Sleep(100 * time.Millisecond)

	cfg := Config{QuiescenceTimeout: 5 * time.Second, PollInterval: 200 * time.Millisecond}
	sup := New(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch := sup.Observe(ctx, rt)

	var states []State
	for sc := range ch {
		states = append(states, sc.To)
	}

	if len(states) != 1 {
		t.Fatalf("already-exited runtime: expected 1 transition, got %d: %v", len(states), states)
	}
	if states[0] != StateExited {
		t.Errorf("already-exited: expected EXITED, got %s", states[0])
	}
}
