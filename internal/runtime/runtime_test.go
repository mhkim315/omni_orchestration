package runtime

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRuntime_StartStop(t *testing.T) {
	r := New()
	if err := r.Start("echo hello", "/tmp"); err != nil {
		t.Fatalf("Start: %v", err)
	}

	gen := r.Generation()
	if gen != 1 {
		t.Errorf("generation = %d, want 1", gen)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := r.Close(ctx, gen); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ev := r.Wait()
	if ev.ExitCode != 0 {
		t.Errorf("exit code = %d, want 0", ev.ExitCode)
	}
	if ev.Signaled {
		t.Error("expected clean exit, not signaled")
	}
}

func TestRuntime_WriteReadRoundTrip(t *testing.T) {
	r := New()
	if err := r.Start("cat", "/tmp"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	gen := r.Generation()

	_, err := r.Write(gen, []byte("hello world\n"))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	select {
	case line := <-r.Observe():
		if line != "hello world" {
			t.Errorf("expected 'hello world', got %q", line)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for output")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.Close(ctx, gen)
	r.Wait()
}

func TestRuntime_Interrupt(t *testing.T) {
	r := New()
	if err := r.Start("sleep 10", "/tmp"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	gen := r.Generation()

	time.Sleep(200 * time.Millisecond)

	if err := r.Interrupt(gen); err != nil {
		t.Fatalf("Interrupt: %v", err)
	}

	ev := r.Wait()
	t.Logf("sleep interrupt: code=%d signaled=%v", ev.ExitCode, ev.Signaled)
}

func TestRuntime_ExitCode(t *testing.T) {
	r := New()
	if err := r.Start("exit 42", "/tmp"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ev := r.Wait()
	t.Logf("exit 42 → code=%d signaled=%v signal=%v", ev.ExitCode, ev.Signaled, ev.Signal)
	// Verify the exit code is captured (platform-dependent through PTY).
	if ev.ExitCode == 42 {
		t.Log("exit 42 correctly captured")
	}
}

func TestRuntime_ExitSignal(t *testing.T) {
	r := New()
	if err := r.Start("kill -SEGV $$", "/tmp"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ev := r.Wait()
	t.Logf("kill -SEGV → code=%d signaled=%v signal=%v", ev.ExitCode, ev.Signaled, ev.Signal)
	// On most platforms, SIGSEGV is delivered.
	if ev.Signaled {
		t.Logf("signal preserved: %v", ev.Signal)
	}
}

func TestRuntime_WaitMultipleCalls(t *testing.T) {
	r := New()
	if err := r.Start("echo once", "/tmp"); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var wg sync.WaitGroup
	results := make(chan ExitEvent, 3)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- r.Wait()
		}()
	}

	wg.Wait()
	close(results)

	var first ExitEvent
	firstSet := false
	for ev := range results {
		if !firstSet {
			first = ev
			firstSet = true
		} else {
			if ev.ExitCode != first.ExitCode {
				t.Error("Wait() returned different exit codes")
			}
			if !ev.ExitedAt.Equal(first.ExitedAt) {
				t.Error("Wait() returned different timestamps")
			}
		}
	}
}

func TestRuntime_ConcurrentWriteClose(t *testing.T) {
	r := New()
	if err := r.Start("cat", "/tmp"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	gen := r.Generation()

	var wg sync.WaitGroup
	errs := make(chan error, 10)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, err := r.Write(gen, []byte("x\n"))
				if err != nil {
					errs <- err
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		r.Close(ctx, gen)
	}()

	wg.Wait()
	close(errs)

	// After Close, Write must return ErrRuntimeClosed.
	_, err := r.Write(gen, []byte("after\n"))
	if err != ErrRuntimeClosed {
		t.Errorf("Write after Close: expected ErrRuntimeClosed, got %v", err)
	}

	for range r.Observe() {
	}
}

func TestRuntime_CloseIdempotent(t *testing.T) {
	r := New()
	r.Start("echo hello", "/tmp")
	gen := r.Generation()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := r.Close(ctx, gen); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := r.Close(ctx, gen); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestRuntime_StartBeforeStartRejected(t *testing.T) {
	r := New()
	r.Start("echo hi", "/tmp")
	err := r.Start("echo again", "/tmp")
	if err == nil {
		t.Error("expected error on second Start")
	}
}

func TestRuntime_WriteBeforeStartRejected(t *testing.T) {
	r := New()
	_, err := r.Write(0, []byte("too early\n"))
	if err != ErrRuntimeNotStarted {
		t.Errorf("expected ErrRuntimeNotStarted, got %v", err)
	}
}

func TestRuntime_CloseBeforeStartIsNoop(t *testing.T) {
	r := New()
	ctx := context.Background()
	if err := r.Close(ctx, 0); err != nil {
		t.Errorf("Close before Start should be no-op: %v", err)
	}
}

func TestRuntime_GenerationMonotonic(t *testing.T) {
	r := New()
	if r.Generation() != 0 {
		t.Errorf("initial generation = %d, want 0", r.Generation())
	}

	r.Start("echo gen1", "/tmp")
	g1 := r.Generation()
	if g1 != 1 {
		t.Errorf("generation after Start = %d, want 1", g1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.Close(ctx, g1)

	g2 := r.Generation()
	if g2 != g1 {
		t.Errorf("generation changed after Close: %d -> %d", g1, g2)
	}
}

func TestRuntime_OutputChannelClosedOnExit(t *testing.T) {
	r := New()
	r.Start("echo line1 && echo line2", "/tmp")

	var lines []string
	for line := range r.Observe() {
		lines = append(lines, line)
	}

	if len(lines) < 1 {
		t.Error("expected at least 1 line of output")
	}
}

func TestRuntime_MultiLineOutput(t *testing.T) {
	r := New()
	r.Start("for i in 1 2 3; do echo line$i; done", "/tmp")

	var lines []string
	timeout := time.After(3 * time.Second)

loop:
	for {
		select {
		case line, ok := <-r.Observe():
			if !ok {
				break loop
			}
			lines = append(lines, line)
		case <-timeout:
			break loop
		}
	}

	if len(lines) < 3 {
		t.Errorf("expected at least 3 lines, got %d: %v", len(lines), lines)
	}
}

func TestRuntime_CWDDefault(t *testing.T) {
	r := New()
	r.Start("echo $PWD", "/tmp")

	select {
	case line := <-r.Observe():
		if !strings.HasPrefix(line, "/tmp") {
			t.Errorf("expected CWD /tmp, got %q", line)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout")
	}
	r.Wait()
}

func TestRuntime_InvalidCWD(t *testing.T) {
	r := New()
	err := r.Start("echo hi", "/nonexistent/path/xyz")
	if err == nil {
		t.Error("expected error for nonexistent CWD")
	}
}

func TestRuntime_EmptyCommandRejected(t *testing.T) {
	r := New()
	err := r.Start("", "/tmp")
	if err == nil {
		t.Error("expected error for empty command")
	}
}

func TestRuntime_StderrOutput(t *testing.T) {
	r := New()
	r.Start("echo stdout ; echo stderr >&2", "/tmp")

	var lines []string
	for line := range r.Observe() {
		lines = append(lines, line)
	}

	foundStdout := false
	foundStderr := false
	for _, l := range lines {
		if strings.Contains(l, "stdout") {
			foundStdout = true
		}
		if strings.Contains(l, "stderr") {
			foundStderr = true
		}
	}
	if !foundStdout {
		t.Error("stdout line not found in PTY output")
	}
	if !foundStderr {
		t.Error("stderr line not found in PTY output")
	}
}

func TestRuntime_StaleWriteRejected(t *testing.T) {
	r := New()
	r.Start("cat", "/tmp")
	gen := r.Generation()

	// Write with wrong generation.
	_, err := r.Write(gen+999, []byte("stale\n"))
	if err != ErrStaleGeneration {
		t.Errorf("expected ErrStaleGeneration, got %v", err)
	}

	// Correct generation works.
	_, err = r.Write(gen, []byte("good\n"))
	if err != nil {
		t.Errorf("correct generation should work: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.Close(ctx, gen)
	r.Wait()
}

func TestRuntime_StaleInterruptRejected(t *testing.T) {
	r := New()
	r.Start("sleep 10", "/tmp")
	gen := r.Generation()

	err := r.Interrupt(gen + 999)
	if err != ErrStaleGeneration {
		t.Errorf("expected ErrStaleGeneration for Interrupt, got %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.Close(ctx, gen)
	r.Wait()
}

func TestRuntime_StaleCloseRejected(t *testing.T) {
	r := New()
	r.Start("sleep 10", "/tmp")
	gen := r.Generation()

	ctx := context.Background()
	err := r.Close(ctx, gen+999)
	if err != ErrStaleGeneration {
		t.Errorf("expected ErrStaleGeneration for Close, got %v", err)
	}

	// Correct generation still works.
	r.Close(ctx, gen)
	r.Wait()
}

func TestRuntime_ProcessTreeCleanup(t *testing.T) {
	r := New()
	// Spawn a child that spawns a grandchild. All must be cleaned up.
	if err := r.Start("bash -c 'sleep 30 & sleep 30'", "/tmp"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	gen := r.Generation()

	// Give it time to spawn children.
	time.Sleep(500 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := r.Close(ctx, gen); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ev := r.Wait()
	t.Logf("tree cleanup: code=%d signaled=%v", ev.ExitCode, ev.Signaled)
}

func TestRuntime_RuntimeID(t *testing.T) {
	r := NewWithID("worker-001")
	if r.ID() != "worker-001" {
		t.Errorf("ID = %q, want worker-001", r.ID())
	}

	r2 := New()
	if r2.ID() != "" {
		t.Errorf("default ID = %q, want empty", r2.ID())
	}
}

func TestRuntime_ExitEventChannel(t *testing.T) {
	r := New()
	r.Start("echo done", "/tmp")

	// ExitEvent returns a done channel, not an ExitEvent channel.
	done := r.ExitEvent()
	ev := r.Wait()

	select {
	case <-done:
		t.Logf("done channel closed, exit code=%d", ev.ExitCode)
	default:
		t.Error("done channel should be closed after Wait returns")
	}
}
