// Package runtime provides a managed PTY worker: a single child process
// launched in a pseudo-terminal with generation-gated I/O, exactly-once exit
// reporting, and safe concurrent access.
//
// Contracts:
//   - Every Runtime instance owns one generation (monotonic, immutable after Start).
//   - Stale writers are rejected: Write/Interrupt/Close after Close returns ErrRuntimeClosed.
//   - ExitEvent is stored once and readable by any number of Wait callers.
//   - All exported methods are safe for concurrent use.
//   - Generation 0 is internal-only (New, Stop compat). Callers use the generation
//     returned by Start or assigned via NewWithID.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/creack/pty"
)

// ErrRuntimeClosed is returned when an operation is attempted on a stopped runtime.
var ErrRuntimeClosed = errors.New("runtime: closed")

// ErrRuntimeNotStarted is returned when an operation is attempted before Start.
var ErrRuntimeNotStarted = errors.New("runtime: not started")

// ErrStaleGeneration is returned when the generation does not match the current runtime.
var ErrStaleGeneration = errors.New("runtime: stale generation")

// ExitEvent is delivered exactly once when the child process exits.
type ExitEvent struct {
	ExitCode int
	ExitedAt time.Time
	Signaled bool
	Signal   syscall.Signal
}

// Runtime manages a single PTY child process.
type Runtime struct {
	mu      sync.Mutex
	cmd     *exec.Cmd
	ptmx    *os.File // PTY master (our end)
	gen     int64
	id      string
	started bool
	closed  bool

	// done is closed exactly once when the child exits.
	done     chan struct{}
	doneOnce sync.Once
	exitVal  ExitEvent

	// output is the line-buffered reader goroutine channel.
	output    chan string
	outputCtx context.CancelFunc
	outputWg  sync.WaitGroup

	// ctx is cancelled on Close.
	ctx    context.Context
	cancel context.CancelFunc
}

// New creates an unstarted Runtime with generation 0.
// generation 0 is reserved — it bypasses generation checks only in the
// Start method (which assigns gen=1) and Stop alias (which delegates to
// Close with gen=0 for backward compat).
func New() *Runtime {
	return &Runtime{
		done: make(chan struct{}),
	}
}

// NewWithID creates an unstarted Runtime with a caller-assigned identity
// and a pre-assigned generation (must be > 0). All subsequent operations
// must present this generation.
func NewWithID(id string, generation int64) *Runtime {
	if generation < 1 {
		generation = 1
	}
	r := &Runtime{
		done: make(chan struct{}),
		id:   id,
		gen:  generation,
	}
	return r
}

// ID returns the runtime identity.
func (r *Runtime) ID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.id
}

// Start spawns the command in a PTY. The working directory may be empty
// (defaults to "/"). Returns an error if already started.
// Start assigns generation 1 if the runtime was created with New() (gen==0).
func (r *Runtime) Start(command string, cwd string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.started {
		return errors.New("runtime: already started")
	}
	if r.closed {
		return ErrRuntimeClosed
	}
	if command == "" {
		return errors.New("runtime: command is required")
	}
	if cwd == "" {
		cwd = "/"
	}

	if fi, err := os.Stat(cwd); err != nil || !fi.IsDir() {
		return fmt.Errorf("runtime: cwd does not exist or is not a directory: %s", cwd)
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())

	// Assign generation on first Start. NewWithID pre-assigns; New
	// starts at 1.
	if r.gen == 0 {
		r.gen = 1
	}

	cmd := exec.Command("bash", "-c", command)
	cmd.Dir = cwd

	// Place the child in its own process group so signalProcessGroup
	// reaches descendants. Setpgid may be restricted on ad-hoc-signed
	// test binaries (macOS); in that case direct signaling applies.
	//
	// We try with Setpgid; if the platform rejects it, we discard the
	// first cmd (Start may have partially executed) and retry without.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: 24, Cols: 80})
	if err != nil {
		// Platform restriction — retry without Setpgid.
		// We intentionally discard the first cmd; StartWithSize guarantees
		// that if it returns an error, the child was not spawned.
		cmd2 := exec.Command("bash", "-c", command)
		cmd2.Dir = cwd
		cmd2.SysProcAttr = &syscall.SysProcAttr{}
		ptmx, err = pty.StartWithSize(cmd2, &pty.Winsize{Rows: 24, Cols: 80})
		if err != nil {
			r.cancel()
			return fmt.Errorf("runtime: pty start: %w", err)
		}
		// R1-A Fix 1: keep cmd2 as the owned process, not the failed cmd.
		r.cmd = cmd2
		r.ptmx = ptmx
	} else {
		r.cmd = cmd
		r.ptmx = ptmx
	}

	r.started = true

	// Start the output reader goroutine.
	oCtx, oCancel := context.WithCancel(r.ctx)
	r.outputCtx = oCancel
	r.output = make(chan string, 64)
	r.outputWg.Add(1)
	go r.readOutput(oCtx, ptmx)

	// Start the exit watcher goroutine.
	go r.watchExit()

	return nil
}

// Write sends data to the PTY. The generation must match the current runtime.
// Returns ErrRuntimeClosed if stopped, ErrStaleGeneration if generation mismatches.
// generation must be > 0 (no zero bypass).
func (r *Runtime) Write(generation int64, input []byte) (int, error) {
	r.mu.Lock()
	if !r.started {
		r.mu.Unlock()
		return 0, ErrRuntimeNotStarted
	}
	if r.closed {
		r.mu.Unlock()
		return 0, ErrRuntimeClosed
	}
	if generation != r.gen {
		r.mu.Unlock()
		return 0, ErrStaleGeneration
	}
	ptmx := r.ptmx
	r.mu.Unlock()

	if ptmx == nil {
		return 0, ErrRuntimeClosed
	}

	n, err := ptmx.Write(input)
	if err != nil {
		if errors.Is(err, io.ErrClosedPipe) || errors.Is(err, os.ErrClosed) {
			return n, ErrRuntimeClosed
		}
	}
	return n, err
}

// Observe returns a channel that receives line-buffered PTY output.
// The channel is closed when the runtime stops.
func (r *Runtime) Observe() <-chan string {
	return r.output
}

// Interrupt sends SIGINT to the child process group.
// The generation must match the current runtime (must be > 0).
func (r *Runtime) Interrupt(generation int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.started || r.cmd == nil || r.cmd.Process == nil {
		return ErrRuntimeClosed
	}
	if generation != r.gen {
		return ErrStaleGeneration
	}
	if r.closed {
		return ErrRuntimeClosed
	}
	return signalProcessGroup(r.cmd.Process.Pid, syscall.SIGINT)
}

// Close terminates the child process and waits for it to exit.
// It closes the PTY (SIGHUP), cancels the context, and waits for
// watchExit to report. If the process doesn't exit within the grace
// period, SIGKILL is sent.
// Close is idempotent — repeated calls are safe.
// generation must match (no zero bypass except via Stop alias).
func (r *Runtime) Close(ctx context.Context, generation int64) error {
	r.mu.Lock()
	if !r.started {
		r.mu.Unlock()
		return nil // idempotent (never started)
	}
	if generation != r.gen {
		r.mu.Unlock()
		return ErrStaleGeneration
	}
	if r.closed {
		r.mu.Unlock()
		return nil // idempotent
	}
	r.closed = true
	cmd := r.cmd
	ptmx := r.ptmx
	cancel := r.cancel
	done := r.done
	r.mu.Unlock()

	// If already exited, return immediately — don't close PTY
	// (the process already captured its exit code). Use a short
	// timeout to avoid racing with watchExit's done close.
	select {
	case <-done:
		return nil
	case <-time.After(200 * time.Millisecond):
		// Not exited yet — proceed with PTY close.
	}

	// Close PTY — sends SIGHUP to the child if it's reading.
	if ptmx != nil {
		ptmx.Close()
	}

	// Cancel context to stop output reader.
	if cancel != nil {
		cancel()
	}

	if cmd == nil || cmd.Process == nil {
		return nil
	}

	graceful := 5 * time.Second
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok {
			if remaining := time.Until(deadline); remaining < graceful && remaining > 0 {
				graceful = remaining
			}
		}
	}

	select {
	case <-done:
		return nil
	case <-time.After(graceful):
		// Force kill.
		signalProcessGroup(cmd.Process.Pid, syscall.SIGKILL)
		select {
		case <-done:
			return nil
		case <-time.After(2 * time.Second):
			return errors.New("runtime: failed to stop process after SIGKILL")
		}
	}
}

// Stop is a backward-compatible alias for Close that uses generation 0.
// generation 0 is only accepted from Stop; direct Close callers must
// present the correct generation.
func (r *Runtime) Stop(ctx context.Context) error {
	r.mu.Lock()
	gen := r.gen
	started := r.started
	closed := r.closed
	r.mu.Unlock()

	if !started || closed {
		return nil
	}
	return r.Close(ctx, gen)
}

// Wait blocks until the child process exits. Returns the stored ExitEvent.
// Safe to call concurrently; all callers receive the same event.
func (r *Runtime) Wait() ExitEvent {
	<-r.done
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.exitVal
}

// ExitEvent returns a channel that is closed when the child exits.
// Use Wait() to get the stored ExitEvent value.
func (r *Runtime) ExitEvent() <-chan struct{} {
	return r.done
}

// Generation returns the current (immutable after Start) generation.
func (r *Runtime) Generation() int64 {
	return atomic.LoadInt64(&r.gen)
}

// PID returns the OS process ID of the child, or 0 if not started.
func (r *Runtime) PID() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cmd != nil && r.cmd.Process != nil {
		return r.cmd.Process.Pid
	}
	return 0
}

// AttachIdentity holds verified process identity for recovery attach.
type AttachIdentity struct {
	PID          int
	Executable   string
	CWD          string
	StartTimeMs  int64 // stored ms from WorkerRecord
	StartTimeSec int64 // verified seconds from ps lstart
	PGID         int
}

// Attach verifies the given PID is alive, validates its identity against
// stored values, and initializes the Runtime as an attached observer.
// Stored generation is used (not incremented). Identity mismatch → fail-closed.
func (r *Runtime) Attach(pid int, id AttachIdentity, generation int64) error {
	if pid <= 0 {
		return ErrRuntimeNotStarted
	}
	if err := syscall.Kill(pid, 0); err != nil {
		return fmt.Errorf("runtime: attach pid %d not alive: %w", pid, err)
	}

	// Verify identity via ps.
	verified, err := verifyAttachIdentity(pid)
	if err != nil {
		return fmt.Errorf("runtime: attach verify pid %d: %w", pid, err)
	}

	// Fix 1: commandMatches exact-path or full-command, not substring.
	if id.Executable != "" && !commandMatches(id.Executable, verified.Executable) {
		return fmt.Errorf("runtime: attach pid %d cmd mismatch: stored=%q actual=%q", pid, id.Executable, verified.Executable)
	}
	// Fix 1: stored StartTime is ms, verified is seconds. Compare as integers.
	if id.StartTimeMs > 0 && verified.StartTimeSec > 0 {
		storedSec := id.StartTimeMs / 1000
		if storedSec != verified.StartTimeSec {
			return fmt.Errorf("runtime: attach pid %d start-time mismatch: stored=%d actual=%d", pid, storedSec, verified.StartTimeSec)
		}
	}
	// Fix 2: compare CWD if available.
	if id.CWD != "" && verified.CWD != "" && id.CWD != verified.CWD {
		return fmt.Errorf("runtime: attach pid %d cwd mismatch: stored=%q actual=%q", pid, id.CWD, verified.CWD)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.started = true
	r.gen = generation // Fix 4: use stored generation, don't increment
	r.id = fmt.Sprintf("attached-%d", pid)
	r.cmd = &exec.Cmd{Process: &os.Process{Pid: pid}}
	r.output = make(chan string, 1)
	r.outputCtx = func() {}

	r.outputWg.Add(1)
	go r.watchAttached(pid)
	return nil
}

// commandMatches checks if the stored command matches the process comm.
// Strips args from stored (e.g. "claude -p --output-format json" → "claude").
// Compares base name against actual comm (stripping leading "-").
// "/usr/local/bin/claude" matches "claude". "bash" matches "-bash".
func commandMatches(stored, actual string) bool {
	// Strip args — only compare executable name.
	if idx := strings.Index(stored, " "); idx > 0 {
		stored = stored[:idx]
	}
	storedBase := filepath.Base(strings.TrimSpace(stored))
	actual = strings.TrimPrefix(strings.TrimSpace(actual), "-")
	return storedBase == actual
}

// verifyAttachIdentity identifies a process via ps with unix-format start time.
func verifyAttachIdentity(pid int) (AttachIdentity, error) {
	// Fix 1: lstart unix format for start-time comparison (seconds since epoch).
	cmd := exec.Command("ps", "-p", fmt.Sprintf("%d", pid), "-o", "lstart=,comm=")
	out, err := cmd.Output()
	if err != nil {
		return AttachIdentity{}, fmt.Errorf("ps: %w", err)
	}
	fields := strings.Fields(strings.TrimSpace(string(out)))
	if len(fields) < 6 {
		return AttachIdentity{}, fmt.Errorf("ps: unexpected output: %q", string(out))
	}
	// Fix 1: lstart is LOCAL time, parse in local location, truncate to seconds.
	startStr := strings.Join(fields[:5], " ")
	t, err := time.ParseInLocation("Mon Jan 2 15:04:05 2006", startStr, time.Local)
	var startSec int64
	if err == nil {
		startSec = t.Unix() // seconds, for comparison with ms/1000
	}
	executable := ""
	if len(fields) >= 6 {
		executable = fields[5]
	}
	return AttachIdentity{PID: pid, Executable: executable, StartTimeSec: startSec}, nil
}

// watchAttached polls kill(pid,0) until the process exits.
// Once dead, emits ExitEvent and closes the done channel.
// For attached processes, exit code is -1 (unknown — not our child).
func (r *Runtime) watchAttached(pid int) {
	defer r.outputWg.Done()
	defer close(r.output)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			if err := syscall.Kill(pid, 0); err != nil {
				ev := ExitEvent{ExitedAt: time.Now(), ExitCode: -1}
				r.doneOnce.Do(func() { r.exitVal = ev; close(r.done) })
				r.mu.Lock()
				r.closed = true
				r.mu.Unlock()
				return
			}
		}
	}
}

// ── Internal ──

func (r *Runtime) readOutput(ctx context.Context, rdr io.Reader) {
	defer r.outputWg.Done()
	buf := make([]byte, 4096)
	var line []byte
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		n, err := rdr.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			for _, b := range chunk {
				if b == '\n' {
					select {
					case r.output <- string(line):
					default:
					}
					line = line[:0]
				} else if b != '\r' {
					line = append(line, b)
				}
			}
		}
		if err != nil {
			if len(line) > 0 {
				select {
				case r.output <- string(line):
				default:
				}
			}
			return
		}
	}
}

func (r *Runtime) watchExit() {
	cmd := r.cmd
	if cmd == nil {
		return
	}
	cmd.Wait()

	// Cancel context to stop output reader.
	if r.cancel != nil {
		r.cancel()
	}
	if r.outputCtx != nil {
		r.outputCtx()
	}

	// Close PTY to unblock the output reader.
	r.mu.Lock()
	if r.ptmx != nil {
		r.ptmx.Close()
	}
	r.closed = true
	r.mu.Unlock()

	// Drain output reader.
	r.outputWg.Wait()
	close(r.output)

	// Build the exit event. Use ProcessState.ExitCode() when available
	// (Go 1.12+), falling back to WaitStatus for signal detection.
	ev := ExitEvent{ExitedAt: time.Now()}
	if cmd.ProcessState != nil {
		ev.ExitCode = cmd.ProcessState.ExitCode()
		if ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok {
			if ws.Signaled() {
				ev.Signaled = true
				ev.Signal = ws.Signal()
			}
		}
	}

	r.doneOnce.Do(func() {
		r.exitVal = ev
		close(r.done)
	})
}

func signalProcessGroup(pid int, sig syscall.Signal) error {
	if pid <= 0 {
		return nil
	}
	// Try process-group signal first (negative PID), fall back to direct.
	if err := syscall.Kill(-pid, sig); err != nil {
		return syscall.Kill(pid, sig)
	}
	return nil
}
