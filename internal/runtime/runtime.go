// Package runtime provides a managed PTY worker: a single child process
// launched in a pseudo-terminal with generation-gated I/O, exactly-once exit
// reporting, and safe concurrent access.
//
// Contracts:
//   - Every Runtime instance owns one generation (monotonic, immutable after Start).
//   - Stale writers are rejected: Write/Interrupt/Stop after Close returns ErrRuntimeClosed.
//   - ExitEvent is stored once and readable by any number of Wait callers.
//   - All exported methods are safe for concurrent use.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
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

// New creates an unstarted Runtime.
func New() *Runtime {
	return &Runtime{
		done: make(chan struct{}),
	}
}

// NewWithID creates an unstarted Runtime with a caller-assigned identity.
func NewWithID(id string) *Runtime {
	r := New()
	r.id = id
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
	r.gen++

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
		r.cmd = cmd2
		r.ptmx = ptmx
	} else {
		r.cmd = cmd
		r.ptmx = ptmx
	}

	r.cmd = cmd
	r.ptmx = ptmx
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
	if generation != 0 && generation != r.gen {
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
// The generation must match the current runtime.
func (r *Runtime) Interrupt(generation int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.started || r.cmd == nil || r.cmd.Process == nil {
		return ErrRuntimeClosed
	}
	if generation != 0 && generation != r.gen {
		return ErrStaleGeneration
	}
	if r.closed {
		return ErrRuntimeClosed
	}
	return signalProcessGroup(r.cmd.Process.Pid, syscall.SIGINT)
}

// Close terminates the child process and waits for it to exit.
// It sends SIGTERM, waits up to graceful seconds, then SIGKILL.
// Close is idempotent — repeated calls are safe.
// The generation must match the current runtime.
func (r *Runtime) Close(ctx context.Context, generation int64) error {
	r.mu.Lock()
	if !r.started {
		r.mu.Unlock()
		return nil // idempotent
	}
	if generation != 0 && generation != r.gen {
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

	// Close PTY first — this sends SIGHUP to the child if it's reading.
	if ptmx != nil {
		ptmx.Close()
	}

	// Cancel the context to stop output reader.
	if cancel != nil {
		cancel()
	}

	if cmd == nil || cmd.Process == nil {
		return nil
	}

	// Send SIGTERM first.
	signalProcessGroup(cmd.Process.Pid, syscall.SIGTERM)

	graceful := 5 * time.Second
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok {
			if remaining := time.Until(deadline); remaining < graceful && remaining > 0 {
				graceful = remaining
			}
		}
	}

	// Wait for exit.
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

// Stop is an alias for Close (backward compatibility).
func (r *Runtime) Stop(ctx context.Context) error {
	return r.Close(ctx, 0)
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
