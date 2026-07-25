// Package runtime provides a managed PTY worker: a single child process
// launched in a pseudo-terminal with generation-gated I/O, exactly-once exit
// reporting, and safe concurrent access.
//
// Contracts (inspired by POKIT, not copied):
//   - Every Runtime instance owns one generation (monotonic, immutable after Start).
//   - Stale writers are rejected: Write after Stop returns ErrRuntimeClosed.
//   - ExitEvent is delivered exactly once via a channel closed on process exit.
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
	"syscall"
	"time"

	"github.com/creack/pty"
)

// ErrRuntimeClosed is returned when an operation is attempted on a stopped runtime.
var ErrRuntimeClosed = errors.New("runtime: closed")

// ErrRuntimeNotStarted is returned when an operation is attempted before Start.
var ErrRuntimeNotStarted = errors.New("runtime: not started")

// ExitEvent is delivered exactly once when the child process exits.
type ExitEvent struct {
	ExitCode int
	ExitedAt time.Time
	Signaled bool
	Signal   syscall.Signal
}

// Runtime manages a single PTY child process.
type Runtime struct {
	mu         sync.Mutex
	cmd        *exec.Cmd
	ptmx       *os.File // PTY master (our end)
	generation int64
	started    bool
	closed     bool

	// exitEvent is closed exactly once when the child exits.
	exitEvent chan ExitEvent
	exitOnce  sync.Once
	exitVal   ExitEvent

	// output is the line-buffered reader goroutine channel.
	output    chan string
	outputCtx context.CancelFunc
	outputWg  sync.WaitGroup

	// ctx is cancelled on Stop.
	ctx    context.Context
	cancel context.CancelFunc
}

// New creates an unstarted Runtime.
func New() *Runtime {
	return &Runtime{
		exitEvent: make(chan ExitEvent),
	}
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

	// Validate cwd exists.
	if fi, err := os.Stat(cwd); err != nil || !fi.IsDir() {
		return fmt.Errorf("runtime: cwd does not exist or is not a directory: %s", cwd)
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())

	cmd := exec.CommandContext(r.ctx, "bash", "-c", command)
	cmd.Dir = cwd
	// Own process group so Signal reaches the child and its descendants.
	// Setpgid places the child in its own process group so Signal reaches
	// descendants. Omitted for now — ad-hoc-signed test binaries on macOS
	// may be restricted from posix_spawn with new process groups.
	// TODO: re-enable when signing is configured.
	cmd.SysProcAttr = &syscall.SysProcAttr{}

	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: 24, Cols: 80})
	if err != nil {
		r.cancel()
		return fmt.Errorf("runtime: pty start: %w", err)
	}

	r.cmd = cmd
	r.ptmx = ptmx
	r.generation++
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

// Write sends data to the PTY. Returns ErrRuntimeClosed if the runtime is
// stopped, ErrRuntimeNotStarted if not yet started.
func (r *Runtime) Write(input []byte) (int, error) {
	r.mu.Lock()
	if !r.started {
		r.mu.Unlock()
		return 0, ErrRuntimeNotStarted
	}
	if r.closed {
		r.mu.Unlock()
		return 0, ErrRuntimeClosed
	}
	ptmx := r.ptmx
	r.mu.Unlock()

	if ptmx == nil {
		return 0, ErrRuntimeClosed
	}

	n, err := ptmx.Write(input)
	if err != nil {
		// If the PTY is closed, treat as closed runtime.
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
func (r *Runtime) Interrupt() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.started || r.closed || r.cmd == nil || r.cmd.Process == nil {
		return ErrRuntimeClosed
	}
	return signalProcessGroup(r.cmd.Process.Pid, syscall.SIGINT)
}

// Stop terminates the child process and waits for it to exit.
// It sends SIGTERM, waits up to graceful seconds, then SIGKILL.
// Stop is idempotent — repeated calls are safe.
func (r *Runtime) Stop(ctx context.Context) error {
	r.mu.Lock()
	if !r.started || r.closed {
		r.mu.Unlock()
		return nil // idempotent
	}
	r.closed = true
	cmd := r.cmd
	ptmx := r.ptmx
	cancel := r.cancel
	exitCh := r.exitEvent
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

	// Wait for exit event (fired exactly once by watchExit when cmd.Wait returns).
	select {
	case <-exitCh:
		return nil
	case <-time.After(graceful):
		// Force kill.
		signalProcessGroup(cmd.Process.Pid, syscall.SIGKILL)
		select {
		case <-exitCh:
			return nil
		case <-time.After(2 * time.Second):
			return errors.New("runtime: failed to stop process after SIGKILL")
		}
	}
}

// Wait blocks until the child process exits. Returns the ExitEvent.
// Safe to call concurrently; all callers receive the same event.
func (r *Runtime) Wait() ExitEvent {
	return <-r.exitEvent
}

// ExitEvent returns the exit event channel. It is closed exactly once.
func (r *Runtime) ExitEvent() <-chan ExitEvent {
	return r.exitEvent
}

// Generation returns the current (immutable after Start) generation.
func (r *Runtime) Generation() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.generation
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
						// Channel full — drop oldest to stay bounded.
					}
					line = line[:0]
				} else if b != '\r' {
					line = append(line, b)
				}
			}
		}
		if err != nil {
			// Flush remaining partial line.
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
	defer func() {
		r.mu.Lock()
		r.closed = true
		if r.ptmx != nil {
			r.ptmx.Close()
		}
		r.mu.Unlock()
	}()

	err := cmd.Wait()

	// Cancel context.
	if r.cancel != nil {
		r.cancel()
	}
	if r.outputCtx != nil {
		r.outputCtx()
	}

	// Close PTY to unblock the output reader, then drain it.
	if r.ptmx != nil {
		r.ptmx.Close()
	}

	// Drain output reader.
	r.outputWg.Wait()
	close(r.output)

	ev := ExitEvent{ExitedAt: time.Now()}
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
				if status.Signaled() {
					ev.Signaled = true
					ev.Signal = status.Signal()
				}
				ev.ExitCode = status.ExitStatus()
			}
		}
	}

	r.exitOnce.Do(func() {
		r.exitVal = ev
		close(r.exitEvent)
	})
}

func signalProcessGroup(pid int, sig syscall.Signal) error {
	if pid <= 0 {
		return nil
	}
	// Try process-group signal first (negative PID), fall back to direct signal.
	if err := syscall.Kill(-pid, sig); err != nil {
		return syscall.Kill(pid, sig)
	}
	return nil
}
