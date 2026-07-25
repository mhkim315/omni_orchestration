// Package supervisor observes a Runtime's liveness and emits state transitions.
// It does not control the Runtime — it is a read-only observer.
//
// State machine:
//
//	ACTIVE ──(silence timeout)──→ QUIESCENT_CANDIDATE
//	ACTIVE ──(clean exit)───────→ EXITED
//	ACTIVE ──(non-zero exit)────→ CRASHED
//	QUIESCENT_CANDIDATE ──(new output)──→ ACTIVE
//	QUIESCENT_CANDIDATE ──(clean exit)──→ EXITED
//	QUIESCENT_CANDIDATE ──(non-zero exit)→ CRASHED
//
// EXITED and CRASHED are terminal states.
package supervisor

import (
	"context"
	"sync"
	"time"

	"github.com/miinanii/omni_orchestration/internal/runtime"
)

// State is a liveness state.
type State string

const (
	StateActive             State = "ACTIVE"
	StateQuiescentCandidate State = "QUIESCENT_CANDIDATE"
	StateExited             State = "EXITED"
	StateCrashed            State = "CRASHED"
	StateUnknown            State = "UNKNOWN"
)

// StateChange is emitted whenever the observed liveness state changes.
type StateChange struct {
	From       State
	To         State
	ObservedAt time.Time
}

// Config holds supervisor configuration.
type Config struct {
	// QuiescenceTimeout is the duration of output silence after which
	// a running process becomes a quiescence candidate.
	QuiescenceTimeout time.Duration

	// PollInterval is how often the supervisor checks for state changes.
	PollInterval time.Duration
}

// DefaultConfig returns the recommended configuration.
func DefaultConfig() Config {
	return Config{
		QuiescenceTimeout: 30 * time.Second,
		PollInterval:      5 * time.Second,
	}
}

// Supervisor observes a single Runtime and emits liveness state transitions.
type Supervisor struct {
	cfg      Config
	onChange func(StateChange)
}

// New creates a Supervisor with the given configuration.
func New(cfg Config) *Supervisor {
	if cfg.QuiescenceTimeout <= 0 {
		cfg.QuiescenceTimeout = 30 * time.Second
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	return &Supervisor{cfg: cfg}
}

// OnStateChange registers a callback invoked on every state transition.
// It is called synchronously from the Observe goroutine, so keep it fast.
func (s *Supervisor) OnStateChange(fn func(StateChange)) {
	s.onChange = fn
}

// Observe starts observing rt and returns a channel of state transitions.
// The channel is closed when the runtime reaches a terminal state (EXITED
// or CRASHED) and the supervisor has finished.
//
// Observe may be called at most once per Supervisor. The returned channel
// is buffered so the supervisor never blocks on a slow consumer.
func (s *Supervisor) Observe(ctx context.Context, rt *runtime.Runtime) <-chan StateChange {
	ch := make(chan StateChange, 16)

	go func() {
		defer close(ch)

		var (
			mu         sync.Mutex
			current    State = StateUnknown
			lastOutput time.Time
			exited     bool
			exitCode   int
		)

		emit := func(to State) {
			mu.Lock()
			from := current
			if from == to {
				mu.Unlock()
				return
			}
			// Terminal states are sticky.
			if from == StateExited || from == StateCrashed {
				mu.Unlock()
				return
			}
			current = to
			mu.Unlock()

			sc := StateChange{From: from, To: to, ObservedAt: time.Now()}

			select {
			case ch <- sc:
			case <-ctx.Done():
				return
			}

			if s.onChange != nil {
				s.onChange(sc)
			}
		}

		outputCh := rt.Observe()
		exitCh := rt.ExitEvent()

		// Bootstrap: if the runtime already exited, the exit channel is
		// closed. A successful non-blocking receive means the event is
		// waiting; a closed channel means the runtime already exited.
		select {
		case ev, ok := <-exitCh:
			if ok {
				exited = true
				exitCode = ev.ExitCode
			}
			// ok==false means the channel is closed: runtime already exited.
			// Use the ExitEvent from Wait() which is always available.
			if !ok {
				ev := rt.Wait()
				exited = true
				exitCode = ev.ExitCode
			}
		default:
		}

		if exited {
			if exitCode == 0 {
				emit(StateExited)
			} else {
				emit(StateCrashed)
			}
			return
		}

		// Runtime is running — emit ACTIVE.
		lastOutput = time.Now()
		emit(StateActive)

		ticker := time.NewTicker(s.cfg.PollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				emit(StateUnknown)
				return

			case _, ok := <-outputCh:
				if ok {
					// New output → reset silence timer, transition to ACTIVE.
					lastOutput = time.Now()
					emit(StateActive)
				}
				// If output channel is closed, the runtime is exiting.
				// Fall through to the exit check on the next tick.

			case ev, ok := <-exitCh:
				if ok {
					exited = true
					exitCode = ev.ExitCode
				}
				// Runtime exited — emit terminal state.
				if exitCode == 0 {
					emit(StateExited)
				} else {
					emit(StateCrashed)
				}
				return

			case <-ticker.C:
				mu.Lock()
				cur := current
				isExited := exited
				code := exitCode
				last := lastOutput
				mu.Unlock()

				if isExited {
					if code == 0 {
						emit(StateExited)
					} else {
						emit(StateCrashed)
					}
					return
				}

				// Check for quiescence.
				silence := time.Since(last)
				if cur == StateActive && silence >= s.cfg.QuiescenceTimeout {
					emit(StateQuiescentCandidate)
				}
				// QUIESCENT_CANDIDATE stays until new output or exit.
				// ACTIVE is restored when output arrives (handled above).
			}
		}
	}()

	return ch
}
