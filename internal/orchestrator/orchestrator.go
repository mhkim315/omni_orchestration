// Package orchestrator integrates worktree, runtime, supervisor, and taskstore
// into a coordinated task-execution loop with quiescence-driven wake messages.
//
// R1-B: VALIDATE runs actual validator, CONTINUE with lease timer,
// unified finalizeAttempt for all exit paths, retry limit.
package orchestrator

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/miinanii/omni_orchestration/internal/runtime"
	"github.com/miinanii/omni_orchestration/internal/supervisor"
	"github.com/miinanii/omni_orchestration/internal/taskstore"
	"github.com/miinanii/omni_orchestration/internal/worktree"
)

const defaultMaxAttempts = 3
const continueLeaseTimeout = 30 * time.Second

type Config struct {
	Repo        string
	Task        string
	Command     string
	CWD         string
	Validator   string
	StorePath   string
	MaxAttempts int // 0 = default (3)
}

type WakeMessage struct {
	AttemptID  string    `json:"attempt_id"`
	State      string    `json:"state"`
	Message    string    `json:"message"`
	Checkpoint string    `json:"checkpoint,omitempty"`
	Dirty      bool      `json:"dirty"`
	Timestamp  time.Time `json:"timestamp"`
}

func (w WakeMessage) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Executor attempt %s became quiescent.\n", w.AttemptID))
	sb.WriteString(fmt.Sprintf("Evidence: runtime %s", strings.ToLower(w.State)))
	if w.Dirty {
		sb.WriteString(", worktree modified")
	}
	if w.Checkpoint != "" {
		sb.WriteString(fmt.Sprintf(", checkpoint %s", w.Checkpoint))
	}
	sb.WriteString(".\n")
	sb.WriteString("Choose: VALIDATE / CONTINUE / RETRY_CLEAN / REPLACE / FAIL\n")
	return sb.String()
}

type Decision string

const (
	DecisionValidate   Decision = "VALIDATE"
	DecisionContinue   Decision = "CONTINUE"
	DecisionRetryClean Decision = "RETRY_CLEAN"
	DecisionReplace    Decision = "REPLACE"
	DecisionFail       Decision = "FAIL"
)

// attemptState holds the live state of one attempt.
type attemptState struct {
	info       *worktree.WorktreeInfo
	rt         *runtime.Runtime
	attempt    *taskstore.Attempt
	worker     *taskstore.WorkerRecord
	checkpoint string
}

type runContext struct {
	ctx       context.Context
	cfg       Config
	store     *taskstore.Store
	wt        *worktree.Manager
	runID     int64
	taskID    int64
	decisions []Decision
	maxAtts   int
}

// Run executes the full orchestrator flow.
func Run(ctx context.Context, cfg Config, store *taskstore.Store, wt *worktree.Manager) ([]Decision, error) {
	if cfg.Repo == "" || cfg.Task == "" || cfg.Command == "" {
		return nil, fmt.Errorf("repo, task, and command are required")
	}
	maxAtts := cfg.MaxAttempts
	if maxAtts <= 0 {
		maxAtts = defaultMaxAttempts
	}

	// Gate 3: recover stale in-progress attempts.
	existing, _ := store.GetActiveAttempts()
	if len(existing) > 0 {
		log.Printf("Gate-3 recovery: %d in-progress attempts cancelled", len(existing))
		for _, a := range existing {
			store.UpdateAttemptStatus(a.ID, taskstore.StatusCancelled)
		}
	}

	run, err := store.CreateRun()
	if err != nil {
		return nil, fmt.Errorf("create run: %w", err)
	}
	task, err := store.CreateTask(run.ID, cfg.Task)
	if err != nil {
		return nil, fmt.Errorf("create task: %w", err)
	}

	rc := &runContext{
		ctx:     ctx,
		cfg:     cfg,
		store:   store,
		wt:      wt,
		runID:   run.ID,
		taskID:  task.ID,
		maxAtts: maxAtts,
	}

	attemptNum := 1
	baseCommit := "HEAD"

	for {
		select {
		case <-ctx.Done():
			return rc.decisions, ctx.Err()
		default:
		}

		// R1-B Fix 4: retry limit.
		if attemptNum > maxAtts {
			store.UpdateTaskStatus(task.ID, taskstore.StatusFailed)
			log.Printf("R1-B: max attempts (%d) exceeded", maxAtts)
			return rc.decisions, nil
		}

		// Create attempt + worktree + runtime.
		ast, err := rc.createAttempt(task.ID, attemptNum, baseCommit)
		if err != nil {
			return rc.decisions, err
		}

		// Observe.
		result := rc.observeAndWait(ast)

		// R1-B Fix 3: unified finalize.
		retry := rc.finalizeAttempt(ast, result)
		if !retry {
			return rc.decisions, nil
		}

		// Retry: new attempt.
		attemptNum++
		if result.decision == DecisionReplace {
			baseCommit = ast.info.Branch
		} else {
			baseCommit = "HEAD"
		}
	}
}

// observeResult captures the outcome of observing one attempt.
type observeResult struct {
	state      supervisor.State
	dirty      bool
	checkpoint string
	decision   Decision
	exited     bool
}

// createAttempt sets up worktree + runtime for one attempt.
func (rc *runContext) createAttempt(taskID int64, num int, baseCommit string) (*attemptState, error) {
	attemptID := fmt.Sprintf("T%d-A%d", taskID, num)

	info, err := rc.wt.Create(rc.cfg.Repo, fmt.Sprintf("T%d", taskID), fmt.Sprintf("%d", num))
	if err != nil {
		return nil, fmt.Errorf("create worktree: %w", err)
	}

	cwd := rc.cfg.CWD
	if cwd == "" {
		cwd = info.Path
	}

	attempt, err := rc.store.CreateAttempt(taskID, num, attemptID, info.Branch, baseCommit)
	if err != nil {
		rc.wt.Remove(info.Path)
		return nil, fmt.Errorf("create attempt: %w", err)
	}
	worker, err := rc.store.RecordWorker(attempt.ID, rc.cfg.Command, cwd, "primary", 1)
	if err != nil {
		rc.wt.Remove(info.Path)
		return nil, fmt.Errorf("record worker: %w", err)
	}

	rt := runtime.New()
	if err := rt.Start(rc.cfg.Command, cwd); err != nil {
		rc.store.EmitEvent(rc.runID, taskID, attempt.ID, worker.ID, "start_failed", nil)
		rc.wt.Remove(info.Path)
		return nil, fmt.Errorf("start runtime: %w", err)
	}
	rc.store.EmitEvent(rc.runID, taskID, attempt.ID, worker.ID, "worker_started", nil)

	// Send task prompt to worker stdin (Gate 2).
	if rc.cfg.Task != "" {
		rt.Write(1, []byte(rc.cfg.Task+"\n"))
	}

	return &attemptState{info: info, rt: rt, attempt: attempt, worker: worker}, nil
}

// observeAndWait watches the runtime until a decision point.
func (rc *runContext) observeAndWait(ast *attemptState) observeResult {
	supCfg := supervisor.DefaultConfig()
	supCfg.QuiescenceTimeout = 10 * time.Second
	supCfg.PollInterval = 2 * time.Second
	sup := supervisor.New(supCfg)
	stateCh := sup.Observe(rc.ctx, ast.rt)

	var result observeResult

	for sc := range stateCh {
		switch sc.To {
		case supervisor.StateActive:
			// Continue observing.

		case supervisor.StateQuiescentCandidate:
			result.dirty = wtIsDirty(rc.wt, ast.info.Path)
			result.checkpoint = recoverAndRecord(rc.ctx, ast.rt, rc.wt, ast.info.Path,
				ast.attempt.ID, rc.store, rc.runID, rc.taskID, ast.attempt.ID, ast.worker.ID, result.dirty)

			// R1-B Fix 1: VALIDATE runs actual validator.
			if rc.cfg.Validator != "" && result.dirty {
				if runValidator(rc.cfg.Validator, ast.info.Path) {
					rc.store.UpdateTaskStatus(rc.taskID, taskstore.StatusCompleted)
					rc.store.UpdateAttemptStatus(ast.attempt.ID, taskstore.StatusCompleted)
					rc.store.EmitEvent(rc.runID, rc.taskID, ast.attempt.ID, ast.worker.ID, "validated", nil)
					rc.wt.Remove(ast.info.Path)
					result.exited = true
					return result
				}
				// REJECT → retry.
				rc.store.EmitEvent(rc.runID, rc.taskID, ast.attempt.ID, ast.worker.ID, "validation_rejected", nil)
				result.decision = DecisionRetryClean
				return result
			}

			// No validator or clean worktree → wake message + decision.
			wake := WakeMessage{
				AttemptID:  fmt.Sprintf("T%d-A%d", rc.taskID, ast.attempt.Number),
				State:      string(sc.To),
				Checkpoint: result.checkpoint,
				Dirty:      result.dirty,
				Timestamp:  time.Now(),
			}
			fmt.Fprint(os.Stderr, wake.String())

			decision := awaitDecision(rc.ctx)
			rc.decisions = append(rc.decisions, decision)

			// R1-B Fix 1: VALIDATE → run validator immediately.
			if decision == DecisionValidate && rc.cfg.Validator != "" && result.dirty {
				if runValidator(rc.cfg.Validator, ast.info.Path) {
					rc.store.UpdateTaskStatus(rc.taskID, taskstore.StatusCompleted)
					rc.store.UpdateAttemptStatus(ast.attempt.ID, taskstore.StatusCompleted)
					rc.store.EmitEvent(rc.runID, rc.taskID, ast.attempt.ID, ast.worker.ID, "validated", nil)
					rc.wt.Remove(ast.info.Path)
					result.exited = true
					return result
				}
				rc.store.EmitEvent(rc.runID, rc.taskID, ast.attempt.ID, ast.worker.ID, "validation_rejected", nil)
				result.decision = DecisionRetryClean
				return result
			}

			// R1-B Fix 2: CONTINUE → lease timer, re-wake after timeout.
			if decision == DecisionContinue || decision == DecisionValidate {
				leaseTimer := time.NewTimer(continueLeaseTimeout)
				defer leaseTimer.Stop()

				select {
				case sc2, ok := <-stateCh:
					if !ok {
						result.exited = true
						return result
					}
					if sc2.To == supervisor.StateExited || sc2.To == supervisor.StateCrashed {
						result.state = sc2.To
						result.exited = true
						return result
					}
					// New state change — continue observing normally.
					continue
				case <-leaseTimer.C:
					// Lease expired — re-wake.
					fmt.Fprint(os.Stderr, wake.String())
					continue
				case <-rc.ctx.Done():
					result.exited = true
					return result
				}
			}

			// Other decisions.
			result.decision = decision
			return result

		case supervisor.StateExited:
			result.exited = true
			result.state = sc.To
			result.dirty = wtIsDirty(rc.wt, ast.info.Path)
			result.checkpoint = recoverAndRecord(rc.ctx, ast.rt, rc.wt, ast.info.Path,
				ast.attempt.ID, rc.store, rc.runID, rc.taskID, ast.attempt.ID, ast.worker.ID, result.dirty)
			return result

		case supervisor.StateCrashed:
			result.state = sc.To
			result.dirty = wtIsDirty(rc.wt, ast.info.Path)
			result.checkpoint = recoverAndRecord(rc.ctx, ast.rt, rc.wt, ast.info.Path,
				ast.attempt.ID, rc.store, rc.runID, rc.taskID, ast.attempt.ID, ast.worker.ID, result.dirty)
			rc.store.UpdateAttemptStatus(ast.attempt.ID, taskstore.StatusFailed)
			rc.store.EmitEvent(rc.runID, rc.taskID, ast.attempt.ID, ast.worker.ID, "worker_crashed", nil)
			supervisor.CleanupWorktree(rc.ctx, rc.wt, ast.info.Path)
			result.decision = awaitDecision(rc.ctx)
			rc.decisions = append(rc.decisions, result.decision)
			return result
		}
	}

	result.exited = true
	return result
}

// R1-B Fix 3: unified finalize for all exit/decision paths.
// Returns true if a retry should happen.
func (rc *runContext) finalizeAttempt(ast *attemptState, result observeResult) bool {
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	ast.rt.Stop(stopCtx)
	cancel()

	if result.decision == DecisionFail || result.decision == DecisionRetryClean || result.decision == DecisionReplace {
		rc.store.UpdateAttemptStatus(ast.attempt.ID, taskstore.StatusCancelled)
		rc.wt.Remove(ast.info.Path)
		return result.decision != DecisionFail
	}

	if result.state == supervisor.StateCrashed {
		return result.decision != DecisionFail
	}

	// R1-B Fix 3: Exited path + validator REJECT → new attempt.
	if result.exited && result.state == supervisor.StateExited {
		if rc.cfg.Validator != "" {
			if runValidator(rc.cfg.Validator, ast.info.Path) {
				rc.store.UpdateTaskStatus(rc.taskID, taskstore.StatusCompleted)
				rc.store.UpdateAttemptStatus(ast.attempt.ID, taskstore.StatusCompleted)
				rc.store.EmitEvent(rc.runID, rc.taskID, ast.attempt.ID, ast.worker.ID, "validated", nil)
			} else {
				// REJECT → retry (same as quiescence path).
				rc.store.UpdateAttemptStatus(ast.attempt.ID, taskstore.StatusFailed)
				rc.store.EmitEvent(rc.runID, rc.taskID, ast.attempt.ID, ast.worker.ID, "validation_rejected", nil)
				rc.wt.Remove(ast.info.Path)
				return true // retry
			}
		} else {
			rc.store.UpdateAttemptStatus(ast.attempt.ID, taskstore.StatusCompleted)
			rc.store.UpdateTaskStatus(rc.taskID, taskstore.StatusCompleted)
		}
		rc.store.EmitEvent(rc.runID, rc.taskID, ast.attempt.ID, ast.worker.ID, "attempt_completed", nil)
		if result.checkpoint != "" {
			supervisor.CleanupWorktree(rc.ctx, rc.wt, ast.info.Path)
		}
		return false
	}

	// Quiescence with decision.
	if !result.exited {
		return result.decision != DecisionFail
	}

	rc.wt.Remove(ast.info.Path)
	return false
}

// recoverAndRecord uses supervisor.Recover() with secret scan.
func recoverAndRecord(ctx context.Context, rt *runtime.Runtime, wt *worktree.Manager,
	worktreePath string, attemptIDVal int64, store *taskstore.Store,
	runID, taskID, attemptID, workerID int64, dirty bool) string {

	if !dirty {
		return ""
	}
	result := supervisor.Recover(ctx, rt, supervisor.RecoveryConfig{
		WorktreePath:      worktreePath,
		AttemptID:         fmt.Sprintf("%d", attemptIDVal),
		SecretScanEnabled: true,
	}, wt)
	if result.BlockedSecret {
		log.Printf("SECRET BLOCKED for attempt %d", attemptIDVal)
		return ""
	}
	if result.CommitSHA != "" {
		store.UpdateAttemptCheckpoint(attemptID, result.CommitSHA)
		log.Printf("RECOVERED: attempt %d checkpoint %s", attemptIDVal, result.CommitSHA)
	}
	return result.CommitSHA
}

func runValidator(command, cwd string) bool {
	if command == "" {
		return true
	}
	cmd := exec.Command("sh", "-c", command)
	if cwd != "" {
		cmd.Dir = cwd
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("VALIDATOR FAIL: %v\n%s", err, out)
		return false
	}
	log.Printf("VALIDATOR PASS: %s", strings.TrimSpace(string(out)))
	return true
}

func wtIsDirty(wt *worktree.Manager, path string) bool {
	if path == "" {
		return false
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return !wt.Status(path).Clean
}

func awaitDecision(ctx context.Context) Decision {
	reader := bufio.NewReader(os.Stdin)
	type result struct {
		line string
		err  error
	}
	for {
		select {
		case <-ctx.Done():
			return DecisionFail
		default:
		}
		ch := make(chan result, 1)
		go func() { line, err := reader.ReadString('\n'); ch <- result{line, err} }()
		select {
		case r := <-ch:
			if r.err == io.EOF {
				return DecisionFail
			}
			if r.err != nil {
				continue
			}
			d := Decision(strings.TrimSpace(r.line))
			switch d {
			case DecisionValidate, DecisionContinue, DecisionRetryClean, DecisionReplace, DecisionFail:
				return d
			default:
				fmt.Fprintf(os.Stderr, "Unknown: %q. Choose: VALIDATE / CONTINUE / RETRY_CLEAN / REPLACE / FAIL\n", d)
			}
		case <-time.After(5 * time.Second):
			return DecisionContinue
		case <-ctx.Done():
			return DecisionFail
		}
	}
}

// OpenStore opens a file-backed or in-memory SQLite store.
func OpenStore(path string) (*taskstore.Store, error) {
	if path == "" {
		return taskstore.NewInMemory()
	}
	dir := filepath.Dir(path)
	if dir != "." {
		os.MkdirAll(dir, 0755)
	}
	return taskstore.New(path)
}
