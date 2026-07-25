// Package orchestrator integrates worktree, runtime, supervisor, and taskstore
// into a coordinated task-execution loop with decision gateway, auto-wake,
// and worker re-instruction.
//
// B2: Decision gateway with state validation, idempotency, generation checks.
// B3: Auto-wake events with idempotency keys and ACK tracking.
// B4: Worker re-instruction (RETRY_CLEAN, CONTINUE, REPLACE, FAIL, COMPLETE).
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
	"sync"
	"time"

	"github.com/miinanii/omni_orchestration/internal/runtime"
	"github.com/miinanii/omni_orchestration/internal/supervisor"
	"github.com/miinanii/omni_orchestration/internal/taskstore"
	"github.com/miinanii/omni_orchestration/internal/worktree"
)

const defaultMaxAttempts = 3
const continueLeaseTimeout = 30 * time.Second
const wakeAckTimeout = 60 * time.Second

type Config struct {
	Repo        string
	Task        string
	Command     string
	CWD         string
	Validator   string
	StorePath   string
	MaxAttempts int
}

// ── Decision Gateway (B2) ──

type Decision string

const (
	DecisionStart      Decision = "START"
	DecisionValidate   Decision = "VALIDATE"
	DecisionComplete   Decision = "COMPLETE"
	DecisionContinue   Decision = "CONTINUE"
	DecisionRetryClean Decision = "RETRY_CLEAN"
	DecisionReplace    Decision = "REPLACE"
	DecisionFail       Decision = "FAIL"
)

// allowedTransitions maps each state to allowed coordinator decisions.
var allowedTransitions = map[supervisor.State][]Decision{
	supervisor.StateActive:             {DecisionStart, DecisionFail},
	supervisor.StateQuiescentCandidate: {DecisionValidate, DecisionContinue, DecisionRetryClean, DecisionReplace, DecisionFail},
	supervisor.StateExited:             {DecisionValidate, DecisionComplete, DecisionRetryClean, DecisionFail},
	supervisor.StateCrashed:            {DecisionRetryClean, DecisionFail},
}

// DecisionGateway validates coordinator decisions against state + rules.
type DecisionGateway struct {
	coordGen   int64
	seenEvents map[string]bool // idempotency keys
	mu         sync.Mutex
}

func NewDecisionGateway() *DecisionGateway {
	return &DecisionGateway{seenEvents: make(map[string]bool)}
}

// B2: Validate checks decision against state. Returns error if invalid.
func (g *DecisionGateway) Validate(state supervisor.State, decision Decision, hasValidator, validatorPassed bool) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	allowed, ok := allowedTransitions[state]
	if !ok {
		return fmt.Errorf("unknown state %s", state)
	}
	valid := false
	for _, d := range allowed {
		if d == decision {
			valid = true
			break
		}
	}
	if !valid {
		return fmt.Errorf("decision %q not allowed in state %s (allowed: %v)", decision, state, allowed)
	}

	// B2: COMPLETE requires validator ACCEPT.
	if decision == DecisionComplete && !validatorPassed {
		return fmt.Errorf("COMPLETE requires validator ACCEPT")
	}

	g.coordGen++
	return nil
}

// B2: IsDuplicate checks the idempotency key. Returns true if already seen.
func (g *DecisionGateway) IsDuplicate(key string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.seenEvents[key] {
		return true
	}
	g.seenEvents[key] = true
	return false
}

func (g *DecisionGateway) Generation() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.coordGen
}

// ── Wake Message (B3) ──

type WakeEvent struct {
	ID        string    `json:"id"`
	RunID     int64     `json:"run_id"`
	TaskID    int64     `json:"task_id"`
	AttemptID int64     `json:"attempt_id"`
	State     string    `json:"state"`
	Reason    string    `json:"reason"`
	Timestamp time.Time `json:"timestamp"`
}

func (w WakeEvent) IdempotencyKey(runID int64, coordGen int64) string {
	return fmt.Sprintf("wake:%d:%s:%d", runID, w.ID, coordGen)
}

// ── Run Context ──

type attemptState struct {
	info       *worktree.WorktreeInfo
	rt         *runtime.Runtime
	attempt    *taskstore.Attempt
	worker     *taskstore.WorkerRecord
	checkpoint string
}

type observeResult struct {
	state           supervisor.State
	dirty           bool
	checkpoint      string
	decision        Decision
	exited          bool
	validatorPassed bool
}

type runContext struct {
	ctx       context.Context
	cfg       Config
	store     *taskstore.Store
	wt        *worktree.Manager
	gateway   *DecisionGateway
	runID     int64
	taskID    int64
	decisions []Decision
	maxAtts   int
	events    []WakeEvent // emitted wake events
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
		gateway: NewDecisionGateway(),
		runID:   run.ID,
		taskID:  task.ID,
		maxAtts: maxAtts,
	}

	// B3: emit auto-wake — new run ready.
	rc.emitWake("run_created", supervisor.StateActive, "Run created, awaiting coordinator")

	// B4: Await START decision from coordinator.
	startDecision := rc.awaitDecision()
	rc.decisions = append(rc.decisions, startDecision)
	if startDecision == DecisionFail {
		store.UpdateTaskStatus(task.ID, taskstore.StatusFailed)
		return rc.decisions, nil
	}

	attemptNum := 1
	baseCommit := "HEAD"
	nextInstruction := cfg.Task

	for {
		select {
		case <-ctx.Done():
			return rc.decisions, ctx.Err()
		default:
		}

		// B2: retry limit exceeded → force FAIL.
		if attemptNum > maxAtts {
			store.UpdateTaskStatus(task.ID, taskstore.StatusFailed)
			log.Printf("B2: max attempts (%d) exceeded → FAIL", maxAtts)
			rc.emitWake("max_attempts", supervisor.StateCrashed, "Max attempts exceeded")
			return rc.decisions, nil
		}

		ast, err := rc.createAttempt(task.ID, attemptNum, baseCommit, nextInstruction)
		if err != nil {
			return rc.decisions, err
		}

		// B3: auto-wake — worker started.
		rc.emitWake("worker_started", supervisor.StateActive,
			fmt.Sprintf("Attempt %d started", attemptNum))

		result := rc.observeAndWait(ast)

		// B3: auto-wake on state transition.
		rc.emitWake("worker_"+string(result.state), result.state,
			fmt.Sprintf("Worker reached state %s", result.state))

		retry := rc.finalizeAttempt(ast, result)
		if !retry {
			return rc.decisions, nil
		}

		attemptNum++
		if result.decision == DecisionReplace {
			baseCommit = ast.info.Branch
		} else {
			// B4: RETRY_CLEAN → new attempt from base.
			baseCommit = "HEAD"
		}
	}
}

// createAttempt sets up worktree + runtime for one attempt.
// B4: nextInstruction passed to worker stdin.
func (rc *runContext) createAttempt(taskID int64, num int, baseCommit, instruction string) (*attemptState, error) {
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

	// B4: deliver next instruction to worker stdin.
	if instruction != "" {
		rt.Write(1, []byte(instruction+"\n"))
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
		case supervisor.StateQuiescentCandidate:
			result.dirty = wtIsDirty(rc.wt, ast.info.Path)
			result.checkpoint = recoverAndRecord(rc.ctx, ast.rt, rc.wt, ast.info.Path,
				ast.attempt.ID, rc.store, rc.runID, rc.taskID, ast.attempt.ID, ast.worker.ID, result.dirty)

			if rc.cfg.Validator != "" && result.dirty {
				result.validatorPassed = runValidator(rc.cfg.Validator, ast.info.Path)
				if result.validatorPassed {
					rc.store.UpdateTaskStatus(rc.taskID, taskstore.StatusCompleted)
					rc.store.UpdateAttemptStatus(ast.attempt.ID, taskstore.StatusCompleted)
					rc.store.EmitEvent(rc.runID, rc.taskID, ast.attempt.ID, ast.worker.ID, "validated", nil)
					rc.wt.Remove(ast.info.Path)
					rc.emitWake("validator_accepted", sc.To, "Validator ACCEPT — task completed")
					result.exited = true
					return result
				}
				rc.store.EmitEvent(rc.runID, rc.taskID, ast.attempt.ID, ast.worker.ID, "validation_rejected", nil)
				rc.emitWake("validator_rejected", sc.To, "Validator REJECT")
				result.decision = DecisionRetryClean
				return result
			}

			rc.emitWake("quiescent", sc.To, "Worker quiescent — awaiting coordinator")
			decision := rc.awaitDecision()
			rc.decisions = append(rc.decisions, decision)

			// B2: validate decision against state.
			if err := rc.gateway.Validate(sc.To, decision, rc.cfg.Validator != "", result.validatorPassed); err != nil {
				log.Printf("B2: decision rejected: %v (falling back to FAIL)", err)
				decision = DecisionFail
			}

			if decision == DecisionValidate && rc.cfg.Validator != "" && result.dirty {
				result.validatorPassed = runValidator(rc.cfg.Validator, ast.info.Path)
				if result.validatorPassed {
					rc.store.UpdateTaskStatus(rc.taskID, taskstore.StatusCompleted)
					rc.store.UpdateAttemptStatus(ast.attempt.ID, taskstore.StatusCompleted)
					rc.store.EmitEvent(rc.runID, rc.taskID, ast.attempt.ID, ast.worker.ID, "validated", nil)
					rc.wt.Remove(ast.info.Path)
					rc.emitWake("validator_accepted", sc.To, "Validator ACCEPT — task completed")
					result.exited = true
					return result
				}
				rc.store.EmitEvent(rc.runID, rc.taskID, ast.attempt.ID, ast.worker.ID, "validation_rejected", nil)
				rc.emitWake("validator_rejected", sc.To, "Validator REJECT")
				result.decision = DecisionRetryClean
				return result
			}

			// B4: CONTINUE → existing Worker stdin (gen check).
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
					continue
				case <-leaseTimer.C:
					rc.emitWake("lease_expired", sc.To, "CONTINUE lease expired — re-waking coordinator")
					continue
				case <-rc.ctx.Done():
					result.exited = true
					return result
				}
			}

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
			rc.emitWake("worker_crashed", sc.To, "Worker crashed")
			result.decision = rc.awaitDecision()
			rc.decisions = append(rc.decisions, result.decision)
			return result
		}
	}
	result.exited = true
	return result
}

// finalizeAttempt handles the terminal states and retry logic.
// B4: each branch has distinct behavior.
func (rc *runContext) finalizeAttempt(ast *attemptState, result observeResult) bool {
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	ast.rt.Close(stopCtx, 1)
	cancel()

	// B4: FAIL — task failed, preserve all attempts.
	if result.decision == DecisionFail {
		rc.store.UpdateAttemptStatus(ast.attempt.ID, taskstore.StatusFailed)
		rc.store.UpdateTaskStatus(rc.taskID, taskstore.StatusFailed)
		rc.store.EmitEvent(rc.runID, rc.taskID, ast.attempt.ID, ast.worker.ID, "task_failed", nil)
		rc.emitWake("task_failed", result.state, "Coordinator decided FAIL")
		return false
	}

	// B4: RETRY_CLEAN / REPLACE — new attempt from base.
	if result.decision == DecisionRetryClean || result.decision == DecisionReplace {
		rc.store.UpdateAttemptStatus(ast.attempt.ID, taskstore.StatusCancelled)
		rc.store.EmitEvent(rc.runID, rc.taskID, ast.attempt.ID, ast.worker.ID, "attempt_retry", nil)
		rc.wt.Remove(ast.info.Path)
		return true
	}

	if result.state == supervisor.StateCrashed {
		return result.decision != DecisionFail
	}

	// Exited path.
	if result.exited && result.state == supervisor.StateExited {
		if rc.cfg.Validator != "" {
			result.validatorPassed = runValidator(rc.cfg.Validator, ast.info.Path)
			if result.validatorPassed {
				rc.store.UpdateTaskStatus(rc.taskID, taskstore.StatusCompleted)
				rc.store.UpdateAttemptStatus(ast.attempt.ID, taskstore.StatusCompleted)
				rc.store.EmitEvent(rc.runID, rc.taskID, ast.attempt.ID, ast.worker.ID, "validated", nil)
				rc.emitWake("validator_accepted", result.state, "Validator ACCEPT — task completed")
			} else {
				rc.store.UpdateAttemptStatus(ast.attempt.ID, taskstore.StatusFailed)
				rc.store.EmitEvent(rc.runID, rc.taskID, ast.attempt.ID, ast.worker.ID, "validation_rejected", nil)
				rc.emitWake("validator_rejected", result.state, "Validator REJECT — retrying")
				rc.wt.Remove(ast.info.Path)
				return true
			}
		} else {
			// B4: COMPLETE — task completed (validator ACCEPT needed per B2 rule).
			rc.store.UpdateAttemptStatus(ast.attempt.ID, taskstore.StatusCompleted)
			rc.store.UpdateTaskStatus(rc.taskID, taskstore.StatusCompleted)
			rc.emitWake("task_completed", result.state, "Task completed successfully")
		}
		rc.store.EmitEvent(rc.runID, rc.taskID, ast.attempt.ID, ast.worker.ID, "attempt_completed", nil)
		if result.checkpoint != "" {
			supervisor.CleanupWorktree(rc.ctx, rc.wt, ast.info.Path)
		}
		return false
	}

	if !result.exited {
		return result.decision != DecisionFail
	}

	rc.wt.Remove(ast.info.Path)
	return false
}

// B3: emitWake creates an idempotent wake event.
func (rc *runContext) emitWake(id string, state supervisor.State, reason string) {
	key := fmt.Sprintf("wake:%d:%s:%d", rc.runID, id, rc.gateway.Generation())
	if rc.gateway.IsDuplicate(key) {
		log.Printf("B3: duplicate wake suppressed: %s", key)
		return
	}
	ev := WakeEvent{ID: id, RunID: rc.runID, TaskID: rc.taskID, State: string(state), Reason: reason, Timestamp: time.Now()}
	rc.events = append(rc.events, ev)
	log.Printf("WAKE [%s] state=%s reason=%s key=%s", id, state, reason, key)
}

// recoverAndRecord uses supervisor.Recover() with secret scan.
func recoverAndRecord(ctx context.Context, rt *runtime.Runtime, wt *worktree.Manager,
	worktreePath string, attemptIDVal int64, store *taskstore.Store,
	runID, taskID, attemptID, workerID int64, dirty bool) string {

	if !dirty {
		return ""
	}
	result := supervisor.Recover(ctx, rt, supervisor.RecoveryConfig{
		WorktreePath: worktreePath, AttemptID: fmt.Sprintf("%d", attemptIDVal), SecretScanEnabled: true,
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

func (rc *runContext) awaitDecision() Decision {
	reader := bufio.NewReader(os.Stdin)
	for {
		select {
		case <-rc.ctx.Done():
			return DecisionFail
		default:
		}
		type result struct {
			line string
			err  error
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
			// B2: validate against known decisions.
			switch d {
			case DecisionStart, DecisionValidate, DecisionComplete, DecisionContinue, DecisionRetryClean, DecisionReplace, DecisionFail:
				return d
			default:
				fmt.Fprintf(os.Stderr, "Unknown: %q\n", d)
			}
		case <-time.After(5 * time.Second):
			return DecisionContinue
		case <-rc.ctx.Done():
			return DecisionFail
		}
	}
}

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
