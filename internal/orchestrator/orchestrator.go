// Package orchestrator integrates worktree, runtime, supervisor, taskstore,
// and coordinator into a coordinated task-execution loop.
//
// B-R1: CoordinatorRuntime.Wake() replaces emitWake/awaitDecision.
// Decision type imported from coordinator package. All wake paths
// go through the coordinator contract.
package orchestrator

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/miinanii/omni_orchestration/internal/coordinator"
	"github.com/miinanii/omni_orchestration/internal/runtime"
	"github.com/miinanii/omni_orchestration/internal/supervisor"
	"github.com/miinanii/omni_orchestration/internal/taskstore"
	"github.com/miinanii/omni_orchestration/internal/worktree"
)

// Re-export coordinator types so existing callers don't break.
type Decision = coordinator.Decision
type WakePacket = coordinator.WakePacket
type WakeResponse = coordinator.WakeResponse

const (
	DecisionStart      = coordinator.DecisionStart
	DecisionValidate   = coordinator.DecisionValidate
	DecisionComplete   = coordinator.DecisionComplete
	DecisionContinue   = coordinator.DecisionContinue
	DecisionRetryClean = coordinator.DecisionRetryClean
	DecisionReplace    = coordinator.DecisionReplace
	DecisionFail       = coordinator.DecisionFail
)

const defaultMaxAttempts = 3

type Config struct {
	Repo        string
	Task        string
	Command     string
	CWD         string
	Validator   string
	StorePath   string
	MaxAttempts int

	// R2: Provider identity for stats recording.
	Provider string
	Model    string

	// B-R1: Coordinator runtime for LLM-driven decisions.
	Coordinator *coordinator.CoordinatorRuntime
}

// ── Decision Gateway (B-R1: wraps coordinator gen checks) ──

type DecisionGateway struct {
	store    *taskstore.Store
	runID    int64
	coordGen int64
	seenKeys map[string]bool // in-memory cache for hot path
	mu       sync.Mutex
}

// NewDecisionGateway creates a gateway backed by the durable store.
// R2 Fix 4: effect keys persisted in SQLite for crash recovery.
func NewDecisionGateway(store *taskstore.Store, runID int64) *DecisionGateway {
	return &DecisionGateway{
		store:    store,
		runID:    runID,
		seenKeys: make(map[string]bool),
	}
}

func (g *DecisionGateway) Validate(state supervisor.State, decision Decision, validatorPassed bool) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if decision == DecisionComplete && !validatorPassed {
		return fmt.Errorf("COMPLETE requires validator ACCEPT")
	}
	g.coordGen++
	return nil
}

// IsDuplicate checks both in-memory cache and durable store.
// Returns true if the effect key has already been processed.
// R2 Fix 4: durable effect key check in SQLite.
func (g *DecisionGateway) IsDuplicate(key string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.seenKeys[key] {
		return true
	}
	if g.store != nil && g.store.HasEffect(g.runID, key) {
		g.seenKeys[key] = true
		return true
	}
	g.seenKeys[key] = true
	return false
}

// RecordEffect persists an effect key to the durable store.
// R2 Fix 4: durable effect key write to SQLite.
// RecordEffect atomically records an effect key. Returns true if newly
// inserted (rowsAffected=1), false if already existed. R7: single INSERT OR IGNORE.
func (g *DecisionGateway) RecordEffect(key string) (bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.store == nil {
		if g.seenKeys[key] {
			return false, nil
		}
		g.seenKeys[key] = true
		return true, nil
	}
	return g.store.RecordEffect(g.runID, key)
}

func (g *DecisionGateway) Generation() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.coordGen
}

// ── Run State ──

type attemptState struct {
	info        *worktree.WorktreeInfo
	rt          *runtime.Runtime
	attempt     *taskstore.Attempt
	worker      *taskstore.WorkerRecord
	checkpoint  string
	instruction string    // B-R1: from coordinator next_instruction
	startTime   time.Time // R2: for duration tracking
}

type observeResult struct {
	state           supervisor.State
	dirty           bool
	checkpoint      string
	decision        Decision
	exited          bool
	validatorPassed bool
	exitCode        int
}

type runContext struct {
	ctx       context.Context
	cfg       Config
	store     *taskstore.Store
	wt        *worktree.Manager
	gateway   *DecisionGateway
	runID     int64
	taskID    int64
	maxAtts   int
	decisions []Decision
	attempts  []*attemptState // all attempts for terminal cleanup
}

// Run executes the full orchestrator flow with coordinator-driven decisions.
func Run(ctx context.Context, cfg Config, store *taskstore.Store, wt *worktree.Manager) ([]Decision, error) {
	if cfg.Repo == "" || cfg.Task == "" || cfg.Command == "" {
		return nil, fmt.Errorf("repo, task, and command are required")
	}
	maxAtts := cfg.MaxAttempts
	if maxAtts <= 0 {
		maxAtts = defaultMaxAttempts
	}

	// C5: Reconcile active attempts — verify worktree exists before cancelling.
	// Never kill owner processes; only cancel stale DB records.
	existing, _ := store.GetActiveAttempts()
	for _, a := range existing {
		if a.Branch != "" && !worktreeExists(cfg.Repo, a.Branch) {
			log.Printf("C5 recovery: attempt %d branch %q worktree gone — cancelling", a.ID, a.Branch)
			store.UpdateAttemptStatus(a.ID, taskstore.StatusCancelled)
		} else {
			log.Printf("C5 recovery: attempt %d branch %q worktree EXISTS — preserving", a.ID, a.Branch)
		}
	}

	run, err := store.CreateRun()
	if err != nil {
		return nil, fmt.Errorf("create run: %w", err)
	}
	// R2: Record run for provider stats (if provider is known).
	if cfg.Provider != "" {
		if err := store.RecordRun(run.ID, cfg.Provider, cfg.Model, "coordinator", cfg.Task, cfg.Repo); err != nil {
			log.Printf("R2: RecordRun failed: %v", err)
		}
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
		gateway: NewDecisionGateway(store, run.ID),
		runID:   run.ID,
		taskID:  task.ID,
		maxAtts: maxAtts,
	}

	// B-R1: Wake coordinator for initial decision.
	coordGen := rc.coordinatorGeneration()
	startResp, err := rc.wakeCoordinator(coordGen, coordinator.WakePacket{
		RunID: run.ID, TaskID: task.ID, TaskTitle: cfg.Task,
		AttemptNumber: 0, AttemptStatus: "pending",
		WorkerState: "pending", ExitCode: 0,
		AllowedDecisions: []string{"VALIDATE", "FAIL"},
	})
	if err != nil {
		log.Printf("B-R1: coordinator wake failed: %v — FAIL (R3: fail-closed)", err)
		startResp = coordinator.WakeResponse{Decision: DecisionFail, Reason: "wake failed", NextInstruction: cfg.Task}
	}
	rc.decisions = append(rc.decisions, startResp.Decision)
	if startResp.Decision == DecisionFail {
		rc.finalizeTerminal(nil, taskstore.StatusFailed)
		return rc.decisions, nil
	}

	attemptNum := 1
	autoValidateCount := 0
	const maxAutoValidate = 3 // C2: max auto-VALIDATE loops before FAIL
	baseCommit := "HEAD"
	// Fix 3: first attempt always receives cfg.Task as the instruction.
	// Subsequent retries receive coordinator next_instruction + cfg.Task.
	nextInstruction := cfg.Task

	for {
		select {
		case <-ctx.Done():
			rc.finalizeTerminal(nil, taskstore.StatusCancelled)
			return rc.decisions, ctx.Err()
		default:
		}

		// C2: no-coordinator VALIDATE loop guard.
		if startResp.Decision == DecisionValidate && rc.cfg.Coordinator == nil {
			autoValidateCount++
			if autoValidateCount > maxAutoValidate {
				rc.finalizeTerminal(nil, taskstore.StatusFailed)
				log.Printf("C2: max auto-VALIDATE (%d) exceeded", maxAutoValidate)
				return rc.decisions, nil
			}
		} else {
			autoValidateCount = 0
		}

		if attemptNum > maxAtts {
			rc.finalizeTerminal(nil, taskstore.StatusFailed)
			log.Printf("B-R1: max attempts (%d) exceeded", maxAtts)
			return rc.decisions, nil
		}

		ast, err := rc.createAttempt(task.ID, attemptNum, baseCommit, nextInstruction)
		if err != nil {
			rc.finalizeTerminal(nil, taskstore.StatusFailed)
			return rc.decisions, err
		}

		result := rc.observeAndWait(ast)

		// B-R1: Wake coordinator with result.
		coordGen = rc.coordinatorGeneration()
		resp, err := rc.wakeCoordinator(coordGen, coordinator.WakePacket{
			RunID: run.ID, TaskID: task.ID, TaskTitle: cfg.Task,
			AttemptNumber: attemptNum, AttemptStatus: string(result.decision),
			WorkerState: string(result.state), ExitCode: result.exitCode,
			CheckpointSHA: result.checkpoint, ValidatorOutput: rc.validatorResult(result),
			AllowedDecisions: rc.allowedDecisions(result),
		})
		if err != nil {
			log.Printf("B-R1: coordinator wake failed: %v — FAIL (R3: fail-closed)", err)
			resp = coordinator.WakeResponse{Decision: DecisionFail, Reason: "wake failed", NextInstruction: cfg.Task}
		}
		// C2: no-coordinator flow — validator PASS → auto-COMPLETE.
		if rc.cfg.Coordinator == nil && result.validatorPassed {
			resp = coordinator.WakeResponse{Decision: DecisionComplete, Reason: "validator passed (no coordinator)", NextInstruction: ""}
		}
		rc.decisions = append(rc.decisions, resp.Decision)

		// R2 Fix 4: Record durable effect key to prevent replay on restart.
		effectKey := fmt.Sprintf("run-%d-att-%d-decision-%s", rc.runID, attemptNum, resp.Decision)
		if rc.gateway.IsDuplicate(effectKey) {
			log.Printf("R2: duplicate effect key %q — skipping decision replay", effectKey)
			rc.finalizeTerminal(ast, taskstore.StatusCancelled)
			return rc.decisions, nil
		}
		if isNew, err := rc.gateway.RecordEffect(effectKey); err != nil {
			log.Printf("R2: RecordEffect failed: %v", err)
		} else if !isNew {
			log.Printf("R2: effect key %q already recorded (race)", effectKey)
		}

		if resp.Decision == DecisionComplete {
			// R2: Authoritative result adoption — verify run_record exists.
			if _, err := rc.store.GetRunRecord(rc.runID); err == nil {
				if err := rc.store.RecordAdoption(rc.runID, attemptNum, true); err != nil {
					log.Printf("R2: RecordAdoption failed: %v", err)
					rc.store.UpdateRunStatus(rc.runID, taskstore.StatusFailed)
					rc.store.UpdateAttemptStatus(ast.attempt.ID, taskstore.StatusFailed)
				}
			} else {
				log.Printf("R2: no run_record for run %d — skipping adoption", rc.runID)
			}
			rc.finalizeTerminal(ast, taskstore.StatusCompleted)
			return rc.decisions, nil
		}
		if resp.Decision == DecisionFail {
			rc.finalizeTerminal(ast, taskstore.StatusFailed)
			return rc.decisions, nil
		}

		// RETRY_CLEAN or REPLACE → new attempt.
		attemptNum++
		// Fix 3: retry receives coordinator instruction + original task.
		nextInstruction = resp.NextInstruction
		if nextInstruction == "" || !strings.Contains(nextInstruction, cfg.Task) {
			nextInstruction = cfg.Task + "\n" + nextInstruction
		}
		if resp.Decision == DecisionReplace {
			baseCommit = ast.info.Branch
		} else {
			baseCommit = "HEAD"
		}
	}
}

func (rc *runContext) coordinatorGeneration() int64 {
	if rc.cfg.Coordinator != nil {
		return rc.cfg.Coordinator.Generation()
	}
	return rc.gateway.Generation()
}

func (rc *runContext) wakeCoordinator(gen int64, pkt coordinator.WakePacket) (coordinator.WakeResponse, error) {
	if rc.cfg.Coordinator != nil {
		return rc.cfg.Coordinator.Wake(rc.ctx, gen, pkt)
	}
	// No coordinator → auto-VALIDATE.
	return coordinator.WakeResponse{Decision: DecisionValidate, Reason: "no coordinator", NextInstruction: rc.cfg.Task}, nil
}

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
	// C5: Resolve symlinks before boundary check (macOS /var→/private/var).
	if r, e := filepath.EvalSymlinks(cwd); e == nil {
		cwd = r
	}
	if r, e := filepath.EvalSymlinks(info.Path); e == nil {
		info.Path = r
	}
	if cwd != info.Path && !isWithinWorktree(cwd, info.Path) {
		rc.wt.Remove(info.Path)
		return nil, fmt.Errorf("CWD %q is outside worktree %q", cwd, info.Path)
	}
	attempt, err := rc.store.CreateAttempt(taskID, num, attemptID, info.Branch, baseCommit)
	if err != nil {
		rc.wt.Remove(info.Path)
		return nil, fmt.Errorf("create attempt: %w", err)
	}

	rt := runtime.New()
	if err := rt.Start(rc.cfg.Command, cwd); err != nil {
		rc.wt.Remove(info.Path)
		return nil, fmt.Errorf("start worker: %w", err)
	}

	// Record worker AFTER Start to capture real PID.
	worker, err := rc.store.RecordWorkerPID(attempt.ID, rc.cfg.Command, cwd, "primary", 1, rt.PID(), 0)
	if err != nil {
		rt.Close(context.Background(), rt.Generation())
		rc.wt.Remove(info.Path)
		return nil, fmt.Errorf("record worker: %w", err)
	}

	// B-R1: Write coordinator instruction to worker stdin.
	if instruction != "" {
		rt.Write(rt.Generation(), []byte(instruction+"\n"))
	}

	ast := &attemptState{
		info: info, rt: rt, attempt: attempt, worker: worker,
		instruction: instruction,
		startTime:   time.Now(), // R2: track attempt duration
	}
	rc.attempts = append(rc.attempts, ast)
	return ast, nil
}

func (rc *runContext) observeAndWait(ast *attemptState) observeResult {
	// Start supervisor.
	cfg := supervisor.Config{QuiescenceTimeout: 30 * time.Second, PollInterval: 5 * time.Second}
	sup := supervisor.New(cfg)

	ctx, cancel := context.WithCancel(rc.ctx)
	defer cancel()

	ch := sup.Observe(ctx, ast.rt)

	var finalState supervisor.State
	var exited bool
	var exitCode int

	for sc := range ch {
		finalState = sc.To
		if sc.To == supervisor.StateExited || sc.To == supervisor.StateCrashed {
			exited = true
			exitCode = ast.rt.Wait().ExitCode
			break
		}
		// Fix 4: QUIESCENT_CANDIDATE → wake coordinator for CONTINUE/other.
		if sc.To == supervisor.StateQuiescentCandidate {
			coordGen := rc.coordinatorGeneration()
			resp, err := rc.wakeCoordinator(coordGen, coordinator.WakePacket{
				RunID: rc.runID, TaskID: rc.taskID, TaskTitle: rc.cfg.Task,
				AttemptNumber: 1, AttemptStatus: "running",
				WorkerState: string(supervisor.StateQuiescentCandidate), ExitCode: 0,
				AllowedDecisions: []string{"CONTINUE", "RETRY_CLEAN", "FAIL"},
			})
			if err == nil && resp.Decision == coordinator.DecisionContinue {
				// Re-arm supervisor via continue-lease loop.
				select {
				case <-time.After(30 * time.Second):
				case <-ctx.Done():
				}
				continue
			}
			if err == nil && resp.Decision == coordinator.DecisionRetryClean {
				ast.rt.Close(ctx, ast.rt.Generation())
				exitCode = ast.rt.Wait().ExitCode
				exited = true
				break
			}
		}
	}

	// Recovery checkpoint.
	result := supervisor.Recover(ctx, ast.rt, supervisor.RecoveryConfig{
		WorktreePath: ast.info.Path, AttemptID: fmt.Sprintf("%d", ast.attempt.ID),
		SecretScanEnabled: true,
	}, rc.wt)
	checkpointSHA := result.CommitSHA
	rc.store.UpdateAttemptCheckpoint(ast.attempt.ID, checkpointSHA)

	// Run validator.
	validatorPassed := false
	if rc.cfg.Validator != "" {
		validatorPassed = rc.runValidator(ast.info.Path, rc.cfg.Validator)
	}

	status := taskstore.StatusCompleted
	if !validatorPassed || exitCode != 0 {
		status = taskstore.StatusFailed
	}
	rc.store.UpdateAttemptStatus(ast.attempt.ID, status)
	rc.store.UpdateWorkerStatus(ast.worker.ID, status)
	// Update in-memory so finalizeTerminal reads current state.
	ast.attempt.Status = status
	ast.worker.Status = status

	// R2: Record attempt outcome for provider stats.
	durationMs := time.Since(ast.startTime).Milliseconds()
	if err := rc.store.RecordAttemptOutcome(rc.runID, ast.attempt.Number, validatorPassed, durationMs); err != nil {
		log.Printf("R2: RecordAttemptOutcome failed: %v", err)
	}

	return observeResult{
		state: finalState, checkpoint: checkpointSHA, exited: exited,
		exitCode: exitCode, validatorPassed: validatorPassed,
		decision: rc.deriveDecision(finalState, validatorPassed),
	}
}

func (rc *runContext) runValidator(worktreePath, validatorCmd string) bool {
	cmd := exec.Command("bash", "-c", validatorCmd)
	cmd.Dir = worktreePath
	out, err := cmd.CombinedOutput()
	result := strings.TrimSpace(string(out))
	if err != nil {
		log.Printf("VALIDATOR FAIL: %v — %s", err, result)
		return false
	}
	log.Printf("VALIDATOR PASS: %s", result)
	return true
}

func (rc *runContext) deriveDecision(state supervisor.State, validatorPassed bool) Decision {
	switch {
	case state == supervisor.StateExited && validatorPassed:
		return DecisionComplete
	case state == supervisor.StateExited || state == supervisor.StateCrashed:
		return DecisionRetryClean
	default:
		return DecisionContinue
	}
}

func (rc *runContext) validatorResult(result observeResult) string {
	if result.validatorPassed {
		return "PASS"
	}
	return "FAIL"
}

func (rc *runContext) allowedDecisions(result observeResult) []string {
	switch {
	case result.validatorPassed:
		return []string{"COMPLETE", "RETRY_CLEAN", "FAIL"}
	case result.exited:
		return []string{"RETRY_CLEAN", "REPLACE", "FAIL"}
	default:
		return []string{"CONTINUE", "RETRY_CLEAN", "FAIL"}
	}
}

// finalizeTerminal closes worker runtimes and updates run/task status.
// Each attempt keeps its own terminal status (not overwritten).
// Only non-terminal workers are closed.
func (rc *runContext) finalizeTerminal(ast *attemptState, status string) {
	// Close only non-terminal workers in tracked attempts.
	for _, a := range rc.attempts {
		if a.rt != nil {
			a.rt.Close(context.Background(), a.rt.Generation())
		}
		// Read current DB status before overwrite — in-memory may be stale.
		if a.attempt != nil {
			if cur, err := rc.store.GetAttempt(a.attempt.ID); err == nil && !isTerminalAttempt(cur.Status) {
				rc.store.UpdateAttemptStatus(a.attempt.ID, status)
			}
		}
		if a.worker != nil {
			if cur, err := rc.store.GetWorker(a.worker.ID); err == nil && !isTerminalWorker(cur.Status) {
				rc.store.UpdateWorkerStatus(a.worker.ID, status)
			}
		}
	}
	if ast != nil {
		if ast.rt != nil {
			ast.rt.Close(context.Background(), ast.rt.Generation())
		}
		if ast.attempt != nil {
			if cur, err := rc.store.GetAttempt(ast.attempt.ID); err == nil && !isTerminalAttempt(cur.Status) {
				rc.store.UpdateAttemptStatus(ast.attempt.ID, status)
			}
		}
		if ast.worker != nil {
			if cur, err := rc.store.GetWorker(ast.worker.ID); err == nil && !isTerminalWorker(cur.Status) {
				rc.store.UpdateWorkerStatus(ast.worker.ID, status)
			}
		}
	}
	rc.store.UpdateRunStatus(rc.runID, status)
	rc.store.UpdateTaskStatus(rc.taskID, status)
}

func isTerminalAttempt(s string) bool {
	return s == taskstore.StatusCompleted || s == taskstore.StatusFailed || s == taskstore.StatusCancelled
}
func isTerminalWorker(s string) bool {
	return s == taskstore.StatusCompleted || s == taskstore.StatusFailed || s == taskstore.StatusCancelled
}

// runValidatorBinary executes an external validator command.
func runValidatorBinary(worktreePath, validatorCmd string) (bool, string) {
	cmd := exec.Command("bash", "-c", validatorCmd)
	cmd.Dir = worktreePath
	out, err := cmd.CombinedOutput()
	result := strings.TrimSpace(string(out))
	if err != nil {
		return false, result
	}
	return true, result
}

// recoverAndRecord runs recovery and records the checkpoint SHA.
func recoverAndRecord(ctx context.Context, rt *runtime.Runtime, wt *worktree.Manager,
	worktreePath string, attemptID int64, store *taskstore.Store, runID, taskID, workerID int64) {
	result := supervisor.Recover(ctx, rt, supervisor.RecoveryConfig{
		WorktreePath: worktreePath, AttemptID: fmt.Sprintf("%d", attemptID),
		SecretScanEnabled: true,
	}, wt)
	if result.Checkpointed {
		store.UpdateAttemptCheckpoint(attemptID, result.CommitSHA)
		store.EmitEvent(runID, taskID, attemptID, workerID, "checkpoint", nil)
	}
}

// OpenStore creates a file-backed task store.
// C5: worktreeExists checks if a git worktree branch directory still exists.
// C5: isWithinWorktree checks that cwd is inside the worktree path.
func isWithinWorktree(cwd, wtPath string) bool {
	rel, err := filepath.Rel(wtPath, cwd)
	if err != nil {
		return false
	}
	// Must not start with ".." (escapes worktree).
	return !strings.HasPrefix(rel, "..") && rel != "."
}

func worktreeExists(repoPath, branch string) bool {
	// C5: use cfg.Repo as git worktree list target, not process CWD.
	cmd := exec.Command("git", "-C", repoPath, "worktree", "list", "--porcelain")
	out, err := cmd.Output()
	if err != nil {
		return false
	}
	return len(out) > 0 && containsStr(string(out), branch)
}

func containsStr(haystack, needle string) bool {
	return len(haystack) > 0 && len(needle) > 0 &&
		(haystack == needle || len(haystack) >= len(needle) && searchHaystack(haystack, needle))
}

func searchHaystack(h, n string) bool {
	for i := 0; i <= len(h)-len(n); i++ {
		if h[i:i+len(n)] == n {
			return true
		}
	}
	return false
}

// StoreStatsAdapter wraps a taskstore.Store to implement coordinator.StatsStore.
// Bridges taskstore.ProviderStatsRow ↔ coordinator.ProviderStats type mismatch.
type StoreStatsAdapter struct {
	store *taskstore.Store
}

// NewStoreStatsAdapter creates a StatsStore from a taskstore.Store.
func NewStoreStatsAdapter(store *taskstore.Store) *StoreStatsAdapter {
	return &StoreStatsAdapter{store: store}
}

// StatsFor implements coordinator.StatsStore.
func (a *StoreStatsAdapter) StatsFor(provider coordinator.ProviderID) coordinator.ProviderStats {
	row := a.store.StatsFor(string(provider))
	return coordinator.ProviderStats{
		Provider:      provider,
		TotalAttempts: row.TotalAttempts,
		Successes:     row.Successes,
		TotalRejects:  row.TotalRejects,
		TotalTimeMs:   row.TotalTimeMs,
	}
}

func OpenStore(path string) (*taskstore.Store, error) {
	return taskstore.New(path)
}

// Ensure fmt is used.
var _ = fmt.Sprintf
