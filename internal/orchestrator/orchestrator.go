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
	DecisionStart      = coordinator.DecisionValidate // initial → START is VALIDATE
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

	// B-R1: Coordinator runtime for LLM-driven decisions.
	Coordinator *coordinator.CoordinatorRuntime
}

// ── Decision Gateway (B-R1: wraps coordinator gen checks) ──

type DecisionGateway struct {
	coordGen   int64
	seenEvents map[string]bool
	mu         sync.Mutex
}

func NewDecisionGateway() *DecisionGateway {
	return &DecisionGateway{seenEvents: make(map[string]bool)}
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

// ── Run State ──

type attemptState struct {
	info        *worktree.WorktreeInfo
	rt          *runtime.Runtime
	attempt     *taskstore.Attempt
	worker      *taskstore.WorkerRecord
	checkpoint  string
	instruction string // B-R1: from coordinator next_instruction
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

	existing, _ := store.GetActiveAttempts()
	for _, a := range existing {
		store.UpdateAttemptStatus(a.ID, taskstore.StatusCancelled)
		log.Printf("B-R1 recovery: cancelled in-progress attempt %d", a.ID)
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

	// B-R1: Wake coordinator for initial decision.
	coordGen := rc.coordinatorGeneration()
	startResp, err := rc.wakeCoordinator(coordGen, coordinator.WakePacket{
		RunID: run.ID, TaskID: task.ID, TaskTitle: cfg.Task,
		AttemptNumber: 0, AttemptStatus: "pending",
		WorkerState: "pending", ExitCode: 0,
		AllowedDecisions: []string{"VALIDATE", "FAIL"},
	})
	if err != nil {
		log.Printf("B-R1: coordinator wake failed: %v — defaulting to VALIDATE", err)
		startResp = coordinator.WakeResponse{Decision: DecisionValidate, Reason: "default", NextInstruction: cfg.Task}
	}
	rc.decisions = append(rc.decisions, startResp.Decision)
	if startResp.Decision == DecisionFail {
		store.UpdateTaskStatus(task.ID, taskstore.StatusFailed)
		return rc.decisions, nil
	}

	attemptNum := 1
	baseCommit := "HEAD"
	// Fix 3: first attempt always receives cfg.Task as the instruction.
	// Subsequent retries receive coordinator next_instruction + cfg.Task.
	nextInstruction := cfg.Task

	for {
		select {
		case <-ctx.Done():
			return rc.decisions, ctx.Err()
		default:
		}

		if attemptNum > maxAtts {
			store.UpdateTaskStatus(task.ID, taskstore.StatusFailed)
			log.Printf("B-R1: max attempts (%d) exceeded → FAIL", maxAtts)
			return rc.decisions, nil
		}

		ast, err := rc.createAttempt(task.ID, attemptNum, baseCommit, nextInstruction)
		if err != nil {
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
			log.Printf("B-R1: coordinator wake failed: %v — defaulting to RETRY_CLEAN", err)
			resp = coordinator.WakeResponse{Decision: DecisionRetryClean, Reason: "wake failed", NextInstruction: cfg.Task}
		}
		rc.decisions = append(rc.decisions, resp.Decision)

		if resp.Decision == DecisionComplete {
			store.UpdateTaskStatus(task.ID, taskstore.StatusCompleted)
			return rc.decisions, nil
		}
		if resp.Decision == DecisionFail {
			store.UpdateTaskStatus(task.ID, taskstore.StatusFailed)
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
		return nil, fmt.Errorf("start worker: %w", err)
	}

	// B-R1: Write coordinator instruction to worker stdin.
	if instruction != "" && instruction != rc.cfg.Task {
		rt.Write(rt.Generation(), []byte(instruction+"\n"))
	}

	return &attemptState{
		info: info, rt: rt, attempt: attempt, worker: worker,
		instruction: instruction,
	}, nil
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
	if rc.cfg.Validator != "" && checkpointSHA != "" {
		validatorPassed = rc.runValidator(ast.info.Path, rc.cfg.Validator)
	}

	status := taskstore.StatusCompleted
	if !validatorPassed || exitCode != 0 {
		status = taskstore.StatusFailed
	}
	rc.store.UpdateAttemptStatus(ast.attempt.ID, status)

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
func OpenStore(path string) (*taskstore.Store, error) {
	return taskstore.New(path)
}

// Ensure fmt is used.
var _ = fmt.Sprintf
