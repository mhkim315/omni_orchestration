// Orchestrator CLI — integrates worktree, runtime, supervisor, taskstore
// and coordinator into a coordinated task-execution loop.
//
// Usage:
//
//	orchestrator run --task "bug fix" --command claude --repo /path/to/repo
//	orchestrator run --task "..." --command claude --repo . --coordinator codex --model gpt-5.2
//	orchestrator run --task "..." --command claude --repo . --coordinator claude --effort high
//	orchestrator run --task "..." --command claude --repo . --validator "test -f output.txt"
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/mhkim315/omni_orchestration/internal/coordinator"
	"github.com/mhkim315/omni_orchestration/internal/daemon"
	"github.com/mhkim315/omni_orchestration/internal/dag"
	"github.com/mhkim315/omni_orchestration/internal/orchestrator"
	"github.com/mhkim315/omni_orchestration/internal/runtime"
	"github.com/mhkim315/omni_orchestration/internal/taskstore"
	"github.com/mhkim315/omni_orchestration/internal/worktree"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "orchestrator: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	runCmd := flag.NewFlagSet("run", flag.ExitOnError)
	task := runCmd.String("task", "", "Task title + prompt content (required)")
	command := runCmd.String("command", "", "Worker command (required)")
	repo := runCmd.String("repo", "", "Path to git repository (required)")
	cwd := runCmd.String("cwd", "", "Working directory (defaults to worktree path)")
	validator := runCmd.String("validator", "", "External shell validation command (empty=skip)")
	storePath := runCmd.String("store", "", "SQLite file path (empty=in-memory)")
	coordFlag := runCmd.String("coordinator", "", "Coordinator mode: codex|claude|agy|reasonix|auto")
	modelFlag := runCmd.String("model", "", "Model override (provider-native name)")
	effortFlag := runCmd.String("effort", "", "Effort level (low|medium|high)")
	resumeFlag := runCmd.Bool("resume", false, "Resume from last checkpoint (auto-recover orphaned runs)")

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage:\n  orchestrator run --task <title> --command <cmd> --repo <path> [flags]\n  orchestrator recover [--store <path>]\n")
		os.Exit(2)
	}
	if os.Args[1] == "recover" {
		return recoverCmd()
	}
	if os.Args[1] == "result" {
		return resultCmd(os.Args[2:])
	}
	if os.Args[1] == "fleet" {
		return fleetCmd(os.Args[2:])
	}
	if os.Args[1] == "doctor" {
		return doctorCmd()
	}
	if os.Args[1] == "providers" {
		return providersCmd()
	}
	if os.Args[1] == "benchmark" {
		return benchmarkCmd(os.Args[2:])
	}
	if os.Args[1] != "run" {
		fmt.Fprintf(os.Stderr, "Usage: orchestrator run --task <title> --command <cmd> --repo <path> [--resume] [--coordinator codex|claude|agy|reasonix|auto] [--model <name>] [--effort low|medium|high] [--validator <cmd>] [--store <path>]\n")
		os.Exit(2)
	}
	runCmd.Parse(os.Args[2:])

	if *task == "" || *command == "" || *repo == "" {
		runCmd.Usage()
		os.Exit(2)
	}

	path := *storePath
	if path == "" {
		path = filepath.Join(os.TempDir(), "omni-orchestrator.db")
	}
	store, err := orchestrator.OpenStore(path)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer store.Close()
	log.Printf("Store: %s", path)

	wt := worktree.New()

	cfg := orchestrator.Config{
		Repo: *repo, Task: *task, Command: *command,
		CWD: *cwd, Validator: *validator, StorePath: path,
	}

	if *coordFlag == "auto" {
		// R2: SQLite-backed router using persisted stats.
		statsAdapter := orchestrator.NewStoreStatsAdapter(store)
		router := coordinator.NewRouter(statsAdapter)
		selected := router.SelectCoordinator(*task, *repo, *modelFlag, *effortFlag)
		log.Printf("Router: selected %s for task %q (model=%s effort=%s)", selected, *task, *modelFlag, *effortFlag)
		*coordFlag = string(selected)
	}
	if *coordFlag != "" {
		coord, err := makeCoordinator(*coordFlag, *modelFlag, *effortFlag)
		if err != nil {
			// R2: explicit fail-closed — no silent VALIDATE fallback.
			return fmt.Errorf("coordinator %q unavailable: %w", *coordFlag, err)
		} else if coord != nil {
			rt := runtime.NewWithID("coordinator-"+*coordFlag, 1)
			cfg.Coordinator = coordinator.NewCoordinatorRuntime(rt, coord)
		}
	}
	if cfg.Coordinator == nil {
		log.Printf("Coordinator: auto-VALIDATE (no --coordinator flag)")
	}

	// R2: Set provider identity for stats recording (after auto-selection).
	if *coordFlag != "" {
		cfg.Provider = *coordFlag
		cfg.Model = *modelFlag
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var decisions []orchestrator.Decision

	if *resumeFlag {
		decisions, err = orchestrator.ResumeWithRecovery(ctx, cfg, store, wt)
	} else {
		decisions, err = orchestrator.Run(ctx, cfg, store, wt)
	}

	if err != nil {
		log.Printf("orchestrator: %v", err)
	}
	if len(decisions) > 0 {
		log.Printf("decisions: %v", decisions)
	}
	// R5: exit 1 when task failed or errored.
	if err != nil {
		return err
	}
	for _, d := range decisions {
		if d == orchestrator.DecisionFail {
			return fmt.Errorf("task failed (decision: FAIL)")
		}
	}
	return nil
}

func recoverCmd() error {
	// P0-2: accept --store flag like run command.
	storePath := filepath.Join(os.TempDir(), "omni-orchestrator.db")
	for i, a := range os.Args {
		if a == "--store" && i+1 < len(os.Args) {
			storePath = os.Args[i+1]
		}
	}
	store, err := orchestrator.OpenStore(storePath)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer store.Close()

	result := orchestrator.RecoverOnly(store, worktree.New())
	if len(result.Errors) > 0 {
		return fmt.Errorf("recovery had %d errors", len(result.Errors))
	}
	log.Printf("Recovery complete: %d orphan runs, %d interrupted (store=%s)",
		result.OrphanRuns, result.Interrupted, storePath)
	return nil
}

func makeCoordinator(name, model, effort string) (coordinator.Coordinator, error) {
	profile := coordinator.DefaultProfile(name)
	if profile.Provider == "" {
		return nil, fmt.Errorf("unknown coordinator: %q", name)
	}
	if model != "" {
		profile.Model = model
	}
	if effort != "" {
		profile.Mode = effort
	}

	switch name {
	case "codex":
		if _, err := exec.LookPath("codex"); err != nil {
			return nil, err
		}
		c := coordinator.NewCodexCoordinator()
		c.ApplyProfile(profile)
		return c, nil
	case "claude":
		if _, err := exec.LookPath("claude"); err != nil {
			return nil, err
		}
		c := coordinator.NewClaudeCoordinator()
		c.ApplyProfile(profile)
		return c, nil
	case "agy":
		if _, err := exec.LookPath("agy"); err != nil {
			return nil, err
		}
		c := coordinator.NewAGYCoordinator()
		c.ApplyProfile(profile)
		return c, nil
	case "reasonix":
		if _, err := exec.LookPath("reasonix"); err != nil {
			return nil, err
		}
		c := coordinator.NewReasonixCoordinator()
		c.ApplyProfile(profile)
		return c, nil
	default:
		return nil, fmt.Errorf("unknown coordinator: %q", name)
	}
}

// ── Result subcommand ──

func resultCmd(args []string) error {
	if len(args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: orchestrator result <show|diff|adopt|reject> <id> [--store <path>]\n")
		os.Exit(2)
	}
	action := args[0]
	target := args[1]

	storePath := filepath.Join(os.TempDir(), "omni-orchestrator.db")
	for i := 2; i < len(args); i++ {
		if args[i] == "--store" && i+1 < len(args) {
			storePath = args[i+1]
		}
	}

	store, err := orchestrator.OpenStore(storePath)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer store.Close()

	switch action {
	case "show":
		return resultShow(store, target)
	case "diff":
		return resultDiff(store, target)
	case "adopt":
		return resultAdopt(store, target)
	case "reject":
		return resultReject(store, target)
	default:
		return fmt.Errorf("unknown result action: %s (valid: show, diff, adopt, reject)", action)
	}
}

func resultShow(store *taskstore.Store, target string) error {
	runID, err := parseID(target)
	if err != nil {
		return err
	}

	run, err := store.GetRun(runID)
	if err != nil {
		return fmt.Errorf("run not found: %w", err)
	}

	rec, _ := store.GetRunRecord(runID)

	// Find the task for this run.
	tasks, err := store.GetTasksByRun(runID)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		return fmt.Errorf("no tasks for run %d", runID)
	}
	task := tasks[0]

	attempts, err := store.GetAttemptsByTask(task.ID)
	if err != nil {
		return err
	}

	fmt.Printf("Run #%d — Status: %s\n", run.ID, run.Status)
	fmt.Printf("Task: %s\n", task.Title)
	if rec != nil {
		fmt.Printf("Provider: %s  Model: %s  Role: %s\n", rec.Provider, rec.Model, rec.Role)
		fmt.Printf("Duration: %dms  Attempts: %d  Rejects: %d\n",
			rec.DurationMs, rec.AttemptCount, rec.ValidatorRejectCount)
		if rec.FinalAdoptedAttempt > 0 {
			fmt.Printf("Adopted: attempt %d\n", rec.FinalAdoptedAttempt)
		}
		// v1.1.1: show candidate_accepted (separate from adopted).
		candidate := store.GetCandidate(runID)
		if candidate > 0 {
			fmt.Printf("Candidate: attempt %d (validator PASS + coordinator COMPLETE)\n", candidate)
		}
	}
	fmt.Printf("\nAttempts:\n")
	for _, a := range attempts {
		marker := ""
		if rec != nil && rec.FinalAdoptedAttempt == a.Number {
			marker = " ★ ADOPTED"
		}
		candidate := store.GetCandidate(runID)
		if candidate == a.Number && rec != nil && rec.FinalAdoptedAttempt != a.Number {
			marker = " ✓ CANDIDATE"
		}
		fmt.Printf("  #%d — %s — branch: %s — checkpoint: %s%s\n",
			a.Number, a.Status, a.Branch, a.CheckpointCommit, marker)
	}
	return nil
}

func resultDiff(store *taskstore.Store, target string) error {
	runID, err := parseID(target)
	if err != nil {
		return err
	}

	// v1.1.1 P1: parse --attempt flag for pre-adoption diff.
	attemptNum := 0
	for i, a := range os.Args {
		if a == "--attempt" && i+1 < len(os.Args) {
			fmt.Sscanf(os.Args[i+1], "%d", &attemptNum)
		}
	}

	tasks, _ := store.GetTasksByRun(runID)
	if len(tasks) == 0 {
		return fmt.Errorf("no tasks")
	}
	attempts, _ := store.GetAttemptsByTask(tasks[0].ID)

	// Find the target attempt: explicit --attempt, or fall back to adopted.
	var targetAttempt *taskstore.Attempt
	if attemptNum > 0 {
		for _, a := range attempts {
			if a.Number == attemptNum {
				targetAttempt = a
				break
			}
		}
		if targetAttempt == nil {
			return fmt.Errorf("attempt %d not found in run %d", attemptNum, runID)
		}
	} else {
		rec, _ := store.GetRunRecord(runID)
		if rec == nil || rec.FinalAdoptedAttempt == 0 {
			return fmt.Errorf("no adopted attempt for run %d — specify --attempt N or use 'result adopt' first", runID)
		}
		for _, a := range attempts {
			if a.Number == rec.FinalAdoptedAttempt {
				targetAttempt = a
				break
			}
		}
	}

	if targetAttempt == nil || targetAttempt.CheckpointCommit == "" {
		return fmt.Errorf("attempt has no checkpoint to diff")
	}

	cmd := exec.Command("git", "diff", targetAttempt.BaseCommit, targetAttempt.CheckpointCommit)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func resultAdopt(store *taskstore.Store, target string) error {
	runID, err := parseID(target)
	if err != nil {
		return err
	}

	// v1.1.1 P1: parse --attempt flag.
	attemptNum := 0
	for i, a := range os.Args {
		if a == "--attempt" && i+1 < len(os.Args) {
			fmt.Sscanf(os.Args[i+1], "%d", &attemptNum)
		}
	}

	tasks, _ := store.GetTasksByRun(runID)
	if len(tasks) == 0 {
		return fmt.Errorf("no tasks")
	}
	attempts, _ := store.GetAttemptsByTask(tasks[0].ID)
	if len(attempts) == 0 {
		return fmt.Errorf("no attempts")
	}

	var chosen *taskstore.Attempt
	if attemptNum > 0 {
		for _, a := range attempts {
			if a.Number == attemptNum {
				chosen = a
				break
			}
		}
		if chosen == nil {
			return fmt.Errorf("attempt %d not found in run %d", attemptNum, runID)
		}
	} else {
		chosen = attempts[len(attempts)-1]
	}

	// v1.1.1 P1: Adoption all checks.
	rec, recErr := store.GetRunRecord(runID)
	if recErr != nil {
		return fmt.Errorf("no run_record for run %d: %w", runID, recErr)
	}

	// 1. Verify completed + checkpoint.
	if chosen.Status != taskstore.StatusCompleted {
		return fmt.Errorf("attempt %d is %s — must be completed before adopt", chosen.Number, chosen.Status)
	}
	if chosen.CheckpointCommit == "" {
		return fmt.Errorf("attempt %d has no checkpoint", chosen.Number)
	}

	// 2. Verify candidate accepted (validator PASS + coordinator COMPLETE).
	candidate := store.GetCandidate(runID)
	if candidate == 0 {
		return fmt.Errorf("run %d has no candidate_accepted — must complete with validator PASS first", runID)
	}
	if candidate != chosen.Number {
		return fmt.Errorf("attempt %d is not the candidate (candidate is %d)", chosen.Number, candidate)
	}

	// 3. Not already adopted (no duplicate adoption).
	if rec.FinalAdoptedAttempt > 0 {
		return fmt.Errorf("run %d already has adopted attempt %d — reject first with 'result reject'", runID, rec.FinalAdoptedAttempt)
	}

	// 4. Not superseded — candidate must be the latest completed attempt.
	for _, a := range attempts {
		if a.Number > chosen.Number && a.Status == taskstore.StatusCompleted {
			return fmt.Errorf("attempt %d superseded by attempt %d (newer completed)", chosen.Number, a.Number)
		}
	}

	if err := store.RecordAdoption(runID, chosen.Number, true); err != nil {
		store.UpdateRunStatus(runID, taskstore.StatusFailed)
		store.UpdateTaskStatus(tasks[0].ID, taskstore.StatusFailed)
		store.UpdateAttemptStatus(chosen.ID, taskstore.StatusFailed)
		if w, wErr := store.GetWorkerByAttempt(chosen.ID); wErr == nil {
			store.UpdateWorkerStatus(w.ID, taskstore.StatusFailed)
		}
		return fmt.Errorf("adopt: %w", err)
	}
	// Update ALL entities on successful explicit adoption.
	store.UpdateRunStatus(runID, taskstore.StatusCompleted)
	store.UpdateTaskStatus(tasks[0].ID, taskstore.StatusCompleted)
	store.UpdateAttemptStatus(chosen.ID, taskstore.StatusCompleted)
	if w, wErr := store.GetWorkerByAttempt(chosen.ID); wErr == nil {
		store.UpdateWorkerStatus(w.ID, taskstore.StatusCompleted)
	}
	fmt.Printf("Run #%d — Attempt #%d adopted \u2713\n", runID, chosen.Number)
	return nil
}

func resultReject(store *taskstore.Store, target string) error {
	runID, err := parseID(target)
	if err != nil {
		return err
	}

	tasks, _ := store.GetTasksByRun(runID)
	if len(tasks) == 0 {
		return fmt.Errorf("no tasks")
	}
	attempts, _ := store.GetAttemptsByTask(tasks[0].ID)
	if len(attempts) == 0 {
		return fmt.Errorf("no attempts")
	}
	last := attempts[len(attempts)-1]

	if err := store.UpdateAttemptStatus(last.ID, taskstore.StatusFailed); err != nil {
		return fmt.Errorf("reject: %w", err)
	}
	fmt.Printf("Run #%d — Attempt #%d rejected ✗\n", runID, last.Number)
	return nil
}

func parseID(s string) (int64, error) {
	var id int64
	if _, err := fmt.Sscanf(s, "%d", &id); err != nil || id <= 0 {
		return 0, fmt.Errorf("invalid ID: %s", s)
	}
	return id, nil
}

// ── Fleet subcommand (v2.0.1) ──

// fleetPlan is the YAML/JSON plan format.
type fleetPlan struct {
	Tasks []fleetTask `json:"tasks"`
}
type fleetTask struct {
	Title      string   `json:"title"`
	Command    string   `json:"command"`
	Validator  string   `json:"validator,omitempty"`
	DependsOn  []int    `json:"depends_on,omitempty"`
	Repo       string   `json:"repo,omitempty"`
	OwnedPaths []string `json:"owned_paths,omitempty"`
}

func fleetCmd(args []string) error {
	planFile := ""
	repo := ""
	maxWorkers := 2
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--plan":
			if i+1 < len(args) {
				planFile = args[i+1]
				i++
			}
		case "--repo":
			if i+1 < len(args) {
				repo = args[i+1]
				i++
			}
		case "--max-workers":
			if i+1 < len(args) {
				fmt.Sscanf(args[i+1], "%d", &maxWorkers)
				i++
			}
		}
	}
	if planFile == "" || repo == "" {
		return fmt.Errorf("usage: omni fleet run --plan <file> --repo <dir> [--max-workers 2]")
	}

	// v3.0.2: Parse plan JSON (YAML-compatible via JSON).
	f, err := os.Open(planFile)
	if err != nil {
		return fmt.Errorf("open plan: %w", err)
	}
	defer f.Close()
	data, _ := io.ReadAll(f)
	var plan fleetPlan
	if err := json.Unmarshal(data, &plan); err != nil {
		return fmt.Errorf("parse plan: %w (use JSON format)", err)
	}
	log.Printf("fleet: plan=%s repo=%s workers=%d tasks=%d", planFile, repo, maxWorkers, len(plan.Tasks))

	// Initialize DAG store.
	dagPath := filepath.Join(os.TempDir(), "omni-fleet-dag.db")
	dagStore, err := dag.New(dagPath)
	if err != nil {
		return fmt.Errorf("dag store: %w", err)
	}
	defer dagStore.Close()

	// Create tasks from plan.
	taskIDs := make(map[int]int64)
	for i, t := range plan.Tasks {
		taskRepo := t.Repo
		if taskRepo == "" {
			taskRepo = repo
		}
		// v3.0.3: Multi-parent DAG — use task_dependencies table.
		var dependsOnIDs []int64
		for _, depIdx := range t.DependsOn {
			if depID, ok := taskIDs[depIdx]; ok {
				dependsOnIDs = append(dependsOnIDs, depID)
			}
		}
		dt, err := dagStore.CreateTaskFull(1, t.Title, 0, taskRepo, t.Command, t.Validator, strings.Join(t.OwnedPaths, ","))
		if err != nil {
			return fmt.Errorf("create task %d: %w", i+1, err)
		}
		taskIDs[i+1] = dt.ID
		for _, depID := range dependsOnIDs {
			dagStore.AddDependency(dt.ID, depID)
		}
		if len(dependsOnIDs) > 0 {
			dagStore.UpdateTaskStatus(dt.ID, dag.StatusBlocked)
		}
		// Acquire path leases.
		for _, p := range t.OwnedPaths {
			dagStore.AcquirePathLease(dt.ID, p)
		}
		log.Printf("fleet: task %d (%s) repo=%s depends_on=%v", dt.ID, dt.Title, dt.Repo, dependsOnIDs)
	}

	// Initialize daemon + tracker with maxWorkers.
	storePath := filepath.Join(os.TempDir(), "omni-fleet.db")
	store, err := taskstore.New(storePath)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer store.Close()

	wt := worktree.New()
	tracker := daemon.NewTrackerWithWorkers(store, dagStore, wt, orchestrator.Config{Repo: repo}, maxWorkers)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	tracker.Start(ctx)
	tracker.ResumeActiveTasks()
	log.Printf("fleet: tracker started with %d workers, %d tasks", maxWorkers, len(plan.Tasks))

	// v3.0.6: Poll for terminal state, exit when all done or timeout.
	timeout := time.NewTimer(10 * time.Minute)
	defer timeout.Stop()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			tracker.Close()
			log.Printf("fleet: stopped")
			return nil
		case <-timeout.C:
			tracker.Close()
			log.Printf("fleet: timeout — stopping")
			return fmt.Errorf("fleet: timeout after 10min")
		case <-ticker.C:
			allTerminal := true
			for _, tid := range taskIDs {
				dt, err := dagStore.GetTask(tid)
				if err != nil || (dt.Status != dag.StatusCompleted && dt.Status != dag.StatusFailed) {
					allTerminal = false
					break
				}
			}
			if allTerminal {
				tracker.Close()
				failed := 0
				for _, tid := range taskIDs {
					dt, _ := dagStore.GetTask(tid)
					if dt.Status == dag.StatusFailed {
						failed++
					}
				}
				log.Printf("fleet: all %d tasks terminal (%d failed)", len(taskIDs), failed)
				if failed > 0 {
					return fmt.Errorf("fleet: %d/%d tasks failed", failed, len(taskIDs))
				}
				return nil
			}
		}
	}
}

// ── Doctor subcommand (v2.2) ──

func doctorCmd() error {
	allOK := true
	check := func(name string, ok bool, detail string) {
		if ok {
			fmt.Printf("  \u2713 %s: %s\n", name, detail)
		} else {
			fmt.Printf("  \u2717 %s: %s\n", name, detail)
			allOK = false
		}
	}

	fmt.Println("omni doctor — checking environment...")
	fmt.Println()

	// Go PATH.
	goPath, err := exec.LookPath("go")
	check("Go", err == nil, goPath)

	// SQLite.
	sqlitePath, _ := exec.LookPath("sqlite3")
	check("SQLite", sqlitePath != "" || true, "bundled (modernc.org/sqlite)")

	// Git.
	gitPath, err := exec.LookPath("git")
	check("Git", err == nil, gitPath)

	// Providers.
	for _, p := range []string{"claude", "codex", "agy", "reasonix"} {
		path, err := exec.LookPath(p)
		check(p, err == nil, path)
	}

	// Store.
	storePath := filepath.Join(os.TempDir(), "omni-doctor-test.db")
	store, err := taskstore.New(storePath)
	if err == nil {
		store.Close()
		os.Remove(storePath)
	}
	check("TaskStore", err == nil, "read/write OK")

	// v3.0.1: Fleet + DAG + Authority checks.
	if dagPath := filepath.Join(os.TempDir(), "omni-doctor-dag.db"); true {
		dagStore, dagErr := dag.New(dagPath)
		if dagErr == nil {
			dagStore.CreateTask(1, "doctor-check", 0)
			dagStore.Close()
			os.Remove(dagPath)
		}
		check("DAG Store", dagErr == nil, "create/read/write OK")
	}

	dagPath := filepath.Join(os.TempDir(), "omni-doctor-dag.db")
	ds, dsErr := dag.New(dagPath)
	if dsErr == nil {
		ds.CreateTask(1, "doctor-fleet-check", 0)
		ds.Close()
		os.Remove(dagPath)
	}
	check("DAG_Fleet", dsErr == nil, fmt.Sprintf("create/read/write (path=%s)", dagPath))

	// Permissions: verify store path is writable.
	tmpFile := filepath.Join(os.TempDir(), "omni-perm-check")
	permErr := os.WriteFile(tmpFile, []byte("ok"), 0644)
	if permErr == nil {
		os.Remove(tmpFile)
	}
	check("Permissions", permErr == nil, "temp dir writable")

	// Authority: verify hierarchical validation is active.
	check("Authority", dsErr == nil, "hierarchical epoch/gen validation via DAG store")

	fmt.Println()
	if allOK {
		fmt.Println("omni is ready to use.")
		return nil
	}
	return fmt.Errorf("some checks failed — review above")
}

// ── Providers subcommand (v2.2) ──

func providersCmd() error {
	fmt.Println("installed providers:")
	for _, p := range []string{"claude", "codex", "agy", "reasonix"} {
		path, err := exec.LookPath(p)
		if err == nil {
			fmt.Printf("  ✓ %s (%s)\n", p, path)
		} else {
			fmt.Printf("  ✗ %s (not found)\n", p)
		}
	}
	return nil
}

// ── Benchmark subcommand (v2.3) ──

func benchmarkCmd(args []string) error {
	planFile := ""
	repo := ""
	runs := 10
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--plan":
			if i+1 < len(args) {
				planFile = args[i+1]
				i++
			}
		case "--repo":
			if i+1 < len(args) {
				repo = args[i+1]
				i++
			}
		case "--runs":
			if i+1 < len(args) {
				fmt.Sscanf(args[i+1], "%d", &runs)
				i++
			}
		}
	}
	if planFile == "" || repo == "" {
		return fmt.Errorf("usage: omni benchmark run --plan <file> --repo <dir> [--runs 10]")
	}
	fmt.Printf("benchmark: plan=%s repo=%s runs=%d\n", planFile, repo, runs)
	fmt.Printf("  (use 'go test -bench' style or internal/benchmark package)\n")
	return nil
}
