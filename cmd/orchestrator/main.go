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
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/miinanii/omni_orchestration/internal/coordinator"
	"github.com/miinanii/omni_orchestration/internal/orchestrator"
	"github.com/miinanii/omni_orchestration/internal/runtime"
	"github.com/miinanii/omni_orchestration/internal/taskstore"
	"github.com/miinanii/omni_orchestration/internal/worktree"
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
		router := coordinator.NewRouter(nil)
		selected := router.SelectCoordinator(*task, *repo, *modelFlag, *effortFlag)
		log.Printf("Router: selected %s for task %q (model=%s effort=%s)", selected, *task, *modelFlag, *effortFlag)
		*coordFlag = string(selected)
	}
	if *coordFlag != "" {
		coord, err := makeCoordinator(*coordFlag, *modelFlag, *effortFlag)
		if err != nil {
			log.Printf("Coordinator: %v — falling back to auto-VALIDATE", err)
		} else if coord != nil {
			rt := runtime.NewWithID("coordinator-"+*coordFlag, 1)
			cfg.Coordinator = coordinator.NewCoordinatorRuntime(rt, coord)
		}
	}
	if cfg.Coordinator == nil {
		log.Printf("Coordinator: auto-VALIDATE (no --coordinator flag)")
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
	return err
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
		fmt.Printf("Duration: %dms  Attempts: %d  Rejects: %d  Adopted: %d\n",
			rec.DurationMs, rec.AttemptCount, rec.ValidatorRejectCount, rec.FinalAdoptedAttempt)
	}
	fmt.Printf("\nAttempts:\n")
	for _, a := range attempts {
		adopted := ""
		if rec != nil && rec.FinalAdoptedAttempt == a.Number {
			adopted = " ★ ADOPTED"
		}
		fmt.Printf("  #%d — %s — branch: %s — checkpoint: %s%s\n",
			a.Number, a.Status, a.Branch, a.CheckpointCommit, adopted)
	}
	return nil
}

func resultDiff(store *taskstore.Store, target string) error {
	runID, err := parseID(target)
	if err != nil {
		return err
	}

	rec, err := store.GetRunRecord(runID)
	if err != nil {
		return fmt.Errorf("run record not found: %w", err)
	}

	if rec.FinalAdoptedAttempt == 0 {
		return fmt.Errorf("no adopted attempt for run %d — use 'result adopt <attemptID>' first", runID)
	}

	// Show git diff of the checkpoint commit vs base.
	tasks, _ := store.GetTasksByRun(runID)
	if len(tasks) == 0 {
		return fmt.Errorf("no tasks")
	}
	attempts, _ := store.GetAttemptsByTask(tasks[0].ID)

	for _, a := range attempts {
		if a.Number == rec.FinalAdoptedAttempt && a.CheckpointCommit != "" {
			cmd := exec.Command("git", "diff", a.BaseCommit, a.CheckpointCommit)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			return cmd.Run()
		}
	}
	return fmt.Errorf("adopted attempt has no checkpoint")
}

func resultAdopt(store *taskstore.Store, target string) error {
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

	// P0-2: verify run_record exists before adoption.
	if _, err := store.GetRunRecord(runID); err != nil {
		return fmt.Errorf("no run_record for run %d: %w", runID, err)
	}
	if err := store.RecordAdoption(runID, last.Number, true); err != nil {
		return fmt.Errorf("adopt: %w", err)
	}
	fmt.Printf("Run #%d — Attempt #%d adopted ✓\n", runID, last.Number)
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
