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

	if len(os.Args) < 2 || os.Args[1] != "run" {
		fmt.Fprintf(os.Stderr, "Usage: orchestrator run --task <title> --command <cmd> --repo <path> [--coordinator codex|claude|agy|reasonix|auto] [--model <name>] [--effort low|medium|high] [--validator <cmd>] [--store <path>]\n")
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

	decisions, err := orchestrator.Run(ctx, cfg, store, wt)
	if err != nil {
		log.Printf("orchestrator: %v", err)
	}
	if len(decisions) > 0 {
		log.Printf("decisions: %v", decisions)
	}
	return err
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
