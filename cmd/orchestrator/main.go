// Orchestrator CLI — integrates worktree, runtime, supervisor, taskstore
// and coordinator into a coordinated task-execution loop.
//
// Usage:
//
//	orchestrator run --task "bug fix" --command claude --repo /path/to/repo
//	orchestrator run --task "..." --command claude --repo . --coordinator codex
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
	coordFlag := runCmd.String("coordinator", "", "Coordinator mode: codex (default: auto-VALIDATE)")

	if len(os.Args) < 2 || os.Args[1] != "run" {
		fmt.Fprintf(os.Stderr, "Usage: orchestrator run --task <title> --command <cmd> --repo <path> [--coordinator codex|claude] [--validator <cmd>] [--store <path>]\n")
		os.Exit(2)
	}
	runCmd.Parse(os.Args[2:])

	if *task == "" || *command == "" || *repo == "" {
		runCmd.Usage()
		os.Exit(2)
	}

	// Store.
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
		Repo:      *repo,
		Task:      *task,
		Command:   *command,
		CWD:       *cwd,
		Validator: *validator,
		StorePath: path,
	}

	// Coordinator wiring (provider-independent).
	if *coordFlag == "auto" {
		// OMNI B: performance-based provider routing.
		router := coordinator.NewRouter(nil) // nil = memory stats (default Codex)
		selected := router.SelectCoordinator(*task, *repo)
		log.Printf("Router: selected %s for task %q", selected, *task)
		switch selected {
		case coordinator.ProviderCodex:
			*coordFlag = "codex"
		case coordinator.ProviderClaude:
			*coordFlag = "claude"
		}
	}
	if *coordFlag == "codex" || *coordFlag == "claude" {
		var coord coordinator.Coordinator
		id := *coordFlag
		if *coordFlag == "codex" {
			if _, err := exec.LookPath("codex"); err != nil {
				log.Printf("codex not found on PATH — falling back to auto-VALIDATE mode")
				id = ""
			} else {
				coord = coordinator.NewCodexCoordinator()
			}
		} else if *coordFlag == "claude" {
			if _, err := exec.LookPath("claude"); err != nil {
				log.Printf("claude not found on PATH — falling back to auto-VALIDATE mode")
				id = ""
			} else {
				coord = coordinator.NewClaudeCoordinator()
			}
		} else if *coordFlag == "agy" {
			if _, err := exec.LookPath("agy"); err != nil {
				log.Printf("agy not found on PATH — falling back to auto-VALIDATE mode")
				id = ""
			} else {
				coord = coordinator.NewAGYCoordinator()
			}
		}
		if id != "" {
			rt := runtime.NewWithID("coordinator-"+id, 1)
			cfg.Coordinator = coordinator.NewCoordinatorRuntime(rt, coord)
			log.Printf("Coordinator: %s", id)
		}
	} else if *coordFlag != "" {
		return fmt.Errorf("unknown coordinator mode: %q (supported: auto, codex, claude, agy)", *coordFlag)
	} else {
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
