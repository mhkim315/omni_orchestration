// Orchestrator CLI — integrates worktree, runtime, supervisor, and taskstore
// into a coordinated task-execution loop.
//
// Usage:
//
//	orchestrator run --task "bug fix" --command claude --repo /path/to/repo
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/miinanii/omni_orchestration/internal/orchestrator"
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
	task := runCmd.String("task", "", "Task title (required)")
	command := runCmd.String("command", "", "Worker command (required)")
	repo := runCmd.String("repo", "", "Path to git repository (required)")
	cwd := runCmd.String("cwd", "", "Working directory (defaults to worktree path)")

	if len(os.Args) < 2 || os.Args[1] != "run" {
		fmt.Fprintf(os.Stderr, "Usage: orchestrator run --task <title> --command <cmd> --repo <path>\n")
		os.Exit(2)
	}
	runCmd.Parse(os.Args[2:])

	if *task == "" || *command == "" || *repo == "" {
		runCmd.Usage()
		os.Exit(2)
	}

	// Open SQLite store (in-memory for now; persist later).
	store, err := taskstore.NewInMemory()
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer store.Close()

	wt := worktree.New()

	cfg := orchestrator.Config{
		Repo:    *repo,
		Task:    *task,
		Command: *command,
		CWD:     *cwd,
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
