// Orchestrator CLI — integrates worktree, runtime, supervisor, and taskstore
// into a coordinated task-execution loop with validator and recovery.
//
// Usage:
//
//	orchestrator run --task "bug fix" --command claude --repo /path/to/repo
//	orchestrator run --task "..." --command claude --repo . --validator "test -f output.txt"
//	orchestrator run --task "..." --command claude --repo . --store /tmp/orch.db
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/miinanii/omni_orchestration/internal/orchestrator"
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

	if len(os.Args) < 2 || os.Args[1] != "run" {
		fmt.Fprintf(os.Stderr, "Usage: orchestrator run --task <title> --command <cmd> --repo <path> [--validator <cmd>] [--store <path>]\n")
		os.Exit(2)
	}
	runCmd.Parse(os.Args[2:])

	if *task == "" || *command == "" || *repo == "" {
		runCmd.Usage()
		os.Exit(2)
	}

	// Gate 2: file-backed store default (in temp dir if not specified).
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
