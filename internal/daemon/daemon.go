// Package daemon provides the OMNI background daemon with HTTP API,
// automatic resume on startup, and graceful shutdown.
//
// v1.6: Core control plane — HTTP CRUD for runs/tasks/attempts.
package daemon

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/mhkim315/omni_orchestration/internal/orchestrator"
	"github.com/mhkim315/omni_orchestration/internal/taskstore"
	"github.com/mhkim315/omni_orchestration/internal/worktree"
)

// ── Daemon ──

type Daemon struct {
	cfg    Config
	store  *taskstore.Store
	wt     *worktree.Manager
	srv    *http.Server
	mu     sync.Mutex
	runs   map[int64]*orchestrator.Config // active run configs
	closed bool
}

type Config struct {
	StorePath  string
	ListenAddr string
	SocketPath string
	TLS        bool
	CertFile   string
	KeyFile    string
	RepoBase   string
}

// New creates a daemon with the given configuration.
func New(cfg Config) (*Daemon, error) {
	store, err := taskstore.New(cfg.StorePath)
	if err != nil {
		return nil, fmt.Errorf("daemon store: %w", err)
	}
	d := &Daemon{
		cfg:   cfg,
		store: store,
		wt:    worktree.New(),
		runs:  make(map[int64]*orchestrator.Config),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/run", d.handleRun)
	mux.HandleFunc("/api/runs", d.handleListRuns)
	mux.HandleFunc("/api/task", d.handleTask)
	mux.HandleFunc("/api/status", d.handleStatus)
	mux.HandleFunc("/api/resume", d.handleResume)
	d.srv = &http.Server{Handler: mux}

	return d, nil
}

// Start begins listening and resumes active runs.
func (d *Daemon) Start() error {
	// Resume active runs from store.
	d.resumeActive()

	// Start HTTP listener.
	ln, err := d.listen()
	if err != nil {
		return err
	}
	log.Printf("daemon: listening on %s", d.listenerAddr())
	go d.srv.Serve(ln)

	return nil
}

// Shutdown gracefully stops the daemon.
func (d *Daemon) Shutdown(ctx context.Context) error {
	d.mu.Lock()
	d.closed = true
	d.mu.Unlock()

	log.Printf("daemon: shutting down gracefully...")
	return d.srv.Shutdown(ctx)
}

// Store returns the underlying task store.
func (d *Daemon) Store() *taskstore.Store { return d.store }

func (d *Daemon) listen() (net.Listener, error) {
	if d.cfg.SocketPath != "" {
		os.Remove(d.cfg.SocketPath)
		return net.Listen("unix", d.cfg.SocketPath)
	}
	if d.cfg.TLS {
		cert, err := tls.LoadX509KeyPair(d.cfg.CertFile, d.cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("tls: %w", err)
		}
		return tls.Listen("tcp", d.cfg.ListenAddr, &tls.Config{Certificates: []tls.Certificate{cert}})
	}
	return net.Listen("tcp", d.cfg.ListenAddr)
}

func (d *Daemon) listenerAddr() string {
	if d.cfg.SocketPath != "" {
		return d.cfg.SocketPath
	}
	return d.cfg.ListenAddr
}

func (d *Daemon) resumeActive() {
	active, err := d.store.GetActiveAttempts()
	if err != nil || len(active) == 0 {
		return
	}
	log.Printf("daemon: found %d active attempts — reconciling", len(active))
	result := orchestrator.Reconcile(d.store, d.wt)
	log.Printf("daemon: reconciled %d interrupted, %d orphan runs", result.Interrupted, result.OrphanRuns)

	// Re-check for recoverable attempts.
	active, _ = d.store.GetActiveAttempts()
	if len(active) > 0 {
		log.Printf("daemon: %d active attempts after reconcile — recovery pending", len(active))
	}
}

// ── HTTP Handlers ──

func (d *Daemon) handleStatus(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	closed := d.closed
	d.mu.Unlock()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  map[bool]string{true: "shutting_down", false: "running"}[closed],
		"version": "1.6.0",
	})
}

func (d *Daemon) handleListRuns(w http.ResponseWriter, r *http.Request) {
	active, _ := d.store.GetActiveAttempts()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"active_attempts": len(active),
	})
}

func (d *Daemon) handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var req struct {
		Task    string `json:"task"`
		Command string `json:"command"`
		Repo    string `json:"repo"`
		Store   string `json:"store"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if req.Repo == "" {
		req.Repo = d.cfg.RepoBase
	}
	if req.Store == "" {
		req.Store = d.cfg.StorePath
	}

	cfg := orchestrator.Config{Repo: req.Repo, Task: req.Task, Command: req.Command}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Minute)
	defer cancel()

	decisions, err := orchestrator.Run(ctx, cfg, d.store, d.wt)
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{"decisions": decisions})
}

func (d *Daemon) handleTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var req struct {
		Title string `json:"title"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	// Create a run + task.
	run, err := d.store.CreateRun()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	task, err := d.store.CreateTask(run.ID, req.Title)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"run_id": run.ID, "task_id": task.ID, "title": task.Title,
	})
}

func (d *Daemon) handleResume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var req struct {
		Task    string `json:"task"`
		Command string `json:"command"`
		Repo    string `json:"repo"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	if req.Repo == "" {
		req.Repo = d.cfg.RepoBase
	}

	cfg := orchestrator.Config{Repo: req.Repo, Task: req.Task, Command: req.Command}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Minute)
	defer cancel()

	decisions, err := orchestrator.ResumeWithRecovery(ctx, cfg, d.store, d.wt)
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{"decisions": decisions})
}

// ── Signal helpers ──

// WaitForSignal blocks until SIGTERM or SIGINT, then calls shutdown.
func WaitForSignal(d *Daemon) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigCh
	log.Printf("daemon: received %v", sig)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := d.Shutdown(ctx); err != nil {
		log.Printf("daemon: shutdown error: %v", err)
	}
}

// Ensure imports.
var _ = fmt.Sprintf
var _ = filepath.Join
var _ = time.Now
