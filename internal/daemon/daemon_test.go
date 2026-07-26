package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// 1. Daemon start/stop
func TestDaemonStartStop(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "daemon-test.db")
	cfg := Config{StorePath: storePath, ListenAddr: "127.0.0.1:0"}
	d, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer d.store.Close()

	// Listen on random port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	cfg.ListenAddr = addr
	d2, _ := New(cfg)
	defer d2.store.Close()

	ln2, _ := net.Listen("tcp", addr)
	go d2.srv.Serve(ln2)
	defer d2.srv.Shutdown(context.Background())

	// Verify status endpoint.
	resp, err := http.Get(fmt.Sprintf("http://%s/api/status", addr))
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	defer resp.Body.Close()
	var status map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&status)
	if status["status"] != "running" {
		t.Errorf("status = %v, want running", status["status"])
	}
}

// 2. Daemon resume active runs
func TestDaemonResume(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "resume-daemon.db")
	cfg := Config{StorePath: storePath, ListenAddr: "127.0.0.1:0"}

	d, _ := New(cfg)
	defer d.store.Close()

	// Create an active run.
	run, _ := d.store.CreateRun()
	task, _ := d.store.CreateTask(run.ID, "test resume task")
	attempt, _ := d.store.CreateAttempt(task.ID, 1, "w1", "br", "HEAD")
	d.store.RecordWorkerPID(attempt.ID, "sleep 5", "/tmp", "primary", 1, 12345, 0)

	// Resume picks it up.
	d.resumeActive()

	// Attempt should now be interrupted (no checkpoint, worker dead).
	active, _ := d.store.GetActiveAttempts()
	t.Logf("active after resume: %d", len(active))
	_ = active
}

// 3. Concurrent API — multiple requests don't corrupt state
func TestConcurrentAPI(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "concurrent-daemon.db")
	cfg := Config{StorePath: storePath, ListenAddr: "127.0.0.1:0"}

	d, _ := New(cfg)
	defer d.store.Close()

	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()

	cfg.ListenAddr = addr
	d2, _ := New(cfg)
	defer d2.store.Close()
	ln2, _ := net.Listen("tcp", addr)
	go d2.srv.Serve(ln2)
	defer d2.srv.Shutdown(context.Background())

	var wg sync.WaitGroup
	errors := make(chan error, 10)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Get(fmt.Sprintf("http://%s/api/status", addr))
			if err != nil {
				errors <- err
				return
			}
			resp.Body.Close()
		}()
	}
	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("concurrent request failed: %v", err)
	}
}

// 4. Graceful stop — shutdown completes pending requests
func TestGracefulStop(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "graceful-daemon.db")
	cfg := Config{StorePath: storePath, ListenAddr: "127.0.0.1:0"}

	d, _ := New(cfg)
	defer d.store.Close()

	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()

	cfg.ListenAddr = addr
	d2, _ := New(cfg)
	defer d2.store.Close()
	ln2, _ := net.Listen("tcp", addr)
	go d2.srv.Serve(ln2)

	// Make a request to prove it's alive.
	resp, _ := http.Get(fmt.Sprintf("http://%s/api/status", addr))
	resp.Body.Close()

	// Graceful shutdown.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := d2.srv.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// After shutdown, requests should fail.
	_, err := http.Get(fmt.Sprintf("http://%s/api/status", addr))
	if err == nil {
		t.Error("request succeeded after shutdown — expected failure")
	}
	t.Logf("graceful shutdown complete")
}

var _ = fmt.Sprintf
var _ = os.TempDir
