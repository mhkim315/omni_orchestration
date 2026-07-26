// omnid is the OMNI background daemon process.
// It exposes HTTP API for run/task/attempt CRUD, automatically
// resumes active runs on startup, and shuts down gracefully.
//
// Usage:
//
//	omnid --store /var/lib/omni/store.db --listen :8443 --tls
//	omnid --store /var/lib/omni/store.db --socket /var/run/omni.sock
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/mhkim315/omni_orchestration/internal/daemon"
)

func main() {
	storePath := flag.String("store", "/var/lib/omni/store.db", "SQLite store path")
	listenAddr := flag.String("listen", "127.0.0.1:8443", "HTTP listen address")
	socketPath := flag.String("socket", "", "Unix socket path (overrides --listen)")
	tlsFlag := flag.Bool("tls", false, "Enable TLS")
	certFile := flag.String("cert", "", "TLS certificate file")
	keyFile := flag.String("key", "", "TLS key file")
	repoBase := flag.String("repo", "", "Default repo path")
	flag.Parse()

	cfg := daemon.Config{
		StorePath:  *storePath,
		ListenAddr: *listenAddr,
		SocketPath: *socketPath,
		TLS:        *tlsFlag,
		CertFile:   *certFile,
		KeyFile:    *keyFile,
		RepoBase:   *repoBase,
	}

	d, err := daemon.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "omnid: %v\n", err)
		os.Exit(1)
	}
	defer d.Store().Close()

	if err := d.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "omnid: start: %v\n", err)
		os.Exit(1)
	}

	log.Printf("omnid: started (store=%s)", *storePath)
	daemon.WaitForSignal(d)
	log.Printf("omnid: stopped")
}
