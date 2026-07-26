package benchmark

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBenchmarkProducesValidCSV(t *testing.T) {
	repoDir := t.TempDir()
	os.WriteFile(filepath.Join(repoDir, "README.md"), []byte("# bench"), 0644)
	runGit(t, repoDir, "init")
	runGit(t, repoDir, "config", "user.email", "bench@test")
	runGit(t, repoDir, "config", "user.name", "Bench")
	runGit(t, repoDir, "add", "README.md")
	runGit(t, repoDir, "commit", "-m", "init")

	plan := &Plan{
		Repo:     repoDir,
		Provider: "claude",
		Modes:    []string{"sequential", "fleet"},
		Tasks: []TaskSpec{
			{Title: "task-1", Command: "echo done1", Validator: "true"},
			{Title: "task-2", Command: "echo done2", Validator: "true"},
			{Title: "task-3", Command: "echo done3", Validator: "true"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	results, err := Run(ctx, plan, 5)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Write CSV.
	var buf bytes.Buffer
	if err := WriteCSV(&buf, results); err != nil {
		t.Fatalf("WriteCSV: %v", err)
	}
	csvStr := buf.String()
	t.Logf("CSV:\n%s", csvStr)

	// Valid CSV: has header + 10 data rows (5 runs × 2 modes).
	lines := strings.Split(strings.TrimSpace(csvStr), "\n")
	if len(lines) < 11 {
		t.Errorf("expected >=11 CSV lines, got %d", len(lines))
	}
	if !strings.Contains(lines[0], "run_id") {
		t.Error("CSV missing header")
	}

	// Summary.
	summary := Summary(results)
	t.Logf("Summary:\n%s", summary)

	// Sequential should be slower than fleet (parallel tasks).
	var seqAvg, fleetAvg int64
	var seqN, fleetN int
	for _, r := range results {
		if r.Mode == "sequential" {
			seqAvg += r.WallClockMs
			seqN++
		} else {
			fleetAvg += r.WallClockMs
			fleetN++
		}
	}
	if seqN > 0 && fleetN > 0 {
		t.Logf("sequential avg: %dms, fleet avg: %dms", seqAvg/int64(seqN), fleetAvg/int64(fleetN))
	}
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}
