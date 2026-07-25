package supervisor

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/miinanii/omni_orchestration/internal/runtime"
	"github.com/miinanii/omni_orchestration/internal/worktree"
)

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

// tmpRepo creates a temporary git repository for testing.
func tmpRepo(t *testing.T) (string, *worktree.Manager) {
	t.Helper()
	dir, err := os.MkdirTemp("", "omni-recovery-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	runGit(t, dir, "init")
	runGit(t, dir, "config", "user.email", "test@omni.local")
	runGit(t, dir, "config", "user.name", "Omni Test")
	// Create an initial commit so worktrees can branch.
	os.WriteFile(filepath.Join(dir, "README.md"), []byte("# test"), 0644)
	runGit(t, dir, "add", "README.md")
	runGit(t, dir, "commit", "-m", "initial")

	return dir, worktree.New()
}

func TestRecovery_DirtyWorktreeCheckpointCreated(t *testing.T) {
	repoDir, wm := tmpRepo(t)

	wt, err := wm.Create(repoDir, "task-1", "attempt-1")
	if err != nil {
		t.Fatalf("Create worktree: %v", err)
	}

	// Make a dirty change.
	dirtyFile := filepath.Join(wt.Path, "output.txt")
	if err := os.WriteFile(dirtyFile, []byte("worker output"), 0644); err != nil {
		t.Fatalf("write dirty file: %v", err)
	}

	// Confirm worktree is dirty.
	report := wm.Status(wt.Path)
	if report.Clean {
		t.Fatal("worktree should be dirty after file write")
	}

	// Simulate a worker that already exited.
	rt := runtime.New()
	rt.Start("echo done", "/tmp")
	rt.Wait()

	cfg := RecoveryConfig{
		WorktreePath:      wt.Path,
		AttemptID:         "attempt-1",
		SecretScanEnabled: true,
	}

	result := Recover(context.Background(), rt, cfg, wm)
	if result.Error != "" {
		t.Fatalf("Recover error: %s", result.Error)
	}
	if !result.Checkpointed {
		t.Fatal("expected checkpoint to be created on dirty worktree")
	}
	if result.CommitSHA == "" {
		t.Fatal("expected non-empty commit SHA")
	}
	if result.BlockedSecret {
		t.Fatal("secret scan should not have blocked clean content")
	}
	t.Logf("checkpoint SHA: %s", result.CommitSHA)
}

func TestRecovery_CleanWorktreeNoCheckpoint(t *testing.T) {
	repoDir, wm := tmpRepo(t)

	wt, err := wm.Create(repoDir, "task-2", "attempt-2")
	if err != nil {
		t.Fatalf("Create worktree: %v", err)
	}

	rt := runtime.New()
	rt.Start("echo done", "/tmp")
	rt.Wait()

	cfg := RecoveryConfig{
		WorktreePath:      wt.Path,
		AttemptID:         "attempt-2",
		SecretScanEnabled: true,
	}

	result := Recover(context.Background(), rt, cfg, wm)
	if result.Error != "" {
		t.Fatalf("Recover error: %s", result.Error)
	}
	if result.Checkpointed {
		t.Fatal("clean worktree should NOT create checkpoint")
	}
	if !result.SkippedClean {
		t.Error("SkippedClean should be true for clean worktree")
	}
}

func TestRecovery_SecretPatternBlocked(t *testing.T) {
	repoDir, wm := tmpRepo(t)

	wt, err := wm.Create(repoDir, "task-3", "attempt-3")
	if err != nil {
		t.Fatalf("Create worktree: %v", err)
	}

	// Write a file containing a secret pattern.
	secretFile := filepath.Join(wt.Path, ".env")
	if err := os.WriteFile(secretFile, []byte("ANTHROPIC_API_KEY=sk-ant-api-1234567890abcdef"), 0644); err != nil {
		t.Fatalf("write secret file: %v", err)
	}

	rt := runtime.New()
	rt.Start("echo done", "/tmp")
	rt.Wait()

	cfg := RecoveryConfig{
		WorktreePath:      wt.Path,
		AttemptID:         "attempt-3",
		SecretScanEnabled: true,
	}

	result := Recover(context.Background(), rt, cfg, wm)
	if result.Error == "" {
		t.Fatal("expected error from blocked secret scan")
	}
	if !result.BlockedSecret {
		t.Fatal("expected BlockedSecret to be true")
	}
	t.Logf("correctly blocked: %s", result.Error)
}

func TestRecovery_SecretScanDisabled(t *testing.T) {
	repoDir, wm := tmpRepo(t)

	wt, err := wm.Create(repoDir, "task-4", "attempt-4")
	if err != nil {
		t.Fatalf("Create worktree: %v", err)
	}

	secretFile := filepath.Join(wt.Path, ".env")
	os.WriteFile(secretFile, []byte("ANTHROPIC_API_KEY=sk-ant-api-1234567890abcdef"), 0644)

	rt := runtime.New()
	rt.Start("echo done", "/tmp")
	rt.Wait()

	cfg := RecoveryConfig{
		WorktreePath:      wt.Path,
		AttemptID:         "attempt-4",
		SecretScanEnabled: false,
	}

	result := Recover(context.Background(), rt, cfg, wm)
	if result.Error != "" {
		t.Fatalf("Recover error: %s", result.Error)
	}
	if !result.Checkpointed {
		t.Fatal("expected checkpoint when secret scan is disabled")
	}
	if result.BlockedSecret {
		t.Fatal("secret scan should not have blocked (disabled)")
	}
}

func TestRecovery_NoWorktree(t *testing.T) {
	rt := runtime.New()
	rt.Start("echo done", "/tmp")
	rt.Wait()

	cfg := RecoveryConfig{WorktreePath: "", AttemptID: "no-wt"}
	result := Recover(context.Background(), rt, cfg, nil)

	if result.Error != "" {
		t.Fatalf("Recover error: %v", result)
	}
	if result.Checkpointed {
		t.Fatal("no-worktree should not checkpoint")
	}
}

func TestRecovery_CheckpointedSuccessfully(t *testing.T) {
	tests := []struct {
		name   string
		result RecoveryResult
		want   bool
	}{
		{"clean success", RecoveryResult{Checkpointed: true, CommitSHA: "abc123"}, true},
		{"skipped clean", RecoveryResult{SkippedClean: true}, true},
		{"blocked secret", RecoveryResult{BlockedSecret: true, Error: "secret"}, false},
		{"error", RecoveryResult{Error: "something failed"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.result.CheckpointedSuccessfully(); got != tt.want {
				t.Errorf("CheckpointedSuccessfully() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestShouldCheckpoint(t *testing.T) {
	dirty := worktree.StatusReport{Clean: false}
	clean := worktree.StatusReport{Clean: true}

	should, _ := ShouldCheckpoint(dirty, true)
	if !should {
		t.Error("dirty worktree: should checkpoint")
	}

	should, _ = ShouldCheckpoint(clean, true)
	if should {
		t.Error("clean worktree: should NOT checkpoint")
	}
}

func TestScanSecrets_MatchesAllPatterns(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "omni-secretscan-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	tests := []struct {
		name    string
		content string
		want    bool
	}{
		{"anthropic key", "sk-ant-api-1234567890abcdef", true},
		{"orca key", "sk-orca-abcdef1234567890", true},
		{"github token", "ghp_123456789012345678901234567890123456", true},
		{"rsa private key", "-----BEGIN RSA PRIVATE KEY-----", true},
		{"openssh private key", "-----BEGIN OPENSSH PRIVATE KEY-----", true},
		{"slack bot token", "xoxb-1234567890-abcdef", true},
		{"safe content", "hello world", false},
		{"safe code", "func main() { println(\"hi\") }", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := filepath.Join(tmpDir, tt.name+".txt")
			os.WriteFile(f, []byte(tt.content), 0644)
			blocked, matched := scanSecrets(tmpDir, []string{filepath.Base(f)})
			if blocked != tt.want {
				t.Errorf("scanSecrets = %v, want %v (matched: %s)", blocked, tt.want, matched)
			}
		})
	}
}

func TestScanSecrets_DirectorySkipped(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "omni-secretscan-dir-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a subdirectory — should be skipped, not crash.
	subDir := filepath.Join(tmpDir, "subdir")
	os.Mkdir(subDir, 0755)

	blocked, _ := scanSecrets(tmpDir, []string{filepath.Base(subDir)})
	if blocked {
		t.Error("directory should not trigger secret scan match")
	}
}
