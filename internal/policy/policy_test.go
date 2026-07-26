package policy

import (
	"testing"
)

// 1. Capability mismatch — provider lacks required capability → reject
func TestCapabilityMismatchReject(t *testing.T) {
	pol := &Policy{
		RequireCaps: []Capability{CapNetworkAccess},
	}
	codex := &ProviderDecl{
		Name: "codex",
		Caps: []Capability{CapModelSelection},
	}
	err := Check(codex, pol)
	if err == nil {
		t.Fatal("expected capability mismatch error")
	}
	t.Logf("capability mismatch (expected): %v", err)
}

// 2. Sandbox enforce — provider requires sandbox but not sandboxed → reject
func TestSandboxEnforce(t *testing.T) {
	// Provider declares sandbox_required but is not sandboxed.
	p := &ProviderDecl{
		Name:      "unsafe-agent",
		Caps:      []Capability{CapSandboxRequired},
		FSScope:   FSWorktree,
		Sandboxed: false,
	}
	err := Check(p, DefaultPolicy())
	if err == nil {
		t.Fatal("expected sandbox enforcement error")
	}
	t.Logf("sandbox enforce (expected): %v", err)

	// Sandboxed provider passes.
	p2 := &ProviderDecl{
		Name:      "safe-agent",
		Caps:      []Capability{CapSandboxRequired},
		FSScope:   FSWorktree,
		Sandboxed: true,
	}
	if err := Check(p2, DefaultPolicy()); err != nil {
		t.Errorf("sandboxed provider rejected: %v", err)
	}
}

// 3. Policy override — custom policy overrides defaults
func TestPolicyOverride(t *testing.T) {
	reg := NewRegistry()

	// Default policy passes codex.
	if err := reg.CheckProvider("codex"); err != nil {
		t.Fatalf("codex rejected by default policy: %v", err)
	}

	// Override: require NetworkAccess — codex doesn't have it.
	strict := &Policy{
		RequireCaps: []Capability{CapNetworkAccess},
	}
	reg.SetPolicy(strict)

	if err := reg.CheckProvider("codex"); err == nil {
		t.Fatal("codex passed strict policy (should have been rejected)")
	}

	// Override: deny model_selection — codex has it, should be rejected.
	deny := &Policy{
		DenyCaps: []Capability{CapModelSelection},
	}
	reg.SetPolicy(deny)

	if err := reg.CheckProvider("codex"); err == nil {
		t.Fatal("codex passed deny policy (should have been rejected)")
	}

	// sandboxed-claude has NetworkAccess — strict policy passes.
	if err := reg.CheckProvider("sandboxed-claude"); err == nil {
		t.Log("sandboxed-claude passes strict network policy (deny model_selection only)")
	}
}

// 4. Filesystem scope enforcement
func TestFileSystemScopeEnforce(t *testing.T) {
	pol := &Policy{MinFSScope: FSFull}

	p := &ProviderDecl{Name: "test", FSScope: FSWorktree}
	err := Check(p, pol)
	if err == nil {
		t.Fatal("worktree scope should not satisfy full minimum")
	}
	t.Logf("fs scope enforce (expected): %v", err)

	p2 := &ProviderDecl{Name: "test2", FSScope: FSFull}
	if err := Check(p2, pol); err != nil {
		t.Errorf("full scope rejected: %v", err)
	}
}

// 5. Default policy passes all built-in providers
func TestDefaultPolicyPassesBuiltins(t *testing.T) {
	reg := NewRegistry()
	for _, p := range reg.ListProviders() {
		if err := Check(p, reg.Policy()); err != nil {
			t.Errorf("built-in provider %q rejected by default policy: %v", p.Name, err)
		}
	}
}
