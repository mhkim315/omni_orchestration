// Package policy provides capability-based access control for OMNI workers.
// Providers declare capabilities; the orchestrator enforces policy before
// dispatching workers.
//
// v1.4: Baseline capability declarations + enforcement gates.
package policy

import (
	"fmt"
	"strings"
)

// ── Capabilities ──

// Capability is a declared provider feature.
type Capability string

const (
	CapModelSelection  Capability = "model_selection"
	CapEffortSelection Capability = "effort_selection"
	CapSandboxRequired Capability = "sandbox_required"
	CapNetworkAccess   Capability = "network_access"
)

// FileSystemScope controls filesystem access for a worker.
type FileSystemScope string

const (
	FSNone     FileSystemScope = "none"
	FSWorktree FileSystemScope = "worktree"
	FSFull     FileSystemScope = "full"
)

// ── Provider Declaration ──

// ProviderDecl describes what a provider supports and requires.
type ProviderDecl struct {
	Name      string          `json:"name"`
	Caps      []Capability    `json:"capabilities"`
	FSScope   FileSystemScope `json:"filesystem_scope"`
	Sandboxed bool            `json:"sandboxed"`
}

// Has returns true if the provider has the given capability.
func (p *ProviderDecl) Has(c Capability) bool {
	for _, cc := range p.Caps {
		if cc == c {
			return true
		}
	}
	return false
}

// DefaultProviders returns the built-in provider declarations.
func DefaultProviders() []*ProviderDecl {
	return []*ProviderDecl{
		{Name: "codex", Caps: []Capability{CapModelSelection}, FSScope: FSWorktree, Sandboxed: false},
		{Name: "claude", Caps: []Capability{CapModelSelection, CapEffortSelection}, FSScope: FSWorktree, Sandboxed: false},
		{Name: "agy", Caps: []Capability{CapModelSelection, CapEffortSelection}, FSScope: FSWorktree, Sandboxed: false},
		{Name: "reasonix", Caps: []Capability{CapModelSelection}, FSScope: FSWorktree, Sandboxed: false},
		{Name: "sandboxed-claude", Caps: []Capability{CapModelSelection, CapEffortSelection, CapNetworkAccess}, FSScope: FSWorktree, Sandboxed: true},
	}
}

// ── Policy ──

// Policy specifies required and denied capabilities for a dispatch.
type Policy struct {
	RequireCaps []Capability    `json:"require_caps"`
	DenyCaps    []Capability    `json:"deny_caps"`
	MinFSScope  FileSystemScope `json:"min_fs_scope"`
}

// DefaultPolicy returns the baseline policy (allow all, worktree scope).
func DefaultPolicy() *Policy {
	return &Policy{
		MinFSScope: FSWorktree,
	}
}

// ── Enforcement ──

// Check verifies that a provider declaration satisfies a policy.
// Returns nil if allowed, or an error describing the violation.
func Check(provider *ProviderDecl, pol *Policy) error {
	if pol == nil {
		pol = DefaultPolicy()
	}

	// Require: provider must have ALL required capabilities.
	for _, c := range pol.RequireCaps {
		if !provider.Has(c) {
			return fmt.Errorf("policy: provider %q lacks required capability %q", provider.Name, c)
		}
	}

	// Deny: provider must NOT have any denied capabilities.
	for _, c := range pol.DenyCaps {
		if provider.Has(c) {
			return fmt.Errorf("policy: provider %q has denied capability %q", provider.Name, c)
		}
	}

	// Sandbox gate: if provider requires sandbox, it must be sandboxed.
	if provider.Has(CapSandboxRequired) && !provider.Sandboxed {
		return fmt.Errorf("policy: provider %q requires sandbox but is not sandboxed", provider.Name)
	}

	// Filesystem scope: provider must meet minimum.
	if !fsScopeSatisfies(provider.FSScope, pol.MinFSScope) {
		return fmt.Errorf("policy: provider %q fs_scope=%q below minimum %q",
			provider.Name, provider.FSScope, pol.MinFSScope)
	}

	return nil
}

// fsScopeSatisfies returns true if actual meets or exceeds minimum.
// none < worktree < full
func fsScopeSatisfies(actual, minimum FileSystemScope) bool {
	order := map[FileSystemScope]int{FSNone: 0, FSWorktree: 1, FSFull: 2}
	return order[actual] >= order[minimum]
}

// ── Registry ──

// Registry holds named provider declarations and an active policy.
type Registry struct {
	providers map[string]*ProviderDecl
	policy    *Policy
}

// NewRegistry creates a registry with default providers and policy.
func NewRegistry() *Registry {
	r := &Registry{
		providers: make(map[string]*ProviderDecl),
		policy:    DefaultPolicy(),
	}
	for _, p := range DefaultProviders() {
		r.providers[p.Name] = p
	}
	return r
}

// GetProvider returns a provider declaration by name.
func (r *Registry) GetProvider(name string) (*ProviderDecl, error) {
	p, ok := r.providers[name]
	if !ok {
		return nil, fmt.Errorf("policy: unknown provider %q", name)
	}
	return p, nil
}

// SetPolicy updates the active policy.
func (r *Registry) SetPolicy(p *Policy) {
	r.policy = p
}

// Policy returns the active policy.
func (r *Registry) Policy() *Policy { return r.policy }

// CheckProvider verifies a named provider against the active policy.
func (r *Registry) CheckProvider(name string) error {
	p, err := r.GetProvider(name)
	if err != nil {
		return err
	}
	return Check(p, r.policy)
}

// ListProviders returns all registered providers.
func (r *Registry) ListProviders() []*ProviderDecl {
	var out []*ProviderDecl
	for _, p := range r.providers {
		out = append(out, p)
	}
	return out
}

// ── String helpers ──

// FormatPolicy returns a human-readable policy summary.
func FormatPolicy(pol *Policy) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Policy:\n")
	if len(pol.RequireCaps) > 0 {
		fmt.Fprintf(&b, "  Require: %v\n", pol.RequireCaps)
	}
	if len(pol.DenyCaps) > 0 {
		fmt.Fprintf(&b, "  Deny: %v\n", pol.DenyCaps)
	}
	fmt.Fprintf(&b, "  Min FS Scope: %s\n", pol.MinFSScope)
	return b.String()
}

var _ = fmt.Sprintf
