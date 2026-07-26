package coordinator

import (
	"fmt"
	"strings"
	"sync"
)

// ProviderID identifies a coordinator provider.
type ProviderID string

const (
	ProviderCodex    ProviderID = "codex"
	ProviderClaude   ProviderID = "claude"
	ProviderAGY      ProviderID = "agy"
	ProviderReasonix ProviderID = "reasonix"
)

// ProviderStats holds aggregate stats for one provider.
type ProviderStats struct {
	Provider      ProviderID
	TotalAttempts int
	Successes     int
	TotalRejects  int
	TotalTimeMs   int64 // cumulative execution time
	ReposMatched  int   // number of repos this provider has been used on
}

// StatsStore provides provider stats for routing decisions.
type StatsStore interface {
	StatsFor(provider ProviderID) ProviderStats
}

// memoryStatsStore is an in-memory implementation for testing.
type memoryStatsStore struct {
	mu    sync.RWMutex
	stats map[ProviderID]ProviderStats
}

func newMemoryStatsStore() *memoryStatsStore {
	return &memoryStatsStore{stats: make(map[ProviderID]ProviderStats)}
}

func (s *memoryStatsStore) Set(provider ProviderID, stats ProviderStats) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats[provider] = stats
}

func (s *memoryStatsStore) StatsFor(provider ProviderID) ProviderStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stats[provider]
}

// Router selects the best coordinator provider based on performance metrics,
// model compatibility, and effort preferences.
type Router struct {
	store     StatsStore
	defaultID ProviderID
}

// NewRouter creates a performance-based provider router.
// If store is nil, a memory store is used (all stats zero) and the default
// provider (Codex) is returned for all queries until stats are populated.
func NewRouter(store StatsStore) *Router {
	if store == nil {
		store = newMemoryStatsStore()
	}
	return &Router{store: store, defaultID: ProviderCodex}
}

// autoEligible returns providers eligible for automatic routing.
// AGY and Reasonix require >= 3 verified samples before auto-routing.
func autoEligible() []ProviderID {
	return []ProviderID{ProviderCodex, ProviderClaude, ProviderAGY, ProviderReasonix}
}

func minAutoSamples(id ProviderID) int {
	switch id {
	case ProviderCodex, ProviderClaude:
		return 3
	case ProviderAGY, ProviderReasonix:
		return 3 // require verified samples before auto-routing
	default:
		return 3
	}
}

// SelectCoordinator returns the best provider for the given task category,
// repo, and optional model/effort preferences.
//
// Scoring formula (v0.5):
//
//	score = success_rate * 0.35 + reject_score * 0.25 + time_score * 0.15
//	      + repo_fit * 0.10 + model_match * 0.10 + effort_match * 0.05
//
// Model preference: if --model is specified, providers supporting that
// model get a +0.10 bonus. Providers that don't support --model are not
// excluded — they just score lower.
//
// If total_attempts < 3 for all eligible providers, returns the default (Codex).
// Ties go to Codex.
func (r *Router) SelectCoordinator(taskCategory, repo, model, effort string) ProviderID {
	candidates := autoEligible()
	if len(candidates) == 0 {
		return r.defaultID
	}

	type scored struct {
		id    ProviderID
		score float64
	}
	var results []scored
	allBelowThreshold := true
	var bestID ProviderID
	var bestScore float64

	// First pass: determine if any candidate meets the threshold.
	for _, id := range candidates {
		stats := r.store.StatsFor(id)
		if stats.TotalAttempts >= minAutoSamples(id) {
			allBelowThreshold = false
			break
		}
	}

	// Second pass: score. Exclude below-threshold providers when others qualify.
	for _, id := range candidates {
		stats := r.store.StatsFor(id)
		minSamples := minAutoSamples(id)

		if !allBelowThreshold && stats.TotalAttempts < minSamples {
			continue // excluded — other eligible candidates exist
		}

		s := r.scoreWithModel(stats, model, effort)

		if s > bestScore || (s == bestScore && id == r.defaultID) {
			bestScore = s
			bestID = id
		}
		results = append(results, scored{id, s})
	}

	if allBelowThreshold {
		// If model is specified, prefer a provider that supports --model.
		if model != "" {
			for _, cand := range candidates {
				if cap := ProviderCapabilities(string(cand)); cap.ModelSelection {
					return cand
				}
			}
		}
		return r.defaultID
	}

	// Ties go to Codex.
	if bestScore == 0 && bestID == "" {
		return r.defaultID
	}
	return bestID
}

// scoreWithModel computes score including model/effort match bonuses.
func (r *Router) scoreWithModel(stats ProviderStats, model, effort string) float64 {
	base := r.score(stats)

	// Model match: +0.10 if model is specified and provider has ModelSelection.
	modelBonus := 0.0
	if model != "" {
		caps := ProviderCapabilities(string(stats.Provider))
		if caps.ModelSelection {
			modelBonus = 0.10
		}
	}

	// Effort match: +0.05 if effort is specified and provider supports it.
	effortBonus := 0.0
	if effort != "" {
		caps := ProviderCapabilities(string(stats.Provider))
		if caps.EffortSelection {
			effortBonus = 0.05
		}
	}

	return base + modelBonus + effortBonus
}

// SelectCoordinatorSimple is the backward-compatible entry point without
// model/effort preferences.
func (r *Router) SelectCoordinatorSimple(taskCategory, repo string) ProviderID {
	return r.SelectCoordinator(taskCategory, repo, "", "")
}

func (r *Router) score(stats ProviderStats) float64 {
	if stats.TotalAttempts == 0 {
		return 0
	}

	successRate := float64(stats.Successes) / float64(stats.TotalAttempts)

	rejectRatio := float64(stats.TotalRejects) / float64(stats.TotalAttempts)
	rejectScore := 1.0 - rejectRatio

	timeScore := 1.0
	if stats.TotalTimeMs > 0 && stats.TotalAttempts > 0 {
		avgSec := float64(stats.TotalTimeMs) / float64(stats.TotalAttempts) / 1000.0
		timeScore = 1.0 / (1.0 + avgSec/60.0)
	}

	repoFit := float64(stats.ReposMatched) / 3.0
	if repoFit > 1.0 {
		repoFit = 1.0
	}

	return successRate*0.35 + rejectScore*0.25 + timeScore*0.15 + repoFit*0.10
}

// SelectCoordinatorByName returns the Coordinator implementation for a
// named provider. Used by the CLI for explicit --coordinator flags.
func SelectCoordinatorByName(name string) (ProviderID, Coordinator, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "codex":
		return ProviderCodex, NewCodexCoordinator(), nil
	case "claude":
		return ProviderClaude, NewClaudeCoordinator(), nil
	case "agy":
		return ProviderAGY, NewAGYCoordinator(), nil
	case "reasonix":
		return ProviderReasonix, NewReasonixCoordinator(), nil
	default:
		return "", nil, fmt.Errorf("unknown coordinator: %q (supported: codex, claude, agy, reasonix)", name)
	}
}

// ProviderNames returns the list of supported provider names.
func ProviderNames() []string {
	return []string{"codex", "claude", "agy", "reasonix"}
}
