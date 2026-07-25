package coordinator

import (
	"fmt"
	"strings"
	"sync"
)

// ProviderID identifies a coordinator provider.
type ProviderID string

const (
	ProviderCodex  ProviderID = "codex"
	ProviderClaude ProviderID = "claude"
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

// Router selects the best coordinator provider based on performance metrics.
type Router struct {
	store     StatsStore
	defaultID ProviderID
}

// NewRouter creates a performance-based provider router.
func NewRouter(store StatsStore) *Router {
	return &Router{store: store, defaultID: ProviderCodex}
}

// SelectCoordinator returns the best provider for the given task category and repo.
// Scoring formula:
//
//	score = success_rate * 0.4 + reject_score * 0.3 + time_score * 0.2 + repo_fit * 0.1
//
// Where:
//   - success_rate = successes / total_attempts (0 if no attempts)
//   - reject_score = 1.0 - (total_rejects / total_attempts) (1.0 if no attempts)
//   - time_score = normalized inverse of avg time (1.0 if no data)
//   - repo_fit = min(1.0, repos_matched / 3) (0 if no data)
//
// If total_attempts < 3 for all providers, returns the default (Codex).
// Ties go to Codex.
func (r *Router) SelectCoordinator(taskCategory, repo string) ProviderID {
	codexStats := r.store.StatsFor(ProviderCodex)
	claudeStats := r.store.StatsFor(ProviderClaude)

	// Minimum sample threshold.
	if codexStats.TotalAttempts < 3 && claudeStats.TotalAttempts < 3 {
		return r.defaultID
	}

	codexScore := r.score(codexStats)
	claudeScore := r.score(claudeStats)

	if claudeScore > codexScore {
		return ProviderClaude
	}
	return ProviderCodex // ties go to Codex
}

func (r *Router) score(stats ProviderStats) float64 {
	if stats.TotalAttempts == 0 {
		return 0
	}

	// Success rate (0.0 - 1.0).
	successRate := float64(stats.Successes) / float64(stats.TotalAttempts)

	// Reject score: fewer rejects = higher score (1.0 - reject_ratio).
	rejectRatio := float64(stats.TotalRejects) / float64(stats.TotalAttempts)
	rejectScore := 1.0 - rejectRatio

	// Time score: normalized inverse of average time.
	// If avg time is 0 (no data), score is 1.0.
	// Otherwise, score = 1.0 / (1.0 + avg_time_seconds).
	timeScore := 1.0
	if stats.TotalTimeMs > 0 && stats.TotalAttempts > 0 {
		avgSec := float64(stats.TotalTimeMs) / float64(stats.TotalAttempts) / 1000.0
		timeScore = 1.0 / (1.0 + avgSec/60.0) // normalize: 60s = 0.5
	}

	// Repo fit: 0.0 - 1.0 based on unique repos used.
	repoFit := float64(stats.ReposMatched) / 3.0
	if repoFit > 1.0 {
		repoFit = 1.0
	}

	return successRate*0.4 + rejectScore*0.3 + timeScore*0.2 + repoFit*0.1
}

// SelectCoordinatorByName returns the Coordinator implementation for a
// named provider. Used by the CLI for explicit --coordinator flags.
func SelectCoordinatorByName(name string) (ProviderID, Coordinator, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "codex":
		return ProviderCodex, NewCodexCoordinator(), nil
	case "claude":
		return ProviderClaude, NewClaudeCoordinator(), nil
	default:
		return "", nil, fmt.Errorf("unknown coordinator: %q (supported: codex, claude)", name)
	}
}

// ProviderNames returns the list of supported provider names.
func ProviderNames() []string {
	return []string{"codex", "claude"}
}
