package coordinator

import (
	"testing"
)

func TestRouter_NoSamples_DefaultCodex(t *testing.T) {
	store := newMemoryStatsStore()
	router := NewRouter(store)

	got := router.SelectCoordinator("bug-fix", "/repo/test")
	if got != ProviderCodex {
		t.Errorf("no samples: expected Codex (default), got %s", got)
	}
}

func TestRouter_CodexBetter(t *testing.T) {
	store := newMemoryStatsStore()
	store.Set(ProviderCodex, ProviderStats{
		Provider: ProviderCodex, TotalAttempts: 10, Successes: 8, TotalRejects: 2,
		TotalTimeMs: 300000, ReposMatched: 5,
	})
	store.Set(ProviderClaude, ProviderStats{
		Provider: ProviderClaude, TotalAttempts: 10, Successes: 5, TotalRejects: 5,
		TotalTimeMs: 600000, ReposMatched: 2,
	})

	router := NewRouter(store)
	got := router.SelectCoordinator("bug-fix", "/repo/test")
	if got != ProviderCodex {
		t.Errorf("80%% success > 50%%: expected Codex, got %s", got)
	}
}

func TestRouter_ClaudeBetter(t *testing.T) {
	store := newMemoryStatsStore()
	store.Set(ProviderCodex, ProviderStats{
		Provider: ProviderCodex, TotalAttempts: 10, Successes: 6, TotalRejects: 4,
		TotalTimeMs: 900000, ReposMatched: 1,
	})
	store.Set(ProviderClaude, ProviderStats{
		Provider: ProviderClaude, TotalAttempts: 10, Successes: 9, TotalRejects: 1,
		TotalTimeMs: 300000, ReposMatched: 3,
	})

	router := NewRouter(store)
	got := router.SelectCoordinator("bug-fix", "/repo/test")
	if got != ProviderClaude {
		t.Errorf("90%% success > 60%%: expected Claude, got %s", got)
	}
}

func TestRouter_TieGoesToCodex(t *testing.T) {
	store := newMemoryStatsStore()
	// Identical stats.
	store.Set(ProviderCodex, ProviderStats{
		Provider: ProviderCodex, TotalAttempts: 10, Successes: 7, TotalRejects: 3,
		TotalTimeMs: 500000, ReposMatched: 2,
	})
	store.Set(ProviderClaude, ProviderStats{
		Provider: ProviderClaude, TotalAttempts: 10, Successes: 7, TotalRejects: 3,
		TotalTimeMs: 500000, ReposMatched: 2,
	})

	router := NewRouter(store)
	got := router.SelectCoordinator("bug-fix", "/repo/test")
	if got != ProviderCodex {
		t.Errorf("tie: expected Codex, got %s", got)
	}
}

func TestRouter_OnlyOneHasSamples(t *testing.T) {
	store := newMemoryStatsStore()
	store.Set(ProviderClaude, ProviderStats{
		Provider: ProviderClaude, TotalAttempts: 10, Successes: 9, TotalRejects: 1,
		TotalTimeMs: 300000, ReposMatched: 3,
	})
	// Codex has 0 samples.

	router := NewRouter(store)
	got := router.SelectCoordinator("bug-fix", "/repo/test")
	// Claude has >= 3 samples and should be selected over default.
	if got != ProviderClaude {
		t.Errorf("Claude has samples, Codex has 0: expected Claude, got %s", got)
	}
}

func TestRouter_BelowSampleThreshold(t *testing.T) {
	store := newMemoryStatsStore()
	store.Set(ProviderCodex, ProviderStats{
		Provider: ProviderCodex, TotalAttempts: 2, Successes: 2, TotalRejects: 0,
	})
	store.Set(ProviderClaude, ProviderStats{
		Provider: ProviderClaude, TotalAttempts: 1, Successes: 1, TotalRejects: 0,
	})

	router := NewRouter(store)
	got := router.SelectCoordinator("bug-fix", "/repo/test")
	// Both below threshold (< 3) → default (Codex).
	if got != ProviderCodex {
		t.Errorf("below threshold: expected default Codex, got %s", got)
	}
}

func TestRouter_RepoFitGlobalFallback(t *testing.T) {
	store := newMemoryStatsStore()
	// Codex has high success but never used on this type of repo.
	store.Set(ProviderCodex, ProviderStats{
		Provider: ProviderCodex, TotalAttempts: 10, Successes: 9, TotalRejects: 1,
		TotalTimeMs: 300000, ReposMatched: 1, // low repo diversity
	})
	// Claude has slightly lower success but wider repo experience.
	store.Set(ProviderClaude, ProviderStats{
		Provider: ProviderClaude, TotalAttempts: 10, Successes: 8, TotalRejects: 2,
		TotalTimeMs: 300000, ReposMatched: 5, // high repo diversity
	})

	router := NewRouter(store)
	got := router.SelectCoordinator("bug-fix", "/repo/test")
	// Codex still wins because success rate dominates (0.4 weight vs 0.1 repo_fit).
	if got != ProviderCodex {
		t.Errorf("success rate should dominate repo_fit: expected Codex, got %s", got)
	}
}

func TestRouter_ScoreCalculation(t *testing.T) {
	// Success: 8/10=0.8, Rejects: 2/10=0.2, Avg time: 30s
	stats := ProviderStats{
		TotalAttempts: 10, Successes: 8, TotalRejects: 2,
		TotalTimeMs: 300000, ReposMatched: 3,
	}

	router := NewRouter(nil)
	score := router.score(stats)

	// successRate = 0.8 * 0.4 = 0.32
	// rejectScore = 0.8 * 0.3 = 0.24
	// timeScore = 1/(1+0.5) = 0.667 * 0.2 = 0.133
	// repoFit = 1.0 * 0.1 = 0.1
	// total ≈ 0.793
	expectedMin := 0.75
	expectedMax := 0.85
	if score < expectedMin || score > expectedMax {
		t.Errorf("score = %f, expected between %f and %f", score, expectedMin, expectedMax)
	}
}

func TestSelectCoordinatorByName(t *testing.T) {
	_, coord, err := SelectCoordinatorByName("codex")
	if err != nil || coord == nil {
		t.Errorf("codex: expected coordinator, got err=%v", err)
	}

	_, coord2, err2 := SelectCoordinatorByName("claude")
	if err2 != nil || coord2 == nil {
		t.Errorf("claude: expected coordinator, got err=%v", err2)
	}

	_, _, err3 := SelectCoordinatorByName("unknown")
	if err3 == nil {
		t.Error("unknown: expected error")
	}
}

func TestProviderNames(t *testing.T) {
	names := ProviderNames()
	if len(names) != 2 {
		t.Errorf("expected 2 providers, got %d", len(names))
	}
}
