package coordinator

import (
	"testing"
)

func TestRouter_NoSamples_DefaultCodex(t *testing.T) {
	store := newMemoryStatsStore()
	router := NewRouter(store)

	got := router.SelectCoordinator("bug-fix", "/repo/test", "", "")
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
	got := router.SelectCoordinator("bug-fix", "/repo/test", "", "")
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
	got := router.SelectCoordinator("bug-fix", "/repo/test", "", "")
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
	got := router.SelectCoordinator("bug-fix", "/repo/test", "", "")
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
	got := router.SelectCoordinator("bug-fix", "/repo/test", "", "")
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
	got := router.SelectCoordinator("bug-fix", "/repo/test", "", "")
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
	got := router.SelectCoordinator("bug-fix", "/repo/test", "", "")
	// With new weights (0.35 success vs 0.10 repo), Claude's repo diversity wins.
	if got != ProviderClaude {
		t.Errorf("repo diversity: expected Claude, got %s", got)
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

	// successRate = 0.8 * 0.35 = 0.28
	// rejectScore = 0.8 * 0.25 = 0.20
	// timeScore = 1/(1+0.5) = 0.667 * 0.15 = 0.10
	// repoFit = 1.0 * 0.10 = 0.10
	// total ≈ 0.68
	expectedMin := 0.60
	expectedMax := 0.75
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
	if len(names) != 4 {
		t.Errorf("expected 4 providers, got %d: %v", len(names), names)
	}
}

// ── v0.5: Model/effort routing expansion ──

func TestRouter_ModelPreferenceRoutesToCapableProvider(t *testing.T) {
	store := newMemoryStatsStore()
	// Both have enough samples; Claude has no --model support.
	store.Set(ProviderCodex, ProviderStats{
		Provider: ProviderCodex, TotalAttempts: 10, Successes: 5, TotalRejects: 5,
	})
	store.Set(ProviderClaude, ProviderStats{
		Provider: ProviderClaude, TotalAttempts: 10, Successes: 5, TotalRejects: 5,
	})
	router := NewRouter(store)

	// With --model flag, providers supporting it get +0.10 bonus.
	got := router.SelectCoordinator("bug-fix", "/repo/test", "gpt-5.2", "")
	// Both support ModelSelection, so it's a tie (both get the bonus).
	// Tie goes to Codex.
	if got != ProviderCodex {
		t.Errorf("model preference: expected Codex (tie), got %s", got)
	}
}

func TestRouter_ModelPreferenceWithEffortBonus(t *testing.T) {
	store := newMemoryStatsStore()
	// Claude has slightly better base stats.
	store.Set(ProviderCodex, ProviderStats{
		Provider: ProviderCodex, TotalAttempts: 10, Successes: 5, TotalRejects: 5,
	})
	store.Set(ProviderClaude, ProviderStats{
		Provider: ProviderClaude, TotalAttempts: 10, Successes: 6, TotalRejects: 4,
	})
	router := NewRouter(store)

	// With --model + --effort: both get model bonus, only Claude gets effort bonus.
	got := router.SelectCoordinator("bug-fix", "/repo/test", "gpt-5.2", "high")
	// Claude: better stats + effort bonus → should win.
	// But Codex doesn't have EffortSelection, so Claude gets +0.05.
	if got != ProviderClaude {
		t.Errorf("effort bonus: expected Claude, got %s", got)
	}
}

func TestRouter_AGYExcludedBelowThreshold(t *testing.T) {
	store := newMemoryStatsStore()
	// AGY has 1 sample — below threshold.
	store.Set(ProviderAGY, ProviderStats{
		Provider: ProviderAGY, TotalAttempts: 1, Successes: 1,
	})
	store.Set(ProviderCodex, ProviderStats{
		Provider: ProviderCodex, TotalAttempts: 0, Successes: 0,
	})
	store.Set(ProviderClaude, ProviderStats{
		Provider: ProviderClaude, TotalAttempts: 0, Successes: 0,
	})

	router := NewRouter(store)
	got := router.SelectCoordinator("bug-fix", "/repo/test", "", "")
	// All below threshold → default Codex. AGY's 1 sample doesn't count.
	if got != ProviderCodex {
		t.Errorf("AGY below threshold: expected Codex, got %s", got)
	}
}

func TestRouter_AGYEligibleAboveThreshold(t *testing.T) {
	store := newMemoryStatsStore()
	store.Set(ProviderAGY, ProviderStats{
		Provider: ProviderAGY, TotalAttempts: 5, Successes: 5, TotalRejects: 0,
		TotalTimeMs: 100000,
	})
	store.Set(ProviderCodex, ProviderStats{
		Provider: ProviderCodex, TotalAttempts: 5, Successes: 2, TotalRejects: 3,
		TotalTimeMs: 500000,
	})
	store.Set(ProviderClaude, ProviderStats{
		Provider: ProviderClaude, TotalAttempts: 5, Successes: 2, TotalRejects: 3,
	})
	router := NewRouter(store)

	got := router.SelectCoordinator("bug-fix", "/repo/test", "", "")
	// AGY: 100% success, fast. Codex: 40%, slow. AGY should win.
	if got != ProviderAGY {
		t.Errorf("AGY above threshold: expected AGY, got %s", got)
	}
}

func TestRouter_ReasonixExcludedBelowThreshold(t *testing.T) {
	store := newMemoryStatsStore()
	store.Set(ProviderReasonix, ProviderStats{
		Provider: ProviderReasonix, TotalAttempts: 2, Successes: 2,
	})
	store.Set(ProviderCodex, ProviderStats{
		Provider: ProviderCodex, TotalAttempts: 10, Successes: 8,
	})
	router := NewRouter(store)

	got := router.SelectCoordinator("bug-fix", "/repo/test", "", "")
	// Reasonix below threshold (2 < 3) → excluded. Codex wins.
	if got != ProviderCodex {
		t.Errorf("Reasonix below threshold: expected Codex, got %s", got)
	}
}

func TestRouter_SelectCoordinatorSimple(t *testing.T) {
	store := newMemoryStatsStore()
	store.Set(ProviderCodex, ProviderStats{
		Provider: ProviderCodex, TotalAttempts: 10, Successes: 8,
	})
	router := NewRouter(store)

	got := router.SelectCoordinatorSimple("bug-fix", "/repo/test")
	if got != ProviderCodex {
		t.Errorf("simple: expected Codex, got %s", got)
	}
}

func TestRouter_ModelPrefersProviderWithCapability(t *testing.T) {
	store := newMemoryStatsStore()
	// All below threshold — model preference drives routing.
	store.Set(ProviderCodex, ProviderStats{Provider: ProviderCodex})
	store.Set(ProviderClaude, ProviderStats{Provider: ProviderClaude})
	router := NewRouter(store)

	// With model specified, prefer first provider with ModelSelection.
	got := router.SelectCoordinator("bug-fix", "/repo/test", "gpt-5.2", "")
	if got != ProviderCodex {
		t.Errorf("model preference below threshold: expected Codex, got %s", got)
	}
}
