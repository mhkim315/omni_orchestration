# OMNI v0.3.1 — Real Provider Smoke Tests

**Date:** 2026-07-26
**Tag:** v0.3.0 (v0.3.1 with START decision fix)

## 7-Decision Parser Verification

All 7 decisions parse correctly in all 4 provider parsers.

| Decision | normalizeDecision | parseDecision (Reasonix) | Codex | Claude | AGY |
|----------|-------------------|--------------------------|-------|--------|-----|
| START | ✅ | ✅ | ✅ | ✅ | ✅ |
| VALIDATE | ✅ | ✅ | ✅ | ✅ | ✅ |
| CONTINUE | ✅ | ✅ | ✅ | ✅ | ✅ |
| RETRY_CLEAN | ✅ | ✅ | ✅ | ✅ | ✅ |
| REPLACE | ✅ | ✅ | ✅ | ✅ | ✅ |
| FAIL | ✅ | ✅ | ✅ | ✅ | ✅ |
| COMPLETE | ✅ | ✅ | ✅ | ✅ | ✅ |

## Provider Smoke Matrix

Real CLI invocation with actual auth using build tag `smoketest` and
`OMNI_SMOKE_TEST=1` env flag. All use the same WakePacket contract.

| Provider | Binary | Version | Duration | Decision | Parse | Exit |
|----------|--------|---------|----------|----------|-------|------|
| Codex | `codex exec --json` | (requires auth) | — | — | PENDING | — |
| Claude | `claude -p --output-format json` | 2.1.219 | — | — | PENDING | — |
| AGY | `agy --print` | 1.1.6 | — | — | PENDING | — |
| Reasonix | `reasonix run --max-steps 0` | v1.17.10 | — | — | PENDING | — |

> **Note:** Real provider smoke tests require valid API keys in the environment.
> Run with:
> ```
> OMNI_SMOKE_TEST=1 go test -tags smoketest -run Smoke -v ./internal/coordinator/ -timeout 300s
> ```

## START Decision Fix (v0.3.1)

Prior to v0.3.1, the `START` decision existed only as a re-export in the
orchestrator (`DecisionStart = coordinator.DecisionValidate`). v0.3.1 adds
`DecisionStart` as a first-class decision in the coordinator contract with
proper parsing in all 4 provider coordinators:

- `coordinator/coordinator.go`: DecisionStart added to const block
- `coordinator/codex_coordinator.go`: START case in normalizeDecision
- `coordinator/reasonix_coordinator.go`: START case in parseDecision  
- `orchestrator/orchestrator.go`: DecisionStart = coordinator.DecisionStart

Other coordinators (Claude, AGY) use shared normalizeDecision from codex_coordinator.go.

## Test Infrastructure

- `coordinator/smoke_test.go`: Build-tag gated (`//go:build smoketest`)
- Env guard: `OMNI_SMOKE_TEST=1` required (skipped otherwise)
- Per-provider individual tests: TestSmoke_CodexOnly, TestSmoke_ClaudeOnly,
  TestSmoke_AGYOnly, TestSmoke_ReasonixOnly
- Full matrix: TestSmoke_AllProviders

## Gate

| Gate | Result |
|------|--------|
| `go build ./...` | ✅ |
| `go vet ./...` | ✅ |
| `go test -race ./...` | ✅ 6/6 PASS |
| `gofmt` | ✅ |
| 7-decision parser verification | ✅ |
| Smoke tests (disabled by default) | ✅ Build-tag gated |
