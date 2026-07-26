# OMNI Orchestrator — Provider Requirements & Capabilities

## Provider Contract

Every coordinator provider implements the `Coordinator` interface:

```go
type Coordinator interface {
    Decide(ctx context.Context, state RunState) (Result, error)
}
```

Input: structured prompt with task, attempt#, status, checkpoint, validator output.
Output: `{"decision": "<DECISION>", "reason": "...", "next_instruction": "..."}`

## Provider Capability Matrix

| Capability | Codex | Claude | AGY | Reasonix |
|-----------|-------|--------|-----|----------|
| **ModelSelection** | ✅ `--model` | ✅ `--model` | ✅ `--model` | ✅ `-model` |
| **EffortSelection** | ❌ | ✅ `--effort low\|med\|high` | ✅ `--effort low\|med\|high` | ❌ |
| **ModelDiscovery** | ❌ config only | ❌ | ✅ `agy models` (11 models) | ❌ config only |
| **EffectiveModel** | ❌ | ❌ | ❌ | ❌ |
| **Built-in timeout** | ❌ | ❌ | ✅ `--print-timeout` | ❌ (wrapped) |
| **Tool restriction** | ✅ `--max-steps 0` | ❌ prompt-only | ❌ prompt-only | ✅ `--max-steps 0` |
| **Non-interactive** | ✅ `codex exec --json` | ✅ `claude -p --output-format json` | ✅ `agy --print` | ✅ `reasonix run` |
| **Session resume** | ✅ `--continue` | ✅ `--continue` | ✅ `--conversation` | ✅ `--resume` |

## Provider Details

### Codex

```
Binary:  codex
Version: 0.145.0+ (tested)
Command: codex exec --json --model <MODEL> "<prompt>"
Timeout: external (context.WithTimeout)
Decision: normalizeDecision() from extractJSON()
```

**Requirements:**
- `codex` on PATH
- Valid API key in `~/.codex/config.toml`
- Model configured in config.toml

### Claude

```
Binary:  claude
Version: 2.1.218+ (tested)
Command: claude -p --output-format json --model <MODEL> --effort <LEVEL> "<prompt>"
Timeout: external (context.WithTimeout)
Decision: normalizeDecision() from extractJSON()
```

**Requirements:**
- `claude` on PATH
- Valid API key (`claude login` or `ANTHROPIC_API_KEY`)

### AGY

```
Binary:  agy
Version: 1.1.6+ (tested)
Command: agy --print --print-timeout 120s --model <MODEL> --effort <LEVEL> "<prompt>"
Timeout: built-in (--print-timeout)
Decision: normalizeDecision() from extractJSON()
```

**Requirements:**
- `agy` on PATH
- Pre-authenticated (provider credentials managed externally)

### Reasonix

```
Binary:  reasonix
Version: v1.17.10+ (tested)
Command: reasonix --max-steps 0 -model <PROVIDER/MODEL> --prompt "<prompt>"
Timeout: external (context.WithTimeout, default 180s)
Decision: parseDecision() from extractJSON()
```

**Requirements:**
- `reasonix` on PATH
- Valid API key in `~/.reasonix/config.toml`

## Auto-Routing

When `--coordinator auto` is specified, the Router selects the best provider
based on historical performance metrics (success rate, execution time,
reject rate, repo diversity) and optional model/effort preferences.

New providers (AGY, Reasonix) require ≥3 verified samples before auto-routing.
Below-threshold providers are excluded when eligible candidates exist.
