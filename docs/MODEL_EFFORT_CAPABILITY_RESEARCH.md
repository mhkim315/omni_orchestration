# OMNI v0.4 — Native Model & Effort Capability Research

**Date:** 2026-07-26
**Purpose:** Catalog each provider's native model selection, effort control,
and programmatic model discovery. No forced common enum — preserve native
values. Design `AgentProfile` + `Capabilities` types.

---

## 1. Per-Provider Capability Table

### Codex

| Capability | Flag | Values | Discovery | Confirmable |
|-----------|------|--------|-----------|-------------|
| **ModelSelection** | `--model <MODEL>` or `-c model=<name>` | Provider-defined model strings (e.g. `gpt-5`, `o3`) | ❌ No built-in list. Must read `~/.codex/config.toml` | ❌ Not in output |
| **EffortSelection** | ❌ None | N/A | N/A | N/A |
| **ModelDiscovery** | ❌ None | N/A | Config file: `~/.codex/config.toml` `[models]` section | ❌ |
| **EffectiveModel** | N/A | — | — | ❌ Not reported in `--json` output |

**Native mode:** `codex` has no effort/reasoning control. Model selection via
provider-specific model strings in TOML config. No programmatic model discovery
CLI command.

### Claude

| Capability | Flag | Values | Discovery | Confirmable |
|-----------|------|--------|-----------|-------------|
| **ModelSelection** | `--model <model>` | Model aliases (e.g. `claude-sonnet-4-5-20251001`, `claude-opus-4-8`) | ❌ No `claude models` command | ❌ Not in stream-json output |
| **EffortSelection** | `--effort <level>` | `low`, `medium`, `high` (maps to Haiku/Sonnet/Opus internally) | ✅ Documented | ❌ |
| **ModelDiscovery** | ❌ None | N/A | Must know model IDs | ❌ |
| **EffectiveModel** | ❌ None | N/A | — | ❌ |

**Native mode:** `claude` uses `--effort low|medium|high` which maps to model
tiers. Model is separate flag. Neither is reported in output.

### AGY

| Capability | Flag | Values | Discovery | Confirmable |
|-----------|------|--------|-----------|-------------|
| **ModelSelection** | `--model <model>` | 11 models: `gemini-3.6-flash-high`, `gemini-3.6-flash-medium`, `gemini-3.6-flash-low`, `gemini-3.5-flash-high`, `gemini-3.5-flash-medium`, `gemini-3.5-flash-low`, `gemini-3.1-pro-high`, `gemini-3.1-pro-low`, `claude-sonnet-4-6`, `claude-opus-4-6-thinking`, `gpt-oss-120b-medium` | ✅ `agy models` | ❌ Not in print output |
| **EffortSelection** | `--effort <level>` | `low`, `medium`, `high` | ✅ In help text | ❌ |
| **ModelDiscovery** | ✅ `agy models` | Returns explicit list of 11 model names | ✅ CLI command | ❌ |
| **EffectiveModel** | ❌ None | N/A | — | ❌ |

**Native mode:** AGY has the richest discovery — `agy models` returns explicit
model names. Model names encode provider+effort implicitly
(`-high`/`-medium`/`-low` suffix) but `--effort` flag is separate.

### Reasonix

| Capability | Flag | Values | Discovery | Confirmable |
|-----------|------|--------|-----------|-------------|
| **ModelSelection** | `-model <provider/name>` | Provider-qualified: `deepseek-pro/deepseek-v4-pro`, `deepseek-flash/deepseek-v4-flash` | ❌ No CLI command. Must read `~/.reasonix/config.toml` `[[providers]]` sections | ❌ |
| **EffortSelection** | ❌ None | N/A | N/A | N/A |
| **ModelDiscovery** | ❌ None | N/A | Config file parsing required | ❌ |
| **EffectiveModel** | ❌ None | N/A | — | ❌ |

**Native mode:** Reasonix uses `provider/model` format. Model = provider
selection implicitly (flash = fast, pro = deep). No effort flag.
`--show-thinking` controls reasoning display, not depth.

---

## 2. Comparison Matrix

| Capability | Codex | Claude | AGY | Reasonix |
|-----------|-------|--------|-----|----------|
| **ModelSelection** | ✅ `--model` | ✅ `--model` | ✅ `--model` | ✅ `-model` |
| **EffortSelection** | ❌ | ✅ `--effort low\|medium\|high` | ✅ `--effort low\|medium\|high` | ❌ |
| **ModelDiscovery** | ❌ | ❌ | ✅ `agy models` | ❌ |
| **EffectiveModel** | ❌ | ❌ | ❌ | ❌ |
| **Reasoning display** | ❌ | ❌ | ❌ | ✅ `-show-thinking` |

### Native Value Preservation

| Provider | Mode Value | Meaning |
|----------|-----------|---------|
| Codex | `reasoning_effort` (not a flag) | Model selection IS effort selection |
| Claude | `--effort low\|medium\|high` | Maps to Haiku/Sonnet/Opus tiers |
| AGY | native model names + `--effort low\|medium\|high` | Model names encode tier implicitly |
| Reasonix | provider/model selection | flash = fast, pro = deep |

---

## 3. Recommended Types

```go
// AgentProfile captures the agent configuration used for a coordinator call.
// Native values are preserved — no forced common enum.
type AgentProfile struct {
    Provider string `json:"provider"` // "codex" | "claude" | "agy" | "reasonix"
    Model    string `json:"model"`    // provider-native model string (e.g. "claude-sonnet-4-6")
    Mode     string `json:"mode"`     // provider-native effort/reasoning value (e.g. "high")
    Role     string `json:"role"`     // "coordinator" | "worker" | "validator"
}

// Capabilities describes what model/effort controls a provider supports.
type Capabilities struct {
    ModelSelection  bool `json:"model_selection"`  // --model flag exists
    EffortSelection bool `json:"effort_selection"` // --effort flag exists
    ModelDiscovery  bool `json:"model_discovery"`  // models can be listed programmatically
    EffectiveModel  bool `json:"effective_model"`  // actual model used is confirmable from output
}
```

### Per-Provider Capabilities

```go
var ProviderCapabilities = map[string]Capabilities{
    "codex":    {ModelSelection: true, EffortSelection: false, ModelDiscovery: false, EffectiveModel: false},
    "claude":   {ModelSelection: true, EffortSelection: true, ModelDiscovery: false, EffectiveModel: false},
    "agy":      {ModelSelection: true, EffortSelection: true, ModelDiscovery: true, EffectiveModel: false},
    "reasonix": {ModelSelection: true, EffortSelection: false, ModelDiscovery: false, EffectiveModel: false},
}
```

### Optional Policy Layer (future)

Fast/balanced/deep is a policy layer on top of native values. Each provider
maps the policy to its native flag:

```go
// PolicyEffort is a coordinator-level effort selection, mapped to
// provider-native flags per provider capabilities.
type PolicyEffort string

const (
    EffortFast     PolicyEffort = "fast"     // cheapest, fastest model
    EffortBalanced PolicyEffort = "balanced" // default model
    EffortDeep     PolicyEffort = "deep"     // strongest reasoning
)

// Map policy to provider-native flags.
func (c *CodexCoordinator) ApplyEffort(e PolicyEffort) {
    switch e {
    case EffortFast:     c.Model = "gpt-4.1-nano"
    case EffortBalanced: c.Model = ""  // use default
    case EffortDeep:     c.Model = "gpt-5"
    }
}
```

---

## 4. Integration Impact

| Provider | Current Coordinator Flags | After Capability Research |
|----------|--------------------------|--------------------------|
| Codex | `--model` only | `AgentProfile{Model}`, Capabilities{ModelSelection,EffortSelection:false} |
| Claude | `--model`, `--effort` unused | `AgentProfile{Model,Mode=effort}`, Capabilities{ModelSelection,EffortSelection} |
| AGY | `--model` unused, `--effort` unused | `AgentProfile{Model,Mode=effort}`, Capabilities{ModelSelection,EffortSelection,ModelDiscovery} |
| Reasonix | `-model` only | `AgentProfile{Model}`, Capabilities{ModelSelection,EffortSelection:false} |

**No orchestrator changes required.** The `AgentProfile` and `Capabilities`
types are additive — they can be added to each coordinator's config struct
without changing the `Coordinator` interface or the orchestrator loop.

---

**Research complete:** 2026-07-26
**Next:** AgentProfile + Capabilities implementation (v0.4.1)
