# OMNI

**Native agents. One reliable team.**

Run Codex, Claude Code, AGY, and Reasonix as managed coordinators without replacing or proxying them.

```
$ omni run --task "Fix the auth race test" --command claude --coordinator auto
```

### Why OMNI?

OMNI is a local-first orchestration runtime for terminal-based coding agents. It doesn't replace your agents — it manages them.

- **Bring Your Own Agent** — Codex, Claude, AGY, Reasonix, or any terminal-based agent
- **Isolated Git worktrees** — every attempt runs in its own branch
- **Validator-gated completion** — shell build gates prove results before acceptance
- **Automatic recovery** — silent stops, crashes, and dirty worktrees are checkpointed
- **Model/effort-aware routing** — learns which provider works best for your tasks
- **No automatic merge** — you review and adopt results explicitly
- **Zero engine changes** across all provider additions — one contract, many agents

### 2-Minute Quick Start

```sh
# Install
git clone https://github.com/mhkim315/omni_orchestration.git
cd omni_orchestration
go build ./cmd/orchestrator

# Run your first task
./orchestrator run \
  --task "Add unit tests for the login handler" \
  --command claude \
  --coordinator auto \
  --repo /path/to/your/project

# After completion
./orchestrator result show <run-id>
./orchestrator result diff <run-id>
./orchestrator result adopt <run-id>
```

### Supported Providers

| Provider | Coordinator | Status |
|----------|-------------|--------|
| Codex | `--coordinator codex` | ✅ Full |
| Claude Code | `--coordinator claude` | ✅ Full |
| AGY | `--coordinator agy` | ✅ Full |
| Reasonix | `--coordinator reasonix` | ⚠️ Limited |

### How It Works

```
User task
  → Coordinator (Codex/Claude/AGY/Reasonix) decides START
  → Executor works in isolated worktree
  → Validator checks result
  → REJECT? Coordinator decides RETRY_CLEAN
  → Executor gets instruction + retries
  → ACCEPT? Coordinator decides COMPLETE
  → User reviews + adopts result
```

### Safety Contracts

- **Generation-gated identity**: stale senders are rejected
- **Exactly-once exit events**: no duplicate task execution
- **Decision-only coordinator**: agents decide, OMNI executes
- **Idempotent wake**: duplicate events create at most one action
- **Restart reconciliation**: kill → restart → resume without data loss

### Current Limitations

- Single executor only (parallel execution planned for v2.0)
- macOS PTY I/O has known buffering issues (8 tests affected)
- AGY and Reasonix require 3 verified samples before auto-routing
- No provider confirms effective model from output

### Roadmap

| Version | Focus |
|---------|-------|
| v1.0 | Reliable single-task orchestration |
| v1.1 | Homebrew install, `omni doctor`, provider discovery |
| v1.2 | Real-world evaluation data |
| v2.0 | Limited parallel execution |

### Development

```sh
git clone https://github.com/mhkim315/omni_orchestration.git
cd omni_orchestration
go test -race ./...    # ALL 6 packages PASS
go vet ./...           # CLEAN
gofmt -l .             # EMPTY
```

**License:** MIT
