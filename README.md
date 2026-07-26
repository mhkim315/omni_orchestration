# OMNI

**Local-first orchestration runtime for native terminal coding agents — now with parallel Fleet execution.**

```
# Single task
omni run --task "Fix auth race" --command claude --coordinator auto --repo .

# Fleet (parallel workers)
omni fleet run --plan workflow.yaml --repo . --max-workers 2
```

## Architecture (11 packages)

`coordinator` `dag` `daemon` `mailbox` `orchestrator` `policy` `runtime` `supervisor` `taskstore` `worktree` `cmd/omni`

## Quick Start

```sh
git clone https://github.com/mhkim315/omni_orchestration.git
cd omni_orchestration
go build ./cmd/omni

# Single task
./omni run --task "Add tests" --command claude --coordinator auto --repo /path/to/project

# Fleet (parallel)
cat > plan.yaml << EOF
tasks:
  - id: backend
    task: Fix race condition
    command: claude
    coordinator: codex
    owned_paths: [internal/auth/]
  - id: tests
    task: Add integration tests
    command: codex
    coordinator: claude
    owned_paths: [tests/]
    depends_on: [backend]
EOF

./omni fleet run --plan plan.yaml --repo /path/to/project --max-workers 2
```

## Supported Providers

| Provider | Coordinator | Model | Effort | Status |
|----------|-------------|-------|--------|--------|
| Codex | `--coordinator codex` | ✅ | ✅ | Full |
| Claude Code | `--coordinator claude` | ✅ | ✅ | Full |
| AGY | `--coordinator agy` | ✅ | ✅ | Full |
| Reasonix | `--coordinator reasonix` | ✅ | — | Limited |

## Safety Contracts

- **Generation-gated identity** — stale senders rejected at every mutation
- **Durable mailbox** — at-least-once delivery with idempotent consumption
- **Path leases** — exclusive file ownership prevents parallel conflicts
- **Hierarchical authority** — run_epoch + task_gen + attempt_gen + worker_lease + base_SHA
- **Durable tombstones** — cancellation recorded before process signals
- **No automatic merge** — explicit user adoption required

## Fleet

- Dynamic worker pool (`--max-workers N`)
- Multi-parent DAG (task depends on multiple parents)
- Path lease conflict detection (overlapping writes serialized)
- Kill/recovery: DAG state survives daemon restart
- Capability policy enforcement per task

## Limitations

- macOS PTY I/O has known buffering issues
- AGY and Reasonix require 3 verified samples before auto-routing
- No provider confirms effective model from output
- ECHILD exit for re-attached workers classified as CRASHED (safe fail-closed)

## Roadmap

| Version | Focus |
|---------|-------|
| v2.1 | Fleet Observability (status, graph, events) |
| v2.2 | Homebrew install, clean-machine proof |
| v2.3 | Benchmark + routing evaluation |
| v2.4 | Operational hardening |
| v3.0 | Multi-repository fleet |

## Development

```sh
git clone https://github.com/mhkim315/omni_orchestration.git
cd omni_orchestration
go test -race ./...    # 11 packages PASS
go vet ./...           # CLEAN
gofmt -l .             # EMPTY
```

**License:** MIT
