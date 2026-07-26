# OMNI Orchestrator — Installation

## Requirements

- **Go** 1.26+ (`go version`)
- **Git** 2.30+ (`git --version`)
- **bash** (for worker commands)
- macOS arm64 or amd64 (Linux planned)

### Optional: Coordinator Providers

| Provider | Install | Auth |
|----------|---------|------|
| Codex | `brew install codex` | `~/.codex/config.toml` |
| Claude | `npm install -g @anthropic-ai/claude-code` | `claude login` |
| AGY | Pre-built binary | `agy models` shows available |
| Reasonix | `brew install reasonix` | `~/.reasonix/config.toml` |

None are required — OMNI works without a coordinator (auto-VALIDATE mode).

## Install

```bash
git clone https://github.com/mhkim315/omni_orchestration.git
cd omni_orchestration
go build -o orchestrator ./cmd/orchestrator/
```

## Verify

```bash
./orchestrator --help
./orchestrator run --help
```

## PATH (optional)

```bash
cp orchestrator /usr/local/bin/
# or
export PATH="$PWD:$PATH"
```
