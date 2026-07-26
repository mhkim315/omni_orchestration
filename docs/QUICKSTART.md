# OMNI Orchestrator — Quickstart (5 minutes)

## 1. Build

```bash
cd omni_orchestration
go build -o orchestrator ./cmd/orchestrator/
```

## 2. Create a test repo

```bash
mkdir /tmp/omni-demo && cd /tmp/omni-demo
git init && git config user.email "demo@omni" && git config user.name "Demo"
echo "# demo" > README.md
git add README.md && git commit -m "initial"
```

## 3. Run your first task

```bash
./orchestrator run \
  --task "create a hello.txt file with 'hello world'" \
  --command "echo 'hello world' > hello.txt" \
  --validator "grep -q 'hello world' hello.txt" \
  --repo /tmp/omni-demo
```

Expected output:
```
VALIDATOR PASS: (no output)
decisions: [VALIDATE COMPLETE]
```

## 4. Run with a coordinator

```bash
# Codex
./orchestrator run --task "create output.txt" --command "echo done > output.txt" \
  --repo /tmp/omni-demo --coordinator codex --model gpt-5.2

# Claude
./orchestrator run --task "create output.txt" --command "echo done > output.txt" \
  --repo /tmp/omni-demo --coordinator claude --effort high

# Auto-select
./orchestrator run --task "create output.txt" --command "echo done > output.txt" \
  --repo /tmp/omni-demo --coordinator auto
```

## 5. Recover after interruption

```bash
# Reconcile orphaned runs
./orchestrator recover

# Resume with recovery
./orchestrator run --resume --task "..." --command "..." --repo /tmp/omni-demo
```

## 6. View results

```bash
./orchestrator result show 1
```
