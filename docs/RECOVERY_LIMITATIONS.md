# OMNI Recovery Limitations

## Post-Restart Attached Non-Child Exit Code

### Limitation

After an orchestrator daemon restart, re-owned worker processes are **not
children** of the new daemon process. The `wait4` system call returns
`ECHILD` for these processes, and the kernel does not deliver their exit
status to the restarted daemon.

### Behavior

When an attached non-child process exits:
1. `kill(pid, 0)` confirms the process is dead (returns `ESRCH`)
2. `wait4(pid, &status, WNOHANG, NULL)` returns `ECHILD` — process is not our child
3. Exit code is recorded as `-1` (unknown)
4. Supervisor state transitions to `CRASHED`
5. Validator is **skipped** (no exit code to validate against)
6. Task is marked `failed`

### Rationale

You cannot resume someone else's process after your own crash. The original
daemon owned the process group and had the authority to collect exit status.
The restarted daemon is a different process with no inheritance of the
original parent-child relationship.

### Correct Outcomes

| Scenario | Exit Code | Supervisor State | Validator | Task Status |
|----------|-----------|-----------------|-----------|-------------|
| Original daemon, child process | Actual exit code | EXITED (0) or CRASHED (!=0) | Runs if exitCode==0 | Completed or Failed |
| Restarted daemon, non-child process | -1 (unknown) | CRASHED | **Skipped** | Failed |

### Design Decision

This is **accepted behavior**, not a bug. Recovery re-owns workers to
preserve checkpoint data and attempt resumption, but the restarted daemon
cannot guarantee it observed the worker's true exit status. Marking the
task as failed and skipping the validator is the correct fail-closed
behavior.
