# Recipe: Claude Code

Verified end to end against a real gate run.

## Loading the contract

Claude Code reads `CLAUDE.md` from the working directory, so the contract is
copied into the agent's worktree as part of setting the run up:

    cp docs/agent-contract.md "$WORKTREE/CLAUDE.md"

## Print mode

A non-interactive run needs its permissions declared up front, or it stops to
ask and a backgrounded process simply waits:

    claude -p "$PC_TASK" \
      --permission-mode acceptEdits \
      --allowedTools Read Edit Write Bash \
      < /dev/null

`Bash` is not optional — `pc submit` is a shell command, and without it the
agent can edit code but never reach the gate.

## Environment

| Variable | Purpose |
|---|---|
| `PC_AGENT` | This agent's name, as it appears in `gate.required` |
| `PC_TASK` | The task text |
| `PC_DB` | The coordination database, so `pc` finds the same bus as the daemon |

`pc` must be on `PATH`, built from the project root.
