# Recipe: pi

Verified end to end against a real gate run. `@earendil-works/pi-coding-agent`.

## Loading the contract

    pi --skill docs/agent-contract.md -p "$PC_TASK" < /dev/null

`--skill` is the right channel: CLI-provided resources load **before** project
trust is resolved, and non-interactive modes never prompt for that trust — so a
contract supplied any other way may simply not be present when the agent
starts.

## The stdin trap — read this before backgrounding anything

`pi -p` merges piped stdin into the prompt. A backgrounded invocation that
inherits an open stdin therefore **blocks forever**, with no error and no
output. Always redirect:

    pi --skill docs/agent-contract.md -p "$PC_TASK" < /dev/null &

This cost two nine-minute live runs before it was understood. It is invisible
until you drive two harnesses side by side, because the other one does not
behave this way.

## Environment

| Variable | Purpose |
|---|---|
| `PC_AGENT` | This agent's name, as it appears in `gate.required` |
| `PC_TASK` | The task text |
| `PC_DB` | The coordination database, so `pc` finds the same bus as the daemon |

`pc` must be on `PATH`. Build it with `go build -o "$BINDIR/pc" ./cmd/pc` from
the project root — not from inside a worktree or the fixture, which is how one
live run silently tested a stale binary for nine minutes.

## What pi does not have

No MCP support, by design, and no background bash. Neither is needed: the
contract only requires running a shell command and reading its exit code.
