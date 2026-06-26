# `pc` CLI + MCP Server — Design Spec

**Status:** Approved (design phase) · **Date:** 2026-06-23 · **Project:** Parallel Consciousness

## Summary

`pc` is the harness-agnostic surface that lets any CLI coding agent (Claude Code,
Codex, OpenCode, …) participate in Parallel Consciousness coordination. It ships
as one binary (`cmd/pc`) exposing the same operations two ways: **shell
subcommands** (universal — every harness can run a shell command) and an **MCP
server** (`pc mcp`, native tools for MCP-capable harnesses). Both skins call one
shared core, `internal/pcops`, which wraps the existing `pkg/gate` + `pkg/bus/sqlite`.

The headline flow: an agent declares "I'm ready at this version" for a gate and
blocks until the cross-service test verdict — with **zero changes to the agent's
internals**. It either calls one MCP tool or runs one shell command.

## Motivation

The library + the SQLite transport make cross-process coordination possible, but a
real coding agent is a different *tool* (Claude Code, Codex, OpenCode) we can't
modify. The one thing they all share is the ability to run shell commands; the
MCP-capable ones additionally speak MCP. `pc` meets both: a CLI for everyone, MCP
for the harnesses that have it — over one implementation so they never drift.

## Principles

1. **One core, many skins.** All logic lives in `internal/pcops`; the CLI and the
   MCP server are thin adapters. No duplicated coordination logic.
2. **Harness-agnostic.** Plain inputs only — a gate id, an opaque version string,
   an agent name. Nothing tied to a specific harness.
3. **Reuse the durable log.** `pc` adds no new coordination semantics; it drives
   `pkg/gate` over `pkg/bus/sqlite`. `Submit`'s correctness rides the durable cursor.
4. **Floor: `go 1.23` (resolved 2026-06-23).** The MCP `go-sdk` hard-requires
   `go 1.23` (every release v0.2.0–v1.2.0 was probed; none supports 1.22). Rather
   than carry a permanent second module to preserve `go 1.22`, the project bumps to
   a single-module `go 1.23` floor — a widely-available release with near-zero
   consumer friction. No cgo is introduced; pure-Go stays intact.

## Scope

**In scope:**
- `cmd/pc` with subcommands: `submit`, `up`, `run-gate`, `watch`, `mcp`.
- `internal/pcops`: `Config`/`LoadConfig`, `Submit`, `Up`, `RunGate`, `Watch`.
- `.pc.yaml` config (DB + gate definitions) with env overrides.
- An MCP server (`pc mcp`) exposing `submit_to_gate` and `gate_status`, built on
  `github.com/modelcontextprotocol/go-sdk`.
- New deps: `go-sdk` (v1.2.0) + `gopkg.in/yaml.v3`; the module floor moves to `go 1.23`.

**Out of scope (deferred):**
- **The WebUI ("Slack for Agents") — the NEXT milestone**, its own brainstorm →
  spec → plan. It reuses `internal/pcops` + the durable log as another skin (an
  HTTP server + SPA + live updates). `pc` deliberately lays that foundation.
- Auth / multi-tenant, multi-machine (rides the Turso work), a long-running agent
  SDK. `submit` is stateless per invocation.

## Configuration

`.pc.yaml` (checked into the repo) is the declarative home for connection + gates:

```yaml
db: /shared/team.db          # overridable by $PC_DB
gates:
  checkout:
    required: [billing, gateway]
    runner: ci-runner          # the agent name that runs the spanning test
    run: make integration-test # command the runner executes when the gate opens
defaults:
  submit_timeout: 5m           # how long `submit` waits for a verdict (default 5m)
```

**Resolution rules:**
- **DB:** `$PC_DB` → else `.pc.yaml` `db` → else error.
- **Agent identity (`submit`):** `--as` → `$PC_AGENT` → error. Must be explicit and
  stable (it is the durable cursor key).
- **Version (`submit`):** `--version` → `git rev-parse HEAD` in the cwd → error.
- **Config file path:** `--config` → `./.pc.yaml`.

With config in place the agent's instruction snippet collapses to
`pc submit --gate checkout`.

## Commands

| Command | Lifetime | Role | Behavior |
|---|---|---|---|
| `pc submit --gate <id> [--as A] [--version V]` | short | the agent | subscribe → declare ready → block for verdict → exit code |
| `pc up [--config …]` | daemon | coordinator host | hosts the coordinator for every gate in `.pc.yaml` |
| `pc run-gate --gate <id> [-- <cmd>]` | daemon | runner | on gate-open, runs the gate's `run` cmd (or the `--` override); reports done/disagree by the command's exit code |
| `pc watch [--config …]` | streams | observer | replays history then streams the live activity feed for configured gates |
| `pc mcp [--config …]` | daemon (stdio) | MCP server | exposes `submit_to_gate` + `gate_status` |

**`pc submit` exit codes** (how a shell-driven agent branches):
- `0` — gate **PASSED**.
- `1` — gate **FAILED** or **STALLED** (the spanning test ran and did not pass).
- `2` — no verdict obtained: config/identity/connection error, or submit timeout
  (distinct from `1` so the agent can tell "didn't run" from "failed").

Canonical agent snippet (`CLAUDE.md` / `AGENTS.md`):
```bash
if pc submit --gate checkout; then echo "passed — proceed"; else echo "failed — read detail, fix, retry"; fi
```

## Shared Core — `internal/pcops`

```go
type GateDef struct {
    Required []string
    Runner   string
    Run      string // command the runner executes (optional; -- overrides)
}
type Config struct {
    DB            string
    Gates         map[string]GateDef
    SubmitTimeout time.Duration
}
func LoadConfig(path string) (Config, error)  // parse .pc.yaml; apply $PC_DB

func Submit(ctx context.Context, cfg Config, gateID, agent, version string) (gate.Verdict, error)
func Up(ctx context.Context, cfg Config) error                     // coordinators for all gates
func RunGate(ctx context.Context, cfg Config, gateID string, override []string) error
func Watch(ctx context.Context, cfg Config) error                  // history + live feed
```

`cmd/pc/main.go` parses subcommands/flags, calls these, and maps `Verdict`/errors to
exit codes. `cmd/pc/mcp.go` registers MCP tools whose handlers call the same functions.

**`Up`** opens the bus, creates one coordinator agent subscribed to every configured
gate's `gate.Topic(id)`, `NewCoordinator` + `Register` each gate's `Spec`, and runs
until `ctx` is cancelled (SIGINT).

**`RunGate`** opens the bus, creates the runner agent (named after the gate's
`runner`), `ServeRunner(fn)` where `fn` runs the gate's `Run` command (or the `--`
override) via `os/exec`: exit 0 → `Verdict{Passed:true}`; non-zero → `Passed:false`
with trimmed stderr as `Detail`. Runs until `ctx` cancelled.

**`Watch`** subscribes (with `WithReplayFromZero`) to all configured gate topics as
an ephemeral observer and prints each message as a turn (`from --intent--> to: body`),
history first then live.

### `Submit` — race-free round semantics

1. `sqlite.Open(cfg.DB)`; create the agent; **`Subscribe(agent, [gate.Topic(id)])`
   first** (cursor resumes from this agent's durable position, or HEAD on first run).
2. `gate.Ready(agent, gateID, version)` — published *after* subscribing, so its seq
   and the coordinator's later verdict are both above our cursor.
3. Read the channel, skipping unrelated traffic (peers' `ready` broadcasts), until
   the **verdict** for this gate arrives — the coordinator's `inform`
   (`body.gate == id`, `body.passed` bool, `body.text` for detail) or a `block`
   routed to this agent — or `SubmitTimeout` elapses.
4. Return the `Verdict`. Because the subscription uses the **agent's durable
   identity**, the next round's `submit` resumes *after* this verdict and cannot
   re-read a stale one. `Submit`'s round-correctness falls out of the durable cursor.

## MCP Server (`pc mcp`)

Stdio MCP server built with the official `github.com/modelcontextprotocol/go-sdk`:
`mcp.NewServer` + typed `mcp.AddTool(server, &mcp.Tool{…}, handler)` +
`mcp.StdioTransport`. Agent-facing tools only (daemons like `up`/`run-gate` are not
tools):

- `submit_to_gate{gate string, version?, agent?}` → `{passed bool, detail string,
  versions map[string]string}` — calls `pcops.Submit`; blocks until verdict/timeout
  (a long-running tool call is acceptable).
- `gate_status{gate?}` → the last known verdict for the gate (+ a short recent-activity
  summary), read-only and non-blocking, so an agent can peek without committing.

Tool handlers are thin wrappers over `pcops`; no MCP/JSON-RPC logic is hand-rolled.

## Dependency & Go Floor (resolved: single module, `go 1.23`)

- **Probe result (2026-06-23):** every `go-sdk` release (v0.2.0–v1.2.0) requires
  `go 1.23`; there is no `go 1.22`-compatible version. `gopkg.in/yaml.v3` (v3.0.1)
  holds `go 1.22`/`1.23`.
- **Decision (maintainer, 2026-06-23):** bump the single module's `go` directive to
  `go 1.23` and add `github.com/modelcontextprotocol/go-sdk` (v1.2.0) +
  `gopkg.in/yaml.v3` (v3.0.1). Remove any `toolchain` line `go get` introduces. The
  nested-module fallback (B) is **not** used — simplicity over preserving the 1.22
  floor, given 1.23 is widely available.
- No cgo is introduced; the project stays pure-Go.

## File Layout

```
internal/pcops/pcops.go        Config, LoadConfig, Submit, Up, RunGate, Watch
internal/pcops/pcops_test.go   ops tested over a temp SQLite bus
cmd/pc/main.go                 subcommand + flag parsing → pcops → exit codes
cmd/pc/mcp.go                  pc mcp: go-sdk server, tools → pcops
.pc.yaml.example               documented sample config
go.mod / go.sum                + go-sdk v1.2.0 + gopkg.in/yaml.v3; floor → go 1.23
```

`internal/pcops` imports the public `pkg/...`; no import cycle. `cmd/demo`,
`cmd/gatedemo`, `cmd/sqlitedemo`, and all `pkg/...` stay unchanged.

## Error Handling

- Missing/!parseable `.pc.yaml`, unknown gate, missing identity/version → clear
  error; CLI exit `2`, MCP tool error.
- DB unreachable → error at `Open` (the bus already pings on open).
- `submit` timeout → exit `2` (distinct from `1`).
- `run-gate` command not found / non-zero → reported as a failing verdict with
  stderr detail (never crashes the runner daemon).
- `up`/`run-gate`/`watch`/`mcp` shut down cleanly on SIGINT (`signal.NotifyContext`).

## Testing

Logic lives in `pcops`, tested over a real temp SQLite bus (no mocks):

| Test | Asserts |
|---|---|
| `LoadConfig` | parses `.pc.yaml`; `$PC_DB` override; unknown gate / bad YAML → error |
| `Submit` pass | with a real `Up` coordinator + a fake/`true` runner → `Passed`, versions correct |
| `Submit` fail | runner exits non-zero → `Passed=false`, detail surfaced |
| `Submit` timeout | no coordinator → timeout error (→ exit 2), distinct from fail |
| `Submit` round-correctness | two sequential submits → second resumes past the first verdict |
| `RunGate` | on gate-open the configured command runs; exit 0 → done, non-zero → disagree+detail |
| exit-code mapping | `pcops` result → `main` returns 0/1/2 (thin table test) |
| MCP tool | `submit_to_gate` handler returns the structured verdict (handler-level; one in-process `go-sdk` client round-trip if cheap) |

Plus `cmd/pc`: `go build`/`go vet`, and a scripted cross-process smoke (start
`pc up` + `pc run-gate`, run `pc submit`, assert exit code) — mirroring the
`sqlitedemo` smoke. All under `-race`.

## What This Sets Up

- **The WebUI (next milestone)** is another skin over `internal/pcops` + the durable
  log: an HTTP server + SPA + live updates. Channels=topics, DMs=direct inboxes,
  threads=`conversation_id`/`in_reply_to`, gate cards from verdicts. Building `pc`
  first establishes the shared ops/read layer it reuses.
- Integration kit (example `CLAUDE.md`/`AGENTS.md` snippets, a Claude Code hook) is a
  small fast-follow now that the surfaces exist.

## Assumptions & Limitations

- Single machine (the SQLite transport's boundary); cross-machine rides the Turso work.
- `submit` is stateless per invocation — it relies on durable cursors for round
  correctness; agent identity must be unique and stable per shared DB.
- `gate_status` reports the last *verdict*; in-progress readiness (live coordinator
  state) is not queryable without the coordinator — full live state is a WebUI concern.
- `pc up` and `pc run-gate` are separate daemons (coordinator vs test-executor); a
  combined convenience (`pc up --with-runner`) is a possible later ergonomic, not in scope.
