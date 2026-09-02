# Agent Runtime, Workspace Leases, and the Two-Agent PoC — Design Spec

**Status:** Approved (design phase) · **Date:** 2026-09-01 · **Project:** Parallel Consciousness

## Summary

This spec covers the first vertical slice of the Parallel Consciousness product
layer: a harness-neutral **agent runtime** boundary (`pkg/runtime`), its first
real adapter over the **pi** coding harness (`internal/runtime/pi`), **exclusive
workspace leases** backed by git worktrees (`pkg/workspace`), a minimal `cmd/pc`
surface, and a **proof of concept** in which two real pi coding agents work in
isolated worktrees, message each other mid-task, and converge on a spanning
integration test through the existing `pkg/gate`.

The slice is deliberately minimal: it builds only what the two-agent PoC
exercises. Every line survives into the product, and the new interfaces are
shaped by a real consumer rather than by speculation.

## Motivation

The coordination kernel (`pkg/protocol`, `pkg/bus`, `pkg/agent`, `pkg/gate`) is
complete and proven, but nothing in the repository launches or supervises a real
coding agent. Coordination is demonstrated only between scripted in-process Go
agents (`cmd/demo`, `cmd/gatedemo`). Consequently:

- No end-to-end product loop can be demonstrated without a human manually
  launching two separate harnesses and pasting in instructions.
- The control plane cannot observe what an agent actually did, so progress
  remains self-reported — contradicting the project's first principle.
- Loop-quality work (roles, context boundaries, adversarial review) is
  unimplementable against harnesses whose context and lifecycle cannot be
  controlled from outside.

pi is a good first runtime because it is built to be driven: MIT-licensed, with
an RPC mode over LF-delimited JSONL explicitly intended for non-Node
integrations, a documented event stream, `steer`/`follow_up`/`abort` control
verbs, and CLI-injectable skills and prompts. Critically, pi ships **no
subagents and no plan mode** by design, so it does not overlap with what this
project builds — it complements it.

Adopting pi carries one real risk: pi's event model, session format, and
extension API quietly becoming the project's domain model, which would hollow out
the harness-agnostic positioning. The module boundaries below exist to make that
structurally impossible rather than merely discouraged.

## Principles

1. **The runtime interface is public; the adapter is private.** `pkg/runtime` is
   stdlib-only and harness-neutral so third parties can write adapters. All
   pi-specific behavior is confined to `internal/runtime/pi`.
2. **No new coordination semantics.** Spawned agents appear on the bus as
   ordinary `pkg/protocol` participants via a `pkg/agent` proxy. `pkg/gate` does
   not know a language model is involved.
3. **Isolation is structural, not advisory.** An agent's workspace is a git
   worktree it holds an exclusive lease on. It cannot see another agent's edits.
4. **Evidence is observed, not reported.** Tool-execution events give the control
   plane an attributable record of files touched and commands run.
5. **The fake runtime comes first.** The loop must be testable in CI with no API
   key, no network, and no tokens.
6. **A negative result is a result.** If the two-agent loop cannot converge, that
   finding changes the product; it is not a defect to be retried away.

## Scope

**In scope:**

- `pkg/runtime` — `Runtime`, `Session`, `Spec`, `Event`, `Outcome` value types.
- `pkg/runtime/runtimetest` — conformance suite for any adapter.
- A scripted fake runtime satisfying the suite.
- `internal/runtime/pi` — adapter over `pi --mode rpc`.
- `pkg/workspace` — worktree-backed exclusive leases with heartbeats.
- `cmd/pc` subcommands: `submit`, `up`, `run-gate`, `watch`, `send`, `run`.
- `fixtures/two-service` — the PoC fixture repository.
- A deterministic integration test of the whole loop over the fake runtime.
- The manual end-to-end PoC over real pi sessions.
- An import-boundary test.

**Out of scope (deferred, deliberately):**

- `pkg/store` (event-sourced domain core) — the gate plus the durable message log
  carry all state the PoC needs.
- `pkg/loop` (loop engine) — the PoC's scenario file is a hand-written loop.
- Containers and Gondolin micro-VMs — `pkg/workspace` is shaped so a boundary
  drops in behind it later.
- `pc mcp` — pi has no MCP by design, and the shell contract is the universal one.
- `pc ask` (blocking request/response), budget *enforcement*, the dashboard,
  Loop Studio, multi-machine operation, lead-agent task planning.

## Module boundaries

Arrows point at dependencies.

```text
              cmd/pc
                 |
                 v
          internal/pcops                  <- composition root
         /    |      |     \
        v     v      v      v
  pkg/gate  pkg/runtime  pkg/workspace  pkg/bus/sqlite
      |      (interface)   (leases)          |
      v          ^                           v
  pkg/agent      |                      pkg/protocol
      |    internal/runtime/pi
      v     <- every pi-ism lives here, and only here
   pkg/bus
```

Rules, enforced by a test rather than by discipline:

- Nothing under `pkg/` may import `internal/runtime/pi`.
- The `pkg/runtime` package itself may import only the standard library. Its
  `runtimetest` subpackage may additionally import `pkg/runtime`.

The test shells out to `go list -deps -json ./...` and parses the result, which
adds no module dependency — consistent with the project's existing discipline
around the dependency set and the Go floor. It fails the build on violation, and
is the mechanism that keeps the harness-agnostic claim true as the codebase
grows.

`pkg/runtime` is public API because a Codex or Claude Code adapter should be
writable by anyone. `internal/runtime/pi` is private because its shape will churn
with pi's 0.84.x release cadence.

**Agent identity.** Because the control plane spawns the process, it assigns the
identity and injects `$PC_AGENT` and `$PC_DB` into the environment. The durable
cursor key is set by PC, never negotiated with a model.

## `pkg/runtime`

```go
package runtime

type Runtime interface {
	Name() string // "pi" — recorded in evidence
	Start(ctx context.Context, spec Spec) (Session, error)
}

type Spec struct {
	Agent   string            // PC-assigned identity -> $PC_AGENT
	Workdir string            // the lease path
	Role    string            // role framing, prefixed onto the initial prompt
	Task    string            // initial prompt
	Env     map[string]string // $PC_DB, provider credentials
	Model   string            // opaque; the adapter maps it
	Budget  Budget
}

type Budget struct {
	Wall   time.Duration // enforced by the caller, not by the session; see below
	Tokens int64         // reported only; 0 means unbounded
}

type Session interface {
	Events() <-chan Event                        // closed when the session ends
	Steer(ctx context.Context, s string) error   // deliver at the next turn boundary
	Follow(ctx context.Context, s string) error  // queue behind pending work
	Interrupt(ctx context.Context) error         // preempt: abort in-flight tool, then the turn
	Close(ctx context.Context) error             // terminate
	Wait(ctx context.Context) (Outcome, error)
}

type EventKind string

const (
	KindStarted      EventKind = "started"
	KindTurnBegan    EventKind = "turn_began"
	KindTurnEnded    EventKind = "turn_ended"
	KindToolUsed     EventKind = "tool_used"
	KindQueueChanged EventKind = "queue_changed"
	KindIdle         EventKind = "idle"
	KindErrored      EventKind = "errored"
	KindExited       EventKind = "exited"
)

type Event struct {
	Kind    EventKind
	At      time.Time
	Agent   string
	Tool    *ToolUse // set when Kind == KindToolUsed
	Pending *Queue   // set when Kind == KindQueueChanged
	Err     string
}

// Queue reports what the adapter has accepted but not yet applied — the
// delivery receipt that separates "delivered" from "acted on". The day-0 spike
// measured those diverging by 23 seconds during a single bash call.
type Queue struct{ Steering, FollowUp int }

type ToolUse struct {
	Name   string // read, write, edit, bash, grep, find, ls
	Target string // path or command
	Detail string
	Ok     bool
}

type ExitReason string

const (
	ExitCompleted   ExitReason = "completed"
	ExitInterrupted ExitReason = "interrupted"
	ExitBudget      ExitReason = "budget"
	ExitError       ExitReason = "error"
)

type Outcome struct {
	Reason   ExitReason
	Tokens   int64
	Duration time.Duration
}
```

Design decisions:

- **`KindIdle` maps to pi's `agent_settled`, never `agent_end`.** pi's
  documentation states that after `agent_end` it may still auto-retry,
  auto-compact and retry, or continue with queued follow-up messages, and that
  `agent_settled` is the event for integrations needing to know pi will not
  continue on its own. Binding `idle` to `agent_end` produces an intermittent
  race in which PC advances the loop while the agent is still working.
- **`Event` carries no raw adapter payload.** A `json.RawMessage` field would be
  the seam through which pi-isms reach callers, and no import test would catch
  it. The adapter writes raw JSONL frames to a per-session transcript file
  instead, preserving debuggability without leaking the boundary.
- **Three control verbs, with `Steer` deliberately *not* a preemption
  primitive.** The day-0 spike established that pi accepts a steer during an
  in-flight tool call, reports it in a queue, and applies it only once the tool
  finishes — measured at 23 seconds behind a `sleep 25`. `Steer` therefore means
  "deliver at the next turn boundary." `IntentBlock` maps to `Steer`, ordinary
  intents map to `Follow`, and only an operator stop or budget kill maps to
  `Interrupt`, which alone preempts in-flight work.
- **Wall-clock budget is enforced by the caller and scenario-wide; tokens are
  reported.** `pkg/runtime` documents `Budget.Wall` as enforced by the caller —
  no adapter is required to kill its own session — and `internal/pcops.Run`
  applies `budget.wall` as a single context deadline over the WHOLE scenario,
  not per session. The same value is passed down in `Spec.Budget` so an adapter
  may also honour it, and none does in Phase A. There is therefore no per-agent
  wall-clock kill: one slow agent ends the entire run rather than being
  terminated on its own. Token enforcement requires a policy decision (kill,
  warn, or degrade the model) that this slice does not need to make.

## `pkg/workspace`

```go
func New(repo, root string) (*Manager, error)

// Acquire creates or reattaches a worktree for agent on branch and takes an
// exclusive lease. Returns ErrLeased if a live holder exists.
func (m *Manager) Acquire(ctx context.Context, agent, branch string) (*Lease, error)

type Lease struct{ Agent, Path, Branch string }

func (l *Lease) Heartbeat(ctx context.Context) error
func (l *Lease) Release(ctx context.Context) error
```

Leases persist in a `leases` table **in the same SQLite file as the bus** — no
second store. Exclusivity comes from a `UNIQUE` constraint on the worktree path.
A lease whose heartbeat is older than a staleness threshold is reclaimable, which
is how a crashed agent's workspace is recovered.

`Lease.Path` becomes `Spec.Workdir`, making isolation structural: agent A cannot
read or write agent B's tree.

The gate runner needs a workspace containing *both* agents' work, so it holds a
third lease and merges the participating branches before running the spanning
test. This is the design spec's "integration agent" in miniature.

## `internal/runtime/pi`

Invocation per session:

```text
pi --mode rpc
   --name <agent>
   --session-dir <run>/<agent>
   --skill <pcdir>/skills/pc-coordination
   --model <mapped from Spec.Model>
```

with `$PC_AGENT`, `$PC_DB`, and provider credentials in the process environment.

**Coordination knowledge is delivered by CLI flag, not by project file.** This
avoids a silent failure: `--mode rpc` never prompts for project trust, and a
freshly created worktree has no saved trust decision, so under pi's default
`defaultProjectTrust: "ask"` any `.pi/skills` written into the worktree would be
ignored without error. Resources passed explicitly by path are loaded before
trust is resolved. The `pc-coordination` skill therefore lives in PC's own
directory, is versioned with PC, and is never written into the user's repository.

**Role framing goes in the initial prompt, not `--system-prompt`.** That flag
replaces pi's default system prompt, which is tuned for coding work and worth
keeping.

**Framing.** pi requires records to be split on `\n` only. Go's
`bufio.ScanLines` satisfies this, but `bufio.Scanner`'s default 64KB token limit
would truncate large frames. The adapter uses `bufio.Reader.ReadBytes('\n')` with
no size cap.

This is measured, not theoretical. The day-0 spike observed a **65,590-byte
frame** — past `bufio.Scanner`'s 65,536-byte default — from a single ordinary
bash command, emitted as a streaming `bash_execution_update` chunk. Those chunks
are *not* subject to the 51,200-byte truncation pi applies to `response`
payloads, so the cap is reachable trivially. The same probe confirmed that
`ReadBytes` preserves U+2028 inside a payload, where Node's `readline` would
split on it.

Event mapping:

| pi RPC event | `runtime.Event` |
|---|---|
| `agent_start` | `started` on first occurrence, else `turn_began` |
| `turn_start` / `turn_end` | `turn_began` / `turn_ended` |
| `tool_execution_end`, `bash_execution_update` | `tool_used` with `ToolUse` |
| `queue_update` | `queue_changed` with pending counts |
| `agent_settled` | `idle` |
| `agent_end` | dropped deliberately |
| `extension_error`, error frames | `errored` |
| process exit | `exited` |
| `auto_retry_*`, `compaction_*` | swallowed — noise, not loop state |

Command mapping:

| `Session` method | pi RPC command |
|---|---|
| `Steer` | `{"type":"steer","message":…}` |
| `Follow` | `{"type":"follow_up","message":…}` |
| `Interrupt` | `abort_bash` (if a tool is in flight), then `clear_queue`, then `abort` |
| `Close` | `abort`, then terminate the process |

pi's version is pinned per scenario. The conformance suite is the canary for
event renames across pi releases.

## Hybrid communication wiring

Agent-to-PC traffic travels **up** through the shell; PC-to-agent traffic travels
**down** through RPC control verbs.

**Up.** The agent's `bash` tool runs `pc submit` or `pc send`, publishing to the
bus as `$PC_AGENT`. This is the harness-agnostic contract, identical for Codex or
Claude Code.

**Down.** Each spawned session gets one `pkg/agent.Agent` acting as its courier
on the bus. Handlers translate intents into session verbs:

```text
IntentBlock       -> session.Steer()   // queued; applied at the next turn boundary
all other intents -> session.Follow()  // queued behind pending work
```

Neither path preempts. A `block` arriving while an agent is running its test
suite waits for that suite to finish, which is the correct trade: aborting the
tool would destroy work the agent is seconds from reporting. `Interrupt` — the
only preempting verb — is reserved for operator stop and budget kill.

**The courier drops any message whose `From.Agent` equals its own identity.** The
spike found that `pkg/bus/sqlite` applies sender filtering only on the topic
path, so a self-addressed direct message *is* delivered. Without this guard, a
model that ran `pc send` addressed to itself would have its own message steered
back at it.

Because delivery is pushed, **`pc inbox` is unnecessary**, and with it the
"did the model remember to poll?" failure mode.

A parked agent — one blocked inside `pc submit` — receives a failing gate verdict
**both ways, not one or the other**. Its `pc submit` returns the verdict as an
exit code and detail, and its courier is subscribed for the whole run and is a
separate process from the parked command, so the coordinator's `IntentBlock` —
sent on the direct path, which `pkg/bus/sqlite` does not sender-filter — is also
accepted and queued as a `Steer`, applied at the first turn boundary after the
park ends. `pkg/agent` additionally auto-acks that block, so the log carries an
extra `Ack` from the parked agent.

The consequence is a duplicate: the agent is told about the same failure twice,
once synchronously and once as queued steering. Suppressing it is **deferred to
Phase B**, where a real agent branches on `pc submit`'s exit code and the
courier can drop a block whose verdict the agent has already consumed. Phase A's
scripted fake never branches on the exit code, so the duplicate is invisible to
it and cannot be designed against yet.

The unsolicited mid-task message in the PoC scenario is still worth keeping: it
is the only step that exercises the steer path against an agent that is
*working* rather than parked, which is the state where next-turn-boundary
delivery is the interesting behaviour.

## `cmd/pc` surface for this slice

| Command | Lifetime | Notes |
|---|---|---|
| `pc submit --gate <id>` | short | as previously spec'd; exit 0 pass, 1 fail/stall, 2 no verdict |
| `pc up` | daemon | gate coordinators |
| `pc run-gate --gate <id>` | daemon | extended: merges participating branches before running |
| `pc watch` | streams | the demo viewport |
| `pc send --to <agent> --intent <intent> <text>` | short | **new** — agent-to-agent messaging |
| `pc run --scenario <file>` | daemon | **new** — acquires leases, spawns runtimes, hosts couriers |

`pc mcp` and `pc ask` are out of scope for this slice.

### Scenario file

```yaml
repo: ./fixtures/two-service
db: .pc/poc.db
gate:
  id: checkout
  required: [billing, gateway]
  runner: integrator
  run: go test ./integration/...
agents:
  - name: billing
    branch: agent/billing
    role: implementer
    task: "Accept a currency field on the charge API in services/billing."
  - name: gateway
    branch: agent/gateway
    role: implementer
    task: "Send a currency field when calling billing from services/gateway."
runner:
  name: integrator
  branch: agent/integration
budget:
  wall: 15m
```

This file is a hand-written loop definition. It is the seed of `pkg/loop` and,
eventually, the artifact Loop Studio generates.

## The PoC

### Fixture repository

`fixtures/two-service/`, plain Go, no new dependencies:

```text
services/billing/     charge handler and its unit tests
services/gateway/     calls billing, and its unit tests
integration/          the spanning test: gateway -> billing
```

The task is chosen so **neither agent can make the spanning test pass alone**:
adding a `currency` field to the charge contract requires billing to accept it
and gateway to send it. `go test ./integration/...` stays red until both have
moved. This makes the gate load-bearing rather than decorative — one agent cannot
fake the loop by doing all the work.

### Run sequence

1. PC acquires three worktree leases: `billing`, `gateway`, `integrator`.
2. It opens the SQLite bus, starts the `checkout` gate coordinator, and starts
   `run-gate` on the integrator worktree.
3. It spawns two pi sessions with role and task, each pinned to its lease path,
   each with a courier on the bus.
4. The agents work independently: read, edit, run their own unit tests.
5. Billing settles the field name and runs
   `pc send --to gateway --intent inform "contract field is amount_minor"` while
   gateway is mid-task. The courier delivers it. Gateway adapts without polling.
6. Each agent runs `pc submit --gate checkout` on completion and parks.
7. Quorum reached: the integrator merges both branches and runs the spanning test.
8. On failure the verdict routes `IntentBlock` to the owners, whose parked
   `pc submit` returns exit 1 with detail; they fix and re-submit. On success it
   returns exit 0 and the run ends.

### Success criteria

1. Gateway demonstrably acts on billing's mid-task message, visible in its
   transcript, with no polling call.
2. Two real agents reach gate quorum.
3. A failing round routes a block that at least one agent acts on.
4. A subsequent round passes the spanning test.
5. Neither worktree contains the other's edits.
6. Wall-clock time and token cost are recorded per run.

### Negative result

If three attempts cannot converge within budget, that is the answer to the
project's central empirical question — whether an N-agent loop beats one good
agent on the same task — and it must change the product's shape rather than be
absorbed by additional retries.

## Failure modes

| Failure | Detection | Response |
|---|---|---|
| pi process dies mid-task | `exited` with no prior `idle` | release the lease, fail the run with attribution; no silent retry, because a crash loop spends real money |
| Agent parks indefinitely | `pc submit` timeout | exit 2 (distinct from failure); the run is marked stalled |
| Runner never executes | gate runner-timeout | already handled by `pkg/gate` |
| Merge conflict in the integrator | `git merge` exit status | verdict FAILED with the conflict as detail, routed to both owners |
| Agent never calls `pc submit` | absent from tool events | the scenario-wide wall-clock budget ends the RUN (there is no per-agent kill — see the Budget note above); recorded as a loop-design failure, not a code defect |
| Agent edits outside its service | `tool_used` paths | recorded, not prevented; prevention requires the container milestone |
| Token runaway | `Outcome.Tokens` | reported, not enforced |
| pi renames an event | pi conformance run fails | pinned pi version; the suite is the canary |

Two of these are worth emphasis. A **merge conflict is the most likely cause of
round-one failure**, not model error, because two agents editing a shared
contract file is the normal case; the loop must treat a conflict as an ordinary
routed failure. And **out-of-scope edits are recorded rather than blocked**,
because worktree isolation bounds visibility, not intent.

## Testing

The first implementation of `runtime.Runtime` is a **scripted fake, not pi**,
following the existing `pkg/bus/bustest` pattern: one conformance suite, several
adapters.

| Layer | Coverage | Runs |
|---|---|---|
| Unit | `pkg/workspace` acquire, conflict, stale reclaim, against a temp git repo | always |
| Conformance | `pkg/runtime/runtimetest` | fake always; pi under `PC_E2E_PI=1` |
| Integration | full `pc run` over the fake, scripted to fail round one and pass round two | always, in CI |
| End-to-end | real pi, real model, recorded | manual only |
| Boundary | import graph: nothing in `pkg/` reaches the pi adapter | always |
| Framing | a frame over 64KB, one containing U+2028, one with CRLF | always |

The integration row is the regression test for the loop itself. It exists only
because the runtime is an interface: without the fake, the loop could be tested
only by spending tokens on a nondeterministic model, which in practice means it
would not be tested.

## Build order

```text
day-0 spike  (complete — see the findings note)
  -> pkg/bus/sqlite cursor monotonicity fix   (prerequisite, see below)
  -> pkg/workspace
  -> pkg/runtime + fake + runtimetest
  -> cmd/pc (submit, send, up, run-gate, watch)
  -> deterministic integration test        (the loop proven, zero tokens)
  -> internal/runtime/pi
  -> pc run --scenario + fixtures/two-service
  -> manual end-to-end PoC
```

The expensive and nondeterministic component comes last, after everything
surrounding it is proven.

## Day-0 spike

**Complete.** All five questions answered against pi `0.84.4`; results and
evidence in
[2026-09-01-day-0-spike-findings.md](./2026-09-01-day-0-spike-findings.md). The
amendments they produced are already folded into the sections above.

### Prerequisite fix: cursor monotonicity

The spike found a latent bug in shipped code. `saveCursor` upserts the stored
position unconditionally, and two subscribers sharing an agent name share one
`cursors` row. A short-lived subscriber exiting behind a long-lived one rewinds
the stored cursor, so the next subscription under that name replays consumed
messages — replaying a `block` would re-steer an agent about a resolved failure.

Unreachable today, and reachable the moment the courier ships alongside
`pc submit`. The fix guards monotonicity:

```sql
ON CONFLICT(agent) DO UPDATE SET last_seq = MAX(cursors.last_seq, excluded.last_seq)
```

It lands first, with a regression test in `pkg/bus/bustest` so both transports
are held to it.

## Open questions

- Whether role framing in the initial prompt is sufficient to hold an agent
  inside its own service, or whether `.pi/APPEND_SYSTEM.md` with `-a` is needed.
- Model selection per role, and its cost profile, which the PoC will measure
  rather than assume.
- Whether the courier should ever escalate a `block` to a preempting
  `Interrupt` — for instance when the in-flight tool has already outlived the
  gate's own deadline. Deferred until the PoC shows real latencies.
- Whether the `go 1.23` floor from `97664d7` can revert to `1.22` for this
  slice, since it was forced solely by the MCP `go-sdk` and `pc mcp` is cut.

## Known limitations

- Worktree isolation bounds visibility, not capability: an agent can still write
  outside its tree. Real containment requires the deferred container boundary.
- Token budgets are observed, not enforced.
- Urgent delivery is not preemption: a `block` reaches a working agent only at
  its next turn boundary, bounded by the duration of the tool call in flight.
- A parked agent — one blocked inside `pc submit` — cannot *act* on the push path
  until that call returns, up to the submit timeout. Gate verdicts arrive as the
  command's exit code precisely because of this. The push message is still
  accepted and queued meanwhile, so a failing verdict reaches a parked agent
  twice; de-duplicating that is Phase B work (see Hybrid communication wiring).
- `Budget.Wall` bounds the whole scenario from `pc run`, not each session
  individually. A single agent that never finishes cannot be killed on its own.
- Single machine, single repository, one gate.
- pi is pinned; event-name churn across pi releases will surface as conformance
  failures requiring adapter updates.
- The scenario file is hand-authored; nothing generates or validates loop shape
  yet.
