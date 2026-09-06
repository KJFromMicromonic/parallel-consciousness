# Phase B — Ship the External-Agent Path — Design Spec

**Status:** Approved (design phase) · **Date:** 2026-09-06 · **Project:** Parallel Consciousness

## Summary

Phase B makes the coordination path that already works **installable and
observable**. It adds `pc watch` (a readable activity feed over the durable
log), closes the round-fence correctness gap, ships a realistic two-service
fixture, and packages the whole thing with setup recipes, `pc init`, config
validation and docs.

It deliberately does **not** build the agent-harness adapter. That inversion is
the point of this spec, and the reason is evidence rather than preference.

## Motivation — what live-fire changed

Phase B was originally scoped as *"PC launches and supervises agents"*: build
`internal/runtime/pi`, then containers, then the loop engine. That ordering
assumed the risky, unproven thing was whether agents could coordinate at all.

Running the system against real models settled that question in the other
direction. Recorded in
[2026-09-02-live-fire-findings.md](./2026-09-02-live-fire-findings.md):

- Two agents from **two different vendors** (pi/gpt-5.5 and Claude Code)
  converged through one gate in 14-24 seconds across runs, including a failing
  round whose detail taught one agent a value **verifiably absent from its own
  worktree**.
- The coordination contract worked with **zero prompt revisions**, and the same
  ~20-line snippet worked verbatim on both harnesses.
- Agents used the bus **three times unprompted**. `pc send` appears nowhere in
  the contract they were given; one of those messages diagnosed a defect in the
  coordinator before its author found it.

So the adapter's headline benefit — that agents coordinate — is already
demonstrated without it. Its remaining unique value is narrower and real:
role-specific context control (Loop Studio's prerequisite), evidence gathered
from observed tool events, and unattended operation. None of those is what a
design partner needs in order to try this.

Meanwhile the sharpest gap in the path that *does* work is observability.
In every live run, answering "what are my agents doing?" required writing SQL
against the message log by hand. `pc watch` is specified in the
original CLI design and has never been built.

## Principles

1. **Ship what is proven; defer what is assumed.** The external-agent path has
   evidence behind it. The adapter has an argument.
2. **Observability is a capability of the durable log, not of the transport.**
   The `bus.Bus` contract stays two methods wide.
3. **Silence is the enemy.** Every failure mode this phase touches — a dropped
   readiness, an invalid config, a fenced lease — currently manifests as a wait
   with no signal. Each becomes an immediate, attributed message.
4. **Claim only what has been verified.** Setup recipes ship for harnesses
   actually driven end to end; anything else gets requirements, not a claim of
   support.

## Scope

**In scope:**

- `pc watch` — replay plus live streaming of the gate's activity feed.
- A read-only history/tail capability on the concrete SQLite bus.
- Verdicts carry the version map they were computed over; `Submit` accepts only
  a verdict that includes its own submitted version.
- A `Nack` when readiness arrives while a round is in flight.
- `ErrLeaseLost` acted on rather than logged.
- A concurrent-`Acquire` test.
- `fixtures/two-service` as its own Go module.
- Setup recipes for pi and Claude Code, over one shared contract snippet.
- `pc init`, a `./.pc.yaml` default for `--config`, and config validation in
  `LoadConfig`.
- `scripts/live-run.sh` — a reproducible live-run harness with pre-flight checks.
- README quickstart and an honest statement of limitations.

**Out of scope (deferred, with reasons):**

| Excluded | Why |
|---|---|
| `internal/runtime/pi` adapter | Its headline claim is already demonstrated; its remaining value is Phase C's case to make |
| JSONL framing tests | They test the adapter; they travel with it |
| Container / micro-VM isolation | Worktrees bound visibility, which suffices while a human launches agents. Containment pairs with unattended operation |
| `pkg/store`, `pkg/loop`, Loop Studio | Unchanged from Phase A's coverage map |
| Web dashboard | `pc watch` is the observability answer for now |
| Multi-machine / Turso | Out |
| Forced gate re-run | The F2 limitation stands, documented |

**Explicitly no longer a blocker:** the conformance suite. The whole-branch
review of PR #2 named exactly two gaps — non-preemption for `Steer`/`Follow`,
and ctx coverage on `Interrupt`/`Wait`. Both are closed, and the suite now
carries 16 properties. Phase C can begin on the adapter without first repairing
the thing that would certify it.

**Stretch, not a deliverable:** Stage 2, the deliberate peer-messaging
experiment. Peer messaging has already been observed three times spontaneously,
which is stronger evidence than a designed test would produce. Once
`scripts/live-run.sh` exists it is a variant scenario and nearly free, so it is
worth doing if the phase has room — but it gates nothing.

## `pc watch`

### Why it cannot be built on the transport abstraction

`bus.Bus` is deliberately two methods, and `Subscribe` filters to messages
addressed to you or to topics you have joined:

```sql
WHERE seq > ? AND (to_agent = ? OR (to_topic IN (…) AND from_agent <> ?))
```

A watcher is the recipient of nothing. Through that contract it would see the
gate topic and nothing else — no routed blocks, no peer `pc send` traffic. In
live-fire those direct messages were the *most* valuable content: every failure
detail and all three emergent peer messages travelled that way.

The requirement is therefore not a missing method on the transport. It is a
different capability that happens to share a backend: **reading history is a
property of a durable log.** The in-memory bus has no retention and could only
satisfy such a method by lying or by becoming stateful — which is the tell.

**Decision:** add a read-only history/tail method to the concrete `*sqlite.Bus`,
not to `bus.Bus`. `internal/pcops` already depends on `pkg/bus/sqlite`
concretely, so this costs nothing architecturally and keeps the transport
contract intact. If a second durable transport ever appears, that is when a
narrow `History` interface earns its existence — not before.

### Shape

```
pc watch --config <file> [--gate <id>] [--full] [--no-follow]
```

Replays recorded history, then streams live by polling the log. `--no-follow`
dumps history and exits, which covers the post-mortem query case without a
second command. `--gate` filters; `--full` disables truncation.

### Output

One readable line per message:

```
15:52:58  billing     → #gate.currency  ready      v=acef4043
15:52:58  coordinator → integrator      request    billing=acef4043 gateway=9c06c1cd
15:52:59  integrator  → coordinator     disagree   SPANNING FAILURE: … want "charged 100 (USD)"
15:52:59  coordinator → #gate.currency  inform     currency FAILED: …
15:52:59  coordinator → billing         block      currency gate failing: …
```

Versions abbreviated to eight characters, bodies summarised per intent, long
details truncated unless `--full`. This is precisely the view that had to be
reconstructed with hand-written SQL three times during live-fire.

## Correctness — verdicts carry their versions

### The defect

Two halves, one of them pre-existing:

1. `onReady` silently drops a readiness that arrives while a round is in flight.
   The agent gets no signal at all.
2. `Submit` accepts any verdict for its gate whose timestamp is later than its
   own `readyAt`. So an agent whose readiness was dropped still accepts that
   round's verdict — **a verdict computed without its version.**

### The fix

**Verdicts become self-describing.** `resolve()` already computes `v.Versions`,
filling it from `gs.ready` when the runner does not supply one, but the
broadcast body is only `{text, gate, passed}` — the version map is discarded at
the moment it becomes useful. Add `versions` to the body.

Note the transport asymmetry: over JSON the map returns as `map[string]any`, not
`map[string]string`. `pkg/gate` already carries `versionsFromBody` for exactly
this; `Submit` decodes the same tolerant way rather than growing a second parser.

**`Submit` matches its own version.** A verdict is accepted only when
`versions[me]` equals the version submitted. That closes the wrong-answer path:
an agent whose readiness was dropped keeps waiting for the round that actually
included it.

**A dropped readiness gets a `Nack`** naming the gate, so silence becomes signal.

`IntentNack` is the correct intent for the same reason `IntentAck` was correct
for F4: `internal/pcops/courier.go` registers handlers for `IntentBlock` and the
six sendable intents, and neither `Ack` nor `Nack` is among them — so it never
reaches an agent's live session. This is the third time that consideration has
decided an intent choice on this project, and it belongs in a comment. An
earlier design that ignored it produced an unbounded feedback loop.

### The interaction that is easy to get wrong

`Submit` must treat a `Nack` as **also satisfying F4's acknowledgement wait** —
a Nack proves a coordinator exists just as well as an Ack does. Otherwise this
new signal trips the "no coordinator" diagnosis added in F4.

On receiving one, `Submit` waits out the in-flight round — declining its verdict
via the version check — and re-declares readiness, bounded by `submit_timeout`.
Retry stays inside the tool rather than being handed to a model: F2 established
that models retry redundantly and expensively, at a measured cost of six minutes
against a fourteen-second loop.

## The two remaining correctness items

**`ErrLeaseLost` acted on.** The heartbeat loop logs a fenced lease and keeps
ticking, so a run would continue working in a directory it no longer owns. It
must stop heartbeating that lease and surface an attributed run failure, in the
same shape as the existing session-death path.

**A concurrent-`Acquire` test.** Lease exclusivity is the property the entire
structural-isolation claim rests on, and it is currently verified by reading the
SQL. Several goroutines contending for one agent's lease: exactly one winner, no
orphan worktree.

## The fixture

`fixtures/two-service/`, committed, as **its own Go module** so Go excludes it
from the root `./...`. It stays out of the project's build, vet and test surface,
and agents can mutate it freely during a demo without touching project source.

Four design constraints, each learned the hard way across successive live runs:

1. **Each half must compile independently.** An earlier arrangement gave
   `billing` the job of adding the `Currency` field and `gateway` the job of
   setting it — so gateway could not compile until billing's change merged.
   Agents would deadlock rather than coordinate. The field exists up front;
   billing renders it, gateway populates it, and both halves build alone.
2. **The agreed value must be undiscoverable from either worktree.** Capable
   models simply read the spanning test and get it right first time, which is
   why the first live stage never reached the failure branch. Supplying it from
   the gate command's environment (`EXPECTED_CURRENCY=USD go test
   ./integration/...`) is realistic — the integration environment owns the
   contract — and is what finally forced a real model to learn a value from a
   failure detail.
3. **One side is seeded wrong.** `gateway` inherits `"EUR"`, so round one fails
   for a realistic reason: an agent inherits existing code, believes it is fine,
   and the gate teaches it otherwise.
4. **The spanning test skips when the variable is absent**, rather than failing,
   so an agent running `go test ./...` locally is not misled — and it reinforces
   the contract's own claim that you cannot run the spanning test yourself.

One repo test copies the fixture to a temp directory and asserts the baseline
genuinely fails, as cheap insurance against the fixture rotting into a passing
state.

The CI integration test keeps its own inline fixture. That is deliberate: the CI
one must be deterministic and disposable, this one is a realistic artifact a
human drives.

## Packaging

**The contract snippet is the portable artifact, and ships as one file.** The
same ~20 lines worked verbatim across pi and Claude Code with zero prompt
revisions — the strongest evidence the project has for its agnostic claim. It
lives once; each recipe points at it and documents only how that harness ingests
it.

**Recipes ship only for harnesses actually verified end to end: pi and Claude
Code.** For anything else, document what a harness needs — run a shell command,
surface exit codes, accept an instruction file — rather than claiming support
that has not been demonstrated.

- **pi** — contract via `--skill <path>`, because CLI-provided resources load
  before project trust is resolved and non-interactive modes never prompt for
  it. Environment: `PC_AGENT`, `PC_DB`, and `pc` on `PATH`.
- **Claude Code** — contract via `CLAUDE.md` in the worktree; print mode needs
  `--permission-mode acceptEdits --allowedTools Read Edit Write Bash`.
- **The stdin trap, prominently.** `pi -p` merges piped stdin into the prompt, so
  a backgrounded invocation with an inherited open stdin blocks forever.
  `< /dev/null` fixes it. This cost two nine-minute live runs and is invisible
  until two harnesses are driven side by side.

**`pc init`** scaffolds a commented `.pc.yaml`, and `--config` gains a default of
`./.pc.yaml` when present, completing what the original CLI design described.

**Validation moves into `LoadConfig`**, not just `init`, so every command fails
fast: `gate.required` names must appear in `agents`, `gate.runner` must match
`runner.name`, names and branches unique, gate id non-empty. Today a mismatch
produces a silent hang to the wall budget. This is the cheapest item in the
phase and removes a fifteen-minute mystery.

**README** gains the two-terminal quickstart, the recipes, and an honest
statement of what is proven versus not: single machine, one gate, externally
launched agents, no forced gate re-run.

## Testing

- **The fence fix gets a test that reproduces the defect**: a round in flight, a
  second agent's readiness dropped, and an assertion that the agent does *not*
  accept the in-flight round's verdict but does accept the later one that
  included it. Written first; it is the fix's entire justification.
- **`pc watch` splits into three layers.** The history/tail method gets unit
  tests for ordering and `fromSeq` filtering. Line formatting becomes a **pure
  function** from message to string, table-tested, keeping format assertions away
  from I/O. One end-to-end test drives a real gate round and asserts
  `--no-follow` emits the expected lines in order. `--follow` is bounded by a
  deadline inside a `select`, never a sleep.
- **Config validation is table-driven** — each invalid shape maps to a specific
  error.
- **`pc init` is tested by round-trip**: init writes a file, `LoadConfig` accepts
  and validates it. Asserting exact bytes would be brittle.
- **`Nack` handling** is tested both ways: the Nack is sent when readiness lands
  mid-round, and `Submit` treats it as satisfying the acknowledgement wait rather
  than reporting a missing coordinator.
- **The F2 interaction** is tested: a cached resubmit still returns its verdict,
  with versions that match.

**Live validation.** Phase B's real proof is a live run, and live runs have their
own flake rate — two failed for reasons outside the code entirely, one because a
stale binary was silently under test and one because a harness blocked on stdin. So the phase ships
**`scripts/live-run.sh`**: pre-flight asserting the binary's provenance and
smoke-testing the harness, then fixture reset, daemon startup, both agents, and a
report. Not CI — an operator tool. It is what would have prevented both non-code
failures, and it makes the demonstration reproducible by someone other than its
author.

## Expected plan decomposition

This spec covers seven workstreams and should not become a single implementation
plan. The natural cut is at the end of the correctness work: one plan for
`pc watch` plus the three correctness items, and a second for the fixture,
recipes, `pc init`, validation, docs and `live-run.sh`. The first produces a
system you can observe and trust; the second produces one someone else can
install. Each gets its own plan, execution and review cycle.

## Sequencing

Observability first, then correctness, then packaging:

```
pc watch  ->  verdict versions + Nack  ->  ErrLeaseLost + concurrent-Acquire test
          ->  fixture  ->  recipes + pc init + validation  ->  docs  ->  live-run.sh
```

`pc watch` leads because every later item in the phase is validated by watching a
run, and because the verdict-versions change is far easier to verify when you can
see what a verdict carries.

## Known limitations after Phase B

- Single machine, single repository, one gate per coordinator.
- Agents are launched by a human; PC does not supervise processes.
- A gate cannot be deliberately re-run on an unchanged version.
- Worktrees bound visibility, not capability: an agent can still write outside
  its own tree.
- Token budgets are observed, not enforced.
- `cursors` rows accumulate one per `pc watch` invocation that replays from zero;
  pruning is deferred.

## Open questions

- Whether `pc watch` should learn a compact summary mode (current round, who the
  gate waits on) before a web dashboard exists, or whether the feed suffices.
- Whether the contract snippet should eventually ship as an installable pi
  package and a Claude Code skill, rather than as a file recipes point at.
- Whether Codex should be added as a third verified harness during this phase or
  left to Phase C; adding it means driving it end to end first, since the
  principle is to claim only what has been verified.
