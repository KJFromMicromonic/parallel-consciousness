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

## Recorded for a later phase — context isolation is a per-role policy, not an invariant

**This changes nothing in Phase B.** It is recorded here because Phase B's spec is
the live planning document, and the finding must be seen before `pkg/loop` is
designed — by which point it would be expensive to discover.

### The finding

Phase A made context isolation structural and global: a git worktree per agent,
one active owner per work item, and the stated principle that *"an independent
reviewer should not inherit the implementer's reasoning or incentive to accept
its own work."*

That principle is correct — but only for some loop shapes. Read as a global
invariant it is wrong, and the loop engine must not encode it as one.

### The evidence

Five loop proposals were written out by hand for deliberately dissimilar goals: a
cross-service contract change, a dependency upgrade across a monorepo, a flaky
test investigation, a security audit, and a performance regression. The exercise
was originally aimed at a UI question, but it surfaced this instead.

Three of the five want isolation, and one wants it very strongly:

- **Cross-service contract change** — each implementer must not see its peers'
  worktrees; that is what makes the spanning test meaningful.
- **Dependency upgrade** — each shard sees only its own packages.
- **Security audit** — the strongest case of the five. The adjudicator deciding
  whether a finding is real must *not* inherit the analyst's reasoning, or it
  rubber-stamps false positives. Isolation is the mechanism of correctness here.

But one wants the opposite:

- **Flaky test investigation** — reproduce, hypothesise, instrument, re-measure,
  repeat. A single investigator building an accumulated model across many
  iterations is the entire value. Parallel isolated agents would each start from
  nothing and rediscover the same dead ends. Here isolation is not a safeguard;
  it is the failure mode.

### The correction

Phase A's mechanism stays right: worktrees and exclusive leases remain the way
isolation is enforced, and isolation remains the sensible default. What changes is
that **the loop proposal must be able to set a context policy per role** —
isolate, or accumulate across iterations — rather than the engine assuming one
globally.

Concretely, when `pkg/loop` is designed, a role needs a context policy alongside
its other attributes, and the spec's isolation principle should be restated as
*"an independent reviewer must not inherit the implementer's reasoning"* — a
constraint on the reviewer relationship specifically, not a property of every
role in every loop.

### Related, from the same exercise

Not every loop terminates in success. Across the five, legitimate terminal states
included green, partial success with a report ("47 shards done, 3 need you"),
"could not fix it — here is what I ruled out", and "wrong shape, replan" when a
bisect finds no single culprit. A loop engine that models only pass and fail
would misrepresent three of the five. Recorded for the same reason and at the
same time; no Phase B work implied.

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

> **Answered.** All three were resolved on 2026-09-09; see the Part 2 addendum
> at the end of this document. Summarised: no `pc watch` summary mode, no Codex
> recipe in this phase, and the contract snippet stays a file recipes point at.
> They are left here as written because the reasoning that resolved them is
> only legible against the question.


- Whether `pc watch` should learn a compact summary mode (current round, who the
  gate waits on) before a web dashboard exists, or whether the feed suffices.
- Whether the contract snippet should eventually ship as an installable pi
  package and a Claude Code skill, rather than as a file recipes point at.
- Whether Codex should be added as a third verified harness during this phase or
  left to Phase C; adding it means driving it end to end first, since the
  principle is to claim only what has been verified.

---

# Part 2 addendum — resolved scope (2026-09-09)

Part 1 shipped (`pc watch` plus the three correctness items) and is open as
PR #4. This addendum records what part 2 actually contains: the open questions
above are now answered, part 1's execution added items this spec never covered,
and one thing this spec asserted turned out to be untrue.

Everything under "The fixture", "Packaging" and "Testing" above carries forward
unchanged and is not restated here. This section covers only what is new or
decided.

## The open questions, answered

**`pc watch` summary mode: no.** The feed suffices for now. Every fact a summary
would compute is already in the feed, and the right time to decide what an
operator actually misses is after watching a real run — not before. Deferred
without a scheduled home.

**Codex as a third verified harness: no, deferred to Phase C.** The principle
that recipes ship only for harnesses driven end to end is the reason: adding
Codex means a full live run first, with its own harness quirks to discover, and
part 2 cannot ship until that run succeeds. Two of the prior live runs each
failed once for reasons outside the code entirely. Phase C is the adapter phase,
so harness work clusters there. Part 2 ships recipes for pi and Claude Code and,
for everything else, documents what a harness needs rather than claiming
support.

**Contract snippet as an installable package: unchanged, still deferred.** It
ships as one file that recipes point at.

## Correction to the "Packaging" section

That section says validation "moves into `LoadConfig`, not just `init`, so every
command fails fast", and lists the rules. That stands. But it implies the
timeout budgets are already coherent, and they are not:

`budget.submit_timeout` is a config field (`config.go:65`) defaulting to five
minutes. The coordinator's runner timeout is **hardcoded** at ten minutes in
`up.go:41` and is not reachable from a scenario file at all. So the shipped
default pair is inverted: a submit nacked by a genuinely long round exhausts its
own context before the round it is waiting for can finish, and returns
`ErrNoVerdict`. Part 1 made that survivable — the submit now prints an
explanatory line rather than sitting silent — but the retry it was built to
enable cannot complete under the default configuration.

Two numbers govern one interaction and only one of them is the operator's. The
fix is to make both theirs:

- Add `budget.runner_timeout`, defaulting to ten minutes, replacing the
  hardcoded `SetRunnerTimeout` call.
- Validate `submit_timeout > runner_timeout` as one of the table-driven
  `LoadConfig` cases.

This belongs in part 2 rather than a later phase because it is the same class as
every other item in the validation work: configuration that produces a silent
hang instead of an error. It also makes both budgets visible in the `.pc.yaml`
that `pc init` scaffolds, which the "bound autonomy" principle wants — budgets
explicit rather than buried in a composition root.

The alternative considered was simply raising `DefaultSubmitTimeout` above ten
minutes. Rejected: cheaper, but it leaves the runner timeout invisible and the
relationship between the two undocumented, so the next reader hits the same
puzzle with no way to see either number.

## Three refactors this spec did not cover

Part 1's reviews deferred four cleanup items with reasons recorded. Three are
folded into part 2. The fourth — replacing three test poll loops with
`Tail`-based waits — stays deferred; it is test-only and buys little.

### Decode helpers belong in `pkg/protocol`

A body value that has crossed the SQLite bus arrives as `map[string]any` or
`[]any` after its JSON round trip and must be coerced back. There are three
definitions of that one concern and eight call sites:

- `pkg/gate/gate.go:469` defines `versionsFromBody`; used at `gate.go:62`.
- `internal/pcops/watch.go:227` defines a byte-equivalent `versionsFromBody`;
  used at `watch.go:59, 84, 97` and `submit.go:131, 212`.
- `internal/pcops/submit.go:385` defines `outstandingFromBody`; used at
  `submit.go:183` and `watch.go:72`.

The two version decoders cannot share today because `internal/arch` forbids
`pkg/` importing `internal/` — correctly, since `pkg/` is public API. But
`pkg/protocol` imports only `time` and `uuid`, both packages already import it,
and this is a wire-format concern, which is what `protocol` is for.

New file `pkg/protocol/body.go`:

```go
// Versions coerces a wire participant→version map back to map[string]string.
func Versions(v any) map[string]string
// Strings coerces a wire string list back to []string.
func Strings(v any) []string
```

All three local definitions are deleted and their table tests move. The
JSON-asymmetry rationale is documented once instead of three times. This matters
beyond tidiness: this branch alone added two producers of version maps, and a
fourth decoder drifting from the other three is a silent wire-format bug.

### `ErrLeaseLost` has two problems

`pcops.ErrLeaseLost` shadows `workspace.ErrLeaseLost` by name while not being
reachable through it, so `errors.Is(runErr, workspace.ErrLeaseLost)` is false —
a trap for a caller reaching for the name they already know. Separately,
`heartbeatLease` matches the workspace sentinel and then reports only
`l.Agent`, discarding the wrapped chain that carries the worktree path and any
SQL context.

Fix both. Define the sentinel so it wraps, giving one error identity rather than
two:

```go
var ErrLeaseLost = fmt.Errorf("pcops: lease lost to another holder: %w", workspace.ErrLeaseLost)
```

and change the loss channel from `chan<- string` to carry a small
`leaseLoss{agent string; err error}`, mirroring the `sessionFailure` struct
already at `run.go:53`, so the run's failure names the agent *and* preserves
what the heartbeat saw.

### `submitWaiter` — making two invariants structural

`Submit` is 279 lines holding a mutex, two accessor closures, three message
handlers, four channels, and an attempt loop with two selects. Three reviews
walked it and none could construct a wrong outcome, so this is not a bug fix.
It is a fix for how the correctness is *maintained*: two invariants are enforced
by repetition, and each has already been got wrong once.

1. **Fence every arriving message against the round boundary.** Three handlers
   carry three byte-identical copies of
   `if m.Timestamp.Before(readAt()) { return nil }`. The `IntentNack` handler
   did not carry it — the whole-branch review's first Important finding — and a
   stale replayed Nack sent `Submit` into the one wait with no `AckTimeout`
   bound, turning a dead coordinator into five minutes of silence reported as
   the wrong error class.
2. **Drain cross-attempt state before declaring readiness.** Three
   near-identical non-blocking drains. The first version drained one of three.

One struct owns exactly the state the handlers and the loop share:

```go
// submitWaiter owns the state Submit's handlers and its attempt loop share:
// the round boundary, and the channels the handlers signal on.
type submitWaiter struct {
	mu      sync.Mutex
	readyAt time.Time

	verdicts chan gate.Verdict      // buffered 1
	acked    chan []string          // buffered 1
	nacked   chan map[string]string // buffered 1
	declined chan struct{}          // buffered 1
}

// fresh reports whether m belongs to the current attempt rather than being
// replayed from the durable cursor. Every handler calls this.
func (w *submitWaiter) fresh(m protocol.Message) bool

// declareReady stamps a new round boundary, drains every cross-attempt
// channel, and publishes readiness — in that order.
func (w *submitWaiter) declareReady(ctx context.Context, a *agent.Agent, gateID, version string) error
```

**`declareReady` publishes rather than merely stamping, and that is the point of
the design.** The current order — stamp, drain, publish — is load-bearing in a
way three consecutive statements do not advertise:

- Stamping before draining means any handler running after the stamp fences
  correctly on arrival, and the drain clears only what was buffered before it.
- Draining before publishing means no reply to *this* attempt can exist yet, so
  the drain cannot discard a legitimate signal.

Reverse either and a bug returns: drain-then-stamp leaves a window where a
pre-stamp message survives in the buffer, and publish-then-drain throws away
this attempt's own acknowledgement. Folding the publish inside the method makes
the ordering unreachable from outside — the caller cannot get it wrong because
it no longer has an order to get right.

What stays out of the struct: the bus, the agent, the config, the gate id, the
version. It owns the round boundary and the four channels, and nothing whose
lifetime differs from those.

**Testing.** No new behaviour, so the existing suite is the specification. All
of these must pass unchanged: `TestSubmitDeclinesAVerdictThatDidNotIncludeIt`,
`TestSubmitRedeclaresAfterNackAndReturnsTheReDeclaredVerdict`,
`TestSubmitIgnoresAVerdictFromAPreviousRound`, and
`TestSubmitRedundantResubmitAnsweredFromCacheDoesNotErrorAsUnacknowledged`. Two
additions pin the invariants at the level they now live at: that `fresh` rejects
a message stamped before the boundary and accepts one after, and that
`declareReady` clears a pre-buffered signal on each of the four channels.

**Residual risk, stated plainly.** This is a pure refactor of the branch's most
delicate function; the only available outcomes are "unchanged" and "worse". The
mitigation is that the tests constraining it are unusually strong — two were
repaired from vacuous versions during part 1, and one asserts a *negative* (that
`Submit` refuses to return while only a foreign-version verdict is available),
which is exactly what a botched refactor trips. The plan must forbid touching
any existing assertion in `submit_test.go`: a failure there is a signal to stop,
not a test to adjust.

## Task order

1. Decode helpers → `pkg/protocol`.
2. `ErrLeaseLost` taxonomy + `leaseLoss` payload.
3. `submitWaiter` extraction.
4. `budget.runner_timeout` + table-driven `LoadConfig` validation.
5. `fixtures/two-service` as its own module + the copy-to-temp baseline-fails test.
6. Contract snippet + pi and Claude Code recipes.
7. `pc init` + `--config` defaulting to `./.pc.yaml`.
8. README.
9. `scripts/live-run.sh`.

**Refactors lead** because they are behaviour-free, so what catches a mistake is
the existing suite — and that suite is at its most constraining exactly as part 1
left it. Tasks 5-9 add a fixture module and an operator script and make the
surface noisier. Do the invisible risky work while the net is tightest.

Two hard ordering constraints: **4 precedes 7**, because `pc init`'s scaffold
must include `budget.runner_timeout` or it ships a template omitting a field
validation now requires; and **9 is last**, because `live-run.sh` drives the
fixture and the recipes.

Tasks 1, 2 and 4 are mechanical with enumerated values. Task 3 needs judgment.
Tasks 5 and 6 are artifacts a human drives, and their correctness is "does a
real agent do the right thing with this", which no unit test settles.

## What part 2 proves, and what it does not

The spec's own principle is to claim only what has been verified, so:

- Tasks 1-4 and 7 are fully covered by automated tests, including one
  table-driven case per invalid config shape and a `pc init` → `LoadConfig`
  round trip rather than byte assertions.
- Task 5's baseline-fails test is insurance against the fixture rotting into a
  passing state. It does not prove the fixture teaches an agent anything.
- Tasks 6, 8 and 9 are **not** verifiable by the suite. A recipe is correct when
  a real harness ingests it and works; `live-run.sh` is an operator tool, not CI.

**The plan's exit gate** is therefore: full suite green under `-race`, the usual
`gofmt`/`vet`/`build`, and `live-run.sh`'s *pre-flight* checks passing against
the real fixture — binary provenance and harness smoke test, the two failures
that each cost a nine-minute run. The live run itself is a separate step taken
after the branch is green, using the script. Folding a manual run into an
automated gate would be the same overclaim this spec warns against everywhere
else.

## Still deferred after part 2

- Replacing `waitForLogged`'s `History` polling and two other test poll loops
  with `Tail`-based waits.
- `pc watch` summary mode; the contract snippet as an installable package;
  Codex as a verified harness.
- Everything under "Known limitations after Phase B" above, unchanged.
