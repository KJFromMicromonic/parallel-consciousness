# Cross-Service Integration-Test Gate — Design Spec

**Status:** Approved (design phase) · **Date:** 2026-06-19 · **Project:** Conclave

## Summary

The first real capability built on Conclave's conversation layer: a **gate** that
coordinates an integration test spanning services owned by different agents.
Several service-owning agents each declare *"I'm ready at version X"* for a named
gate. When every required participant has declared ready, the gate **opens**, a
designated **runner** is told to go, it executes the spanning test and reports a
**verdict**, and Conclave **broadcasts pass/fail** — routing a failure back to the
owners as a `block` so the conversation continues.

This is a handoff a shared markdown doc fundamentally cannot express, which is the
whole pitch of the project.

## Motivation

Conclave was born from running coding agents in parallel on a distributed system —
**one agent per service** — where the painful coordination failure is *cross-service
test gating*: an integration test spans services owned by different agents, and today
nobody knows when all sides are at a compatible state. So spanning tests run too early
(flaky), or not at all. The gate makes "are we all ready to test together?" an explicit,
observable handshake instead of guesswork.

## Principles

1. **Coordinate, don't execute.** Conclave resolves the readiness barrier and asks a
   runner to go; it never shells out or runs a test itself. This keeps it a pure
   coordination layer, true to the project thesis ("a bus moves bytes; Conclave is the
   conversation on top").
2. **Harness-agnostic.** Conclave coordinates *processes*. Whatever drives a process —
   Claude Code, OpenCode, a script, a human — is opaque to it. The gate API therefore
   traffics only in plain data (a gate id, a version string, a verdict), never anything
   tied to a specific agent harness. This is what lets a future thin CLI/sidecar
   (`conclave ready --gate G --version $(git rev-parse HEAD)`) map 1:1 onto the API so
   any harness can participate by shelling out.
3. **Single-process for v0.1.** Built on the existing in-memory bus. Cross-process
   transport and a durable readiness ledger are deferred deliberately, to be revisited
   once we're confident in the in-process coordination model.

## Scope

**In scope (this milestone):**
- A new `pkg/gate` package: a reusable barrier/coordinator primitive.
- One new protocol intent: `IntentReady`.
- A new runnable example, `cmd/gatedemo`.
- A runner-unresponsive timeout (also lands the roadmap's deadline-enforcement item).
- Unit tests for the coordinator state machine + the demo as an integration smoke test.

**Out of scope (deferred, by decision):**
- Cross-process / multi-machine transport (rides with the Redpanda/Kafka adapter).
- A durable, replayable readiness ledger (the blackboard-topology evolution).
- The harness-agnostic CLI/sidecar (designed-for, not built).
- Semantic version *compatibility* checking (see Simplification 1 below).

## The Model

Three roles, all of them ordinary Conclave agents:

- **Participants** — the service-owning agents (e.g. `billing-svc`, `gateway`). Each
  declares readiness when its own work reaches a state it believes is compatible.
- **Coordinator** — an embeddable component that *one* agent hosts (a dedicated
  `gatekeeper`, or the runner itself). It owns the gate's state and turns scattered
  `ready` signals into a decision. It is not separate infrastructure.
- **Runner** — the agent designated to execute the spanning test. Conclave only ever
  *asks* it to go and *listens* for its verdict.

### New protocol intent

- `IntentReady` (`"ready"`) — a distinct speech act: *"I'm at a compatible state for
  gate G."* Terminal (expects no direct reply); `WantsResponse()` returns false for it.
  Body carries `{gate, version}`.

Everything else reuses existing intents:
- `request` — coordinator → runner: "go" (carries gate id + participant versions).
- `done` — runner → coordinator: pass.
- `block` / `disagree` — runner → coordinator: fail.
- `inform` — coordinator → gate topic: the broadcast verdict.

### Lifecycle (one round)

```
gateway   --ready(v=a1b2)--> #gate.checkout      coordinator records: 1/2
billing   --ready(v=c3d4)--> #gate.checkout      coordinator records: 2/2 -> OPEN
coord     --request-------->  runner             {gate, versions:{gateway:a1b2, billing:c3d4}}
runner    ...runs the spanning test (opaque to Conclave)...
runner    --done----------->  coord              {pass, report}
coord     --inform-------->  #gate.checkout      "checkout PASSED @ a1b2/c3d4"
                                                  round clears; re-arms for the next state
```

On failure the runner replies `block`/`disagree`; the coordinator broadcasts the fail
**and** routes a `block` to the owners, so the existing block-handling machinery picks
up the conversation.

### Two deliberate simplifications for v0.1

1. **Quorum, not compatibility-checking.** "Ready" means a participant *asserts* it is
   compatible. The coordinator only checks that all required participants are ready.
   Whether the versions are *actually* mutually compatible is proven by the test
   passing — not by the coordinator comparing version strings. Versions are recorded and
   handed to the runner, never semantically judged.
2. **Re-arm by clearing.** After a verdict the gate clears its readiness and waits for
   the next round. No history/rounds are retained in-process (that is the durable-ledger
   evolution, deferred).

## API Surface

`pkg/gate/gate.go`:

```go
// Spec defines one gate: a spanning test across services.
type Spec struct {
    ID       string   // "checkout-flow"
    Required []string // participant agents whose readiness is needed
    Runner   string   // agent designated to run the test
}

// Verdict is what a runner reports back and what gets broadcast.
type Verdict struct {
    GateID   string
    Passed   bool
    Detail   string            // failing test name, error, etc.
    Versions map[string]string // participant -> version that was tested
}

// Coordinator hosts gates on top of an agent. One agent hosts it.
type Coordinator struct { /* ... */ }
func NewCoordinator(a *agent.Agent) *Coordinator
func (c *Coordinator) Register(spec Spec)        // declare a gate it coordinates
func (c *Coordinator) OnVerdict(func(Verdict))   // optional hook (logging/tests)

// Participant side — one helper, harness-agnostic (plain strings):
func Ready(ctx context.Context, a *agent.Agent, gateID, version string) error

// Runner side — register the test executor. Conclave calls fn when the gate
// opens; fn runs the test however it likes and returns the verdict.
func ServeRunner(a *agent.Agent, fn func(ctx context.Context, gateID string, versions map[string]string) Verdict)
```

`ServeRunner` is the seam that enforces "coordinate, don't execute": Conclave hands the
runner a gate + versions and gets a `Verdict` back. Whether `fn` shells out to `go test`,
hits CI, or returns a canned result (in the demo) is none of Conclave's business. It is
also exactly the shape a future `conclave run-gate --gate G -- <cmd>` CLI wraps.

### Wiring

The coordinator subscribes to a per-gate topic `gate.<id>`; `Ready(...)` broadcasts
`IntentReady` there. Topic fan-out already skips the sender, so participants harmlessly
observe each other's readiness (useful for situational awareness later). Verdicts
broadcast on the same topic.

## Package Layout (additions only)

```
pkg/protocol   + IntentReady constant; WantsResponse() returns false for it
pkg/gate       NEW — gate.go (Spec, Coordinator, Verdict, Ready, ServeRunner) + gate_test.go
cmd/gatedemo   NEW — billing-svc + gateway + gatekeeper + runner; PASS round then FAIL round
cmd/demo       UNCHANGED — the original block/re-sequence story stays as a second example
```

Nothing existing is rewritten; this is purely additive.

## Error Handling

The failure modes that actually matter for a gate:

1. **Runner hangs / never replies.** The coordinator sets a deadline when it dispatches
   to the runner (using the envelope's `Deadline` field, currently unused). On timeout it
   broadcasts `gate <id> STALLED: runner unresponsive`, routes a `block` to owners, then
   re-arms. This also delivers the roadmap's deadline-enforcement item.
2. **Runner reports fail.** Verdict broadcast + `block` to owners with `Detail`. Re-arm.
3. **Duplicate / out-of-order `ready`.** Dedup by participant, last-version-wins; open
   only when the full required set is present.
4. **Dropped `ready` (known limitation).** The in-memory bus drops on a full inbox; a
   dropped readiness signal would stall the gate silently. For single-process v0.1 the
   coordinator gets a generously buffered inbox, and this is **documented as a known
   limitation** — the real fix (acked/durable delivery) rides with the transport work.
   Named honestly rather than pretended solved.

## Testing Strategy

TDD. The coordinator state machine is the heart; the real in-memory bus is the test
substrate (in-process, no mocks). Tests written before implementation:

| Test | Asserts |
|---|---|
| `ready` accumulation | partial readiness does **not** open the gate |
| open condition | last required `ready` -> exactly one `request` to the runner |
| pass path | runner `done` -> `inform` PASSED broadcast; gate re-arms |
| fail path | runner `block`/`disagree` -> fail broadcast **+** `block` routed to owners |
| runner timeout | no runner reply within deadline -> STALLED broadcast + re-arm |
| duplicate ready | same participant twice -> no double-open; last version wins |
| ready before peers | out-of-order arrival still opens once the full set lands |
| re-arm | a second full round after a verdict triggers a second run |

The `OnVerdict` hook makes assertions clean (no log-scraping). `cmd/gatedemo` doubles as
the integration smoke test.

## Demo Narrative (`go run ./cmd/gatedemo`)

```
Round 1 — happy path:
  gateway   --ready----> #gate.checkout   (v=a1b2)        [1/2]
  billing   --ready----> #gate.checkout   (v=c3d4)        [2/2 -> OPEN]
  gatekeeper--request--> runner           {checkout, ...}
  runner    --done-----> gatekeeper       pass
  gatekeeper--inform---> #gate.checkout   "checkout PASSED"

Round 2 — a regression:
  billing   --ready----> #gate.checkout   (v=e5f6)        [1/2]
  gateway   --ready----> #gate.checkout   (v=a1b2)        [2/2 -> OPEN]
  gatekeeper--request--> runner
  runner    --disagree-> gatekeeper       "checkout_test: 402 from billing"
  gatekeeper--inform---> #gate.checkout   "checkout FAILED: ..."
  gatekeeper--block----> billing          "checkout gate failing on your change"
```

## How This Reinforces the Roadmap

- Delivers the **deadline-enforcement** roadmap item (via runner timeout).
- Sets up the **blackboard-topology** item — the coordinator's in-memory readiness state
  is precisely what becomes a durable `gate.<id>` ledger once transport lands.
- The `ServeRunner` seam + plain-data API is the shape the **harness-agnostic CLI** wraps.
- `pkg/gate` is the first real consumer the future **conformance suite** must satisfy.

## Assumptions

- Each service is represented by a long-running process that embeds the Conclave library
  and stays subscribed to the bus. What *drives* that process is out of scope.
- v0.1 runs in a single process (in-memory bus); the design does not yet attempt
  cross-process correctness.
