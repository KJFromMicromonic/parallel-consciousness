# Live-Fire Findings — running `pc` against real coding agents

**Status:** Stages 0 and 1 complete · **Date:** 2026-09-02 · **Follows:** Phase A (PR #1), contract hardening (PR #2)

First time the coordination layer has been driven by real language models rather
than the scripted fake. Two harnesses, two vendors: **pi** (gpt-5.5, OpenAI
subscription) and **Claude Code**. Fixture is a throwaway two-service Go repo
whose spanning test requires `"charged 100 (USD)"` — a format neither service can
satisfy alone.

## Stage 0 — one real agent, one gate

**Result: the shell contract works, with zero prompt revisions.** The model was
given the coordination instructions as a pi skill and was NOT told to read them.
It edited both services, committed, and ran `pc submit --gate currency` on its
own initiative. Verified from the bus log, not its self-report:

```
1|billing     |#gate.currency|ready  |{"gate":"currency","version":"unversioned"}
2|coordinator |integrator    |request|{"gate":"currency","versions":{"billing":"unversioned"}}
3|integrator  |coordinator   |done   |{"detail":"","gate":"currency"}
4|coordinator |#gate.currency|inform |{"gate":"currency","passed":true,"text":"currency PASSED"}
```

Three separate OS processes — coordinator, runner, agent — coordinating through
the SQLite bus.

### F1 (fixed) — `--version` was dead, so verdicts were unattributable

`cmd/pc` fell back to the literal `"unversioned"` when `--version` was absent,
which is the normal case since the documented agent instruction is just
`pc submit --gate <id>`. So every participant reported the same string and
`Verdict.Versions` carried no information at all.

The pc CLI design spec states the resolution order explicitly — `--version` →
`git rev-parse HEAD` in the cwd → error — and the implementation had skipped the
middle step and replaced the error with a constant. **This survived twelve task
reviews, a whole-branch review and a fix wave**, because no test asserted version
content and the fake's version string was equally meaningless. It took one real
run against a real repo to expose. Fixed in `4b81763`; HEAD is the correct answer
even with uncommitted work, because the gate only ever merges commits.

### F2 — agents submit redundantly

The model ran `pc submit` twice for one change, costing two full spanning-test
executions. Reproduced in Stage 1. Harmless on this fixture; on a real suite it
doubles cost and wall-clock per redundant submit. The contract snippet now says
"Do not submit again" on exit 0; whether that suppresses it is untested.

## Stage 1 — two real agents, two vendors, one gate

Each agent owned one service, in its own git worktree, and was told not to touch
the other's. Neither could satisfy the spanning test alone. Contract delivered
per-harness: pi via `--skill`, Claude Code via `CLAUDE.md` — an asymmetry that is
itself a finding about how harness-agnostic this really is.

**Both agents did their half correctly and independently:**

| Agent | Harness | Change | Version on the bus |
|---|---|---|---|
| billing | pi / gpt-5.5 | `charged %d` → `charged %d (%s)` with the currency | `c73d2142` |
| gateway | Claude Code | `Currency: "EUR"` → `"USD"` | `b52180d0` |

Merged, the spanning test passes. **The version fix is confirmed live** — real,
distinct, per-agent SHAs.

### F3 (open, blocking) — cold-start daemon race kills a daemon

**Starting `pc up` and `pc run-gate` simultaneously against a database file that
does not yet exist kills one of them:**

```
open bus: connect sqlite ".../pc.db": database is locked (5) (SQLITE_BUSY)
```

The failure is at `sqlite.Open`'s `PingContext` — connection establishment, not a
write. Reproduced deterministically, and **the victim varies between runs**
(first the coordinator, then the runner), confirming a genuine race rather than a
fixed ordering.

This is NOT the pragma-order flake fixed in PR #2. That one was rare (~1 in 10)
and only under parallel `go test`. This is a first-run failure in the documented
two-terminal setup — the way any real user starts the system. Likely mechanism:
processes racing to create the file and apply `journal_mode=WAL`, which needs an
exclusive lock on a fresh database.

**Consequence observed:** with no coordinator, both agents declared readiness
into a void and their `pc submit` blocked for the full 8-minute submit timeout.

### F4 (open) — a missing coordinator is an 8-minute silent block

An agent has no way to tell "the gate has not run yet" from "there is no gate".
It simply parks until `submit_timeout`. A fast-fail when no coordinator is
observable, or a much shorter default with a clear message, would turn eight
silent minutes into an immediate diagnosis.

### F5 — the contract held under failure, which is the stronger test

Claude Code, blocked by the dead coordinator, diagnosed it unprompted:

> "Someone has to restart the gate runner so it establishes a bus cursor …
> To be explicit about status: by this project's own definition I am **not done**
> — `pc submit` has not exited 0."

It refused to claim success, cited the contract's exit-0 rule back, and correctly
identified the missing component. A model honouring the contract while *failing*
is far better evidence than one honouring it on the happy path.

## Stage 1, re-run — the full loop, two vendors, including the failure branch

With F3 fixed the coordinator stayed up, and the loop ran to convergence. The
currency code the two services had to agree on was supplied by the integration
environment (`EXPECTED_CURRENCY=USD` on the gate command) and was verified absent
from both agents' worktrees, so neither could discover it locally.

```
 1|gateway    |#gate.currency|ready   |version 9c06c1cd   (unchanged — still EUR)
 2|billing    |#gate.currency|ready   |version ae7015b3   (renders the currency)
 3|coordinator|integrator    |request |both versions
 4|integrator |coordinator   |disagree|SPANNING FAILURE ... = "charged 100 (EUR)", want "charged 100 (USD)"
 5|coordinator|#gate.currency|inform  |currency FAILED           13:10:09
 6|coordinator|billing       |block   |with the detail
 7|coordinator|gateway       |block   |with the detail
 9|billing    |#gate.currency|ready   |ae7015b3 (unchanged — correctly concluded its half was fine)
10|gateway    |#gate.currency|ready   |version 8f338e61   ← new commit: "send USD, the currency billing expects"
12|coordinator|integrator    |request |both versions
13|integrator |coordinator   |done    |
14|coordinator|#gate.currency|inform  |currency PASSED           13:10:23
```

**The exit-1 branch works.** Gateway received exit 1, read the detail, learned a
value it could not have known from its own worktree, fixed only its own service,
committed, and re-submitted. Its own log: *"the billing side was already
correct … My local code just needed the fix. I did not change anything in
`billing/`."* — it respected the ownership boundary while fixing its half.

**Two rounds, two vendors, failure routing and convergence: 14 seconds.**

### F6 — agents coordinate without being asked to

Billing sent gateway an unprompted `pc send`, carrying a real diagnosis:

> `currency gate failed after billing now renders charge.Currency:`
> `billing.Accept(gateway.Send()) was "charged 100 (EUR)", want "charged 100 (USD)".`
> `Billing branch committed as ae7015b; gateway appears to still send EUR …`

Nothing in the task or the contract snippet asked for this. The contract only
documents `pc submit`; `pc send` was mentioned nowhere. pi worked out that
another participant needed information it had, named its own commit, and
diagnosed the other service's fault. This is the conversation-layer thesis
arriving on its own, and it is the strongest single result of the exercise.

### F2 revisited — redundant submits cost 25× the loop itself

Billing submitted four times: `13:10:08`, `13:10:21`, `13:10:28`, `13:15:56`.
The last two landed **after** the gate had already passed at `13:10:23`. With
gateway finished, quorum was unreachable, so each blocked for the full
`submit_timeout` before returning exit 2.

**The loop took 14 seconds. The redundant submits took about six minutes** — they
are the entire reason the run exceeded a nine-minute budget. F2 is not cosmetic;
it is the dominant cost in a real run. The contract snippet already says "Do not
submit again" on exit 0 and that did not suppress it, so this needs a mechanism,
not wording: `pc submit` should decline (or return immediately) when the agent's
current version has already been accepted in a resolved round.

## F2 fixed, in two attempts — both of my designs were wrong first

**Attempt 1 (rejected): answer a redundant submit with a direct message to the sender.**
It created an unbounded feedback loop. Couriers subscribe with `nil` topics, so
they forward *direct* messages into the agent's live session; a cached verdict
sent direct was forwarded back into the agent, which resubmitted, which produced
another cached reply. Because the cache path bypassed `resolve`, the round
counter never advanced and nothing bounded it — 2,589 goroutines in 8 seconds.
The implementer stopped and reported rather than editing `courier.go` to make
the instruction work, which was the right call.

**Attempt 2 (rejected): compare only the submitting agent's version.**
Fixed the hang — a live run went from timing out past 560s to converging in
212s — but served a **stale verdict**:

```
 5 coordinator -> #gate.currency [inform] currency FAILED          (round 1, gateway still EUR)
 9 gateway     -> #gate.currency [ready]  version 08cab33c         <- NEW commit: fixed to USD
10 billing     -> #gate.currency [ready]  version acef4043         <- unchanged since round 1
11 coordinator -> #gate.currency [inform] currency FAILED (cached) <- WRONG
```

Billing's own version was unchanged, so the guard answered from cache — but
gateway had already moved. The cached path does not record readiness, so quorum
was never reached and gateway's fix was ignored. Cost: an extra round trip and an
unnecessary billing commit.

**Shipped: sticky invalidation.** Any participant declaring a version that
differs from the remembered round invalidates the cached verdict for that gate,
so a cached answer is only ever served when genuinely nothing has moved.

Teeth-verified by removing the invalidation and reproducing the exact stale log
line. `pkg/gate` is at 19 passing tests.

### F6 revisited — three unprompted peer messages, none of them asked for

The contract snippet documents only `pc submit`. `pc send` is mentioned nowhere
in it. Agents nonetheless used the bus three separate times:

1. **billing -> gateway**, after a failing round: *"currency gate failed after
   billing now renders charge.Currency: … was "charged 100 (EUR)", want
   "charged 100 (USD)". Billing branch committed as ae7015b; gateway appears to
   still send EUR"* — a diagnosis of the other service's fault.
2. **gateway -> billing**, catching a defect in the coordinator: *"Stale
   verdict — gateway already sends USD. Gateway commit 08cab33 changed
   gateway.Send() … The FAIL you read showing EUR was the coordinator's cached
   verdict"* — an agent diagnosing the bug in attempt 2 above and warning its
   peer.
3. **gateway -> billing**, after waiting 7m45s with no peer ever appearing:
   *"gateway is ready on agent/gateway @585bc3c: Charge.Currency is now "USD".
   The currency gate still fails, and the remaining gap is on billing side
   only"* — correct attribution of the remaining work, with its own commit named.

This is the conversation-layer thesis arriving unbidden, and case 2 is an agent
finding a coordinator bug before the author did.

### F4 reinforced — a missing peer is indistinguishable from a slow one

Case 3 above is F4 with a number attached: gateway had no way to learn that
billing was never coming, so it waited **7 minutes 45 seconds** and then
improvised. A "no other participant has been seen for this gate" signal would
turn that into an immediate, accurate escalation. This is now the highest-value
remaining fix.

### A note on live-run reliability

Two of five live runs failed for reasons outside the system: one because I built
the binary from the wrong directory and silently tested a stale build, one
because pi produced no output at all (a smoke test passed immediately
afterwards, so most likely subscription rate-limiting after many runs in a day).
Neither was a defect in `pc`. Worth knowing that live validation against real
harnesses has a meaningful flake rate of its own, and that a run should always
begin by asserting which build is under test.

## What has now been demonstrated

All five items this document originally left open have moved:

- **F3 (cold-start daemon race) is fixed.** Measured 11/20 → 0/20 failing
  starts. `pc up` and `pc run-gate` can now be started simultaneously against a
  database file that does not yet exist.
- **F2 (redundant submits) is fixed**, via sticky invalidation, after two
  rejected designs (a direct-message reply that produced an unbounded feedback
  loop, then a submitter-only version comparison that served a stale cached
  verdict). See "F2 fixed, in two attempts" above.
- **F4 (unacknowledged readiness) is fixed.** `pc submit` now diagnoses a
  missing coordinator in about ten seconds instead of silently blocking for
  the full submit timeout.
- **The exit-1 branch has been exercised, successfully, by a real model.**
  Gateway received a failing verdict, read the detail, learned a value (the
  expected currency) it could not have known from its own worktree, fixed only
  its own service — leaving billing untouched — and re-submitted to a pass.
  See "Stage 1, re-run" above.

## Still open

- **Stage 2 as a deliberate test.** Every peer-to-peer `pc send` observed so
  far (F6, F6 revisited) happened spontaneously, mid-task, without ever being
  designed as an experiment. The conversation-layer thesis has evidence, not a
  test: Stage 2 should set up a scenario that specifically requires unprompted
  peer messaging to succeed, rather than continuing to rely on it showing up
  on its own.
- **The conformance gaps this closes.** Two gaps in `pkg/runtime/runtimetest`
  — no property proving `Steer`/`Follow` do NOT preempt in-flight work, and no
  ctx-cancellation coverage for `Interrupt` and `Wait` — are being closed
  alongside this rewrite.
- **The round fence is a timestamp, not a round id.** `pkg/gate`'s staleness
  check compares heartbeat/round timestamps rather than an explicit round
  identifier, which is weaker than it looks under clock skew or a very fast
  re-run.
- **`ErrLeaseLost` is logged but not acted on.** A holder that receives it
  learns it no longer owns the workspace, but nothing today stops that holder
  from continuing to use the worktree it was just fenced out of.
- **No concurrent-`Acquire` test.** `pkg/workspace`'s exclusivity is enforced
  by the `INSERT ... ON CONFLICT` in SQLite, which should make concurrent
  `Acquire` calls for the same path safe by construction, but there is no test
  that actually races two callers against it to confirm that in practice.
