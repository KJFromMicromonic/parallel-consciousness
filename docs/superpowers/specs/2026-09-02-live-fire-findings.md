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

## Still untested

**The exit-1 branch.** Stage 0's model got the task right first time; Stage 1
never reached a verdict because the coordinator died. So no real model has yet
read a failing gate's detail, fixed its own service, and re-submitted — the loop's
central behaviour. Fix F3, re-run Stage 1, and it should follow: the fixture is
already seeded so gateway inherits a wrong `"EUR"`.

## Recommended order

1. **F3** — blocking; nothing else can be tested until two daemons can start.
2. Re-run Stage 1 to exercise exit-1 and convergence.
3. **F4** — makes every future failure diagnosable in seconds.
4. Stage 2 — the mid-task `pc send` communication proof.
5. **F2** — cheap, and it halves gate cost when it bites.
