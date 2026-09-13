# Sticky Readiness — Design Spec

**Status:** proposed, 2026-09-13
**Supersedes nothing.** Amends the round model established in
`docs/superpowers/specs/2026-09-06-phase-b-design.md`.

## Summary

Readiness in `pkg/gate` is currently an *event*: a participant declares it, the
round consumes it, and `resolve` clears the set. This spec makes readiness a
*standing claim about a version* — "my half is ready at X" — which persists
until that participant declares a different one.

The change is small in code and consequential in behaviour: it removes a
deadlock in which a participant whose half is already correct starves the gate
permanently by finishing its work and exiting.

It is paired with a second change that makes verdicts truthful about what was
actually merged, because standing readiness makes the existing gap between
"the version a participant declared" and "the commit the runner merged" far
more likely to bite.

## Motivation — what a live run found

A two-vendor live run (pi as `billing`, Claude Code as `gateway`) failed to
converge. The bus log:

```
19:03:36  billing  → ready v=5592dfd0      (baseline — billing's half was already correct)
19:03:48  gateway  → ready v=5592dfd0
          → quorum → runner runs → FAIL (gateway stamps EUR)
          → resolve clears gs.ready                    gate.go:396
19:04:48  gateway  → ready v=bf38367a      (gateway fixed itself)
19:04:48  coordinator → gateway  ack  waiting on billing
          → billing never returns. One runner request, total.
```

`billing` did nothing wrong. Its half was already correct, it submitted, the
round failed *for gateway's reason*, and its own log reads: *"Billing's half is
ready. The gate is still waiting on/failing in gateway."* It concluded it was
done and exited. The agent contract says *"Re-submit after each fix"* — billing
had no fix to make.

Because `resolve` clears `gs.ready` after every round, `gateway`'s corrected
re-submission could never reach quorum again. **An agent that is already correct
starves the gate by terminating.**

The alternative — instructing agents in the contract to stay alive and
re-submit whenever a peer changes something — pushes retry logic onto the model.
Live-fire finding F2 already measured what that costs: six minutes of redundant
submits against a fourteen-second loop.

## Decisions taken

**Readiness is a standing claim with no expiry.** Rejected: a TTL or round-count
expiry, which reintroduces the deadlock on a slower timescale and makes the
bound a guess; and leaving `pkg/gate` alone in favour of contract wording, for
the F2 reason above.

**Versions become truthful rather than advisory.** The runner reports the SHAs
it actually merged. Rejected: pinning the merge to the declared versions
(`git merge <sha>` instead of `git merge <branch>`), which would be truthful by
construction and is conceptually cleaner — but `pc submit --version` is
documented as "an opaque version string", and making it a hard git ref would
break that contract for anyone passing a label rather than a SHA.

**Rejected: readiness as bare presence.** Dropping version values from
`gs.ready` entirely and letting the runner be the only version authority is a
simpler model, but it breaks the F2 verdict cache (keyed on version per
participant) and `Submit`'s version guard (which must confirm its *own* version
was tested) — the two mechanisms this project has already broken and repaired
once each.

---

## 1. The quorum rule

`resolve` stops clearing `gs.ready` (`gate.go:396`) and instead **updates** it
from the verdict's versions, which after section 2 are the SHAs the runner
merged. A standing claim therefore tracks what was last genuinely tested rather
than what was last asserted.

Quorum logic is untouched:

```go
recorded := required && !gs.inflight              // gate.go:253
full := recorded && len(gs.ready) == len(gs.spec.Required)   // gate.go:257
```

Because `full` still requires a *newly recorded* readiness, no round ever opens
spontaneously. One opens only when a submission completes the set — and the set
now persists between rounds, so `gateway`'s corrected re-submit finds
`billing`'s standing claim waiting.

The update is a **per-key merge, not a wholesale replacement**: each participant
named in the verdict has its standing claim overwritten, and any participant the
verdict does not name keeps the claim it had. Section 2 establishes that the key
sets agree, so in practice every key is covered — but stating the rule means a
verdict that ever named a subset could not silently erase the rest of the
quorum.

Where the runner cannot report versions (a merge that failed before any SHA was
captured), `resolve`'s existing backfill at `gate.go:393` applies. The backfill
copies `gs.ready` into the verdict, so updating `gs.ready` from it is a no-op
and standing claims are left exactly as they were — preserving current behaviour
on that path.

### Consequences

**The version guard stops being tautological.** Today `Submit`'s guard
(`submit.go:100`) compares against a value the coordinator copied from what
`Submit` itself declared, so it can essentially only fail across round
boundaries. With truthful versions, an agent whose branch moved after submitting
sees a verdict naming the SHA that was really merged, declines it, and waits.
That is correct: the gate tested something other than what was claimed. See
section 4 for the new outcome this requires.

**A nacked agent that exits leaves a stale standing claim.** Its readiness sits
at an old version while its branch tip has moved. A later round merges the tip
and reports the tip's SHA, so the verdict remains honest about what *ran*; what
goes stale is the record of who agreed to what. For an agent using `pc submit`,
the Nack-and-re-declare loop from Phase B part 1 self-heals this. It persists
only for an agent that abandons mid-round, which PC cannot detect because it
does not supervise processes.

---

## 2. Truthful verdicts

Today the runner's reported versions are discarded. `internal/pcops/rungate.go`
sets `Versions: versions` on all three of its return paths (lines 57, 61, 63),
and `ServeRunner`'s reply body (`gate.go:54`) carries only `{"gate", "detail"}`.
The coordinator then builds the verdict with no versions at all
(`gate.go:375`) and lets `resolve` backfill from `gs.ready`. Nobody noticed
because the backfill produced identical values; standing readiness breaks that
equivalence.

Four changes, forming one chain:

1. **`mergeAll` reports what it merged.** After each successful
   `git merge --no-edit -q <branch>`, capture `git rev-parse <branch>` — the tip
   that just went in — and return `map[branch]sha` alongside its existing detail
   and error. On a failed merge it reports nothing: the tree is `merge --abort`ed,
   so nothing coherent was tested and there is no honest SHA to name.
   (`rungate.go:91`)

2. **`StartRunner` translates branch → participant.** It already holds `cfg`, so
   it builds the map from `cfg.Agents` directly rather than relying on
   `branchesFromConfig`'s positional ordering (`cmd/pc/main.go:356`). That
   ordering is an implementation detail of a different package, and making it
   load-bearing across the boundary would break silently.

3. **`ServeRunner`'s reply carries the versions.** Body gains a `"versions"`
   field. This is what makes `rungate.go`'s existing assignments meaningful.

4. **The coordinator prefers them.** `onVerdictMsg` decodes
   `protocol.Versions(m.Body["versions"])` onto the Verdict; `resolve`'s existing
   `if v.Versions == nil` backfill then fires only when the runner reported
   nothing.

**The backfill becomes the compatibility path rather than the only path.** A
runner that does not report — an older build, or a third-party implementation of
`ServeRunner`'s contract — gets exactly today's behaviour. This is additive to
the wire protocol, not a breaking change.

**Why the merge must filter to required participants.** An earlier draft of
this section claimed the key sets agree by construction, reasoning that
`Config.validate` requires every `gate.required` name to appear in `agents`.
That is true and it is the wrong inclusion. It gives `required` ⊆ `agents`;
the merge needs `agents` ⊆ `required`, and nothing enforces that —
`internal/pcops/config.go:175` explicitly permits an agent that is not in
`gate.required`, while the runner reports a version for every branch it
merged. So a verdict can and does name a participant the gate does not
require.

Under the old wholesale clear that was harmless: the extra key vanished at the
end of the round. Under standing readiness nothing clears `gs.ready`, so the
extra key is permanent — and quorum is an exact length equality, which can
then never be true again. The gate runs exactly once and starves forever, with
`outstandingFor` still reporting "waiting on nobody" because it iterates
`Required`. Discovered in review by reproduction, not by reading: a three-agent
config with two required participants opens round two before this change and
starves after it.

`resolve` therefore merges only keys in `Spec.Required`, the same filter
`onReady` applies to incoming readiness. A name the filter drops could not have
entered `gs.ready` through the front door either, so the filter loses nothing.

---

## 3. Acknowledgement signals

**`outstandingFor` (`gate.go:328`) needs no change, and its emptiness after the
first full quorum is accurate rather than misleading.** Under standing
readiness, once every participant has submitted once, any subsequent submit
completes the set immediately and opens a round — there is no waiting state
left. The list is therefore populated in exactly the situation where waiting
exists, and empty afterwards because there is genuinely nothing to wait for.

**The Nack's `testing` field (`gate.go:310`) also stays correct untouched.** It
reports `copyMap(gs.ready)`, which under standing readiness includes claims from
participants who did not submit this round. That is right: the round *is*
testing their standing version.

**Add a `"running"` bool to the Ack body.** An empty `outstanding` currently
means two different things — "you completed quorum, a round is starting" and
"nothing to report" — and under standing readiness the first becomes the common
case. Both surfaces render it as silence today: `Submit` prints its stderr line
only when the list is non-empty, and `pc watch`'s `IntentAck` case falls through
to a bare `coordinator → gateway  ack`. `full` is already computed on that line,
so carrying it costs one field and makes the most common acknowledgement legible
in both places.

---

## 4. Interaction with the F2 verdict cache

**Sticky invalidation is a prerequisite, not merely a past repair.** The cache
replays a remembered verdict when the sender's version matches what that round
tested (`gate.go:226-246`). Without sticky invalidation, standing readiness
would be unsound: `gateway` resubmitting an unchanged version would hit the
cache and receive a verdict for a combination that no longer holds, because
`billing`'s standing claim had moved underneath it. Discarding `lastVerdict` the
moment any participant diverges (`gate.go:246`) is exactly what prevents this.

**Truthful verdicts make the cache more accurate.** The check becomes "was this
agent tested at the version it is now declaring", measured against what was
really merged rather than echoed back. An agent resubmitting at its true HEAD
after its branch moved gets a legitimate cache hit, because that SHA genuinely
was tested. And since `resolve` updates `gs.ready` from the same truthful set,
the standing claims and the cache agree by construction.

**No redundant-round hazard exists.** Reaching a nil cache requires someone to
have diverged; the round that followed rewrote the cache to include everyone's
current versions, so a subsequent unchanged resubmit hits it. Mid-flight
resubmits are dropped to a Nack before any of this. No path opens a round
testing a combination that was just tested.

### The new failure mode, and the outcome it requires

An agent that commits *after* submitting will see a verdict naming the SHA
actually merged, not the one it declared, and the guard correctly declines it.
But nothing then wakes it: the post-Ack select (`submit.go:204`) waits only on
`verdicts` and `ctx`, so it burns the full `SubmitTimeout` and returns
`ErrNoVerdict` — a mystery twelve-minute hang, which is the F4 failure class
Phase B spent effort eliminating.

`Submit` gains a distinct sentinel for "the gate tested you at a different
version than you declared", alongside `ErrNoVerdict` and `ErrNotAcknowledged`.
It is actionable — the agent should resubmit at its true HEAD — and without it
this design trades a deadlock for a timeout.

---

## 5. Testing

Three tests carry the design:

**The deadlock test.** Two participants; A submits once and its `Submit`
returns; B fails, fixes, resubmits; the gate runs and passes. Today this hangs
forever. The single-line production change that breaks it is restoring
`gs.ready = make(...)` in `resolve`. **A must genuinely never resubmit** — a
version in which A retries proves nothing.

**The new-sentinel test** must assert `Submit` returns *fast*, well inside
`SubmitTimeout` — not merely that it returns an error. With a generous deadline,
"returns an error eventually" passes against exactly the hanging behaviour the
sentinel exists to prevent. Follow the shape of the existing
`TestSubmitDiagnosesAMissingCoordinatorWellInsideTheSubmitTimeout`.

**The no-redundant-round test** counts runner invocations across a sequence
containing unchanged resubmits. Asserting on a count rather than on a verdict is
what detects a cache that has stopped working, since a broken cache still
produces correct verdicts — just expensively.

Also required: truthful versions reaching the verdict; sticky invalidation still
holding under standing readiness (the F2 scenario replayed in the new world);
back-compat when a runner reports no versions; and the `running` bool pinned in
the gate test and in `watch`'s rendering.

Every test in this repo is subject to the standing question: *what single-line
production change would make this fail?* Nine tests that could not answer it
have been found and repaired across Phase A and B.

## Known limitations

**Standing readiness is in-memory and does not survive a coordinator restart.**
`gateState` is rebuilt empty and `StartCoordinator` subscribes at HEAD, so after
a `pc up` restart every participant must submit once more before quorum can
form — reintroducing the exact deadlock for any agent that has already exited.
The durable log holds those readiness messages, and `sqlite.History` — built in
Phase B part 1 precisely to read the log without `Subscribe`'s recipient filter
— is the mechanism that would let a restarting coordinator rebuild standing
claims. Deliberately not folded in here: it is a separate concern with its own
correctness questions about how far back to replay.

**An agent that abandons mid-round leaves a stale claim about who agreed to
what.** The verdict stays honest about what ran; the attribution goes stale. PC
cannot detect it because it does not supervise processes.

**Unchanged from Phase B:** a gate cannot be deliberately re-run on an unchanged
version (the cache makes this slightly more entrenched); one gate per
coordinator; worktrees bound visibility, not capability; budgets are observed,
not enforced.

**This does nothing about defection.** An agent can still satisfy a weak gate
alone. That is a property of how a fixture's assertions are written — each must
have exactly one owner — and no kernel change substitutes for it.
