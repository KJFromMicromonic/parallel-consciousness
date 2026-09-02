# Day-0 Spike — Findings

**Status:** Complete · **Date:** 2026-09-01 · **Amends:**
[2026-09-01-runtime-workspace-poc-design.md](./2026-09-01-runtime-workspace-poc-design.md)

Environment: pi `0.84.4`, provider `openai`, model `gpt-5.5` (subscription auth),
`pkg/bus/sqlite` at `46eafee`. Probe code was throwaway and lives outside the
repository, in the session scratchpad.

## Summary

| # | Question | Answer |
|---|---|---|
| 1 | Can Go drive `pi --mode rpc` to `agent_settled`? | **Yes**, cleanly; event order matches the spec's mapping |
| 2 | Does an injected `steer` change behavior mid-run? | **Yes**, decisively |
| 3 | Does `steer` land while a `bash` call is in flight? | **No — queued, applied only at the next turn boundary** |
| 4 | Does sender filtering hold across processes? | Partly: topic path yes, **direct path no**; the cursor hazard was different than hypothesized |
| 5 | Do real frames exceed 64KB? | **Yes — 65,590 bytes observed** |

Two findings change the design. One finds a bug in shipped code.

## Q1 — Go can drive pi over RPC

Observed sequence for a single tool-using prompt:

```text
response → agent_start → turn_start → tool_execution_start → tool_execution_end
→ turn_end → turn_start → turn_end → agent_end → agent_settled
```

This matches the spec's event mapping without change. `agent_end` immediately
preceded `agent_settled` in every probe, but that is the no-retry case; the
mapping to `agent_settled` remains correct for the general case and stays.

## Q2 — Steer works, and it is emphatic

Prompt: run `ls -la`, then write `original.txt`. A `steer` was injected at the
first `tool_execution_start` redirecting the agent to write `steered.txt`
instead. Result after settle:

```text
original.txt   absent
steered.txt    EXISTS
```

The agent abandoned the original instruction entirely. `queue_update` fired
immediately on injection, giving the control plane a delivery receipt.

## Q3 — Steer does NOT preempt an in-flight tool call

**This is the material design delta.**

Prompt: run `sleep 25 && echo finished`. A `steer` was injected two seconds into
the bash call.

```text
[ 2.0s] tool_execution_start
[ 4.0s] -> STEER injected
[ 4.0s] queue_update  {"steering":["STOP the sleep. …"],"followUp":[]}
[27.0s] tool_execution_end          ← bash ran its full 25s
[27.0s] queue_update  {"steering":[],"followUp":[]}   ← only now drained
[29.4s] agent_settled
```

The steer was accepted without error and queued visibly, but was **applied only
after the tool call completed** — 23 seconds after being sent.

Consequences:

1. **`Steer` is not a preemption primitive.** It is "deliver at the next turn
   boundary." Urgent-message latency is bounded by the remaining duration of
   whatever tool call is in flight — for a coding agent running a test suite,
   potentially minutes.
2. **`abort_bash` is the actual preemption primitive** (an RPC command found
   while probing, not in the original spec). True interruption is
   `abort_bash` → then steer.
3. **A parked agent cannot be reached by steer at all** until its blocking call
   returns. An agent sitting in `pc submit` for the full 5-minute timeout will
   not see a queued steer until then. This *confirms* the hybrid design: the gate
   verdict must arrive as `pc submit`'s exit code, because the push path is
   unavailable in exactly that state.
4. **Aborting a tool destroys work.** A `block` arriving while an agent runs its
   test suite should be queued, not preempted — the agent will read it seconds
   later anyway. Tool abort is reserved for operator stop and budget kill. This
   is a courier policy decision, not a new interface verb.

## Q4 — Sender filtering and the cursor model

The filter in `selectQuery` is:

```sql
WHERE seq > ? AND (to_agent = ? OR (to_topic IN (…) AND from_agent <> ?))
```

Sender filtering applies **only to the topic branch**.

**Q4a — the hypothesized hazard was wrong.** Two `Bus` instances on one file,
both subscribed as `"A"`, each received **all 10** direct messages. The stored
cursor is a *resume point* read once at `Subscribe`; each poller then advances
its own in-memory position. Live subscribers do not steal each other's messages.
The courier and a concurrent `pc submit` can coexist under one agent name.

**Q4b — self-delivery on the direct path is real.** A message published by `B`
addressed to `B` **was delivered to B**. A model that runs
`pc send --to <its own name>` would have its own message steered back at it.
The courier needs a guard dropping messages where `From.Agent` equals its own
identity.

**Q4c — topic filtering works.** `C` did not receive its own broadcast to a
topic it subscribes to.

### Bug found in shipped code

`saveCursor` upserts unconditionally:

```sql
ON CONFLICT(agent) DO UPDATE SET last_seq = excluded.last_seq
```

Two subscribers sharing an agent name share one `cursors` row, and there is no
monotonicity guard. A short-lived subscriber that exits behind a long-lived one
**rewinds the stored cursor**, so the next subscription under that name replays
already-consumed messages. Replaying a `block` would re-steer an agent about an
already-resolved failure.

This is latent today (nothing yet runs two same-named subscribers) and becomes
reachable the moment the courier ships alongside `pc submit`. Fix:

```sql
ON CONFLICT(agent) DO UPDATE SET last_seq = MAX(cursors.last_seq, excluded.last_seq)
```

Tracked as a prerequisite task in the implementation plan, with a regression test
in `pkg/bus/bustest` so both adapters are held to it.

## Q5 — Frames do exceed 64KB

Measured with pi's direct `bash` RPC command, so no model tokens were spent.

| Probe | Result |
|---|---|
| `head -c 400000 /dev/urandom \| base64` | largest frame **65,590 bytes (64.1 KB)** |
| same, `response` payload | `output` capped at 51,200 bytes, `truncated: true`, `fullOutputPath` set |
| `printf 'before\xe2\x80\xa8after'` | **U+2028 survived intact inside one frame** |

Two things follow. The 64KB hazard is **real and trivially reachable** — one
ordinary bash command exceeded `bufio.Scanner`'s 65,536-byte default, and it came
from a streaming `bash_execution_update` chunk, which is *not* subject to the
truncation pi applies to `response` payloads. And Go's `bufio.Reader.ReadBytes('\n')`
is confirmed safe on Unicode separators where Node's `readline` is not.

The spec's framing decision was correct and is now backed by measurement.

## Incidental findings worth keeping

- **`abort_bash`** — cancels an in-flight bash command. The missing preemption
  primitive; folded into `Session.Interrupt`.
- **Direct `bash` RPC command** — runs a shell command with no model turn, and
  its output enters the agent's context on the *next* prompt. A cheap way for the
  control plane to inject observations without spending a turn or interrupting.
  Not used in the PoC, but recorded as an option.
- **`queue_update`** — reports pending steering and follow-up queues verbatim.
  This is a delivery receipt: it lets PC distinguish "message delivered" from
  "message acted on," which matters now that Q3 shows those differ by up to the
  length of a tool call. Added to the event mapping.
- **Go floor.** `go.mod` still declares `go 1.22`. The bump to `go 1.23`
  (`97664d7`) was forced solely by the MCP `go-sdk`, and this slice cuts
  `pc mcp`. Nothing in this slice requires 1.23, so the floor decision can be
  deferred until the MCP skin is actually built.

## Amendments applied to the design spec

1. `Steer` documented as next-turn-boundary delivery, not preemption.
2. `Session.Interrupt` redefined as `abort_bash` → `clear_queue` → `abort`.
3. Courier policy: `IntentBlock` queues via `Steer`; tool abort is reserved for
   operator stop and budget kill.
4. Courier drops messages whose `From.Agent` is its own identity.
5. `queue_update` → `KindQueueChanged` added to the event mapping.
6. `saveCursor` monotonicity fix added as a prerequisite, with a conformance test.
7. Known limitations: urgent-delivery latency is bounded by in-flight tool
   duration; messages to a parked agent wait for the park to end.
8. Framing rationale now cites the measured 65,590-byte frame.
