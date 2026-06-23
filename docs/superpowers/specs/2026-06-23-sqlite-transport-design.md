# SQLite-Backed Cross-Process Transport — Design Spec

**Status:** Approved (design phase) · **Date:** 2026-06-23 · **Project:** Parallel Consciousness

## Summary

A second `bus.Bus` adapter, `pkg/bus/sqlite`, that lets agents in **separate OS
processes** coordinate through a single shared SQLite file. Messages are an
append-only, `seq`-ordered log; each agent has a durable read cursor and learns
of new messages by polling. This delivers cross-process transport, and — because
the log is durable and replayable — it simultaneously provides the foundation for
the durable readiness ledger / blackboard topology and fixes the in-memory bus's
"a late or restarted subscriber misses everything" limitation.

The driving goal is to enhance **parallel coding agents**: a real coding agent
(a separate Claude Code / OpenCode process working on one service) joins the
coordination simply by pointing at the shared DB file.

## Motivation

v0.1 runs in a single process on the in-memory bus, so the actual use case —
one coding agent per service, in its own process — cannot yet participate. SQLite
gives cross-process coordination on one machine with zero infrastructure: no
broker, no daemon, just a file. It keeps reads local and sub-millisecond, and its
append-only nature is exactly PROTOCOL.md's "durable state = append-only
coordination topic."

## Principles

1. **Pure Go, no cgo.** Use `modernc.org/sqlite` so `go get`, cross-compilation,
   and static binaries all work with no C toolchain — important for an
   open-source repo people clone. The dependency lives only in `pkg/bus/sqlite`;
   the core (`pkg/bus` interface + in-memory default) and `go run ./cmd/demo`
   stay zero-dependency.
2. **Adapter behind the existing interface.** It implements `bus.Bus`
   (`Publish`/`Subscribe`) unchanged, so `pkg/agent`, `pkg/gate`, and the gate
   demo work over it without modification. The driver is not a one-way door —
   Turso/libSQL can replace it later behind the same interface.
3. **The log is the durable state.** An append-only, `seq`-ordered table is the
   coordination topic; durable per-agent cursors make replay and resume real.
4. **Harness-agnostic by construction.** Joining the bus is "open this file."
   No harness-specific assumptions; this is what the future `pc` CLI wraps.

## Scope

**In scope:**
- `pkg/bus/sqlite`: a `bus.Bus` implementation over a shared SQLite file.
- A shared conformance suite (`pkg/bus/bustest`) run against both in-memory and SQLite.
- A cross-process demo, `cmd/sqlitedemo` (role flags, multi-terminal).
- New dependency: `modernc.org/sqlite` (pure Go).

**Out of scope (deferred):**
- Vector / semantic search over the log (needs libSQL; a later milestone).
- Cross-*machine* operation (Turso embedded replicas / a broker).
- An explicit delivery-acknowledgement protocol (current semantics match in-memory).
- Automatic retention/pruning (a manual `Prune` is provided).
- The harness-agnostic `pc` CLI (this milestone makes it straightforward; it is built separately).

## DB Driver

`modernc.org/sqlite` — pure-Go, CGo-free, drop-in `database/sql` driver. Standard
SQLite covers every transport need (WAL multi-process access, an append-only
table, a monotonic `seq`, cursor polling). The vector features that would require
libSQL/cgo are out of scope here.

## Architecture & Placement

New package `pkg/bus/sqlite`. Constructor:

```go
func Open(ctx context.Context, path string, opts ...Option) (*Bus, error)
```

`*Bus` satisfies `bus.Bus`. A single shared `*sql.DB` is opened with
`journal_mode(WAL)`, `busy_timeout(5000)`, `synchronous(NORMAL)` — the
combination that lets multiple processes share one file safely.

## Schema

Created on `Open` if absent:

```sql
CREATE TABLE messages (
  seq             INTEGER PRIMARY KEY AUTOINCREMENT,  -- canonical total order
  id              TEXT NOT NULL,
  conversation_id TEXT NOT NULL,
  in_reply_to     TEXT,
  from_agent      TEXT,
  to_agent        TEXT,    -- '' when this is a topic broadcast
  to_topic        TEXT,    -- '' when this is a direct message
  intent          TEXT NOT NULL,
  body            TEXT,    -- JSON-encoded map[string]any
  ts              TEXT NOT NULL,  -- RFC3339 UTC (envelope Timestamp)
  deadline        TEXT     -- RFC3339 UTC, nullable (envelope Deadline)
);
CREATE INDEX idx_messages_to_agent ON messages(to_agent, seq);
CREATE INDEX idx_messages_to_topic ON messages(to_topic, seq);

CREATE TABLE cursors (            -- durable per-agent read position
  agent     TEXT PRIMARY KEY,
  last_seq  INTEGER NOT NULL
);
```

### Ordering

`seq` (AUTOINCREMENT) is the canonical, gap-free total order — immune to clock
skew. The wall-clock `ts` is metadata only (and reconstructs the envelope's
`Timestamp`/`Deadline`).

### Routing (reader-side)

`Publish` only INSERTs a row (`to_agent` set for direct, `to_topic` for
broadcast). Each subscriber's poller selects rows matching its own agent name or
its own subscribed topics, so no subscriptions table is needed and topic
membership stays in the subscribing process.

### Identity & cursors

The agent name is the durable identity and the `cursors` primary key (names are
already stable: "billing", "gatekeeper", …). Cursor/replay semantics:
- **Returning agent** (cursor row exists): resumes exactly after its `last_seq`.
- **New agent** (no cursor): defaults to **current HEAD** (`max(seq)`) so it
  isn't drowned in history.
- **`Replay` option:** start a new agent at 0 to consume the full log (e.g., an
  auditor/observer).

## Publish

JSON-encode `Body`; one prepared INSERT; `seq` auto-assigned. Concurrent writes
from different processes serialize (SQLite's single-writer rule); `busy_timeout`
makes contenders wait rather than fail. Returns an error only if contention
exceeds the timeout (caller retries).

## Subscribe

`Subscribe(ctx, agent, topics) (<-chan protocol.Message, error)` resolves the
cursor (resume / HEAD / 0 per above), then spawns a poller returning a buffered
channel. The poller loops on a ticker (default `PollInterval` 25ms):

```sql
SELECT seq, ... FROM messages
WHERE seq > :cursor
  AND ( to_agent = :me
        OR (to_topic IN (:topics) AND from_agent != :me) )
ORDER BY seq LIMIT :batch;
```

Each row → decode → send on the channel → advance cursor; the cursor is
persisted to `cursors` after each delivered batch. On `ctx.Done()` it flushes the
cursor and closes the channel.

**Skip-sender applies only to topic broadcasts** (the `from_agent != :me` is
inside the topic clause); direct messages always reach `to_agent`. This mirrors
the in-memory bus exactly — required, since both pass the same conformance suite.

**Delivery semantics:** a message is "delivered" when handed to the channel
(same as in-memory); the cursor persists right after, so a crash mid-handling can
skip the last in-flight message (at-most-once on crash). An explicit ack protocol
is out of scope.

## Retention

Keep-all by default (the durable ledger / replayable readiness). An
operator-driven `Prune(ctx, beforeSeq int64) error` deletes old rows to reclaim
space; no automatic pruning in v0.1.

## Options

- `PollInterval` (default 25ms) — coordination feels live without busy-spinning.
- `BusyTimeout` (default 5s).
- `BatchSize` (default 256) — max rows per poll.
- `Replay` (default resume-or-HEAD; alternative: from-zero).
- `ErrorHook func(error)` — receives non-fatal poller errors (default: log to stderr).

## Error Handling

- `Open` failures (bad path, schema error) return at construction.
- Poller query errors go to `ErrorHook` and the poll is retried next tick — a
  transient DB lock never kills a subscription.
- A row whose `Body` JSON won't decode is sent to `ErrorHook`, skipped, and the
  cursor advances past it (never wedges the subscription).
- `Publish` returns SQLITE_BUSY-class errors after the busy timeout; caller retries.

## Testing

**Shared conformance suite** — `pkg/bus/bustest` exports
`Run(t *testing.T, newBus func(t *testing.T) bus.Bus)`, asserting the contract
every adapter must honor:
- direct delivery to the addressed agent;
- topic fan-out to multiple subscribers;
- topic broadcast does **not** echo to the sender;
- a direct message addressed to self **is** delivered;
- ordered-per-recipient (publish N → received in `seq`/publish order);
- inbox isolation (an agent only receives its inbox + subscribed topics).

Run against **InMemory** (retrofit, `pkg/bus/bus_test.go`) and **SQLite**
(`pkg/bus/sqlite/sqlite_test.go`). The suite covers only the shared contract;
subscribers subscribe before publishing so both backends deliver.

**SQLite-specific tests:**
- **Cross-process:** two `Bus` instances on the same file — publish on one,
  receive on the other (equivalent to two processes for SQLite).
- **Resume:** read to seq N → close → reopen + re-subscribe → resumes after N
  with no redelivery.
- **Replay-from-zero:** an observer with `Replay` sees full history.
- **Prune** removes old rows without breaking active cursors.

All run with `t.TempDir()` DBs, a short `PollInterval`, and `-race`.

## Demo

`cmd/sqlitedemo` with `--db <path>` and `--role`:
- `--role gatekeeper --db /tmp/team.db` — hosts the gate coordinator + runner
  over the SQLite bus (the `checkout` gate from the existing gate demo).
- `--role service --name billing --version c3d4 --db /tmp/team.db` (and
  `gateway`) — declares readiness.

Run in three terminals, three OS processes coordinate one gate through the shared
file — the exact shape of three coding agents on three services. The README
documents the three commands.

## File Layout

```
pkg/bus/sqlite/sqlite.go       Open, Publish, Subscribe, poller, Prune, Options, schema
pkg/bus/sqlite/sqlite_test.go  adapter-specific tests + bustest.Run(SQLite)
pkg/bus/bustest/bustest.go     shared conformance: Run(t, newBus)
pkg/bus/bus_test.go            bustest.Run against InMemory (retrofit)
cmd/sqlitedemo/main.go         cross-process gate demo (--db, --role, --name, --version)
go.mod / go.sum                + modernc.org/sqlite
```

`pkg/bus/sqlite` imports `pkg/bus` (interface) and `pkg/protocol`; no import cycle.

## How This Advances the Roadmap

- Delivers **cross-process transport** (the headline).
- Lays the **durable readiness ledger / blackboard** foundation (append-only log + replay).
- Forces the **conformance suite** into existence (a second adapter sets the bar).
- Sets up: **Turso embedded replicas** for cross-machine (swap the driver behind
  `bus.Bus`); **vector / semantic memory** (add a table + libSQL); the
  **harness-agnostic `pc` CLI** (a thin wrapper that `Open`s this bus and calls
  `gate.Ready`/`ServeRunner`, so any coding-agent harness joins by pointing at the DB).

## Assumptions & Limitations

- **Single machine.** SQLite is a local file; cross-machine is a later tier (Turso/Kafka).
- **Single-writer.** Writes serialize across processes; fine at coordination
  scale (dozens/sec), not a high-throughput stream. This is the honest boundary
  where a broker takes over.
- **Poll latency.** Up to one `PollInterval` (default 25ms) of delivery latency —
  negligible for coordination, and tunable.
- **Agent name = identity.** Names must be unique and stable per shared DB;
  two live processes using the same name share a cursor (don't do that).
- **At-most-once on crash** mid-handling (matches the in-memory bus).
- **Connection pool is unbounded.** `database/sql`'s pool is not capped. At coordination scale (dozens of msgs/sec) this is fine, but under heavy write contention many connections could contend for SQLite's single write lock. Follow-up: bound the pool — but NOT a naive `SetMaxOpenConns(1)`, because the poller holds its read connection while sending decoded rows on the channel, so a slow consumer could then starve writers. The correct fix is to scan a batch fully (close the rows cursor) before sending on the channel, then cap the pool.
- **First-time agents start at HEAD.** The "fixes the in-memory bus's late-subscriber-misses-everything" benefit applies to *returning* agents (those with a durable cursor). A brand-new agent with no cursor starts at the current head, so a coordinator started AFTER a service has already published readiness will miss it. Start coordinators before services declare readiness, or use `WithReplayFromZero` for an agent that must see history.
