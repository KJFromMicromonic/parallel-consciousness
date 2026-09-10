# Phase B Part 1 — Observability and Correctness — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a running gate observable from the terminal, and close the round-fence correctness gap plus two smaller deferred items.

**Architecture:** Observability reads the durable log, not the transport — a read-only `History`/`Tail` capability lands on the concrete `*sqlite.Bus`, never on the two-method `bus.Bus` contract. Line formatting is a pure function so format assertions stay away from I/O. The correctness work makes verdicts self-describing (they carry the version map they were computed over), has `Submit` accept only a verdict that includes its own version, and replaces a silently-dropped readiness with a `Nack`.

**Tech Stack:** Go 1.22, `modernc.org/sqlite` (pure Go), `gopkg.in/yaml.v3`, stdlib only otherwise.

**Spec:** `docs/superpowers/specs/2026-09-06-phase-b-design.md`. Read the sections `pc watch`, `Correctness — verdicts carry their versions`, and `The two remaining correctness items` before starting. The live-fire evidence behind them is in `docs/superpowers/specs/2026-09-02-live-fire-findings.md`.

## Global Constraints

- Module path is `github.com/KJFromMicromonic/parallel-consciousness`.
- **Go floor stays `go 1.22`.** Do not bump it. Do not run `go get`; every dependency needed is present.
- **`modernc.org/sqlite` stays pinned at `v1.33.1`.** Never `go get modernc.org/sqlite@latest` — newer versions raise the Go floor.
- **No new module dependencies.** Standard library only, plus what go.mod already has.
- Pure Go; no cgo.
- **Do not widen `bus.Bus`.** It stays exactly `Publish` and `Subscribe`. The in-memory bus has no retention and could only satisfy a history method by lying.
- **`pkg/runtime` must keep importing only the standard library.** `internal/arch/arch_test.go` enforces this and also fails if anything under `pkg/` imports `internal/`.
- `go test ./...` must pass with no network access and no API keys.
- No `time.Sleep` and no retry loops in tests. A bounded `time.After` inside a `select` used as a failure deadline is correct and expected.
- House style: doc comments explain *why*; errors wrapped with `%w`; test helpers call `t.Helper()`.
- Do not weaken or delete any existing assertion. Every current test must keep passing, in particular `TestRunConvergesAfterAFailingRound`, `TestRunStopsAfterTheRoundCap`, `TestSubmitIgnoresAVerdictFromAPreviousRound`, and the F2 cache tests in `pkg/gate`.

### Three hazards this codebase has already been bitten by

Read these before writing code; each cost real time to discover.

1. **The courier forwards direct messages into live agent sessions.** `internal/pcops/courier.go` registers handlers for `IntentBlock` and the six sendable intents (`inform`, `request`, `propose`, `agree`, `disagree`, `done`). It registers **nothing** for `IntentAck` or `IntentNack`, which is exactly why those two are safe for coordinator→agent signalling. An earlier design answered a cached verdict with a direct `IntentInform`; the courier forwarded it into the agent's session, the agent resubmitted, and that produced an unbounded loop — 2,589 goroutines in 8 seconds. **Do not add courier handlers for `Ack`/`Nack`.**
2. **JSON transport flattens maps.** Over `pkg/bus/sqlite` a body travels as JSON, so a `map[string]string` returns as `map[string]any`. `pkg/gate` already carries `versionsFromBody` for this; `internal/pcops` will need its own equivalent (Task 5). Do not assume `map[string]string`.
3. **A new subscriber starts at the log's HEAD, and the durable cursor is keyed by agent name alone.** Anything that needs to *replay* history must not go through `Subscribe`.

---

## File Structure

| File | Responsibility |
|---|---|
| `pkg/bus/sqlite/history.go` (create) | read-only `Record`, `History`, `Tail` over the durable log |
| `pkg/bus/sqlite/history_test.go` (create) | ordering, `fromSeq` filtering, tail delivery, ctx cancellation |
| `internal/pcops/watch.go` (create) | `FormatRecord` (pure) and `Watch` (drives History/Tail) |
| `internal/pcops/watch_test.go` (create) | table-driven format tests; one end-to-end round |
| `cmd/pc/main.go` (modify) | `watch` subcommand, thin skin over `pcops.Watch` |
| `pkg/gate/gate.go` (modify) | `versions` in the verdict broadcast; `Nack` on mid-round readiness |
| `internal/pcops/submit.go` (modify) | version-matched verdict acceptance; `Nack` handling and re-declare |
| `internal/pcops/run.go` (modify) | surface `ErrLeaseLost` as an attributed run failure |
| `pkg/workspace/workspace_test.go` (modify) | concurrent-`Acquire` exclusivity test |

---

### Task 1: Read-only history and tail on the SQLite bus

The transport contract stays two methods wide; this capability belongs to the durable log. `pc watch` needs to see *every* message — including direct blocks and peer `pc send` traffic — and `Subscribe`'s filter (`to_agent = ? OR (to_topic IN (…) AND from_agent <> ?)`) can never show a non-recipient those.

**Files:**
- Create: `pkg/bus/sqlite/history.go`
- Test: `pkg/bus/sqlite/history_test.go`

**Interfaces:**
- Consumes: existing unexported helpers `buildMessage` and the `Bus` fields `db`, `poll`, `batch`, `closed`.
- Produces:
  - `type Record struct { Seq int64; Msg protocol.Message }`
  - `func (b *Bus) History(ctx context.Context, fromSeq int64) ([]Record, error)`
  - `func (b *Bus) Tail(ctx context.Context, fromSeq int64) (<-chan Record, error)`

- [ ] **Step 1: Write the failing test**

Create `pkg/bus/sqlite/history_test.go`:

```go
package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

func newBus(t *testing.T) *sqlite.Bus {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "bus.db"), sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func pubDirect(t *testing.T, b *sqlite.Bus, from, to, text string) {
	t.Helper()
	m := protocol.New(protocol.Address{Agent: from}, protocol.Address{Agent: to},
		protocol.IntentInform, map[string]any{"text": text})
	if err := b.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
}

// History must return every message in seq order, including direct messages
// between two OTHER agents — which Subscribe can never show a non-recipient.
func TestHistoryReturnsEveryMessageInOrder(t *testing.T) {
	b := newBus(t)
	pubDirect(t, b, "billing", "gateway", "first")
	pubDirect(t, b, "gateway", "billing", "second")

	recs, err := b.History(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
	if recs[0].Seq >= recs[1].Seq {
		t.Fatalf("not in seq order: %d then %d", recs[0].Seq, recs[1].Seq)
	}
	if recs[0].Msg.Body["text"] != "first" || recs[1].Msg.Body["text"] != "second" {
		t.Fatalf("wrong order or bodies: %+v", recs)
	}
	if recs[0].Msg.From.Agent != "billing" || recs[0].Msg.To.Agent != "gateway" {
		t.Fatalf("addressing lost: %+v", recs[0].Msg)
	}
}

func TestHistoryHonoursFromSeq(t *testing.T) {
	b := newBus(t)
	pubDirect(t, b, "a", "b", "one")
	pubDirect(t, b, "a", "b", "two")

	all, err := b.History(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	rest, err := b.History(context.Background(), all[0].Seq)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 1 || rest[0].Msg.Body["text"] != "two" {
		t.Fatalf("fromSeq not honoured: %+v", rest)
	}
}

// Tail replays what is already there, then delivers what arrives after.
func TestTailReplaysThenFollows(t *testing.T) {
	b := newBus(t)
	pubDirect(t, b, "a", "b", "before")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := b.Tail(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}

	if got := recvRecord(t, ch); got.Msg.Body["text"] != "before" {
		t.Fatalf("replay: got %v", got.Msg.Body["text"])
	}
	pubDirect(t, b, "a", "b", "after")
	if got := recvRecord(t, ch); got.Msg.Body["text"] != "after" {
		t.Fatalf("follow: got %v", got.Msg.Body["text"])
	}
}

func TestTailStopsWhenContextEnds(t *testing.T) {
	b := newBus(t)
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := b.Tail(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	for range ch { // must drain and close, not block
	}
}

func recvRecord(t *testing.T, ch <-chan sqlite.Record) sqlite.Record {
	t.Helper()
	select {
	case r, ok := <-ch:
		if !ok {
			t.Fatal("tail channel closed early")
		}
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a record")
		return sqlite.Record{}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/bus/sqlite/ -run 'TestHistory|TestTail' -v`
Expected: FAIL to compile — `undefined: sqlite.Record`, `b.History`, `b.Tail`

- [ ] **Step 3: Write the implementation**

Create `pkg/bus/sqlite/history.go`:

```go
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

// Record is one entry of the durable log, with the sequence number that
// orders it. Callers use Seq as a resume point.
type Record struct {
	Seq int64
	Msg protocol.Message
}

// History returns every message after fromSeq, in sequence order.
//
// This deliberately lives on *Bus rather than on the bus.Bus interface.
// Reading history is a property of a durable log, not of a message
// transport: the in-memory bus has no retention and could satisfy such a
// method only by lying. Observability therefore depends on the concrete
// type, and the transport contract stays two methods wide.
//
// Unlike Subscribe, this applies no recipient filter — an observer is the
// recipient of nothing, and the messages worth watching (routed failure
// blocks, agent-to-agent traffic) are precisely the ones Subscribe would
// hide from it.
//
// The result is unbounded on purpose: it is intended for observation over a
// scenario's log, not for a hot path.
func (b *Bus) History(ctx context.Context, fromSeq int64) ([]Record, error) {
	rows, err := b.db.QueryContext(ctx, `
		SELECT seq, id, conversation_id, in_reply_to, from_agent, to_agent,
		       to_topic, intent, body, ts, deadline
		FROM messages WHERE seq > ? ORDER BY seq`, fromSeq)
	if err != nil {
		return nil, fmt.Errorf("read history from %d: %w", fromSeq, err)
	}
	defer rows.Close()

	var out []Record
	for rows.Next() {
		var (
			seq                                                     int64
			id, conv, inReplyTo, fromAgent, toAgent, toTopic        string
			intent, bodyS, ts                                       string
			deadline                                                sql.NullString
		)
		if err := rows.Scan(&seq, &id, &conv, &inReplyTo, &fromAgent, &toAgent,
			&toTopic, &intent, &bodyS, &ts, &deadline); err != nil {
			return nil, fmt.Errorf("scan history row: %w", err)
		}
		m, err := buildMessage(id, conv, inReplyTo, fromAgent, toAgent, toTopic, intent, bodyS, ts, deadline)
		if err != nil {
			return nil, fmt.Errorf("decode seq %d: %w", seq, err)
		}
		out = append(out, Record{Seq: seq, Msg: m})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate history: %w", err)
	}
	return out, nil
}

// Tail replays everything after fromSeq, then delivers new records as they
// land, polling at the bus's configured interval. The channel closes when ctx
// ends or the bus closes.
func (b *Bus) Tail(ctx context.Context, fromSeq int64) (<-chan Record, error) {
	out := make(chan Record, b.batch)
	go func() {
		defer close(out)
		cursor := fromSeq
		t := time.NewTicker(b.poll)
		defer t.Stop()
		for {
			recs, err := b.History(ctx, cursor)
			if err != nil {
				if ctx.Err() == nil {
					b.onErr(fmt.Errorf("tail: %w", err))
				}
				return
			}
			for _, r := range recs {
				select {
				case out <- r:
					cursor = r.Seq
				case <-ctx.Done():
					return
				case <-b.closed:
					return
				}
			}
			select {
			case <-t.C:
			case <-ctx.Done():
				return
			case <-b.closed:
				return
			}
		}
	}()
	return out, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/bus/sqlite/ -count=1`
Expected: PASS, including every pre-existing test.

- [ ] **Step 5: Confirm the boundary still holds**

Run: `go test ./internal/arch/ -count=1`
Expected: PASS — nothing under `pkg/` gained an `internal/` import, and `bus.Bus` was not widened.

- [ ] **Step 6: Commit**

```bash
git add pkg/bus/sqlite/history.go pkg/bus/sqlite/history_test.go
git commit -m "feat(bus/sqlite): read-only History and Tail over the durable log

Observability is a property of the durable log, not of the transport, so
this lands on the concrete *Bus and bus.Bus stays two methods wide. Unlike
Subscribe it applies no recipient filter: an observer is the recipient of
nothing, and routed blocks and peer traffic are exactly what it must see."
```

---

### Task 2: Format one log record as one readable line

A pure function, so format assertions never touch I/O or a database.

**Files:**
- Create: `internal/pcops/watch.go`
- Test: `internal/pcops/watch_test.go`

**Interfaces:**
- Consumes: `sqlite.Record` from Task 1.
- Produces: `func FormatRecord(r sqlite.Record, full bool) string`

- [ ] **Step 1: Write the failing test**

Create `internal/pcops/watch_test.go`:

```go
package pcops_test

import (
	"strings"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

func rec(from, toAgent, toTopic string, intent protocol.Intent, body map[string]any) sqlite.Record {
	m := protocol.New(protocol.Address{Agent: from},
		protocol.Address{Agent: toAgent, Topic: toTopic}, intent, body)
	m.Timestamp = time.Date(2026, 9, 8, 15, 52, 58, 0, time.UTC)
	return sqlite.Record{Seq: 1, Msg: m}
}

func TestFormatRecord(t *testing.T) {
	cases := []struct {
		name string
		in   sqlite.Record
		want []string // substrings that must all appear
	}{
		{
			name: "ready abbreviates the version",
			in:   rec("billing", "", "gate.currency", protocol.IntentReady, map[string]any{"gate": "currency", "version": "acef4043778b966dc1cae9819e282d65f6b95e22"}),
			want: []string{"15:52:58", "billing", "#gate.currency", "ready", "v=acef4043"},
		},
		{
			name: "request lists each participant's version",
			in:   rec("coordinator", "integrator", "", protocol.IntentRequest, map[string]any{"gate": "currency", "versions": map[string]any{"billing": "acef4043778b966dc1cae9819e282d65f6b95e22"}}),
			want: []string{"coordinator", "integrator", "request", "billing=acef4043"},
		},
		{
			name: "inform shows the verdict text",
			in:   rec("coordinator", "", "gate.currency", protocol.IntentInform, map[string]any{"gate": "currency", "passed": false, "text": "currency FAILED: boom"}),
			want: []string{"inform", "currency FAILED: boom"},
		},
		{
			name: "block shows the routed detail",
			in:   rec("coordinator", "billing", "", protocol.IntentBlock, map[string]any{"gate": "currency", "text": "currency gate failing: boom"}),
			want: []string{"billing", "block", "currency gate failing"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := pcops.FormatRecord(c.in, false)
			for _, w := range c.want {
				if !strings.Contains(got, w) {
					t.Errorf("line %q missing %q", got, w)
				}
			}
		})
	}
}

func TestFormatRecordTruncatesUnlessFull(t *testing.T) {
	long := strings.Repeat("x", 400)
	r := rec("integrator", "coordinator", "", protocol.IntentDisagree,
		map[string]any{"gate": "currency", "detail": long})

	short := pcops.FormatRecord(r, false)
	if len(short) > 200 {
		t.Errorf("not truncated: %d chars", len(short))
	}
	if !strings.Contains(short, "…") {
		t.Error("truncation not marked")
	}
	if full := pcops.FormatRecord(r, true); !strings.Contains(full, long) {
		t.Error("--full did not include the whole detail")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/pcops/ -run TestFormatRecord -v`
Expected: FAIL to compile — `undefined: pcops.FormatRecord`

- [ ] **Step 3: Write the implementation**

Create `internal/pcops/watch.go`:

```go
package pcops

import (
	"fmt"
	"sort"
	"strings"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

// maxSummary bounds a formatted line's body summary. Long test output and
// merge conflicts routinely run to thousands of characters, which makes a
// feed unreadable; --full opts out.
const maxSummary = 120

// abbrevVersion shortens a git SHA to the conventional short form. Versions
// are opaque strings by contract, so a shorter one is returned unchanged.
func abbrevVersion(v string) string {
	if len(v) > 8 {
		return v[:8]
	}
	return v
}

// FormatRecord renders one log record as one readable line.
//
// Pure by design: it takes a record and returns a string, so the format can
// be asserted exhaustively in table tests without a database, a bus, or a
// running gate anywhere in sight.
func FormatRecord(r sqlite.Record, full bool) string {
	to := r.Msg.To.Agent
	if to == "" {
		to = "#" + r.Msg.To.Topic
	}
	line := fmt.Sprintf("%s  %-11s → %-15s %-10s %s",
		r.Msg.Timestamp.Format("15:04:05"), r.Msg.From.Agent, to,
		string(r.Msg.Intent), summarise(r.Msg, full))
	return strings.TrimRight(line, " ")
}

// summarise picks the most informative field for each intent.
func summarise(m protocol.Message, full bool) string {
	switch m.Intent {
	case protocol.IntentReady:
		if v, ok := m.Body["version"].(string); ok {
			return "v=" + abbrevVersion(v)
		}
	case protocol.IntentRequest:
		if vs := versionsFromBody(m.Body["versions"]); len(vs) > 0 {
			names := make([]string, 0, len(vs))
			for n := range vs {
				names = append(names, n)
			}
			sort.Strings(names) // stable output; map order is not
			parts := make([]string, 0, len(names))
			for _, n := range names {
				parts = append(parts, n+"="+abbrevVersion(vs[n]))
			}
			return strings.Join(parts, " ")
		}
	case protocol.IntentAck, protocol.IntentNack:
		if out := outstandingFromBody(m.Body["outstanding"]); len(out) > 0 {
			return "waiting on " + strings.Join(out, ", ")
		}
	}
	// Everything else: whichever human-readable field is present.
	for _, k := range []string{"text", "detail"} {
		if s, ok := m.Body[k].(string); ok && s != "" {
			return clip(s, full)
		}
	}
	return ""
}

func clip(s string, full bool) string {
	s = strings.ReplaceAll(strings.TrimSpace(s), "\n", " ")
	if full || len(s) <= maxSummary {
		return s
	}
	return s[:maxSummary] + "…"
}
```

Note: `versionsFromBody` and `outstandingFromBody` are referenced here. `outstandingFromBody` already exists in `internal/pcops/submit.go`. `versionsFromBody` is added in Task 5 — until then, this will not compile, so **add it now** in `internal/pcops/watch.go` and Task 5 will use it rather than redefining it:

```go
// versionsFromBody decodes a participant→version map that has crossed the
// bus. Over pkg/bus/sqlite a body is JSON, so a map[string]string is
// delivered as map[string]any; pkg/gate carries its own copy of this for
// the same reason. Anything that is not a string is skipped rather than
// guessed at.
func versionsFromBody(v any) map[string]string {
	switch m := v.(type) {
	case map[string]string:
		return m
	case map[string]any:
		out := make(map[string]string, len(m))
		for k, raw := range m {
			if s, ok := raw.(string); ok {
				out[k] = s
			}
		}
		return out
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/pcops/ -run TestFormatRecord -v`
Expected: PASS, both tests.

- [ ] **Step 5: Commit**

```bash
git add internal/pcops/watch.go internal/pcops/watch_test.go
git commit -m "feat(pcops): pure FormatRecord for one log line per message

Pure on purpose: the format is asserted in table tests with no database,
bus or gate involved. Also adds versionsFromBody, since a map[string]string
crosses the JSON transport as map[string]any."
```

---

### Task 3: `pcops.Watch` and the `pc watch` subcommand

**Files:**
- Modify: `internal/pcops/watch.go`
- Modify: `internal/pcops/watch_test.go`
- Modify: `cmd/pc/main.go`

**Interfaces:**
- Consumes: `sqlite.History`/`sqlite.Tail` (Task 1), `FormatRecord` (Task 2), `Config`/`LoadConfig`.
- Produces: `func Watch(ctx context.Context, cfg Config, gateID string, full, follow bool, out io.Writer) error`

- [ ] **Step 1: Write the failing test**

Append to `internal/pcops/watch_test.go`:

```go
// Watch must render a real round: readiness, the runner request, the verdict
// and the routed blocks — including the direct messages Subscribe would hide.
func TestWatchRendersACompletedRound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{
		DB:            db,
		GateID:        "g",
		Gate:          pcops.GateDef{Required: []string{"billing"}, Runner: "runner"},
		SubmitTimeout: 20 * time.Second,
	}

	cstop, err := pcops.StartCoordinator(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cstop()

	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	run, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.ServeRunner(run, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: false, Detail: "spanning test failed"}
	})
	go run.Run(ctx)

	if _, err := pcops.Submit(ctx, cfg, "g", "billing", "v1"); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	var buf bytes.Buffer
	if err := pcops.Watch(ctx, cfg, "g", false, false, &buf); err != nil {
		t.Fatalf("Watch: %v", err)
	}
	got := buf.String()
	for _, want := range []string{
		"billing", "ready", "v=v1",
		"runner", "request",
		"disagree",
		"inform", "g FAILED",
		"block",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("watch output missing %q\n--- got ---\n%s", want, got)
		}
	}
}

func TestWatchFiltersByGate(t *testing.T) {
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "bus.db")
	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })

	mine := protocol.New(protocol.Address{Agent: "a"}, protocol.Address{Topic: gate.Topic("mine")},
		protocol.IntentInform, map[string]any{"gate": "mine", "text": "KEEP"})
	other := protocol.New(protocol.Address{Agent: "a"}, protocol.Address{Topic: gate.Topic("other")},
		protocol.IntentInform, map[string]any{"gate": "other", "text": "DROP"})
	for _, m := range []protocol.Message{mine, other} {
		if err := b.Publish(ctx, m); err != nil {
			t.Fatal(err)
		}
	}

	var buf bytes.Buffer
	if err := pcops.Watch(ctx, pcops.Config{DB: db}, "mine", false, false, &buf); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); !strings.Contains(got, "KEEP") || strings.Contains(got, "DROP") {
		t.Fatalf("gate filter wrong:\n%s", got)
	}
}
```

Add these imports to the test file: `bytes`, `context`, `path/filepath`, and the `agent`, `gate`, `sqlite` packages.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/pcops/ -run TestWatch -v`
Expected: FAIL to compile — `undefined: pcops.Watch`

- [ ] **Step 3: Write the implementation**

Append to `internal/pcops/watch.go`:

```go
// Watch renders the gate's activity feed to out: everything already recorded,
// then — when follow is set — everything that arrives afterwards.
//
// It reads the durable log directly rather than subscribing, for two reasons.
// A new subscriber starts at the log's HEAD, so it would see no history at
// all; and Subscribe filters to messages addressed to the subscriber, so an
// observer would miss the routed failure blocks and agent-to-agent traffic
// that are the most useful things to watch.
//
// gateID filters to one gate when non-empty; a message belongs to a gate if
// it rides that gate's topic or names it in its body.
func Watch(ctx context.Context, cfg Config, gateID string, full, follow bool, out io.Writer) error {
	b, err := sqlite.Open(ctx, cfg.DB, sqlite.WithPollInterval(250*time.Millisecond))
	if err != nil {
		return fmt.Errorf("open bus: %w", err)
	}
	defer b.Close()

	write := func(r sqlite.Record) error {
		if !matchesGate(r, gateID) {
			return nil
		}
		_, err := fmt.Fprintln(out, FormatRecord(r, full))
		return err
	}

	if !follow {
		recs, err := b.History(ctx, 0)
		if err != nil {
			return err
		}
		for _, r := range recs {
			if err := write(r); err != nil {
				return err
			}
		}
		return nil
	}

	ch, err := b.Tail(ctx, 0)
	if err != nil {
		return err
	}
	for r := range ch {
		if err := write(r); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func matchesGate(r sqlite.Record, gateID string) bool {
	if gateID == "" {
		return true
	}
	if r.Msg.To.Topic == gate.Topic(gateID) {
		return true
	}
	id, _ := r.Msg.Body["gate"].(string)
	return id == gateID
}
```

Add imports to `internal/pcops/watch.go`: `context`, `io`, `time`, and the `gate` and `sqlite` packages.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/pcops/ -count=1`
Expected: PASS, including every pre-existing test.

- [ ] **Step 5: Wire the subcommand**

In `cmd/pc/main.go`, add `case "watch": os.Exit(cmdWatch(ctx, os.Args[2:]))` to the dispatch alongside `submit`, `send`, `up` and `run-gate`, update the usage string to `pc <submit|send|up|run-gate|watch> [flags]`, and add:

```go
// cmdWatch streams the gate's activity feed. --config is required for the
// same reason as up and run-gate: a feed without a gate definition cannot
// filter, and a coordinator's database alone does not say which gates exist.
func cmdWatch(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	config := fs.String("config", "", "scenario file (required)")
	gateID := fs.String("gate", "", "only show this gate (default: everything)")
	full := fs.Bool("full", false, "do not truncate long details")
	noFollow := fs.Bool("no-follow", false, "print recorded history and exit")
	fs.Parse(args)

	if *config == "" {
		fmt.Fprintln(os.Stderr, "pc watch: --config is required")
		return 2
	}
	cfg, err := pcops.LoadConfig(*config)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	id := *gateID
	if id == "" {
		id = cfg.GateID
	}
	if err := pcops.Watch(ctx, cfg, id, *full, !*noFollow, os.Stdout); err != nil &&
		!errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "pc watch: %v\n", err)
		return 2
	}
	return 0
}
```

- [ ] **Step 6: Verify the CLI**

Run: `go build ./... && go run ./cmd/pc 2>&1 | head -1`
Expected: usage line listing `watch`.
Run: `go test ./cmd/pc/ -count=1`
Expected: PASS, including the pre-existing tests.

- [ ] **Step 7: Commit**

```bash
git add internal/pcops/watch.go internal/pcops/watch_test.go cmd/pc/main.go
git commit -m "feat(pc): pc watch streams the gate's activity feed

Reads the durable log rather than subscribing: a new subscriber starts at
HEAD so it would see no history, and Subscribe's recipient filter would
hide the routed blocks and peer traffic most worth watching. --no-follow
covers the post-mortem case without a second command."
```

---

### Task 4: Verdicts carry the versions they were computed over

**Files:**
- Modify: `pkg/gate/gate.go` (the broadcast inside `resolve`)
- Test: `pkg/gate/gate_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces: the verdict broadcast body gains `"versions"`, a `map[string]string` that crosses the bus as `map[string]any`.

- [ ] **Step 1: Write the failing test**

Add to `pkg/gate/gate_test.go`:

```go
// A verdict must say what it tested. Without this, a participant cannot tell
// whether a broadcast verdict covered its own version — which is what lets a
// dropped readiness silently accept someone else's round.
func TestVerdictBroadcastCarriesTheVersionsItTested(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	b := bus.NewInMemory(64)
	coord, err := agent.New(ctx, b, "coordinator", []string{gate.Topic("g")})
	if err != nil {
		t.Fatal(err)
	}
	c := gate.NewCoordinator(coord)
	c.Register(gate.Spec{ID: "g", Required: []string{"billing"}, Runner: "runner"})
	go coord.Run(ctx)

	run, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.ServeRunner(run, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: true}
	})
	go run.Run(ctx)

	// An observer on the gate topic sees the broadcast.
	obs, err := agent.New(ctx, b, "observer", []string{gate.Topic("g")})
	if err != nil {
		t.Fatal(err)
	}
	seen := make(chan protocol.Message, 4)
	obs.On(protocol.IntentInform, func(_ context.Context, _ *agent.Agent, m protocol.Message) *protocol.Message {
		select {
		case seen <- m:
		default:
		}
		return nil
	})
	go obs.Run(ctx)

	participant, err := agent.New(ctx, b, "billing", []string{gate.Topic("g")})
	if err != nil {
		t.Fatal(err)
	}
	go participant.Run(ctx)
	if err := gate.Ready(ctx, participant, "g", "abc123"); err != nil {
		t.Fatal(err)
	}

	select {
	case m := <-seen:
		vs, ok := m.Body["versions"]
		if !ok {
			t.Fatalf("broadcast has no versions: %+v", m.Body)
		}
		got := map[string]string{}
		switch typed := vs.(type) {
		case map[string]string:
			got = typed
		case map[string]any:
			for k, raw := range typed {
				if s, ok := raw.(string); ok {
					got[k] = s
				}
			}
		}
		if got["billing"] != "abc123" {
			t.Fatalf("versions = %+v, want billing=abc123", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no verdict broadcast observed")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/gate/ -run TestVerdictBroadcastCarries -v`
Expected: FAIL with `broadcast has no versions`

- [ ] **Step 3: Write the implementation**

In `pkg/gate/gate.go`, inside `resolve`, change the broadcast body from:

```go
map[string]any{"text": text, "gate": gateID, "passed": v.Passed},
```

to:

```go
// versions makes the verdict self-describing: it says which participant was
// tested at which version. A participant needs that to tell whether this
// verdict covered its own submission — a gate id and a passed bool cannot.
// Without it, a readiness dropped mid-round would silently accept the
// in-flight round's verdict, one computed without its version at all.
map[string]any{"text": text, "gate": gateID, "passed": v.Passed, "versions": copyMap(v.Versions)},
```

`copyMap` already exists in this file and returns a `map[string]string`; the JSON transport delivers it as `map[string]any`, which consumers must tolerate.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/gate/ -count=1 && go test ./... -count=1`
Expected: PASS everywhere.

- [ ] **Step 5: Commit**

```bash
git add pkg/gate/gate.go pkg/gate/gate_test.go
git commit -m "feat(gate): verdict broadcasts carry the versions they tested

A gate id plus a passed bool cannot tell a participant whether a verdict
covered its own submission. resolve already computes Verdict.Versions; it
was being discarded at exactly the moment it became useful."
```

---

### Task 5: `Submit` accepts only a verdict that includes its own version

This is the fix for the actual defect, so its test reproduces the defect first.

**Files:**
- Modify: `internal/pcops/submit.go`
- Test: `internal/pcops/submit_test.go`

**Interfaces:**
- Consumes: `versionsFromBody` (added in Task 2), the `versions` body field (Task 4).
- Produces: no signature change to `Submit`.

- [ ] **Step 1: Write the failing test**

Add to `internal/pcops/submit_test.go`:

```go
// The defect: a readiness that lands while a round is already in flight is
// dropped by pkg/gate, but Submit would still accept that round's verdict —
// one computed without its version. It must decline it and wait for the round
// that actually included it.
func TestSubmitDeclinesAVerdictThatDidNotIncludeIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{
		DB:            db,
		GateID:        "g",
		Gate:          pcops.GateDef{Required: []string{"billing"}, Runner: "runner"},
		SubmitTimeout: 30 * time.Second,
	}
	cstop, err := pcops.StartCoordinator(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cstop()

	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	run, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.ServeRunner(run, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: true, Versions: versions}
	})
	go run.Run(ctx)

	// A verdict for a DIFFERENT version of billing is already on the log,
	// published exactly as the coordinator would publish it.
	stale := protocol.New(protocol.Address{Agent: "coordinator"},
		protocol.Address{Topic: gate.Topic("g")}, protocol.IntentInform,
		map[string]any{"gate": "g", "passed": true, "text": "g PASSED",
			"versions": map[string]any{"billing": "SOMEONE-ELSES-VERSION"}})
	if err := b.Publish(ctx, stale); err != nil {
		t.Fatal(err)
	}

	v, err := pcops.Submit(ctx, cfg, "g", "billing", "MY-VERSION")
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if v.Versions["billing"] != "MY-VERSION" {
		t.Fatalf("accepted a verdict for %q, want MY-VERSION", v.Versions["billing"])
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/pcops/ -run TestSubmitDeclines -v`
Expected: FAIL — Submit accepts the stale broadcast, and `v.Versions` is empty or holds `SOMEONE-ELSES-VERSION`.

- [ ] **Step 3: Write the implementation**

In `internal/pcops/submit.go`, inside the `IntentInform` handler, after the existing `passed` and `readyAt` checks, add the version guard and carry the versions through:

```go
// A gate id and a passed bool do not identify a round. Accept a verdict
// only when it says it tested THIS agent at exactly the version submitted.
// pkg/gate drops a readiness that arrives while a round is already in
// flight, so without this guard an agent would accept the in-flight
// round's verdict — computed entirely without its version.
versions := versionsFromBody(m.Body["versions"])
if versions[agentName] != version {
	return nil
}
```

and include `Versions: versions` in the `gate.Verdict` sent on the `verdicts` channel.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/pcops/ -count=1 && go test ./... -count=1`
Expected: PASS everywhere, including `TestSubmitIgnoresAVerdictFromAPreviousRound` and the F2 cache tests.

- [ ] **Step 5: Commit**

```bash
git add internal/pcops/submit.go internal/pcops/submit_test.go
git commit -m "fix(pcops): Submit accepts only a verdict that tested its own version

pkg/gate drops a readiness that lands mid-round, so Submit would otherwise
accept a verdict computed without its version at all. The test reproduces
that sequence rather than asserting the fix's shape."
```

---

### Task 6: A dropped readiness gets a `Nack`, and `Submit` re-declares

**Files:**
- Modify: `pkg/gate/gate.go` (`onReady`)
- Modify: `internal/pcops/submit.go`
- Test: `pkg/gate/gate_test.go`, `internal/pcops/submit_test.go`

**Interfaces:**
- Consumes: `outstandingFor` (exists in `pkg/gate`), `outstandingFromBody` (exists in `internal/pcops/submit.go`), `versionsFromBody` (Task 2).
- Produces: a `Nack` reply body `{"gate": <id>, "outstanding": []string}`; no signature changes.

- [ ] **Step 1: Write the failing tests**

Add to `pkg/gate/gate_test.go`:

```go
// A readiness that arrives while a round is in flight is dropped. Silence
// there is indistinguishable from "no coordinator" and from "a peer is never
// coming" — all three looked identical in a live run.
func TestReadinessDroppedMidRoundIsNacked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	b := bus.NewInMemory(64)
	coord, err := agent.New(ctx, b, "coordinator", []string{gate.Topic("g")})
	if err != nil {
		t.Fatal(err)
	}
	c := gate.NewCoordinator(coord)
	c.SetRunnerTimeout(10 * time.Second) // hold the round in flight
	c.Register(gate.Spec{ID: "g", Required: []string{"billing", "gateway"}, Runner: "runner"})
	go coord.Run(ctx)

	// No runner is registered, so once quorum forms the round stays in flight.
	first, err := agent.New(ctx, b, "billing", []string{gate.Topic("g")})
	if err != nil {
		t.Fatal(err)
	}
	go first.Run(ctx)
	second, err := agent.New(ctx, b, "gateway", []string{gate.Topic("g")})
	if err != nil {
		t.Fatal(err)
	}
	nacks := make(chan protocol.Message, 2)
	second.On(protocol.IntentNack, func(_ context.Context, _ *agent.Agent, m protocol.Message) *protocol.Message {
		select {
		case nacks <- m:
		default:
		}
		return nil
	})
	go second.Run(ctx)

	if err := gate.Ready(ctx, first, "g", "v1"); err != nil {
		t.Fatal(err)
	}
	if err := gate.Ready(ctx, second, "g", "v1"); err != nil {
		t.Fatal(err)
	}
	// Quorum has formed and the round is in flight; a further readiness from
	// gateway must now be nacked rather than silently dropped.
	if err := gate.Ready(ctx, second, "g", "v2"); err != nil {
		t.Fatal(err)
	}

	select {
	case m := <-nacks:
		if id, _ := m.Body["gate"].(string); id != "g" {
			t.Fatalf("nack body = %+v, want gate g", m.Body)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("readiness dropped mid-round produced no Nack")
	}
}
```

Add to `internal/pcops/submit_test.go`:

```go
// A Nack proves a coordinator exists just as well as an Ack does. If Submit
// treated it as silence it would report ErrNotAcknowledged and undo F4.
func TestSubmitTreatsANackAsAcknowledgement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{
		DB:            db,
		GateID:        "g",
		Gate:          pcops.GateDef{Required: []string{"billing"}, Runner: "runner"},
		SubmitTimeout: 30 * time.Second,
	}
	cstop, err := pcops.StartCoordinator(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cstop()

	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	run, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.ServeRunner(run, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: true, Versions: versions}
	})
	go run.Run(ctx)

	v, err := pcops.Submit(ctx, cfg, "g", "billing", "v1")
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if !v.Passed {
		t.Fatalf("verdict = %+v", v)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/gate/ -run TestReadinessDroppedMidRound -v`
Expected: FAIL with `readiness dropped mid-round produced no Nack`

- [ ] **Step 3: Implement the Nack in `pkg/gate`**

In `onReady`, the `recorded` variable already distinguishes a recorded readiness from a dropped one. Where `required && !recorded` — that is, a required participant whose readiness was dropped because `gs.inflight` — build a `Nack` reply instead of leaving `reply` nil:

```go
} else if required {
	// The readiness was dropped because a round is already in flight.
	// Saying so turns an indistinguishable silence into a signal: a
	// blocked `pc submit` can otherwise not tell "the gate has not run
	// yet" from "no coordinator is running" from "a peer is never
	// coming". outstandingFor reports who the in-flight round is still
	// waiting on, which is what makes the message actionable.
	//
	// IntentNack for the same reason as the IntentAck above: the courier
	// registers no handler for it, so it reaches pcops.Submit's own
	// subscription and never gets forwarded into a live agent session.
	// Do not add a courier handler for it.
	nack := m.Reply(protocol.Address{Agent: c.a.Name}, protocol.IntentNack, map[string]any{
		"gate":        gateID,
		"outstanding": outstandingFor(gs),
	})
	reply = &nack
}
```

- [ ] **Step 4: Implement Nack handling in `Submit`**

Three additions to `internal/pcops/submit.go`.

**(a) `readyAt` must become race-safe.** It is currently written once before
`go a.Run(ctx)`, which publishes it to the handler goroutine. Re-assigning it
per attempt would be a data race, so guard it:

```go
var (
	readyMu sync.Mutex
	readyAt time.Time
)
readAt := func() time.Time { readyMu.Lock(); defer readyMu.Unlock(); return readyAt }
setReadyAt := func(t time.Time) { readyMu.Lock(); readyAt = t; readyMu.Unlock() }
```

Replace the handler's `m.Timestamp.Before(readyAt)` with `m.Timestamp.Before(readAt())`.

**(b) Two new channels and a Nack handler.** `declined` is the signal that an
in-flight round has resolved: a verdict for this gate that could not name this
agent's version is exactly that proof.

```go
// nacked carries the coordinator's IntentNack: this readiness was dropped
// because a round was already in flight. IntentNack for the same reason as
// IntentAck — the courier registers no handler for it, so it reaches this
// subscription and is never forwarded into a live agent session.
nacked := make(chan []string, 1)
a.On(protocol.IntentNack, func(_ context.Context, _ *agent.Agent, m protocol.Message) *protocol.Message {
	if id, _ := m.Body["gate"].(string); id != gateID {
		return nil
	}
	select {
	case nacked <- outstandingFromBody(m.Body["outstanding"]):
	default:
	}
	return nil
})

// declined fires when a verdict for this gate arrives that did NOT test this
// agent's version. After a Nack that is the proof the in-flight round has
// resolved, which is when re-declaring readiness can succeed.
declined := make(chan struct{}, 1)
```

In the `IntentInform` handler, where the Task 5 version guard rejects a verdict,
signal it before returning:

```go
versions := versionsFromBody(m.Body["versions"])
if versions[agentName] != version {
	select {
	case declined <- struct{}{}:
	default:
	}
	return nil
}
```

**(c) Wrap the wait in an attempt loop.** Replace the single
declare-then-wait sequence with:

```go
// Retry lives here rather than being returned to the caller. A Nack means
// this readiness was dropped, so waiting is futile until the in-flight round
// resolves — but handing that retry to a model is worse: F2 measured six
// minutes of redundant submits against a fourteen-second loop. Keeping it
// inside the tool also keeps the three documented exit codes meaningful
// instead of adding a fourth outcome for an agent to mishandle.
for {
	setReadyAt(time.Now())
	if err := gate.Ready(ctx, a, gateID, version); err != nil {
		return gate.Verdict{}, fmt.Errorf("declare ready: %w", err)
	}

	// Wait to learn a coordinator is there at all. A Nack proves that just
	// as well as an Ack does; treating it as silence would regress F4 into
	// a spurious ErrNotAcknowledged.
	select {
	case outstanding := <-acked:
		if len(outstanding) > 0 {
			fmt.Fprintf(os.Stderr, "pc submit: gate %q waiting on %s\n",
				gateID, strings.Join(outstanding, ", "))
		}
	case <-nacked:
		if !waitForRoundToResolve(ctx, declined, verdicts) {
			return gate.Verdict{}, ErrNoVerdict
		}
		continue
	case v := <-verdicts:
		return v, nil
	case <-time.After(ackDeadline):
		return gate.Verdict{}, fmt.Errorf("%w: %q", ErrNotAcknowledged, gateID)
	case <-ctx.Done():
		return gate.Verdict{}, ErrNoVerdict
	}

	// Acknowledged: wait for the verdict that tested this version.
	select {
	case v := <-verdicts:
		return v, nil
	case <-nacked:
		if !waitForRoundToResolve(ctx, declined, verdicts) {
			return gate.Verdict{}, ErrNoVerdict
		}
		continue
	case <-ctx.Done():
		return gate.Verdict{}, ErrNoVerdict
	}
}
```

with this helper beside `Submit`:

```go
// waitForRoundToResolve blocks until the in-flight round that displaced our
// readiness has resolved, reported by a verdict we declined. It returns false
// when the context ended first. A verdict that DOES match ours can still
// arrive here — a race we win — so it is drained into verdicts' buffer for the
// caller's next select rather than dropped.
func waitForRoundToResolve(ctx context.Context, declined <-chan struct{}, verdicts chan gate.Verdict) bool {
	select {
	case <-declined:
		return true
	case v := <-verdicts:
		select {
		case verdicts <- v:
		default:
		}
		return true
	case <-ctx.Done():
		return false
	}
}
```

Add `sync`, `os` and `strings` to the imports if they are not already present,
and name the existing acknowledgement deadline `ackDeadline` if it is currently
an inline literal.

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./pkg/gate/ ./internal/pcops/ -count=1 && go test ./... -count=1`
Expected: PASS everywhere.

- [ ] **Step 6: Commit**

```bash
git add pkg/gate/gate.go pkg/gate/gate_test.go internal/pcops/submit.go internal/pcops/submit_test.go
git commit -m "feat(gate): Nack a readiness dropped mid-round; Submit re-declares

Silence there was indistinguishable from a missing coordinator and from a
peer that never comes. Submit treats a Nack as acknowledgement (or F4
regresses into a spurious ErrNotAcknowledged) and retries internally,
because models retry redundantly and expensively."
```

---

### Task 7: `ErrLeaseLost` becomes an attributed run failure

**Files:**
- Modify: `internal/pcops/run.go`
- Test: `internal/pcops/run_test.go`

**Interfaces:**
- Consumes: `workspace.ErrLeaseLost` (exists), `startHeartbeat`/`heartbeatLease`.
- Produces: `var ErrLeaseLost = errors.New("pcops: lease lost to another holder")`; `startHeartbeat` gains a channel parameter for reporting loss.

- [ ] **Step 1: Write the failing test**

Add to `internal/pcops/run_test.go`:

```go
// A fenced lease means this run no longer owns its worktree. Continuing to
// work in it is worse than stopping: the heartbeat must stop and the run must
// fail with attribution, the same shape as the session-death path.
func TestRunFailsWhenALeaseIsLost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	repo := twoServiceRepo(t)
	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{
		Repo:   repo,
		DB:     db,
		GateID: "checkout",
		Gate: pcops.GateDef{
			Required: []string{"billing"},
			Runner:   "integrator",
			Run:      "sh check.sh",
		},
		Agents:        []pcops.AgentDef{{Name: "billing", Branch: "agent/billing", Role: "implementer", Task: "x"}},
		Runner:        pcops.AgentDef{Name: "integrator", Branch: "agent/integration"},
		SubmitTimeout: 20 * time.Second,
		Wall:          50 * time.Second,
	}

	// An agent that never submits, so Run stays in its wait while the lease
	// is stolen underneath it.
	r := fake.New(map[string]fake.Script{
		"billing": {OnStart: []fake.Action{fake.Emit{Tool: runtime.ToolUse{Name: "read", Target: "x", Ok: true}}}},
	})

	errs := make(chan error, 1)
	go func() { _, err := pcops.Run(ctx, cfg, r); errs <- err }()

	// Steal billing's lease: a second Manager with a very short TTL reclaims
	// it, which rotates the holder token and fences the original holder.
	stealer, err := workspace.New(ctx, repo, filepath.Join(filepath.Dir(db), "worktrees"), db)
	if err != nil {
		t.Fatal(err)
	}
	defer stealer.Close()
	stealer.SetTTL(1 * time.Nanosecond)
	deadline := time.After(30 * time.Second)
	for {
		if _, err := stealer.Acquire(ctx, "billing", "agent/billing"); err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("never managed to reclaim billing's lease")
		case <-time.After(200 * time.Millisecond):
		}
	}

	select {
	case err := <-errs:
		if !errors.Is(err, pcops.ErrLeaseLost) {
			t.Fatalf("Run err = %v, want ErrLeaseLost", err)
		}
		if !strings.Contains(err.Error(), "billing") {
			t.Fatalf("error does not name the agent: %v", err)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("Run did not fail after its lease was lost")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/pcops/ -run TestRunFailsWhenALeaseIsLost -v`
Expected: FAIL — `undefined: pcops.ErrLeaseLost`, and once defined, Run waits out its wall budget instead.

- [ ] **Step 3: Write the implementation**

In `internal/pcops/run.go`:

Define the sentinel next to `ErrSessionDied`:

```go
// ErrLeaseLost means a lease this run holds was reclaimed by another holder.
// Continuing would mean working in a directory the run no longer owns, so it
// stops and reports which agent's lease was lost — the same attributed-failure
// shape as ErrSessionDied.
var ErrLeaseLost = errors.New("pcops: lease lost to another holder")
```

Give `startHeartbeat` a channel to report a fenced lease on, and have `heartbeatLease` stop ticking when it sees one:

```go
func startHeartbeat(ctx context.Context, l *workspace.Lease, lost chan<- string) (stop func()) {
	hctx, cancel := context.WithCancel(ctx)
	go heartbeatLease(hctx, l, lost)
	return cancel
}
```

Inside `heartbeatLease`'s error branch, distinguish a fenced lease from a transient failure:

```go
if err := l.Heartbeat(ctx); err != nil {
	if errors.Is(err, workspace.ErrLeaseLost) {
		// Not transient: another holder owns this workspace now. Stop
		// heartbeating a lease we do not hold, and tell Run.
		select {
		case lost <- l.Agent:
		default:
		}
		return
	}
	// A transient failure must not kill the run — the next tick may renew.
	log.Printf("pcops: heartbeat for %s: %v", l.Agent, err)
}
```

In `Run`, create `lost := make(chan string, 1+len(cfg.Agents))`, pass it to every `startHeartbeat` call including the runner's, and add a case to the verdict `select`:

```go
case agentName := <-lost:
	return gate.Verdict{GateID: cfg.GateID}, fmt.Errorf("%w: %s", ErrLeaseLost, agentName)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/pcops/ -count=1 && go test ./... -count=1`
Expected: PASS everywhere.

- [ ] **Step 5: Commit**

```bash
git add internal/pcops/run.go internal/pcops/run_test.go
git commit -m "fix(pcops): a lost lease stops the heartbeat and fails the run

A fenced lease was logged and then ignored, so a run would keep working in
a directory it no longer owned until the gate resolved. Now it stops and
reports which agent's lease was lost."
```

---

### Task 8: Concurrent-`Acquire` exclusivity test

Lease exclusivity is the property the whole structural-isolation claim rests on, and it is currently verified only by reading the SQL.

**Files:**
- Modify: `pkg/workspace/workspace_test.go`

**Interfaces:**
- Consumes: `New`, `Acquire`, `ErrLeased`, and the `newRepo` helper already in `pkg/workspace/git_test.go`.
- Produces: nothing.

- [ ] **Step 1: Write the failing test**

Add to `pkg/workspace/workspace_test.go`:

```go
// Exclusivity is decided by a single conditional UPSERT rather than a
// read-then-write in Go, which is what makes it safe under contention. That
// has only ever been verified by reading the SQL; this exercises it.
func TestConcurrentAcquireYieldsExactlyOneWinner(t *testing.T) {
	ctx := context.Background()
	repo := newRepo(t)
	m := newManager(t, repo)

	const contenders = 8
	type result struct {
		lease *Lease
		err   error
	}
	results := make(chan result, contenders)
	start := make(chan struct{})
	for i := 0; i < contenders; i++ {
		go func() {
			<-start // release them together
			l, err := m.Acquire(ctx, "billing", "agent/billing")
			results <- result{l, err}
		}()
	}
	close(start)

	var winners int
	for i := 0; i < contenders; i++ {
		r := <-results
		switch {
		case r.err == nil:
			winners++
		case errors.Is(r.err, ErrLeased):
			// expected for every loser
		default:
			t.Fatalf("unexpected error: %v", r.err)
		}
	}
	if winners != 1 {
		t.Fatalf("%d winners, want exactly 1", winners)
	}
}
```

- [ ] **Step 2: Run test to verify it exercises the property**

Run: `go test ./pkg/workspace/ -run TestConcurrentAcquire -count=5 -v`
Expected: PASS. If it fails, exclusivity is genuinely broken and that is a Critical finding — report it rather than adjusting the test.

- [ ] **Step 3: Run with the race detector**

Run: `go test ./pkg/workspace/ -run TestConcurrentAcquire -race -count=5`
Expected: PASS, no data race.

- [ ] **Step 4: Commit**

```bash
git add pkg/workspace/workspace_test.go
git commit -m "test(workspace): exercise lease exclusivity under contention

Eight goroutines released together contend for one agent's lease; exactly
one wins and the rest get ErrLeased. The property the structural-isolation
claim rests on was previously verified only by reading the SQL."
```

---

## Final verification

Run all of these before considering the plan complete:

- `gofmt -l pkg internal cmd` — empty. (`cmd/sqlitedemo/main.go` is a pre-existing violation from before this work; leave it.)
- `go vet ./...` and `go build ./...` — clean.
- `go test ./... -count=1` — all pass.
- `go test ./... -race -count=1` — clean.
- `go test ./pkg/gate/ ./internal/pcops/ ./pkg/workspace/ -count=5` — deterministic.
- `go run ./cmd/pc` — usage lists `watch`.
