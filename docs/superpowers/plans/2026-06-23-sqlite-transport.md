# SQLite-Backed Cross-Process Transport — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a pure-Go SQLite-backed `bus.Bus` adapter so agents in separate processes coordinate through one shared database file, plus a shared conformance suite and a cross-process demo.

**Architecture:** A new `pkg/bus/sqlite` package implements the existing `bus.Bus` interface over a shared SQLite file: `Publish` INSERTs into an append-only, `seq`-ordered `messages` table; `Subscribe` runs a polling goroutine that delivers rows newer than a durable per-agent cursor. A shared `pkg/bus/bustest` conformance suite validates both the in-memory and SQLite adapters against one contract.

**Tech Stack:** Go 1.22, `modernc.org/sqlite` (pure-Go, no cgo), standard library.

## Global Constraints

- Go version stays `go 1.22`; do not raise it.
- Module path is exactly `github.com/KJFromMicromonic/parallel-consciousness`; use it verbatim in every import.
- New dependency `modernc.org/sqlite` is confined to `pkg/bus/sqlite`, the conformance/demo, and their tests. `pkg/protocol`, `pkg/agent`, the `pkg/bus` interface + in-memory impl stay free of it; `go run ./cmd/demo` must not pull it in.
- The adapter implements the existing `bus.Bus` interface **unchanged** (`Publish(ctx, Message) error`, `Subscribe(ctx, agent, topics) (<-chan Message, error)`). No interface changes.
- DB opened with `journal_mode(WAL)`, `busy_timeout(5000)`, `synchronous(NORMAL)`.
- `seq` (AUTOINCREMENT) is the canonical total order; the skip-sender rule applies **only** to topic broadcasts; the agent name is the cursor identity.
- The only change permitted outside the new packages is the gate `versions` transport-safety fix in Task 8 (`pkg/gate/gate.go`).
- TDD: write the failing test first; commit after each green task.

## File Structure

- `pkg/bus/sqlite/sqlite.go` — **create**: `Open`, `Close`, `Publish`, `Subscribe`, poller, cursors, `Prune`, `Option`s, schema.
- `pkg/bus/sqlite/sqlite_test.go` — **create**: adapter tests + cross-process/resume/replay/prune + runs `bustest.Run`.
- `pkg/bus/bustest/bustest.go` — **create**: shared conformance suite `Run(t, newBus)`.
- `pkg/bus/bus_test.go` — **create**: runs the conformance suite against `InMemory`.
- `pkg/gate/gate.go` — **modify**: make `ServeRunner`'s `versions` decoding transport-safe.
- `cmd/sqlitedemo/main.go` — **create**: cross-process gate demo (`--db`, `--role`, `--name`, `--version`).
- `go.mod` / `go.sum` — **modify**: add `modernc.org/sqlite`.

---

### Task 1: Package skeleton — dependency, `Open`/`Close`, schema, options

**Files:**
- Create: `pkg/bus/sqlite/sqlite.go`
- Test: `pkg/bus/sqlite/sqlite_test.go`
- Modify: `go.mod`, `go.sum`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `sqlite.Open(ctx context.Context, path string, opts ...sqlite.Option) (*sqlite.Bus, error)`
  - `(*sqlite.Bus).Close() error`
  - Options: `WithPollInterval(time.Duration)`, `WithBatchSize(int)`, `WithErrorHook(func(error))`, `WithReplayFromZero()`
  - Unexported fields used by later tasks: `b.db *sql.DB`, `b.poll time.Duration`, `b.batch int`, `b.onErr func(error)`, `b.replayZero bool`

- [ ] **Step 1: Add the dependency**

Run: `go get modernc.org/sqlite@latest`
Expected: `go.mod` gains a `require modernc.org/sqlite ...` line; `go.sum` updated.

- [ ] **Step 2: Write the failing test**

Create `pkg/bus/sqlite/sqlite_test.go`:

```go
package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
)

func TestOpenCreatesSchema(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "bus.db")

	b, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer b.Close()

	// Inspect the file with an independent connection.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tables := map[string]bool{}
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		tables[n] = true
	}
	if !tables["messages"] || !tables["cursors"] {
		t.Fatalf("missing tables, got %v", tables)
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./pkg/bus/sqlite/ -run TestOpenCreatesSchema -v`
Expected: FAIL — `undefined: sqlite.Open` / no Go files in package.

- [ ] **Step 4: Create `pkg/bus/sqlite/sqlite.go`**

```go
// Package sqlite provides a SQLite-backed bus.Bus adapter: agents in separate
// processes coordinate through a single shared database file. Messages are an
// append-only, seq-ordered log; each agent has a durable read cursor and learns
// of new messages by polling. Pure Go via modernc.org/sqlite (no cgo).
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS messages (
  seq             INTEGER PRIMARY KEY AUTOINCREMENT,
  id              TEXT NOT NULL,
  conversation_id TEXT NOT NULL,
  in_reply_to     TEXT,
  from_agent      TEXT,
  to_agent        TEXT,
  to_topic        TEXT,
  intent          TEXT NOT NULL,
  body            TEXT,
  ts              TEXT NOT NULL,
  deadline        TEXT
);
CREATE INDEX IF NOT EXISTS idx_messages_to_agent ON messages(to_agent, seq);
CREATE INDEX IF NOT EXISTS idx_messages_to_topic ON messages(to_topic, seq);
CREATE TABLE IF NOT EXISTS cursors (
  agent    TEXT PRIMARY KEY,
  last_seq INTEGER NOT NULL
);`

// Option configures a Bus.
type Option func(*Bus)

// WithPollInterval sets how often a subscriber polls for new messages (default 25ms).
func WithPollInterval(d time.Duration) Option { return func(b *Bus) { b.poll = d } }

// WithBatchSize sets the max rows read per poll (default 256).
func WithBatchSize(n int) Option { return func(b *Bus) { b.batch = n } }

// WithErrorHook sets the callback for non-fatal poller errors (default: log to stderr).
func WithErrorHook(fn func(error)) Option { return func(b *Bus) { b.onErr = fn } }

// WithReplayFromZero makes a brand-new agent (no stored cursor) start at the
// beginning of the log instead of at the current head.
func WithReplayFromZero() Option { return func(b *Bus) { b.replayZero = true } }

// Bus is a SQLite-backed bus.Bus. Safe for concurrent use, and safe to share a
// file across processes.
type Bus struct {
	db         *sql.DB
	poll       time.Duration
	batch      int
	onErr      func(error)
	replayZero bool
}

// Open opens (creating if needed) the SQLite database at path and ensures the schema exists.
func Open(ctx context.Context, path string, opts ...Option) (*Bus, error) {
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %q: %w", path, err)
	}
	b := &Bus{
		db:    db,
		poll:  25 * time.Millisecond,
		batch: 256,
		onErr: func(err error) { log.Printf("sqlite bus: %v", err) },
	}
	for _, o := range opts {
		o(b)
	}
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	return b, nil
}

// Close closes the underlying database.
func (b *Bus) Close() error { return b.db.Close() }
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./pkg/bus/sqlite/ -run TestOpenCreatesSchema -v && go build ./... && go vet ./...`
Expected: PASS; build and vet clean.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum pkg/bus/sqlite/sqlite.go pkg/bus/sqlite/sqlite_test.go
git commit -m "feat(bus/sqlite): package skeleton, Open/Close, schema, options"
```

---

### Task 2: `Publish`

**Files:**
- Modify: `pkg/bus/sqlite/sqlite.go`
- Test: `pkg/bus/sqlite/sqlite_test.go`

**Interfaces:**
- Consumes: `*sqlite.Bus`, `protocol.Message`, `protocol.New`, `protocol.Address`, `protocol.Intent*`.
- Produces: `(*sqlite.Bus).Publish(ctx context.Context, m protocol.Message) error` — INSERTs one row; `Body` stored as JSON text (empty string when nil); `Timestamp`/`Deadline` stored RFC3339Nano UTC (`deadline` NULL when zero).

- [ ] **Step 1: Write the failing test**

Append to `pkg/bus/sqlite/sqlite_test.go` (and add `"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"` to its imports):

```go
func TestPublishInsertsRow(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "bus.db")
	b, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	msg := protocol.New(
		protocol.Address{Agent: "billing"},
		protocol.Address{Topic: "gate.checkout"},
		protocol.IntentReady,
		map[string]any{"gate": "checkout", "version": "c3d4"},
	)
	if err := b.Publish(ctx, msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var fromAgent, toTopic, intent, body string
	row := db.QueryRow(`SELECT from_agent, to_topic, intent, body FROM messages WHERE id = ?`, msg.ID)
	if err := row.Scan(&fromAgent, &toTopic, &intent, &body); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if fromAgent != "billing" || toTopic != "gate.checkout" || intent != "ready" {
		t.Fatalf("row = from=%q topic=%q intent=%q", fromAgent, toTopic, intent)
	}
	if !strings.Contains(body, `"version":"c3d4"`) {
		t.Fatalf("body = %q", body)
	}
}
```

Add `"strings"` to the test imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/bus/sqlite/ -run TestPublishInsertsRow -v`
Expected: FAIL — `b.Publish undefined`.

- [ ] **Step 3: Add `Publish` to `pkg/bus/sqlite/sqlite.go`**

Add `"encoding/json"` and the protocol import (`"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"`) to the import block, then append:

```go
// Publish appends a message to the log.
func (b *Bus) Publish(ctx context.Context, m protocol.Message) error {
	body := ""
	if m.Body != nil {
		raw, err := json.Marshal(m.Body)
		if err != nil {
			return fmt.Errorf("marshal body: %w", err)
		}
		body = string(raw)
	}
	var deadline any
	if !m.Deadline.IsZero() {
		deadline = m.Deadline.UTC().Format(time.RFC3339Nano)
	}
	_, err := b.db.ExecContext(ctx, `
		INSERT INTO messages
		  (id, conversation_id, in_reply_to, from_agent, to_agent, to_topic, intent, body, ts, deadline)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.ID, m.ConversationID, m.InReplyTo, m.From.Agent, m.To.Agent, m.To.Topic,
		string(m.Intent), body, m.Timestamp.UTC().Format(time.RFC3339Nano), deadline,
	)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/bus/sqlite/ -run TestPublishInsertsRow -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/bus/sqlite/sqlite.go pkg/bus/sqlite/sqlite_test.go
git commit -m "feat(bus/sqlite): Publish appends a message row"
```

---

### Task 3: `Subscribe` + poller (delivery, skip-sender, ordering, isolation, new-agent-at-HEAD)

**Files:**
- Modify: `pkg/bus/sqlite/sqlite.go`
- Test: `pkg/bus/sqlite/sqlite_test.go`

**Interfaces:**
- Consumes: everything from Tasks 1–2.
- Produces: `(*sqlite.Bus).Subscribe(ctx context.Context, agent string, topics []string) (<-chan protocol.Message, error)`. A brand-new agent starts at the current head (`max(seq)`); the poller delivers rows where `to_agent = agent` OR (`to_topic` in `topics` AND `from_agent <> agent`), in `seq` order, every `poll` interval; the channel closes on `ctx.Done()`. The cursor is written to the `cursors` table after each delivered batch (read side wired in Task 4). After this task `var _ bus.Bus = (*Bus)(nil)` holds.

- [ ] **Step 1: Write the failing tests**

Append to `pkg/bus/sqlite/sqlite_test.go` (add imports `"time"`, and `"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus"`):

```go
// recv reads one message or fails after 2s.
func recv(t *testing.T, ch <-chan protocol.Message) protocol.Message {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
		return protocol.Message{}
	}
}

// expectNone asserts nothing arrives within d.
func expectNone(t *testing.T, ch <-chan protocol.Message, d time.Duration) {
	t.Helper()
	select {
	case m := <-ch:
		t.Fatalf("unexpected message: %+v", m)
	case <-time.After(d):
	}
}

func openBus(t *testing.T, ctx context.Context) (*sqlite.Bus, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bus.db")
	b, err := sqlite.Open(ctx, path, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b, path
}

func inform(from, toAgent, text string) protocol.Message {
	return protocol.New(protocol.Address{Agent: from}, protocol.Address{Agent: toAgent},
		protocol.IntentInform, map[string]any{"text": text})
}

func TestImplementsBusInterface(t *testing.T) {
	var _ bus.Bus = (*sqlite.Bus)(nil)
}

func TestSubscribeDirectDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, _ := openBus(t, ctx)

	ch, err := b.Subscribe(ctx, "a", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(ctx, inform("b", "a", "hi")); err != nil {
		t.Fatal(err)
	}
	if m := recv(t, ch); m.Body["text"] != "hi" || m.From.Agent != "b" {
		t.Fatalf("got %+v", m)
	}
}

func TestSubscribeTopicSkipsSender(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, _ := openBus(t, ctx)

	ch, err := b.Subscribe(ctx, "a", []string{"t"})
	if err != nil {
		t.Fatal(err)
	}
	// a broadcasts to its own topic; must not receive its own message.
	topicMsg := protocol.New(protocol.Address{Agent: "a"}, protocol.Address{Topic: "t"},
		protocol.IntentInform, map[string]any{"text": "self"})
	if err := b.Publish(ctx, topicMsg); err != nil {
		t.Fatal(err)
	}
	expectNone(t, ch, 100*time.Millisecond)
}

func TestSubscribeOrderedPerRecipient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, _ := openBus(t, ctx)

	ch, err := b.Subscribe(ctx, "a", nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := b.Publish(ctx, inform("b", "a", "m"+string(rune('0'+i)))); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 10; i++ {
		want := "m" + string(rune('0'+i))
		if m := recv(t, ch); m.Body["text"] != want {
			t.Fatalf("position %d: got %v want %q", i, m.Body["text"], want)
		}
	}
}

func TestSubscribeInboxIsolation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, _ := openBus(t, ctx)

	chA, err := b.Subscribe(ctx, "a", nil)
	if err != nil {
		t.Fatal(err)
	}
	chB, err := b.Subscribe(ctx, "b", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(ctx, inform("c", "a", "for-a")); err != nil {
		t.Fatal(err)
	}
	if m := recv(t, chA); m.Body["text"] != "for-a" {
		t.Fatalf("a got %+v", m)
	}
	expectNone(t, chB, 100*time.Millisecond)
}

func TestNewAgentStartsAtHead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, _ := openBus(t, ctx)

	// Publish before anyone subscribes; a new agent should NOT see history.
	if err := b.Publish(ctx, inform("b", "a", "old")); err != nil {
		t.Fatal(err)
	}
	ch, err := b.Subscribe(ctx, "a", nil)
	if err != nil {
		t.Fatal(err)
	}
	expectNone(t, ch, 100*time.Millisecond)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/bus/sqlite/ -run 'Subscribe|NewAgent|ImplementsBus' -v`
Expected: FAIL — `b.Subscribe undefined` (and the interface assertion won't compile).

- [ ] **Step 3: Implement `Subscribe`, the poller, and helpers**

Add `"strings"` to the import block of `pkg/bus/sqlite/sqlite.go`, then append:

```go
// Subscribe returns a channel of messages addressed to agent (directly or via a
// subscribed topic). A new agent starts at the current head of the log; the
// channel closes when ctx is cancelled.
func (b *Bus) Subscribe(ctx context.Context, agent string, topics []string) (<-chan protocol.Message, error) {
	cursor, err := b.initCursor(ctx, agent)
	if err != nil {
		return nil, err
	}
	out := make(chan protocol.Message, b.batch)
	go b.poll_(ctx, agent, topics, cursor, out)
	return out, nil
}

// initCursor decides where a subscription starts. Until Task 4 wires durable
// resume, a new agent starts at the current head of the log.
func (b *Bus) initCursor(ctx context.Context, agent string) (int64, error) {
	return b.headSeq(ctx)
}

func (b *Bus) headSeq(ctx context.Context) (int64, error) {
	var head sql.NullInt64
	if err := b.db.QueryRowContext(ctx, `SELECT MAX(seq) FROM messages`).Scan(&head); err != nil {
		return 0, fmt.Errorf("head seq: %w", err)
	}
	if head.Valid {
		return head.Int64, nil
	}
	return 0, nil
}

func (b *Bus) poll_(ctx context.Context, agent string, topics []string, cursor int64, out chan<- protocol.Message) {
	defer close(out)
	ticker := time.NewTicker(b.poll)
	defer ticker.Stop()
	for {
		n, next, err := b.deliverBatch(ctx, agent, topics, cursor, out)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			b.onErr(err)
		} else {
			if next > cursor {
				cursor = next
				b.saveCursor(ctx, agent, cursor)
			}
			if n == b.batch {
				continue // full batch: keep draining without waiting a tick
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (b *Bus) deliverBatch(ctx context.Context, agent string, topics []string, cursor int64, out chan<- protocol.Message) (int, int64, error) {
	query, args := selectQuery(agent, topics, cursor, b.batch)
	rows, err := b.db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, cursor, err
	}
	defer rows.Close()

	n := 0
	last := cursor
	for rows.Next() {
		var (
			seq                                                              int64
			id, conv, inReplyTo, fromAgent, toAgent, toTopic, intent, bodyS string
			ts                                                              string
			deadline                                                        sql.NullString
		)
		if err := rows.Scan(&seq, &id, &conv, &inReplyTo, &fromAgent, &toAgent, &toTopic, &intent, &bodyS, &ts, &deadline); err != nil {
			return n, last, err
		}
		m, derr := buildMessage(id, conv, inReplyTo, fromAgent, toAgent, toTopic, intent, bodyS, ts, deadline)
		if derr != nil {
			b.onErr(fmt.Errorf("decode seq %d: %w", seq, derr))
			last = seq // skip the bad row but advance past it
			n++
			continue
		}
		select {
		case out <- m:
			last = seq
			n++
		case <-ctx.Done():
			return n, last, ctx.Err()
		}
	}
	if err := rows.Err(); err != nil {
		return n, last, err
	}
	return n, last, nil
}

func selectQuery(agent string, topics []string, cursor int64, batch int) (string, []any) {
	var sb strings.Builder
	sb.WriteString(`SELECT seq, id, conversation_id, in_reply_to, from_agent, to_agent, to_topic, intent, body, ts, deadline FROM messages WHERE seq > ? AND (to_agent = ?`)
	args := []any{cursor, agent}
	if len(topics) > 0 {
		sb.WriteString(` OR (to_topic IN (`)
		for i, t := range topics {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString("?")
			args = append(args, t)
		}
		sb.WriteString(`) AND from_agent <> ?)`)
		args = append(args, agent)
	}
	sb.WriteString(`) ORDER BY seq LIMIT ?`)
	args = append(args, batch)
	return sb.String(), args
}

func buildMessage(id, conv, inReplyTo, fromAgent, toAgent, toTopic, intent, bodyS, ts string, deadline sql.NullString) (protocol.Message, error) {
	m := protocol.Message{
		ID:             id,
		ConversationID: conv,
		InReplyTo:      inReplyTo,
		From:           protocol.Address{Agent: fromAgent},
		To:             protocol.Address{Agent: toAgent, Topic: toTopic},
		Intent:         protocol.Intent(intent),
	}
	if bodyS != "" {
		var body map[string]any
		if err := json.Unmarshal([]byte(bodyS), &body); err != nil {
			return protocol.Message{}, err
		}
		m.Body = body
	}
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return protocol.Message{}, err
	}
	m.Timestamp = t
	if deadline.Valid && deadline.String != "" {
		d, err := time.Parse(time.RFC3339Nano, deadline.String)
		if err != nil {
			return protocol.Message{}, err
		}
		m.Deadline = d
	}
	return m, nil
}

// saveCursor persists an agent's read position. Errors are non-fatal (reported
// via the error hook); the read side is wired in Task 4.
func (b *Bus) saveCursor(ctx context.Context, agent string, seq int64) {
	_, err := b.db.ExecContext(ctx,
		`INSERT INTO cursors (agent, last_seq) VALUES (?, ?)
		 ON CONFLICT(agent) DO UPDATE SET last_seq = excluded.last_seq`,
		agent, seq)
	if err != nil && ctx.Err() == nil {
		b.onErr(fmt.Errorf("save cursor for %q: %w", agent, err))
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/bus/sqlite/ -race -v && go build ./... && go vet ./...`
Expected: PASS (all sqlite tests); build and vet clean.

- [ ] **Step 5: Commit**

```bash
git add pkg/bus/sqlite/sqlite.go pkg/bus/sqlite/sqlite_test.go
git commit -m "feat(bus/sqlite): Subscribe + polling delivery (skip-sender, ordered, isolated)"
```

---

### Task 4: Durable cursors — resume + replay-from-zero

**Files:**
- Modify: `pkg/bus/sqlite/sqlite.go`
- Test: `pkg/bus/sqlite/sqlite_test.go`

**Interfaces:**
- Consumes: everything from Task 3 (`saveCursor` already writes the cursor).
- Produces: a returning agent (cursor row exists) resumes after `last_seq`; a new agent starts at head unless `WithReplayFromZero()` was set, in which case it starts at 0.

- [ ] **Step 1: Write the failing tests**

Append to `pkg/bus/sqlite/sqlite_test.go`:

```go
func TestResumeFromStoredCursor(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "bus.db")

	open := func() *sqlite.Bus {
		b, err := sqlite.Open(ctx, path, sqlite.WithPollInterval(5*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}

	// First session: subscribe, receive two messages, then stop.
	b1 := open()
	c1, cancel1 := context.WithCancel(ctx)
	ch1, err := b1.Subscribe(c1, "a", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := b1.Publish(ctx, inform("x", "a", "one")); err != nil {
		t.Fatal(err)
	}
	if err := b1.Publish(ctx, inform("x", "a", "two")); err != nil {
		t.Fatal(err)
	}
	if m := recv(t, ch1); m.Body["text"] != "one" {
		t.Fatalf("got %v", m.Body["text"])
	}
	if m := recv(t, ch1); m.Body["text"] != "two" {
		t.Fatalf("got %v", m.Body["text"])
	}
	time.Sleep(80 * time.Millisecond) // let the cursor persist after delivery
	cancel1()
	b1.Close()

	// Publish more while "a" is away.
	b2 := open()
	defer b2.Close()
	if err := b2.Publish(ctx, inform("x", "a", "three")); err != nil {
		t.Fatal(err)
	}

	// Second session resumes after "two" — must see only "three".
	c2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	ch2, err := b2.Subscribe(c2, "a", nil)
	if err != nil {
		t.Fatal(err)
	}
	if m := recv(t, ch2); m.Body["text"] != "three" {
		t.Fatalf("resume delivered %v, want three", m.Body["text"])
	}
}

func TestReplayFromZero(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := filepath.Join(t.TempDir(), "bus.db")
	b, err := sqlite.Open(ctx, path, sqlite.WithPollInterval(5*time.Millisecond), sqlite.WithReplayFromZero())
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	for _, txt := range []string{"h1", "h2", "h3"} {
		if err := b.Publish(ctx, inform("x", "obs", txt)); err != nil {
			t.Fatal(err)
		}
	}
	ch, err := b.Subscribe(ctx, "obs", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"h1", "h2", "h3"} {
		if m := recv(t, ch); m.Body["text"] != want {
			t.Fatalf("got %v want %q", m.Body["text"], want)
		}
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/bus/sqlite/ -run 'Resume|Replay' -v`
Expected: FAIL — `TestResumeFromStoredCursor` re-reads from head and times out waiting for "three" (it instead receives nothing because head skipped it); `TestReplayFromZero` times out (new agent starts at head, sees no history).

- [ ] **Step 3: Replace `initCursor` to honor the stored cursor and the replay option**

In `pkg/bus/sqlite/sqlite.go`, add `"errors"` to the import block and replace the Task-3 `initCursor` with:

```go
// initCursor decides where a subscription starts: a returning agent (cursor row
// present) resumes after its last_seq; a new agent starts at head, or at 0 if
// WithReplayFromZero was set.
func (b *Bus) initCursor(ctx context.Context, agent string) (int64, error) {
	var last int64
	err := b.db.QueryRowContext(ctx, `SELECT last_seq FROM cursors WHERE agent = ?`, agent).Scan(&last)
	switch {
	case err == nil:
		return last, nil
	case errors.Is(err, sql.ErrNoRows):
		if b.replayZero {
			return 0, nil
		}
		return b.headSeq(ctx)
	default:
		return 0, fmt.Errorf("load cursor for %q: %w", agent, err)
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/bus/sqlite/ -race -v`
Expected: PASS (all sqlite tests, including resume/replay).

- [ ] **Step 5: Commit**

```bash
git add pkg/bus/sqlite/sqlite.go pkg/bus/sqlite/sqlite_test.go
git commit -m "feat(bus/sqlite): durable cursors — resume + replay-from-zero"
```

---

### Task 5: `Prune`

**Files:**
- Modify: `pkg/bus/sqlite/sqlite.go`
- Test: `pkg/bus/sqlite/sqlite_test.go`

**Interfaces:**
- Consumes: Tasks 1–4.
- Produces: `(*sqlite.Bus).Prune(ctx context.Context, beforeSeq int64) (int64, error)` — deletes messages with `seq < beforeSeq`, returns rows removed. Does not touch `cursors`.

- [ ] **Step 1: Write the failing test**

Append to `pkg/bus/sqlite/sqlite_test.go`:

```go
func TestPruneRemovesOldRows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, path := openBus(t, ctx)

	for _, txt := range []string{"a", "b", "c", "d"} {
		if err := b.Publish(ctx, inform("x", "y", txt)); err != nil {
			t.Fatal(err)
		}
	}
	removed, err := b.Prune(ctx, 3) // delete seq 1 and 2
	if err != nil {
		t.Fatal(err)
	}
	if removed != 2 {
		t.Fatalf("removed = %d, want 2", removed)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("remaining = %d, want 2", count)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/bus/sqlite/ -run TestPrune -v`
Expected: FAIL — `b.Prune undefined`.

- [ ] **Step 3: Add `Prune`**

Append to `pkg/bus/sqlite/sqlite.go`:

```go
// Prune deletes messages with seq < beforeSeq and returns the number removed.
// Cursors are untouched; callers prune only below positions all subscribers have
// already passed.
func (b *Bus) Prune(ctx context.Context, beforeSeq int64) (int64, error) {
	res, err := b.db.ExecContext(ctx, `DELETE FROM messages WHERE seq < ?`, beforeSeq)
	if err != nil {
		return 0, fmt.Errorf("prune: %w", err)
	}
	return res.RowsAffected()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/bus/sqlite/ -run TestPrune -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/bus/sqlite/sqlite.go pkg/bus/sqlite/sqlite_test.go
git commit -m "feat(bus/sqlite): Prune old messages"
```

---

### Task 6: Shared conformance suite + in-memory retrofit

**Files:**
- Create: `pkg/bus/bustest/bustest.go`
- Create: `pkg/bus/bus_test.go`

**Interfaces:**
- Consumes: `bus.Bus`, `bus.NewInMemory`, `protocol.*`.
- Produces: `bustest.Run(t *testing.T, newBus func(t *testing.T) bus.Bus)` — runs the shared transport contract as subtests. `newBus` returns a fresh empty bus and registers cleanup via `t.Cleanup`.

- [ ] **Step 1: Write the conformance suite (this is the test artifact) and the failing in-memory runner**

Create `pkg/bus/bustest/bustest.go`:

```go
// Package bustest holds the transport conformance suite that every bus.Bus
// adapter must pass. Call Run from an adapter's _test package.
package bustest

import (
	"context"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

// Run executes the conformance suite. newBus must return a fresh, empty bus and
// register any cleanup via t.Cleanup.
func Run(t *testing.T, newBus func(t *testing.T) bus.Bus) {
	t.Run("DirectDelivery", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		b := newBus(t)
		ch := sub(t, b, ctx, "a", nil)
		pub(t, b, ctx, "b", "a", "", "hi")
		if m := recv(t, ch); m.Body["text"] != "hi" || m.From.Agent != "b" {
			t.Fatalf("got %+v", m)
		}
	})

	t.Run("TopicFanout", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		b := newBus(t)
		chA := sub(t, b, ctx, "a", []string{"t"})
		chB := sub(t, b, ctx, "b", []string{"t"})
		pub(t, b, ctx, "c", "", "t", "broadcast")
		if m := recv(t, chA); m.Body["text"] != "broadcast" {
			t.Fatalf("a got %+v", m)
		}
		if m := recv(t, chB); m.Body["text"] != "broadcast" {
			t.Fatalf("b got %+v", m)
		}
	})

	t.Run("TopicSkipsSender", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		b := newBus(t)
		ch := sub(t, b, ctx, "a", []string{"t"})
		pub(t, b, ctx, "a", "", "t", "self")
		expectNone(t, ch, 150*time.Millisecond)
	})

	t.Run("DirectToSelfDelivered", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		b := newBus(t)
		ch := sub(t, b, ctx, "a", nil)
		pub(t, b, ctx, "a", "a", "", "mine")
		if m := recv(t, ch); m.Body["text"] != "mine" {
			t.Fatalf("got %+v", m)
		}
	})

	t.Run("OrderedPerRecipient", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		b := newBus(t)
		ch := sub(t, b, ctx, "a", nil)
		want := []string{"m0", "m1", "m2", "m3", "m4"}
		for _, w := range want {
			pub(t, b, ctx, "b", "a", "", w)
		}
		for i, w := range want {
			if m := recv(t, ch); m.Body["text"] != w {
				t.Fatalf("position %d: got %v want %q", i, m.Body["text"], w)
			}
		}
	})

	t.Run("InboxIsolation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		b := newBus(t)
		chA := sub(t, b, ctx, "a", nil)
		chB := sub(t, b, ctx, "b", nil)
		pub(t, b, ctx, "c", "a", "", "for-a")
		if m := recv(t, chA); m.Body["text"] != "for-a" {
			t.Fatalf("a got %+v", m)
		}
		expectNone(t, chB, 150*time.Millisecond)
	})
}

func sub(t *testing.T, b bus.Bus, ctx context.Context, agent string, topics []string) <-chan protocol.Message {
	t.Helper()
	ch, err := b.Subscribe(ctx, agent, topics)
	if err != nil {
		t.Fatalf("Subscribe(%q): %v", agent, err)
	}
	return ch
}

func pub(t *testing.T, b bus.Bus, ctx context.Context, from, toAgent, toTopic, text string) {
	t.Helper()
	m := protocol.New(protocol.Address{Agent: from}, protocol.Address{Agent: toAgent, Topic: toTopic},
		protocol.IntentInform, map[string]any{"text": text})
	if err := b.Publish(ctx, m); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func recv(t *testing.T, ch <-chan protocol.Message) protocol.Message {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
		return protocol.Message{}
	}
}

func expectNone(t *testing.T, ch <-chan protocol.Message, d time.Duration) {
	t.Helper()
	select {
	case m := <-ch:
		t.Fatalf("unexpected message: %+v", m)
	case <-time.After(d):
	}
}
```

Create `pkg/bus/bus_test.go`:

```go
package bus_test

import (
	"testing"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/bustest"
)

func TestInMemoryConformance(t *testing.T) {
	bustest.Run(t, func(t *testing.T) bus.Bus {
		return bus.NewInMemory(64)
	})
}
```

- [ ] **Step 2: Run to verify it fails (then passes) for in-memory**

Run: `go test ./pkg/bus/ -run TestInMemoryConformance -v`
Expected: it compiles and runs. If any subtest FAILS, that is a real in-memory bug to investigate; the in-memory bus is expected to satisfy the contract, so the target state is PASS. (The "failing first" here is the suite not existing — once written, in-memory should pass.)

- [ ] **Step 3: (No production code change needed.)**

The in-memory bus already implements the contract; this task adds the reusable suite and proves it green. If a subtest fails, fix the genuine bug it exposes before committing.

- [ ] **Step 4: Confirm green**

Run: `go test ./pkg/bus/... -race -v`
Expected: PASS (in-memory conformance + existing sqlite tests).

- [ ] **Step 5: Commit**

```bash
git add pkg/bus/bustest/bustest.go pkg/bus/bus_test.go
git commit -m "test(bus): shared conformance suite + in-memory retrofit"
```

---

### Task 7: SQLite conformance + cross-process test

**Files:**
- Modify: `pkg/bus/sqlite/sqlite_test.go`

**Interfaces:**
- Consumes: `bustest.Run`, `sqlite.Open`, `bus.Bus`.
- Produces: SQLite passes the shared conformance suite; a cross-process test proves two `Bus` instances on one file deliver to each other.

- [ ] **Step 1: Write the tests**

Append to `pkg/bus/sqlite/sqlite_test.go` (add `"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/bustest"` to imports):

```go
func TestSQLiteConformance(t *testing.T) {
	bustest.Run(t, func(t *testing.T) bus.Bus {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		path := filepath.Join(t.TempDir(), "bus.db")
		b, err := sqlite.Open(ctx, path, sqlite.WithPollInterval(5*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { b.Close() })
		return b
	})
}

func TestCrossProcessDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := filepath.Join(t.TempDir(), "bus.db")

	// Two independent Bus instances on the same file == two processes.
	b1, err := sqlite.Open(ctx, path, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer b1.Close()
	b2, err := sqlite.Open(ctx, path, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()

	ch, err := b2.Subscribe(ctx, "a", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := b1.Publish(ctx, inform("b", "a", "cross")); err != nil {
		t.Fatal(err)
	}
	if m := recv(t, ch); m.Body["text"] != "cross" {
		t.Fatalf("cross-process delivered %+v", m)
	}
}
```

- [ ] **Step 2: Run the tests**

Run: `go test ./pkg/bus/sqlite/ -race -run 'Conformance|CrossProcess' -v`
Expected: PASS. A conformance failure here is a real adapter bug — fix it in `sqlite.go` and re-run.

- [ ] **Step 3: Full sweep**

Run: `go test ./... -race`
Expected: all packages PASS.

- [ ] **Step 4: Commit**

```bash
git add pkg/bus/sqlite/sqlite_test.go
git commit -m "test(bus/sqlite): conformance suite + cross-process delivery"
```

---

### Task 8: Make the gate's `versions` decoding transport-safe

**Files:**
- Modify: `pkg/gate/gate.go`
- Test: `pkg/gate/gate_test.go`

**Interfaces:**
- Consumes: existing `gate.ServeRunner`, `gate.Verdict`.
- Produces: `ServeRunner` decodes `body["versions"]` via a helper that accepts both `map[string]string` (in-memory bus) and `map[string]any` (JSON transport). Behavior over the in-memory bus is unchanged.

**Context:** over the SQLite bus the body round-trips through JSON, so the runner request's `versions` arrives as `map[string]any`, not `map[string]string`. Without this fix the runner receives `nil` versions and the cross-process demo's regression round can't trigger.

- [ ] **Step 1: Write the failing test**

Append to `pkg/gate/gate_test.go`:

```go
func TestServeRunnerDecodesJSONVersions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := bus.NewInMemory(8)

	runner, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	var seen map[string]string
	gate.ServeRunner(runner, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		seen = versions
		return gate.Verdict{GateID: gateID, Passed: true}
	})
	go runner.Run(ctx)

	// Simulate a JSON transport: versions arrives as map[string]any.
	m := sendAndCaptureReply(t, ctx, b, "runner", map[string]any{
		"gate":     "checkout",
		"versions": map[string]any{"billing": "e5f6"},
	})
	if m.Intent != protocol.IntentDone {
		t.Fatalf("intent = %q, want done", m.Intent)
	}
	if seen["billing"] != "e5f6" {
		t.Fatalf("versions = %v, want billing=e5f6", seen)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/gate/ -run TestServeRunnerDecodesJSONVersions -v`
Expected: FAIL — `seen["billing"]` is empty because the current `map[string]string` assertion yields `nil` for a `map[string]any` value.

- [ ] **Step 3: Add the helper and use it in `ServeRunner`**

In `pkg/gate/gate.go`, replace the body of `ServeRunner`'s versions extraction. Change this line and its comment:

```go
		// NOTE: in-memory bus only — it passes Body by reference, so versions is
		// a map[string]string. A serializing transport (JSON/Kafka) would deliver
		// map[string]any here; revisit when the transport adapter lands.
		versions, _ := m.Body["versions"].(map[string]string)
```

to:

```go
		// Transport-safe: the in-memory bus passes versions as map[string]string;
		// a JSON transport (e.g. pkg/bus/sqlite) delivers map[string]any.
		versions := versionsFromBody(m.Body["versions"])
```

Then add the helper at the end of `pkg/gate/gate.go`:

```go
// versionsFromBody coerces a wire "versions" value into map[string]string,
// accepting both the in-memory map[string]string and a JSON map[string]any.
func versionsFromBody(v any) map[string]string {
	switch mm := v.(type) {
	case map[string]string:
		return mm
	case map[string]any:
		out := make(map[string]string, len(mm))
		for k, val := range mm {
			if s, ok := val.(string); ok {
				out[k] = s
			}
		}
		return out
	default:
		return nil
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/gate/ -race -v`
Expected: PASS (the new JSON test plus all existing gate tests — the existing `map[string]string` test still works because the helper handles that case).

- [ ] **Step 5: Commit**

```bash
git add pkg/gate/gate.go pkg/gate/gate_test.go
git commit -m "fix(gate): transport-safe versions decoding (map[string]any from JSON)"
```

---

### Task 9: `cmd/sqlitedemo` — cross-process gate demo

**Files:**
- Create: `cmd/sqlitedemo/main.go`

**Interfaces:**
- Consumes: `sqlite.Open`, `agent.New`, `agent.Agent.{Run,Send}`, `gate.{Topic,Spec,Verdict,NewCoordinator,Ready,ServeRunner}`, `protocol.IntentBlock`.
- Produces: a `main` with `--db`, `--role` (`gatekeeper`|`service`), `--name`, `--version`; multiple processes sharing one `--db` coordinate the `checkout` gate.

- [ ] **Step 1: Create `cmd/sqlitedemo/main.go`**

```go
// Command sqlitedemo runs the cross-service test gate over the SQLite bus so
// agents in separate OS processes coordinate through one shared database file.
//
// Terminal 1 (long-running coordinator + runner):
//	go run ./cmd/sqlitedemo --role gatekeeper --db /tmp/team.db
//
// Terminals 2 and 3 (services declaring readiness):
//	go run ./cmd/sqlitedemo --role service --name gateway --version a1b2 --db /tmp/team.db
//	go run ./cmd/sqlitedemo --role service --name billing --version c3d4 --db /tmp/team.db
//
// A billing version of "e5f6" makes the runner fail, demonstrating the gate
// routing a block back to the owner.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

const gateID = "checkout"

func main() {
	log.SetFlags(log.Ltime)
	role := flag.String("role", "", "gatekeeper | service")
	dbPath := flag.String("db", "", "path to the shared SQLite file")
	name := flag.String("name", "", "service agent name (service role)")
	version := flag.String("version", "", "service version token (service role)")
	flag.Parse()

	if *dbPath == "" {
		log.Fatal("--db is required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	b, err := sqlite.Open(ctx, *dbPath)
	if err != nil {
		log.Fatalf("open bus: %v", err)
	}
	defer b.Close()

	switch *role {
	case "gatekeeper":
		runGatekeeper(ctx, b)
	case "service":
		if *name == "" || *version == "" {
			log.Fatal("--name and --version are required for the service role")
		}
		runService(ctx, b, *name, *version)
	default:
		log.Fatalf("unknown --role %q (want gatekeeper|service)", *role)
	}
}

func runGatekeeper(ctx context.Context, b *sqlite.Bus) {
	gk, err := agent.New(ctx, b, "gatekeeper", []string{gate.Topic(gateID)})
	if err != nil {
		log.Fatalf("gatekeeper: %v", err)
	}
	coord := gate.NewCoordinator(gk)
	coord.Register(gate.Spec{ID: gateID, Required: []string{"billing", "gateway"}, Runner: "runner"})
	coord.OnVerdict(func(v gate.Verdict) {
		if v.Passed {
			log.Printf("  ✓ gate %q PASSED  versions=%v", v.GateID, v.Versions)
		} else {
			log.Printf("  ✗ gate %q FAILED: %s", v.GateID, v.Detail)
		}
	})

	runner, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		log.Fatalf("runner: %v", err)
	}
	gate.ServeRunner(runner, func(ctx context.Context, id string, versions map[string]string) gate.Verdict {
		if versions["billing"] == "e5f6" {
			return gate.Verdict{GateID: id, Passed: false, Detail: "checkout_test: 402 from billing"}
		}
		return gate.Verdict{GateID: id, Passed: true}
	})

	go gk.Run(ctx)
	go runner.Run(ctx)

	log.Printf("gatekeeper + runner up; waiting for readiness on gate %q (Ctrl-C to stop)", gateID)
	<-ctx.Done()
}

func runService(ctx context.Context, b *sqlite.Bus, name, version string) {
	a, err := agent.New(ctx, b, name, []string{gate.Topic(gateID)})
	if err != nil {
		log.Fatalf("service %s: %v", name, err)
	}
	a.On(protocol.IntentBlock, func(ctx context.Context, ag *agent.Agent, m protocol.Message) *protocol.Message {
		log.Printf("  [%s] gate is failing on my change — will investigate", name)
		return nil
	})
	go a.Run(ctx)

	if err := gate.Ready(ctx, a, gateID, version); err != nil {
		log.Fatalf("declare ready: %v", err)
	}
	log.Printf("[%s] declared ready at %q; watching for the verdict (Ctrl-C to stop)", name, version)

	// Stay up long enough to observe the verdict/block, then exit.
	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second):
	}
}
```

- [ ] **Step 2: Build and vet**

Run: `go build ./... && go vet ./...`
Expected: clean, exit 0.

- [ ] **Step 3: Smoke-test the cross-process flow (pass round)**

Run:
```bash
DB=$(mktemp -u /tmp/pc-XXXX.db)
go run ./cmd/sqlitedemo --role gatekeeper --db "$DB" > /tmp/pc-gk.log 2>&1 &
GK=$!
sleep 2
go run ./cmd/sqlitedemo --role service --name gateway --version a1b2 --db "$DB"
go run ./cmd/sqlitedemo --role service --name billing --version c3d4 --db "$DB"
sleep 2
kill "$GK" 2>/dev/null
echo "----- gatekeeper log -----"
cat /tmp/pc-gk.log
```
Expected: the gatekeeper log shows the request to the runner and `✓ gate "checkout" PASSED`. (Re-run with `--name billing --version e5f6` to see `✗ ... FAILED` and the `[billing] … will investigate` line.)

- [ ] **Step 4: Full sweep**

Run: `go test ./... -race && go vet ./...`
Expected: all PASS, vet clean.

- [ ] **Step 5: Commit**

```bash
git add cmd/sqlitedemo/main.go
git commit -m "feat(demo): cross-process gate demo over the SQLite bus"
```

---

## Self-Review

**1. Spec coverage:**
- `pkg/bus/sqlite` adapter implementing `bus.Bus` → Tasks 1–5. ✓
- Pure-Go `modernc.org/sqlite`, confined to the subpackage → Task 1 (import only there). ✓
- WAL/busy_timeout/synchronous DSN → Task 1. ✓
- `seq` order, reader-side routing, skip-sender-on-topics-only → Task 3 (`selectQuery`). ✓
- Identity + resume-or-HEAD + replay → Tasks 3–4. ✓
- Publish/JSON body/deadline → Task 2. ✓
- Poller, delivery semantics, error hook (decode-skip, query-retry) → Task 3. ✓
- Retention via manual `Prune` → Task 5. ✓
- Conformance suite run against both backends → Tasks 6–7. ✓
- Cross-process test → Task 7. ✓
- JSON `versions` transport-safety (the spec's deferred item, now due for the demo) → Task 8. ✓
- Role-based multi-terminal demo → Task 9. ✓
- Options (PollInterval, BatchSize, ErrorHook, ReplayFromZero) → Task 1. ✓ (BusyTimeout is fixed in the DSN at 5000ms per the spec; not a runtime option — consistent with "busy_timeout(5000)".)

**2. Placeholder scan:** No TBD/TODO/"add error handling". Every code step has complete code; the conformance helpers (`recv`/`expectNone`/`sub`/`pub`) are defined once in `bustest` and once in the sqlite test package (different packages — not duplication across one package). Task 6's "failing first" is the suite-not-existing, explicitly noted.

**3. Type consistency:** `Open(ctx,string,...Option)(*Bus,error)`, `Publish(ctx,Message)error`, `Subscribe(ctx,string,[]string)(<-chan Message,error)`, `Prune(ctx,int64)(int64,error)`, `WithPollInterval/WithBatchSize/WithErrorHook/WithReplayFromZero`, internal `initCursor`/`headSeq`/`poll_`/`deliverBatch`/`selectQuery`/`buildMessage`/`saveCursor`, and `versionsFromBody(any)map[string]string` are used identically across tasks. `initCursor` is introduced in Task 3 (head-only) and replaced in Task 4 (resume + replay) — the only signature-stable evolution, and its sole caller (`Subscribe`) is unchanged.
