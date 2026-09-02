// Package sqlite provides a SQLite-backed bus.Bus adapter: agents in separate
// processes coordinate through a single shared database file. Messages are an
// append-only, seq-ordered log; each agent has a durable read cursor and learns
// of new messages by polling. Pure Go via modernc.org/sqlite (no cgo).
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	msqlite "modernc.org/sqlite"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

var errBusClosed = errors.New("sqlite bus: closed")

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
	closed     chan struct{}
	closeOnce  sync.Once
}

// Open opens (creating if needed) the SQLite database at path and ensures the schema exists.
func Open(ctx context.Context, path string, opts ...Option) (*Bus, error) {
	db, err := OpenDB(ctx, path)
	if err != nil {
		return nil, err
	}
	b := &Bus{
		db:     db,
		poll:   25 * time.Millisecond,
		batch:  256,
		onErr:  func(err error) { log.Printf("sqlite bus: %v", err) },
		closed: make(chan struct{}),
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

// OpenDB is the single place that owns "how this project opens its database
// file". Every package that touches this SQLite file — the bus and the
// workspace lease store alike — must go through it rather than building its
// own DSN, because the two already diverged once: workspace.go copied
// sqlite.go's DSN (including a since-fixed pragma-order bug) and inherited
// its flaw. One helper means one thing to get right.
//
// It builds the DSN with busy_timeout first and synchronous(NORMAL), pings to
// establish the connection, and then — deliberately as a separate step, see
// below — sets WAL explicitly with a bounded, jittered retry.
//
// # Why WAL is not in the DSN
//
// journal_mode(WAL) used to be a third `_pragma` param applied during
// connection establishment, same as busy_timeout and synchronous. That races
// on a COLD (not-yet-existing) database file: setting journal_mode takes
// SQLite's exclusive lock, and — unlike an ordinary write — SQLite does not
// route that lock through the busy handler. It returns SQLITE_BUSY
// immediately instead of retrying, so two processes opening the same fresh
// path at once can both hit it, and raising busy_timeout does not help (a
// throwaway probe measured a 6x longer timeout leaving the failure rate
// unchanged). The same probe reproduced the race 5 times in 15 trials with
// just two processes racing a cold DSN-embedded journal_mode(WAL); a warm
// database (schema and WAL already established), or the same cold DSN with
// journal_mode removed, never failed once. Do NOT "tidy" this back into the
// DSN — that reintroduces the cold-start race this function exists to avoid.
//
// Because the journal mode SQLite settles on is persisted in the database
// file's header, this cost is paid only once per fresh file: whichever
// process gets there first sets it, and every later Open (including from
// other processes) observes "wal" already on the row PRAGMA returns and never
// enters the retry loop.
func OpenDB(ctx context.Context, path string) (*sql.DB, error) {
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %q: %w", path, err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect sqlite %q: %w", path, err)
	}
	if err := ensureWAL(ctx, db); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable WAL on %q: %w", path, err)
	}
	return db, nil
}

// walRetryAttempts, walRetryCap and walRetryBudget bound ensureWAL's retry: a
// handful of attempts, each waiting a short jittered backoff, totalling no
// more than about two seconds before giving up.
const (
	walRetryAttempts = 8
	walRetryCap      = 250 * time.Millisecond
	walRetryBudget   = 2 * time.Second
)

// ensureWAL sets journal_mode=WAL as a query (not a connection-time pragma —
// see OpenDB) and retries on SQLITE_BUSY with a jittered backoff. Jitter
// matters: without it, two processes racing the same cold file back off in
// lockstep and collide again on their very next attempt.
//
// PRAGMA journal_mode returns the mode SQLite actually settled on as a row;
// this reads it and refuses to proceed unless it really is "wal" (SQLite
// silently falls back to a different mode in some configurations, e.g. an
// in-memory or read-only database, and proceeding on such a database would
// leave callers believing they have WAL's concurrent-access guarantees when
// they don't).
func ensureWAL(ctx context.Context, db *sql.DB) error {
	deadline := time.Now().Add(walRetryBudget)
	var lastErr error
	for attempt := 0; attempt < walRetryAttempts; attempt++ {
		mode, err := readJournalMode(ctx, db)
		if err == nil {
			if !strings.EqualFold(mode, "wal") {
				return fmt.Errorf("sqlite reported journal_mode %q, want wal", mode)
			}
			return nil
		}
		if !isSQLiteBusy(err) {
			return fmt.Errorf("set journal_mode=WAL: %w", err)
		}
		lastErr = err
		if attempt == walRetryAttempts-1 || !time.Now().Before(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(walBackoff(attempt)):
		}
	}
	return fmt.Errorf("set journal_mode=WAL: gave up after %d attempts: %w", walRetryAttempts, lastErr)
}

func readJournalMode(ctx context.Context, db *sql.DB) (string, error) {
	var mode string
	err := db.QueryRowContext(ctx, `PRAGMA journal_mode=WAL`).Scan(&mode)
	return mode, err
}

// walBackoff is full-jitter exponential backoff capped at walRetryCap: attempt
// 0 waits up to ~15ms, doubling each attempt until the cap.
func walBackoff(attempt int) time.Duration {
	base := 15 * time.Millisecond << attempt
	if base <= 0 || base > walRetryCap { // <=0 catches overflow from the shift
		base = walRetryCap
	}
	return time.Duration(rand.Int63n(int64(base) + 1))
}

// sqliteBusyCode is SQLITE_BUSY from sqlite3.h. It's hardcoded rather than
// imported from modernc.org/sqlite/lib (a codegen'd implementation package
// not meant as a stable public surface) — the numeric SQLite result codes are
// part of SQLite's own C ABI and have not changed across any released
// version.
const sqliteBusyCode = 5

// isSQLiteBusy reports whether err is (or wraps) SQLITE_BUSY. modernc.org/sqlite
// returns a typed *msqlite.Error whose Code() is the numeric result code; the
// string fallback covers the (unobserved but cheap-to-guard) case where a
// future driver version reports busy without that concrete type.
func isSQLiteBusy(err error) bool {
	var sqlErr *msqlite.Error
	if errors.As(err, &sqlErr) {
		return sqlErr.Code() == sqliteBusyCode
	}
	msg := err.Error()
	return strings.Contains(msg, "SQLITE_BUSY") || strings.Contains(msg, "database is locked")
}

// Close stops all subscription pollers and closes the underlying database.
func (b *Bus) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return b.db.Close()
}

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
			select {
			case <-b.closed:
				return
			default:
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
		case <-b.closed:
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
			seq                                                             int64
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
		case <-b.closed:
			return n, last, errBusClosed
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
//
// The upsert is monotonic (MAX): two subscribers sharing an agent name share one
// cursors row, and a short-lived one exiting behind a long-lived one must never
// rewind the stored position, or the next subscription under that name replays
// already-consumed messages.
func (b *Bus) saveCursor(ctx context.Context, agent string, seq int64) {
	_, err := b.db.ExecContext(ctx,
		`INSERT INTO cursors (agent, last_seq) VALUES (?, ?)
		 ON CONFLICT(agent) DO UPDATE SET last_seq = MAX(cursors.last_seq, excluded.last_seq)`,
		agent, seq)
	if err != nil && ctx.Err() == nil {
		b.onErr(fmt.Errorf("save cursor for %q: %w", agent, err))
	}
}

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
