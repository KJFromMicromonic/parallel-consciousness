// Package sqlite provides a SQLite-backed bus.Bus adapter: agents in separate
// processes coordinate through a single shared database file. Messages are an
// append-only, seq-ordered log; each agent has a durable read cursor and learns
// of new messages by polling. Pure Go via modernc.org/sqlite (no cgo).
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	_ "modernc.org/sqlite"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
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
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect sqlite %q: %w", path, err)
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
