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
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

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
			seq                                                              int64
			id, conv, inReplyTo, fromAgent, toAgent, toTopic, intent, bodyS string
			ts                                                               string
			deadline                                                         sql.NullString
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
func (b *Bus) saveCursor(ctx context.Context, agent string, seq int64) {
	_, err := b.db.ExecContext(ctx,
		`INSERT INTO cursors (agent, last_seq) VALUES (?, ?)
		 ON CONFLICT(agent) DO UPDATE SET last_seq = excluded.last_seq`,
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
