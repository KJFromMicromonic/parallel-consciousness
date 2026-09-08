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
				if ctx.Err() != nil {
					return
				}
				select {
				case <-b.closed:
					return
				default:
				}
				b.onErr(fmt.Errorf("tail: %w", err))
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
