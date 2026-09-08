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
