package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
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
