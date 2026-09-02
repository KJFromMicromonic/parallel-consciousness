package pcops_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime/fake"
)

func TestCourierDeliversAMessageIntoTheSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	dir := t.TempDir()
	r := fake.New(map[string]fake.Script{
		"gateway": {
			OnSteer: func(text string) []fake.Action {
				return []fake.Action{fake.Write{Path: "received.txt", Content: text}}
			},
		},
	})
	sess, err := r.Start(ctx, runtime.Spec{Agent: "gateway", Workdir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close(ctx)

	db := filepath.Join(t.TempDir(), "bus.db")
	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })

	// StartCourier calls agent.New synchronously, which calls bus.Subscribe.
	// Subscribe pins the subscriber's cursor via a blocking initCursor query
	// before it spawns its poller and returns, so once StartCourier has
	// returned with a nil error the courier cannot miss a message published
	// afterward. No sleep is needed here.
	if err := pcops.StartCourier(ctx, b, "gateway", sess, nil); err != nil {
		t.Fatal(err)
	}

	if err := pcops.Send(ctx, pcops.Config{DB: db}, "billing", "gateway", "inform", "amount_minor"); err != nil {
		t.Fatal(err)
	}

	deadline := time.After(10 * time.Second)
	for {
		if body, err := os.ReadFile(filepath.Join(dir, "received.txt")); err == nil {
			if string(body) != "amount_minor" {
				t.Fatalf("received %q", body)
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("courier never delivered the message into the session")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// The sqlite bus filters senders only on the topic path, so a self-addressed
// direct message is delivered. Without a guard the courier would steer an agent
// with its own outbound message.
//
// Proving the drop requires a negative assertion, and a sleep cannot prove
// absence. Instead this test relies on the sqlite bus's per-recipient
// ordering guarantee: it publishes the self-addressed message first, then a
// legitimate message from a different sender, and waits only for the
// legitimate message's effect. Because messages for one recipient are
// delivered and dispatched strictly in publish order, observing the second
// message's effect proves the first has already been handled (and dropped)
// by the time the assertion runs.
func TestCourierIgnoresItsOwnMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	dir := t.TempDir()
	r := fake.New(map[string]fake.Script{
		"billing": {
			// Write a file named after the received text so the test can tell
			// which message (if any) produced an effect.
			OnSteer: func(text string) []fake.Action {
				return []fake.Action{fake.Write{Path: text + ".txt", Content: text}}
			},
		},
	})
	sess, err := r.Start(ctx, runtime.Spec{Agent: "billing", Workdir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close(ctx)

	db := filepath.Join(t.TempDir(), "bus.db")
	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })

	// See TestCourierDeliversAMessageIntoTheSession: StartCourier returning
	// with a nil error already guarantees the courier's cursor is pinned, so
	// no sleep is needed before publishing.
	if err := pcops.StartCourier(ctx, b, "billing", sess, nil); err != nil {
		t.Fatal(err)
	}

	// billing addresses itself. This must be dropped.
	self := protocol.New(protocol.Address{Agent: "billing"}, protocol.Address{Agent: "billing"},
		protocol.IntentInform, map[string]any{"text": "echo"})
	if err := b.Publish(ctx, self); err != nil {
		t.Fatal(err)
	}

	// A legitimate message from a different sender, published second. Its
	// delivery is the sync point: the sqlite bus dispatches messages to one
	// recipient strictly in seq order, so observing this message's effect
	// proves the self-addressed message above was already processed.
	legit := protocol.New(protocol.Address{Agent: "gateway"}, protocol.Address{Agent: "billing"},
		protocol.IntentInform, map[string]any{"text": "legit"})
	if err := b.Publish(ctx, legit); err != nil {
		t.Fatal(err)
	}

	deadline := time.After(10 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(dir, "legit.txt")); err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("courier never delivered the legitimate message into the session")
		case <-time.After(50 * time.Millisecond):
		}
	}

	if _, err := os.Stat(filepath.Join(dir, "echo.txt")); err == nil {
		t.Fatal("courier steered the agent with its own message")
	}
}
