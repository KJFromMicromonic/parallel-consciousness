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
