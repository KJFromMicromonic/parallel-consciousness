package gate_test

import (
	"context"
	"testing"
	"time"

	"github.com/yourname/conclave/pkg/agent"
	"github.com/yourname/conclave/pkg/bus"
	"github.com/yourname/conclave/pkg/gate"
	"github.com/yourname/conclave/pkg/protocol"
)

// recvMsg reads one message or fails the test after 2s.
func recvMsg(t *testing.T, ch chan protocol.Message) protocol.Message {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
		return protocol.Message{}
	}
}

func TestTopic(t *testing.T) {
	if got := gate.Topic("checkout"); got != "gate.checkout" {
		t.Fatalf("Topic = %q, want %q", got, "gate.checkout")
	}
}

func TestReadyBroadcastsToGateTopic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := bus.NewInMemory(8)

	// Observer subscribed to the gate topic captures the readiness signal.
	obs, err := agent.New(ctx, b, "obs", []string{gate.Topic("checkout")})
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan protocol.Message, 1)
	obs.On(protocol.IntentReady, func(ctx context.Context, a *agent.Agent, m protocol.Message) *protocol.Message {
		got <- m
		return nil
	})
	go obs.Run(ctx)

	// Sender only publishes; it needs no Run loop.
	sender, err := agent.New(ctx, b, "billing", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := gate.Ready(ctx, sender, "checkout", "v1"); err != nil {
		t.Fatal(err)
	}

	m := recvMsg(t, got)
	if m.Intent != protocol.IntentReady {
		t.Fatalf("intent = %q, want ready", m.Intent)
	}
	if m.From.Agent != "billing" {
		t.Fatalf("from = %q, want billing", m.From.Agent)
	}
	if m.Body["gate"] != "checkout" || m.Body["version"] != "v1" {
		t.Fatalf("body = %v, want gate=checkout version=v1", m.Body)
	}
}
