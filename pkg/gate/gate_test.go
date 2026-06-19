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

// sendAndCaptureReply wires a caller that sends one request to `to` and
// captures the runner's reply (done or disagree).
func sendAndCaptureReply(t *testing.T, ctx context.Context, b *bus.InMemory, to string, body map[string]any) protocol.Message {
	t.Helper()
	caller, err := agent.New(ctx, b, "caller", nil)
	if err != nil {
		t.Fatal(err)
	}
	reply := make(chan protocol.Message, 1)
	capture := func(ctx context.Context, a *agent.Agent, m protocol.Message) *protocol.Message {
		reply <- m
		return nil
	}
	caller.On(protocol.IntentDone, capture)
	caller.On(protocol.IntentDisagree, capture)
	go caller.Run(ctx)

	if err := caller.Send(ctx, protocol.New(
		protocol.Address{Agent: "caller"},
		protocol.Address{Agent: to},
		protocol.IntentRequest,
		body,
	)); err != nil {
		t.Fatal(err)
	}
	return recvMsg(t, reply)
}

func TestServeRunnerRepliesDoneOnPass(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := bus.NewInMemory(8)

	runner, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.ServeRunner(runner, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		if versions["billing"] != "v1" {
			t.Errorf("runner got versions %v, want billing=v1", versions)
		}
		return gate.Verdict{GateID: gateID, Passed: true}
	})
	go runner.Run(ctx)

	m := sendAndCaptureReply(t, ctx, b, "runner", map[string]any{
		"gate":     "checkout",
		"versions": map[string]string{"billing": "v1"},
	})
	if m.Intent != protocol.IntentDone {
		t.Fatalf("intent = %q, want done", m.Intent)
	}
}

func TestServeRunnerRepliesDisagreeOnFail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := bus.NewInMemory(8)

	runner, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.ServeRunner(runner, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: false, Detail: "boom"}
	})
	go runner.Run(ctx)

	m := sendAndCaptureReply(t, ctx, b, "runner", map[string]any{"gate": "checkout"})
	if m.Intent != protocol.IntentDisagree {
		t.Fatalf("intent = %q, want disagree", m.Intent)
	}
	if m.Body["detail"] != "boom" {
		t.Fatalf("detail = %v, want boom", m.Body["detail"])
	}
}
