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

// --- coordinator test harness ---

type harness struct {
	ctx     context.Context
	cancel  context.CancelFunc
	bus     *bus.InMemory
	gateID  string
	coord   *gate.Coordinator
	verdict chan gate.Verdict
	blocks  chan protocol.Message
	parts   map[string]*agent.Agent
}

// setupGate stands up a gatekeeper hosting a coordinator for spec, plus an
// optional runner. If runnerFn is nil, no runner is registered (the gate will
// later stall — used by the timeout test). Participants are created lazily and
// each captures blocks routed to it.
func setupGate(t *testing.T, spec gate.Spec, runnerFn func(string, map[string]string) gate.Verdict) *harness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	b := bus.NewInMemory(64)

	gk, err := agent.New(ctx, b, "gatekeeper", []string{gate.Topic(spec.ID)})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	coord := gate.NewCoordinator(gk)
	coord.Register(spec)
	h := &harness{
		ctx: ctx, cancel: cancel, bus: b, gateID: spec.ID, coord: coord,
		verdict: make(chan gate.Verdict, 8),
		blocks:  make(chan protocol.Message, 8),
		parts:   map[string]*agent.Agent{},
	}
	coord.OnVerdict(func(v gate.Verdict) { h.verdict <- v })
	go gk.Run(ctx)

	if runnerFn != nil {
		r, err := agent.New(ctx, b, spec.Runner, nil)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		gate.ServeRunner(r, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
			return runnerFn(gateID, versions)
		})
		go r.Run(ctx)
	}
	return h
}

// part returns (creating + running once) a participant agent that captures any
// IntentBlock routed to it.
func (h *harness) part(t *testing.T, name string) *agent.Agent {
	t.Helper()
	if a, ok := h.parts[name]; ok {
		return a
	}
	a, err := agent.New(h.ctx, h.bus, name, nil)
	if err != nil {
		t.Fatal(err)
	}
	a.On(protocol.IntentBlock, func(ctx context.Context, ag *agent.Agent, m protocol.Message) *protocol.Message {
		h.blocks <- m
		return nil
	})
	go a.Run(h.ctx)
	h.parts[name] = a
	return a
}

func (h *harness) ready(t *testing.T, who, version string) {
	t.Helper()
	if err := gate.Ready(h.ctx, h.part(t, who), h.gateID, version); err != nil {
		t.Fatal(err)
	}
}

func recvVerdict(t *testing.T, ch chan gate.Verdict) gate.Verdict {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for verdict")
		return gate.Verdict{}
	}
}

func checkoutSpec() gate.Spec {
	return gate.Spec{ID: "checkout", Required: []string{"billing", "gateway"}, Runner: "runner"}
}

func passRunner(gateID string, versions map[string]string) gate.Verdict {
	return gate.Verdict{GateID: gateID, Passed: true}
}

// --- tests ---

func TestPartialReadinessDoesNotOpen(t *testing.T) {
	h := setupGate(t, checkoutSpec(), passRunner)
	defer h.cancel()

	h.ready(t, "billing", "v1") // only one of two required

	select {
	case v := <-h.verdict:
		t.Fatalf("gate opened with partial readiness: %+v", v)
	case <-time.After(250 * time.Millisecond):
		// good: no verdict
	}
}

func TestFullReadinessOpensAndPasses(t *testing.T) {
	h := setupGate(t, checkoutSpec(), passRunner)
	defer h.cancel()

	// Out-of-order on purpose: gateway before billing still opens.
	h.ready(t, "gateway", "g1")
	h.ready(t, "billing", "b1")

	v := recvVerdict(t, h.verdict)
	if !v.Passed {
		t.Fatalf("verdict = %+v, want passed", v)
	}
	if v.Versions["billing"] != "b1" || v.Versions["gateway"] != "g1" {
		t.Fatalf("versions = %v, want billing=b1 gateway=g1", v.Versions)
	}
}

func TestDuplicateReadyLastVersionWins(t *testing.T) {
	h := setupGate(t, checkoutSpec(), passRunner)
	defer h.cancel()

	h.ready(t, "billing", "v1")
	h.ready(t, "billing", "v2") // same participant again; must not open alone

	select {
	case v := <-h.verdict:
		t.Fatalf("opened with a single participant: %+v", v)
	case <-time.After(200 * time.Millisecond):
	}

	h.ready(t, "gateway", "g1")
	v := recvVerdict(t, h.verdict)
	if v.Versions["billing"] != "v2" {
		t.Fatalf("billing version = %q, want last-write v2", v.Versions["billing"])
	}
}

func TestFailVerdictBroadcastsAndBlocksOwners(t *testing.T) {
	failRunner := func(gateID string, versions map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: false, Detail: "checkout_test: 402"}
	}
	h := setupGate(t, checkoutSpec(), failRunner)
	defer h.cancel()

	h.ready(t, "billing", "b1")
	h.ready(t, "gateway", "g1")

	v := recvVerdict(t, h.verdict)
	if v.Passed {
		t.Fatalf("verdict = %+v, want failed", v)
	}
	if v.Detail != "checkout_test: 402" {
		t.Fatalf("detail = %q", v.Detail)
	}

	owners := map[string]bool{}
	for i := 0; i < 2; i++ {
		m := recvMsg(t, h.blocks)
		if m.Intent != protocol.IntentBlock {
			t.Fatalf("intent = %q, want block", m.Intent)
		}
		owners[m.To.Agent] = true
	}
	if !owners["billing"] || !owners["gateway"] {
		t.Fatalf("blocked owners = %v, want billing+gateway", owners)
	}
}

func TestGateReArmsAfterVerdict(t *testing.T) {
	h := setupGate(t, checkoutSpec(), passRunner)
	defer h.cancel()

	h.ready(t, "billing", "b1")
	h.ready(t, "gateway", "g1")
	if v := recvVerdict(t, h.verdict); !v.Passed {
		t.Fatalf("round 1 verdict = %+v", v)
	}

	// Second full round must trigger a second run.
	h.ready(t, "billing", "b2")
	h.ready(t, "gateway", "g2")
	v := recvVerdict(t, h.verdict)
	if !v.Passed || v.Versions["billing"] != "b2" {
		t.Fatalf("round 2 verdict = %+v, want passed billing=b2", v)
	}
}

func TestRunnerTimeoutStalls(t *testing.T) {
	// nil runnerFn => no runner agent exists, so the request is never answered.
	h := setupGate(t, checkoutSpec(), nil)
	defer h.cancel()
	h.coord.SetRunnerTimeout(80 * time.Millisecond)

	h.ready(t, "billing", "b1")
	h.ready(t, "gateway", "g1")

	v := recvVerdict(t, h.verdict)
	if v.Passed {
		t.Fatalf("verdict = %+v, want stalled failure", v)
	}
	if v.Detail != "runner unresponsive" {
		t.Fatalf("detail = %q, want \"runner unresponsive\"", v.Detail)
	}

	owners := map[string]bool{}
	for i := 0; i < 2; i++ {
		m := recvMsg(t, h.blocks)
		owners[m.To.Agent] = true
	}
	if !owners["billing"] || !owners["gateway"] {
		t.Fatalf("blocked owners = %v, want billing+gateway", owners)
	}
}
