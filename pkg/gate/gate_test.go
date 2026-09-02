package gate_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
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
	informs chan protocol.Message
	acks    chan protocol.Message
	parts   map[string]*agent.Agent
}

// setupGate stands up a gatekeeper hosting a coordinator for spec, plus an
// optional runner. If runnerFn is nil, no runner is registered (the gate will
// later stall — used by the timeout test). Participants are created lazily and
// each captures blocks routed to it. An observer subscribed to the gate topic
// captures every broadcast Inform — real or resolved-from-cache — the same way
// a real pcops.Submit call would see it, so tests can assert on the exact text
// a caller receives, including the cached-answer marker.
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
		informs: make(chan protocol.Message, 8),
		acks:    make(chan protocol.Message, 8),
		parts:   map[string]*agent.Agent{},
	}
	coord.OnVerdict(func(v gate.Verdict) { h.verdict <- v })
	go gk.Run(ctx)

	obs, err := agent.New(ctx, b, "obs", []string{gate.Topic(spec.ID)})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	obs.On(protocol.IntentInform, func(ctx context.Context, ag *agent.Agent, m protocol.Message) *protocol.Message {
		h.informs <- m
		return nil
	})
	go obs.Run(ctx)

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
	a.On(protocol.IntentAck, func(ctx context.Context, ag *agent.Agent, m protocol.Message) *protocol.Message {
		h.acks <- m
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

func TestServeRunnerDecodesJSONVersions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := bus.NewInMemory(8)

	runner, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	var seen map[string]string
	gate.ServeRunner(runner, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		seen = versions
		return gate.Verdict{GateID: gateID, Passed: true}
	})
	go runner.Run(ctx)

	// Simulate a JSON transport: versions arrives as map[string]any.
	m := sendAndCaptureReply(t, ctx, b, "runner", map[string]any{
		"gate":     "checkout",
		"versions": map[string]any{"billing": "e5f6"},
	})
	if m.Intent != protocol.IntentDone {
		t.Fatalf("intent = %q, want done", m.Intent)
	}
	if seen["billing"] != "e5f6" {
		t.Fatalf("versions = %v, want billing=e5f6", seen)
	}
}

// --- F2: a resubmit at an already-tested version is answered from the
// remembered verdict instead of hanging until submit_timeout. See gate.go's
// gateState doc comment and onReady. ---

func TestResubmitSameVersionReturnsRecordedPassVerdictPromptly(t *testing.T) {
	h := setupGate(t, checkoutSpec(), passRunner)
	defer h.cancel()

	h.ready(t, "billing", "b1")
	h.ready(t, "gateway", "g1")
	v := recvVerdict(t, h.verdict)
	if !v.Passed {
		t.Fatalf("round 1 verdict = %+v, want passed", v)
	}
	recvMsg(t, h.informs) // drain round 1's broadcast

	// billing submits the exact same version again. Quorum can never re-form
	// (gateway is done and won't resubmit), so without the fix this would
	// hang until submit_timeout. recvVerdict/recvMsg bound the wait with
	// time.After and fail the test if nothing arrives — the test would hit
	// its deadline, not the process, if the fix were absent or if the cached
	// path skipped resolve (see the doc comment on gateState).
	h.ready(t, "billing", "b1")

	v2 := recvVerdict(t, h.verdict)
	if !v2.Passed {
		t.Fatalf("cached-round verdict = %+v, want passed", v2)
	}

	m := recvMsg(t, h.informs)
	if passed, _ := m.Body["passed"].(bool); !passed {
		t.Fatalf("cached inform passed = %v, want true", m.Body["passed"])
	}
	if m.Body["gate"] != "checkout" {
		t.Fatalf("cached inform gate = %v, want checkout", m.Body["gate"])
	}
	text, _ := m.Body["text"].(string)
	if !strings.Contains(text, "(already tested at this version)") {
		t.Fatalf("cached inform text = %q, want the cached-answer marker", text)
	}
}

func TestResubmitSameVersionReturnsRecordedFailVerdictPromptly(t *testing.T) {
	failRunner := func(gateID string, versions map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: false, Detail: "checkout_test: 402"}
	}
	h := setupGate(t, checkoutSpec(), failRunner)
	defer h.cancel()

	h.ready(t, "billing", "b1")
	h.ready(t, "gateway", "g1")
	v := recvVerdict(t, h.verdict)
	if v.Passed {
		t.Fatalf("round 1 verdict = %+v, want failed", v)
	}
	recvMsg(t, h.informs) // drain round 1's broadcast
	recvMsg(t, h.blocks)  // drain round 1's two owner blocks
	recvMsg(t, h.blocks)

	// billing re-submits, unchanged, after a failure: it must be told it
	// still fails, promptly — a remembered failing verdict is exactly as
	// answerable as a passing one.
	h.ready(t, "billing", "b1")

	v2 := recvVerdict(t, h.verdict)
	if v2.Passed {
		t.Fatalf("cached-round verdict = %+v, want failed", v2)
	}

	m := recvMsg(t, h.informs)
	if passed, _ := m.Body["passed"].(bool); passed {
		t.Fatalf("cached inform passed = %v, want false", m.Body["passed"])
	}
	text, _ := m.Body["text"].(string)
	if !strings.Contains(text, "(already tested at this version)") {
		t.Fatalf("cached inform text = %q, want the cached-answer marker", text)
	}
	if !strings.Contains(text, "checkout_test: 402") {
		t.Fatalf("cached inform text = %q, want the original failure detail preserved", text)
	}

	// A cached FAILING round still routes blocks to both owners exactly like
	// any other failing round — the failure path is unchanged by caching.
	owners := map[string]bool{}
	for i := 0; i < 2; i++ {
		bm := recvMsg(t, h.blocks)
		owners[bm.To.Agent] = true
	}
	if !owners["billing"] || !owners["gateway"] {
		t.Fatalf("blocked owners = %v, want billing+gateway", owners)
	}
}

func TestResubmitDifferentVersionRecordsReadinessNormally(t *testing.T) {
	h := setupGate(t, checkoutSpec(), passRunner)
	defer h.cancel()

	h.ready(t, "billing", "b1")
	h.ready(t, "gateway", "g1")
	recvVerdict(t, h.verdict)
	recvMsg(t, h.informs) // drain round 1's broadcast

	// billing committed something since: a genuinely new version must be
	// recorded as fresh readiness, not answered from the stale cache — so
	// with only billing resubmitted, the gate stays partial (no verdict).
	// This is the assertion that stops the fix from breaking the actual loop.
	h.ready(t, "billing", "b2")
	select {
	case v := <-h.verdict:
		t.Fatalf("gate opened with partial readiness: %+v", v)
	case <-time.After(200 * time.Millisecond):
	}

	h.ready(t, "gateway", "g2")
	v := recvVerdict(t, h.verdict)
	if !v.Passed || v.Versions["billing"] != "b2" || v.Versions["gateway"] != "g2" {
		t.Fatalf("round 2 verdict = %+v, want passed billing=b2 gateway=g2", v)
	}
	m := recvMsg(t, h.informs)
	if text, _ := m.Body["text"].(string); strings.Contains(text, "already tested") {
		t.Fatalf("round 2 inform text = %q, must not carry the cached-answer marker", text)
	}
}

func TestResubmitWhileInFlightIsNotAnsweredFromCache(t *testing.T) {
	// The runner answers the first request that reaches it and then blocks
	// forever, so round 1 resolves normally (giving the gate a remembered
	// verdict) while round 2 stays genuinely in flight for the rest of the
	// test.
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	var calls int32
	runnerFn := func(gateID string, versions map[string]string) gate.Verdict {
		if atomic.AddInt32(&calls, 1) == 1 {
			return gate.Verdict{GateID: gateID, Passed: true}
		}
		<-block
		return gate.Verdict{}
	}
	h := setupGate(t, checkoutSpec(), runnerFn)
	defer h.cancel()
	h.coord.SetRunnerTimeout(2 * time.Second) // long enough not to fire mid-test

	h.ready(t, "billing", "b1")
	h.ready(t, "gateway", "g1")
	recvVerdict(t, h.verdict)
	recvMsg(t, h.informs)

	// Round 2 opens with new versions (so quorum reaching it is not itself a
	// cache hit) and stalls forever inside the runner: genuinely inflight.
	h.ready(t, "billing", "b2")
	h.ready(t, "gateway", "g2")

	// billing resubmits its ROUND-1 version while round 2 is inflight. Even
	// though that version still matches round 1's remembered verdict,
	// inflight must win: no synthetic resolution while a round is genuinely
	// in progress.
	h.ready(t, "billing", "b1")

	select {
	case m := <-h.informs:
		t.Fatalf("unexpected inform while inflight: %+v", m)
	case v := <-h.verdict:
		t.Fatalf("unexpected verdict while inflight: %+v", v)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestPerGateIsolationOfRememberedVerdict(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := bus.NewInMemory(64)

	gk, err := agent.New(ctx, b, "gatekeeper", []string{gate.Topic("checkout"), gate.Topic("shipping")})
	if err != nil {
		t.Fatal(err)
	}
	coord := gate.NewCoordinator(gk)
	// checkout resolves alone (single required participant); shipping needs
	// two, so it stays unresolved after only billing submits — that's what
	// lets this test tell "resolved from checkout's cache" (bug) apart from
	// "recorded as ordinary readiness on shipping" (correct).
	coord.Register(gate.Spec{ID: "checkout", Required: []string{"billing"}, Runner: "runner"})
	coord.Register(gate.Spec{ID: "shipping", Required: []string{"billing", "gateway"}, Runner: "runner"})
	verdicts := make(chan gate.Verdict, 8)
	coord.OnVerdict(func(v gate.Verdict) { verdicts <- v })
	go gk.Run(ctx)

	runner, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.ServeRunner(runner, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: true}
	})
	go runner.Run(ctx)

	billing, err := agent.New(ctx, b, "billing", nil)
	if err != nil {
		t.Fatal(err)
	}
	go billing.Run(ctx)

	// Resolve checkout only.
	if err := gate.Ready(ctx, billing, "checkout", "b1"); err != nil {
		t.Fatal(err)
	}
	v := recvVerdict(t, verdicts)
	if v.GateID != "checkout" || !v.Passed {
		t.Fatalf("verdict = %+v, want checkout passed", v)
	}

	// billing, at the SAME version, submits to shipping — a DIFFERENT gate
	// that has never resolved and needs a second participant. If the
	// remembered verdict were shared across gates instead of kept per-gate,
	// this would incorrectly resolve shipping from checkout's memory with
	// only one of its two required participants ready.
	if err := gate.Ready(ctx, billing, "shipping", "b1"); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-verdicts:
		t.Fatalf("shipping incorrectly resolved from checkout's cache: %+v", v)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestManyIdenticalResubmitsResolveOneRoundEachWithoutRunningAway guards
// against exactly the regression the rejected direct-send design produced: a
// cached answer that triggers more cached answers, unboundedly. Each
// resubmit here must settle in its own recvVerdict/recvMsg pair — never more,
// never fewer — proving the mechanism itself cannot cascade even under
// sustained, rapid, identical resubmission.
func TestManyIdenticalResubmitsResolveOneRoundEachWithoutRunningAway(t *testing.T) {
	h := setupGate(t, checkoutSpec(), passRunner)
	defer h.cancel()

	h.ready(t, "billing", "b1")
	h.ready(t, "gateway", "g1")
	recvVerdict(t, h.verdict)
	recvMsg(t, h.informs)

	const n = 50
	for i := 0; i < n; i++ {
		h.ready(t, "billing", "b1")
		v := recvVerdict(t, h.verdict)
		if !v.Passed {
			t.Fatalf("resubmit %d verdict = %+v, want passed", i, v)
		}
		m := recvMsg(t, h.informs)
		if text, _ := m.Body["text"].(string); !strings.Contains(text, "already tested") {
			t.Fatalf("resubmit %d inform text = %q, want the cached-answer marker", i, text)
		}
	}

	// Nothing further should be queued: each resubmit produced exactly one
	// verdict and one broadcast, not a cascade.
	select {
	case v := <-h.verdict:
		t.Fatalf("unexpected extra verdict after %d resubmits: %+v", n, v)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestOtherParticipantMovingInvalidatesTheCacheForEveryone encodes the exact
// live-run sequence that exposed the submitter-only version guard: round 1
// fails with gateway still on its old version; gateway then commits a fix
// and resubmits a NEW version; billing resubmits its UNCHANGED version
// immediately after. The submitter-only guard answered billing from the
// round-1 cache (still FAILED) because it only ever compared billing's own
// version — even though gateway's fix had already made the remembered
// verdict stale for the combination as a whole. Since the cached path skips
// recording readiness, gateway's fix was then simply never picked up.
//
// The fix must invalidate the remembered verdict the moment ANY required
// participant's version diverges from what the remembered round tested, so
// billing's very next unchanged resubmit records ordinary readiness instead
// of a second stale answer, quorum completes with gateway's new version, and
// a fresh round runs immediately.
func TestOtherParticipantMovingInvalidatesTheCacheForEveryone(t *testing.T) {
	var calls int32
	runnerFn := func(gateID string, versions map[string]string) gate.Verdict {
		if atomic.AddInt32(&calls, 1) == 1 {
			return gate.Verdict{GateID: gateID, Passed: false, Detail: "gateway still EUR"}
		}
		return gate.Verdict{GateID: gateID, Passed: true} // round 2: reflects gateway's fix
	}
	h := setupGate(t, checkoutSpec(), runnerFn)
	defer h.cancel()

	// Round 1: fails (gateway on its old version).
	h.ready(t, "billing", "b1")
	h.ready(t, "gateway", "eur1")
	v := recvVerdict(t, h.verdict)
	if v.Passed {
		t.Fatalf("round 1 verdict = %+v, want failed", v)
	}
	recvMsg(t, h.informs)
	recvMsg(t, h.blocks)
	recvMsg(t, h.blocks)

	// gateway fixes and resubmits a NEW version: this must invalidate the
	// remembered verdict for the gate as a whole, not just for gateway.
	h.ready(t, "gateway", "usd1")

	// billing resubmits its version UNCHANGED. It must be recorded as
	// ordinary readiness — NOT answered from the now-stale round-1 cache —
	// completing quorum with gateway's new version and triggering a fresh
	// round.
	h.ready(t, "billing", "b1")

	v2 := recvVerdict(t, h.verdict)
	if !v2.Passed {
		t.Fatalf("round 2 verdict = %+v, want passed (a fresh round reflecting gateway's fix)", v2)
	}
	if v2.Versions["gateway"] != "usd1" || v2.Versions["billing"] != "b1" {
		t.Fatalf("round 2 versions = %v, want gateway=usd1 billing=b1", v2.Versions)
	}
	m := recvMsg(t, h.informs)
	if text, _ := m.Body["text"].(string); strings.Contains(text, "already tested") {
		t.Fatalf("round 2 inform text = %q, must NOT carry the cached-answer marker: this must be a fresh run, not a cached reply", text)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("runner invoked %d times, want exactly 2 (round 1 + the fresh round 2)", got)
	}

	// After this genuine resolve, the cache works again from that point:
	// billing resubmits its round-2 version unchanged and gets an immediate
	// cached PASS, with no third runner invocation.
	h.ready(t, "billing", "b1")
	v3 := recvVerdict(t, h.verdict)
	if !v3.Passed {
		t.Fatalf("post-invalidation cached verdict = %+v, want passed", v3)
	}
	m3 := recvMsg(t, h.informs)
	if text, _ := m3.Body["text"].(string); !strings.Contains(text, "already tested at this version") {
		t.Fatalf("post-invalidation inform text = %q, want the cached-answer marker", text)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("runner invoked %d times after the cached resubmit, want still 2 (no extra run)", got)
	}
}

// TestBothParticipantsUnchangedStillAnsweredFromCache is the case the cache
// is actually for: neither participant's version differs from the
// remembered round, so neither resubmit invalidates it — both are answered
// promptly from cache, in either order.
func TestBothParticipantsUnchangedStillAnsweredFromCache(t *testing.T) {
	h := setupGate(t, checkoutSpec(), passRunner)
	defer h.cancel()

	h.ready(t, "billing", "b1")
	h.ready(t, "gateway", "g1")
	recvVerdict(t, h.verdict)
	recvMsg(t, h.informs)

	h.ready(t, "billing", "b1")
	v1 := recvVerdict(t, h.verdict)
	if !v1.Passed {
		t.Fatalf("billing's cached verdict = %+v, want passed", v1)
	}
	m1 := recvMsg(t, h.informs)
	if text, _ := m1.Body["text"].(string); !strings.Contains(text, "already tested at this version") {
		t.Fatalf("billing's cached inform text = %q, want the marker", text)
	}

	h.ready(t, "gateway", "g1")
	v2 := recvVerdict(t, h.verdict)
	if !v2.Passed {
		t.Fatalf("gateway's cached verdict = %+v, want passed", v2)
	}
	m2 := recvMsg(t, h.informs)
	if text, _ := m2.Body["text"].(string); !strings.Contains(text, "already tested at this version") {
		t.Fatalf("gateway's cached inform text = %q, want the marker", text)
	}
}

// --- F4: acknowledge every recorded readiness with who the gate is still
// waiting on, so a blocked pc submit can tell "not run yet" from "no
// coordinator" from "a peer is never coming". See onReady and
// docs/superpowers/specs/2026-09-02-live-fire-findings.md, F4 and "F4
// reinforced". ---

// TestOnReadyAcksTheSubmitterWithOutstandingParticipants is the core F4 case:
// a lone submitter to a two-participant gate gets back an immediate ack
// naming the peer it is still waiting on, rather than the total silence a
// live run against real coding agents actually produced.
func TestOnReadyAcksTheSubmitterWithOutstandingParticipants(t *testing.T) {
	h := setupGate(t, checkoutSpec(), passRunner)
	defer h.cancel()

	h.ready(t, "billing", "b1")

	ack := recvMsg(t, h.acks)
	if ack.Intent != protocol.IntentAck {
		t.Fatalf("intent = %q, want ack", ack.Intent)
	}
	if ack.To.Agent != "billing" {
		t.Fatalf("ack.To.Agent = %q, want billing", ack.To.Agent)
	}
	if ack.Body["gate"] != "checkout" {
		t.Fatalf("ack.Body[gate] = %v, want checkout", ack.Body["gate"])
	}
	outstanding, ok := ack.Body["outstanding"].([]string)
	if !ok || len(outstanding) != 1 || outstanding[0] != "gateway" {
		t.Fatalf("ack.Body[outstanding] = %v, want [gateway]", ack.Body["outstanding"])
	}

	// Quorum is still incomplete: this must not have opened the gate.
	select {
	case v := <-h.verdict:
		t.Fatalf("gate opened with partial readiness: %+v", v)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestOnReadyAcksWithEmptyOutstandingWhenQuorumCompletes covers the detail
// called out explicitly in the design: the participant that completes
// quorum is acknowledged too, with an EMPTY outstanding list, and the round
// still opens and resolves normally right after — the ack does not replace
// or delay the verdict.
func TestOnReadyAcksWithEmptyOutstandingWhenQuorumCompletes(t *testing.T) {
	h := setupGate(t, checkoutSpec(), passRunner)
	defer h.cancel()

	h.ready(t, "billing", "b1")
	firstAck := recvMsg(t, h.acks)
	if out, _ := firstAck.Body["outstanding"].([]string); len(out) != 1 || out[0] != "gateway" {
		t.Fatalf("billing's ack outstanding = %v, want [gateway]", firstAck.Body["outstanding"])
	}

	h.ready(t, "gateway", "g1")
	secondAck := recvMsg(t, h.acks)
	if secondAck.To.Agent != "gateway" {
		t.Fatalf("second ack.To.Agent = %q, want gateway", secondAck.To.Agent)
	}
	outstanding, ok := secondAck.Body["outstanding"].([]string)
	if !ok || len(outstanding) != 0 {
		t.Fatalf("gateway's ack outstanding = %v, want empty", secondAck.Body["outstanding"])
	}

	// The round still opens and resolves — the ack is additive, not a
	// replacement for the verdict.
	v := recvVerdict(t, h.verdict)
	if !v.Passed {
		t.Fatalf("verdict = %+v, want passed", v)
	}
}

// TestOnReadyDoesNotAckACachedResubmit locks in the documented exception: the
// F2 cache path (see gateState's doc comment) answers a redundant identical
// resubmit via resolve's own IntentInform broadcast, without ever recording
// readiness — so it must not also send an IntentAck. pcops.Submit relies on
// this: it treats a verdict's arrival as satisfying its own ack wait, and a
// spurious ack here would be harmless but would mean this test is the only
// thing pinning down "cache hits skip the ack" as intentional rather than
// accidental.
func TestOnReadyDoesNotAckACachedResubmit(t *testing.T) {
	h := setupGate(t, checkoutSpec(), passRunner)
	defer h.cancel()

	h.ready(t, "billing", "b1")
	recvMsg(t, h.acks)
	h.ready(t, "gateway", "g1")
	recvMsg(t, h.acks)
	recvVerdict(t, h.verdict)
	recvMsg(t, h.informs) // drain round 1's broadcast

	// billing resubmits the exact same version: answered from cache.
	h.ready(t, "billing", "b1")

	v2 := recvVerdict(t, h.verdict)
	if !v2.Passed {
		t.Fatalf("cached-round verdict = %+v, want passed", v2)
	}
	recvMsg(t, h.informs) // the cached-round broadcast, unaffected by this test

	select {
	case ack := <-h.acks:
		t.Fatalf("unexpected ack on a cached resubmit: %+v", ack)
	case <-time.After(200 * time.Millisecond):
	}
}
