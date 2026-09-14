package pcops_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

// A single-participant gate whose runner always passes: submit must return a
// passing verdict rather than time out.
func TestSubmitReturnsThePassingVerdict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{DB: db, SubmitTimeout: 10 * time.Second}

	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })

	coord, err := agent.New(ctx, b, "coordinator", []string{gate.Topic("g")})
	if err != nil {
		t.Fatal(err)
	}
	c := gate.NewCoordinator(coord)
	c.Register(gate.Spec{ID: "g", Required: []string{"billing"}, Runner: "runner"})
	go coord.Run(ctx)

	run, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.ServeRunner(run, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: true}
	})
	go run.Run(ctx)

	v, err := pcops.Submit(ctx, cfg, "g", "billing", "v1")
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if !v.Passed {
		t.Fatalf("verdict = %+v, want passed", v)
	}
	if v.Detail != "" {
		t.Fatalf("verdict.Detail = %q, want empty on a passing verdict", v.Detail)
	}
}

// FIX 4: an empty version silently disables Submit's headline correctness
// guard (versions[agentName] != version), because versionsFromBody returns
// "" for a participant absent from a verdict entirely — so "" == "" would
// PASS the guard for a verdict that never tested this agent at all. `pc
// submit` cannot reach this (resolveVersion errors rather than returning a
// placeholder), but Submit is a package-level function, so it must reject
// this itself rather than rely on every caller getting resolveVersion right.
// No bus, coordinator or runner needed: this must fail before Submit ever
// opens one.
func TestSubmitRejectsAnEmptyVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := pcops.Submit(ctx, pcops.Config{DB: filepath.Join(t.TempDir(), "bus.db")}, "g", "billing", "")
	if err == nil {
		t.Fatal("Submit with an empty version = nil error, want one rejecting it")
	}
	if !strings.Contains(err.Error(), "version") {
		t.Errorf("error %q does not mention the empty version", err.Error())
	}
}

// A single-participant gate whose runner always fails: submit must return a
// nil error alongside a non-passing verdict that carries the failure detail —
// distinct from ErrNoVerdict, which means no verdict arrived at all.
func TestSubmitReturnsAFailingVerdict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{DB: db, SubmitTimeout: 10 * time.Second}

	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })

	coord, err := agent.New(ctx, b, "coordinator", []string{gate.Topic("g")})
	if err != nil {
		t.Fatal(err)
	}
	c := gate.NewCoordinator(coord)
	c.Register(gate.Spec{ID: "g", Required: []string{"billing"}, Runner: "runner"})
	go coord.Run(ctx)

	run, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.ServeRunner(run, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: false, Detail: "spanning test failed"}
	})
	go run.Run(ctx)

	v, err := pcops.Submit(ctx, cfg, "g", "billing", "v1")
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if v.Passed {
		t.Fatalf("verdict = %+v, want not passed", v)
	}
	if v.Detail == "" || !strings.Contains(v.Detail, "failed") {
		t.Fatalf("verdict.Detail = %q, want it to mention the failure", v.Detail)
	}
}

func TestSubmitTimesOutWithoutACoordinator(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cfg := pcops.Config{
		DB:            filepath.Join(t.TempDir(), "bus.db"),
		SubmitTimeout: 300 * time.Millisecond,
	}
	if _, err := pcops.Submit(ctx, cfg, "g", "billing", "v1"); !errors.Is(err, pcops.ErrNoVerdict) {
		t.Fatalf("Submit err = %v, want ErrNoVerdict", err)
	}
}

// --- F4: distinguish "no coordinator acknowledged this gate" from "the gate
// just hasn't opened yet". See docs/superpowers/specs/2026-09-02-live-fire-
// findings.md, F4 and "F4 reinforced": a live run parked for the full
// 8-minute submit timeout with zero signal when the coordinator was dead,
// and separately waited 7m45s for a peer that never showed. ---

// TestSubmitDiagnosesAMissingCoordinatorWellInsideTheSubmitTimeout is the
// core F4 regression test: no coordinator is running at all — nothing is
// even subscribed to the gate topic — and SubmitTimeout is deliberately set
// far longer than pcops.AckTimeout. Without the fix, Submit would block for
// the entire SubmitTimeout with the wrong error (ErrNoVerdict) and no way to
// tell a dead coordinator from one that simply hasn't reached quorum yet.
// The deadline on ctx below is itself the thing that would fail this test if
// Submit hung: it is set shorter than SubmitTimeout but comfortably longer
// than AckTimeout, so a hang manifests as a test timeout, not a slow pass.
func TestSubmitDiagnosesAMissingCoordinatorWellInsideTheSubmitTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), pcops.AckTimeout+15*time.Second)
	defer cancel()
	cfg := pcops.Config{
		DB:            filepath.Join(t.TempDir(), "bus.db"),
		SubmitTimeout: 5 * time.Minute, // far longer than AckTimeout on purpose
	}

	start := time.Now()
	_, err := pcops.Submit(ctx, cfg, "g", "billing", "v1")
	elapsed := time.Since(start)

	if !errors.Is(err, pcops.ErrNotAcknowledged) {
		t.Fatalf("Submit err = %v, want ErrNotAcknowledged", err)
	}
	// Comfortably inside SubmitTimeout (5m): proves this returned from the
	// ack deadline, not some other path racing the outer ctx.
	if elapsed > pcops.AckTimeout+10*time.Second {
		t.Fatalf("Submit took %v to diagnose a missing coordinator, want well under SubmitTimeout", elapsed)
	}
}

// TestSubmitWaitsForVerdictAfterAckWhenQuorumIncomplete proves the ack wait
// and the verdict wait are genuinely two different phases: a coordinator IS
// running and DOES ack (quickly, since bus traffic is fast), but the gate
// requires a second participant that never submits. Submit must not confuse
// "acknowledged, still waiting" with "acknowledged, done" — it keeps
// blocking past AckTimeout and eventually reports ErrNoVerdict, not
// ErrNotAcknowledged, once its own (short) SubmitTimeout expires.
func TestSubmitWaitsForVerdictAfterAckWhenQuorumIncomplete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{DB: db, SubmitTimeout: 2 * time.Second}

	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })

	coord, err := agent.New(ctx, b, "coordinator", []string{gate.Topic("g")})
	if err != nil {
		t.Fatal(err)
	}
	c := gate.NewCoordinator(coord)
	c.Register(gate.Spec{ID: "g", Required: []string{"billing", "gateway"}, Runner: "runner"})
	go coord.Run(ctx)

	run, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.ServeRunner(run, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: true}
	})
	go run.Run(ctx)

	start := time.Now()
	_, err = pcops.Submit(ctx, cfg, "g", "billing", "v1") // gateway never submits
	elapsed := time.Since(start)

	if !errors.Is(err, pcops.ErrNoVerdict) {
		t.Fatalf("Submit err = %v, want ErrNoVerdict (acknowledged, but no verdict)", err)
	}
	if errors.Is(err, pcops.ErrNotAcknowledged) {
		t.Fatal("Submit reported ErrNotAcknowledged despite a live, acking coordinator")
	}
	if elapsed >= pcops.AckTimeout {
		t.Fatalf("Submit took %v, want it to return at SubmitTimeout (2s), well under AckTimeout (%v)", elapsed, pcops.AckTimeout)
	}
}

// TestSubmitRedundantResubmitAnsweredFromCacheDoesNotErrorAsUnacknowledged is
// the regression test called out explicitly in the design as the single most
// likely way to break something: gate.go's F2 cache path (see gateState's
// doc comment) answers a redundant identical resubmit straight from a
// remembered verdict, via resolve's own IntentInform broadcast — and
// deliberately WITHOUT recording readiness, so no IntentAck is ever sent for
// it. If Submit's ack wait didn't also accept a verdict as satisfying it,
// every such resubmit would regress into a spurious ErrNotAcknowledged
// instead of the cached verdict it used to return before this change.
func TestSubmitRedundantResubmitAnsweredFromCacheDoesNotErrorAsUnacknowledged(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{DB: db, SubmitTimeout: 10 * time.Second}

	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })

	coord, err := agent.New(ctx, b, "coordinator", []string{gate.Topic("g")})
	if err != nil {
		t.Fatal(err)
	}
	c := gate.NewCoordinator(coord)
	c.Register(gate.Spec{ID: "g", Required: []string{"billing"}, Runner: "runner"})
	go coord.Run(ctx)

	run, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.ServeRunner(run, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: true}
	})
	go run.Run(ctx)

	v1, err := pcops.Submit(ctx, cfg, "g", "billing", "v1")
	if err != nil {
		t.Fatalf("first Submit: %v", err)
	}
	if !v1.Passed {
		t.Fatalf("first verdict = %+v, want passed", v1)
	}

	// Same agent name, same version: the coordinator answers this from
	// gateState's remembered verdict, not a fresh round.
	v2, err := pcops.Submit(ctx, cfg, "g", "billing", "v1")
	if err != nil {
		t.Fatalf("redundant Submit: %v, want nil (must not regress into ErrNotAcknowledged)", err)
	}
	if errors.Is(err, pcops.ErrNotAcknowledged) {
		t.Fatal("redundant Submit reported ErrNotAcknowledged for a cache-hit resubmit")
	}
	if !v2.Passed {
		t.Fatalf("redundant verdict = %+v, want passed", v2)
	}
	// FIX 6: the cache path is the one place a verdict is REPLAYED rather
	// than freshly computed, which makes it the most load-bearing place to
	// assert that Versions survives the replay rather than merely Passed.
	if v2.Versions["billing"] != "v1" {
		t.Fatalf("cached verdict versions = %+v, want billing=v1", v2.Versions)
	}
}

// A verdict already in the log must not be mistaken for this round's answer.
// pkg/bus/sqlite resumes a subscription from the STORED cursor whenever a row
// exists for that agent name, so a `pc submit` under a name whose cursor sits
// behind a previous round's Inform used to be handed that Inform and return it
// instantly — with the wrong outcome.
//
// The setup reproduces exactly that state rather than simulating it: a warmup
// subscription under the agent's own name advances and persists its cursor, the
// bus holding it is closed, and only then is the stale PASSING verdict
// published. Submit then runs against a coordinator whose runner FAILS, so the
// two rounds are distinguishable by outcome, not just by content.
func TestSubmitIgnoresAVerdictFromAPreviousRound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{DB: db, SubmitTimeout: 20 * time.Second}

	warm, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })

	// Persist a cursor for "billing" that sits behind the stale verdict. Two
	// warmup messages, not one: the poller saves the cursor for a batch only
	// after delivering it, so receiving the SECOND message is what proves the
	// first one's position was already written.
	in, err := warm.Subscribe(ctx, "billing", []string{gate.Topic("g")})
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"warmup-1", "warmup-2"} {
		if err := b.Publish(ctx, protocol.New(
			protocol.Address{Agent: "warmup"}, protocol.Address{Agent: "billing"},
			protocol.IntentInform, map[string]any{"text": text})); err != nil {
			t.Fatal(err)
		}
		select {
		case <-in:
		case <-time.After(15 * time.Second):
			t.Fatalf("warmup message %q never arrived", text)
		}
	}
	// Closing the bus stops that poller for good, so nothing can advance the
	// stored cursor past the stale verdict published next.
	warm.Close()

	stale := protocol.New(
		protocol.Address{Agent: "coordinator"}, protocol.Address{Topic: gate.Topic("g")},
		protocol.IntentInform,
		map[string]any{"text": "g PASSED", "gate": "g", "passed": true})
	if err := b.Publish(ctx, stale); err != nil {
		t.Fatal(err)
	}

	coord, err := agent.New(ctx, b, "coordinator", []string{gate.Topic("g")})
	if err != nil {
		t.Fatal(err)
	}
	c := gate.NewCoordinator(coord)
	c.Register(gate.Spec{ID: "g", Required: []string{"billing"}, Runner: "runner"})
	go coord.Run(ctx)

	run, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.ServeRunner(run, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: false, Detail: "this round failed"}
	})
	go run.Run(ctx)

	v, err := pcops.Submit(ctx, cfg, "g", "billing", "v2")
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if v.Passed {
		t.Fatal("Submit returned the previous round's passing verdict")
	}
	if !strings.Contains(v.Detail, "this round failed") {
		t.Fatalf("verdict.Detail = %q, want this round's failure detail", v.Detail)
	}
}

// A single required participant's readiness completes quorum on its first
// Ready, so this exercises the acknowledged-then-verdict path end to end
// through the retry loop: one attempt, an Ack, then the verdict.
//
// Named for what it does, deliberately. It was called
// TestSubmitTreatsANackAsAcknowledgement, but with Required:["billing"] the
// coordinator records the readiness and ACKS it — no Nack is ever produced,
// so the name promised coverage the body never provided. The Nack path is
// covered by TestSubmitRedeclaresAfterNackAndReturnsTheReDeclaredVerdict,
// which asserts a real IntentNack reached the log before proceeding.
func TestSubmitCompletesQuorumOnItsFirstAttempt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{
		DB:            db,
		GateID:        "g",
		Gate:          pcops.GateDef{Required: []string{"billing"}, Runner: "runner"},
		SubmitTimeout: 30 * time.Second,
	}
	cstop, err := pcops.StartCoordinator(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cstop()

	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	run, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.ServeRunner(run, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: true, Versions: versions}
	})
	go run.Run(ctx)

	v, err := pcops.Submit(ctx, cfg, "g", "billing", "v1")
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if !v.Passed {
		t.Fatalf("verdict = %+v", v)
	}
}

// waitForLogged polls the durable log (the same way pcops.Watch reads it)
// until match reports true for some record, or fails the test if ctx ends
// first. Submit exposes no external hook for "the coordinator processed my
// Nack" or "round 1 has resolved", so the log itself is the only place a test
// can look for deterministic proof of either.
func waitForLogged(t *testing.T, ctx context.Context, b *sqlite.Bus, match func(protocol.Message) bool) {
	t.Helper()
	for {
		recs, err := b.History(ctx, 0)
		if err != nil {
			t.Fatalf("history: %v", err)
		}
		for _, r := range recs {
			if match(r.Msg) {
				return
			}
		}
		select {
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("context ended waiting for a log entry")
		}
	}
}

// TestSubmitRedeclaresAfterNackAndReturnsTheReDeclaredVerdict exercises the
// Nack retry path end to end through Submit: nacked, declined, and
// waitForRoundToResolve are otherwise exercised by no test in this package.
//
// Round 1 (billing@v1 + gateway@v1) is formed by publishing IntentReady
// directly rather than through agent.New-backed senders named "billing" and
// "gateway": Submit will separately create its OWN agent named "billing", and
// pkg/bus/sqlite has no notion of a message "already claimed" by one
// subscriber under a name — each Subscribe independently re-scans the log
// from its own cursor, so two live subscriptions sharing a name would each
// receive their own copy of anything addressed to it (this bus's Ack/Nack
// replies are direct, not topic broadcasts). A raw Publish creates no
// subscription and so cannot collide with Submit's.
//
// The runner is gated exactly like TestSubmitDeclinesAVerdictThatDidNotIncludeIt
// so round 1 stays genuinely in flight until released, but waitForLogged is
// what actually proves the ordering: Submit's billing@v2 readiness must be
// nacked (proving it reached the coordinator while round 1 was still open)
// before the runner is released, and round 1's own resolution must be
// observed on the log before gateway re-declares — otherwise gateway's
// re-declare could itself race an inflight round and be silently dropped
// with nobody listening for its Nack.
func TestSubmitRedeclaresAfterNackAndReturnsTheReDeclaredVerdict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{
		DB:            db,
		GateID:        "g",
		Gate:          pcops.GateDef{Required: []string{"billing", "gateway"}, Runner: "runner"},
		SubmitTimeout: 45 * time.Second,
	}
	cstop, err := pcops.StartCoordinator(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cstop()

	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })

	// The runner blocks every call until release is closed, then answers
	// with exactly the versions it was asked to test: round 1 stalls in
	// flight until we choose to let it go, and round 2 (formed by billing's
	// own re-declare plus gateway's) resolves immediately afterward since
	// release is by then already closed.
	run, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan map[string]string, 4)
	release := make(chan struct{})
	gate.ServeRunner(run, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		select {
		case entered <- versions:
		default:
		}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return gate.Verdict{GateID: gateID, Passed: true, Versions: versions}
	})
	go run.Run(ctx)

	publishReady := func(who, version string) {
		t.Helper()
		msg := protocol.New(
			protocol.Address{Agent: who}, protocol.Address{Topic: gate.Topic("g")},
			protocol.IntentReady, map[string]any{"gate": "g", "version": version})
		if err := b.Publish(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}
	publishReady("billing", "v1")
	publishReady("gateway", "v1")

	select {
	case vs := <-entered:
		if vs["billing"] != "v1" || vs["gateway"] != "v1" {
			t.Fatalf("round 1 entered with %+v, want billing=v1 gateway=v1", vs)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("round 1 never opened")
	}

	// Submit, as billing, at a new version while round 1 is genuinely in
	// flight: this readiness must be dropped and nacked.
	type result struct {
		v   gate.Verdict
		err error
	}
	done := make(chan result, 1)
	go func() {
		v, err := pcops.Submit(ctx, cfg, "g", "billing", "v2")
		done <- result{v, err}
	}()

	// Proof that the Nack actually happened — i.e. that Submit's readiness
	// reached the coordinator while round 1 was still open — rather than a
	// race where round 1 resolves first and billing@v2 is recorded as
	// ordinary new readiness instead.
	waitForLogged(t, ctx, b, func(m protocol.Message) bool {
		gateID, _ := m.Body["gate"].(string)
		return m.Intent == protocol.IntentNack && m.To.Agent == "billing" && gateID == "g"
	})

	close(release) // let round 1 resolve

	// Proof that round 1 has actually resolved (gs.ready re-armed, gs.inflight
	// cleared) before gateway re-declares — otherwise gateway's own readiness
	// could race the still-resolving round 1 and be nacked with nobody
	// listening.
	waitForLogged(t, ctx, b, func(m protocol.Message) bool {
		gateID, _ := m.Body["gate"].(string)
		passed, _ := m.Body["passed"].(bool)
		return m.Intent == protocol.IntentInform && gateID == "g" && passed
	})
	publishReady("gateway", "v2")

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("Submit: %v", r.err)
		}
		if r.v.Versions["billing"] != "v2" {
			t.Fatalf("Submit returned a verdict over %+v, want billing=v2", r.v.Versions)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Submit never returned after re-declaring past the Nack")
	}
}

// The defect: pkg/gate drops a readiness that lands while a round is already
// in flight, but Submit would still accept that round's verdict — one computed
// without its version. The runner is gated so the ordering is deterministic:
// the foreign verdict is the ONLY verdict on the log at the moment Submit could
// wrongly accept it, and the genuine one cannot arrive until we release it.
//
// The foreign verdict names a DIFFERENT participant, which is what this test's
// name means by "did not include it": a round billing was never part of. It
// deliberately does not name billing at some other version — that message says
// the gate merged and tested a later commit of billing's OWN branch, and the
// honest answer to it is ErrVersionMismatch rather than continued silence (see
// TestSubmitReportsWhenTheGateTestedADifferentVersion). The property guarded
// here is the same under either shape: Submit must never return a gate.Verdict
// that was computed without this agent's version.
func TestSubmitDeclinesAVerdictThatDidNotIncludeIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{
		DB:            db,
		GateID:        "g",
		Gate:          pcops.GateDef{Required: []string{"billing"}, Runner: "runner"},
		SubmitTimeout: 45 * time.Second,
	}
	cstop, err := pcops.StartCoordinator(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cstop()

	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })

	run, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Entering the runner proves three things at once: readiness was declared,
	// the round is in flight, and it is in flight AT MY-VERSION. It is also
	// strictly after Submit set readyAt, so anything published from here on
	// clears the timestamp fence and reaches the version guard under test.
	entered := make(chan map[string]string, 1)
	release := make(chan struct{})
	gate.ServeRunner(run, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		select {
		case entered <- versions:
		default:
		}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return gate.Verdict{GateID: gateID, Passed: true, Versions: versions}
	})
	go run.Run(ctx)

	type result struct {
		v   gate.Verdict
		err error
	}
	done := make(chan result, 1)
	go func() {
		v, err := pcops.Submit(ctx, cfg, "g", "billing", "MY-VERSION")
		done <- result{v, err}
	}()

	select {
	case vs := <-entered:
		if vs["billing"] != "MY-VERSION" {
			t.Fatalf("runner entered with versions %+v, want billing=MY-VERSION", vs)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("runner was never invoked, so the round never started")
	}

	// A verdict for a round that did not include billing at all — it names a
	// different participant entirely — published exactly as the coordinator
	// publishes one.
	stale := protocol.New(protocol.Address{Agent: "coordinator"},
		protocol.Address{Topic: gate.Topic("g")}, protocol.IntentInform,
		map[string]any{"gate": "g", "passed": true, "text": "g PASSED",
			"versions": map[string]any{"gateway": "SOMEONE-ELSES-VERSION"}})
	if err := b.Publish(ctx, stale); err != nil {
		t.Fatal(err)
	}

	// THE ASSERTION THAT CATCHES THE DEFECT. The foreign verdict is the only
	// verdict available; a Submit without the version guard accepts it and
	// returns here. With the guard it must keep waiting.
	select {
	case r := <-done:
		t.Fatalf("Submit returned %+v (err %v) on a verdict that did not name billing at all (it tested gateway at %q)",
			r.v, r.err, "SOMEONE-ELSES-VERSION")
	case <-time.After(3 * time.Second):
		// Still waiting, correctly.
	}

	close(release) // let the round that actually included billing finish

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("Submit: %v", r.err)
		}
		if r.v.Versions["billing"] != "MY-VERSION" {
			t.Fatalf("Submit returned a verdict over %+v, want billing=MY-VERSION", r.v.Versions)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Submit never returned after the genuine verdict was broadcast")
	}
}

// Truthful verdicts mean an agent that commits again after submitting sees a
// verdict naming the sha actually merged, and the version guard correctly
// declines it. Nothing then wakes it: the post-ack select waits only on
// verdicts and ctx, so it burns the full SubmitTimeout and reports ErrNoVerdict
// — the F4 failure class this project spent a phase eliminating.
//
// The elapsed-time assertion is the point. With a generous deadline, "returns
// an error eventually" passes against exactly the hang this exists to prevent.
func TestSubmitReportsWhenTheGateTestedADifferentVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{
		DB:            db,
		GateID:        "g",
		Gate:          pcops.GateDef{Required: []string{"billing"}, Runner: "runner"},
		SubmitTimeout: 45 * time.Second,
	}
	cstop, err := pcops.StartCoordinator(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cstop()

	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	run, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	// The runner reports a sha that is NOT what billing declares below.
	gate.ServeRunner(run, func(_ context.Context, gateID string, _ map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: true, Versions: map[string]string{"billing": "actually-merged"}}
	})
	go run.Run(ctx)

	start := time.Now()
	_, err = pcops.Submit(ctx, cfg, "g", "billing", "declared")
	elapsed := time.Since(start)

	if !errors.Is(err, pcops.ErrVersionMismatch) {
		t.Fatalf("Submit err = %v, want ErrVersionMismatch", err)
	}
	// Must return promptly, not at SubmitTimeout. AckTimeout is 10s and the
	// round resolves in well under that.
	if elapsed > 30*time.Second {
		t.Fatalf("Submit took %v to report a version mismatch; it waited out its budget instead of reporting", elapsed)
	}
}

// TestSubmitDoesNotReportAMismatchAgainstItsOwnStandingClaim pins the
// `&& w.isRecorded()` fence in submit.go's IntentInform handler — the one
// thing standing between a healthy submit and an ErrVersionMismatch naming
// the agent's own stale claim, on a branch that never moved.
//
// What makes the fence load-bearing is that readiness is now a STANDING
// claim. After a Nack, the round that displaced this submit resolves and, in
// doing so, names us — at whatever version our sticky claim last recorded,
// which is the OLD one, from before the Nack. That is proof the displacing
// round is over, not a version mismatch to act on, and the only thing that
// tells the two apart is whether the coordinator recorded THIS attempt's
// readiness: an Ack (recorded) means the verdict answers our own round and a
// differing version is real; a Nack, or no answer yet, means it does not.
//
// Why a fake coordinator rather than the real one: the mismatch this guards
// against needs an Ack and a Nack for a SINGLE Ready, which one Coordinator
// never produces — onReady answers each Ready with exactly one of the two.
// Two `pc up` coordinators on one database do produce it (they share a
// cursors row and can both read a batch before either saves it), and that is
// the configuration this reproduces by publishing both replies by hand.
// Publishing directly also keeps the messages off any second subscription
// under the name "billing", which would otherwise compete with Submit's own
// agent for them.
//
// The assertion that catches a deleted fence is the SECOND stale inform, the
// one delivered while attempt 2 is waiting for its acknowledgement. The first
// one cannot catch it: Submit is inside waitForRoundToResolve there, and that
// function's `mismatched` arm treats a mismatch as resolution too, so with or
// without the fence attempt 2 begins either way. (That arm is itself correct
// and deliberate — see its comment — which is exactly how one change on this
// branch came to mask the only plausible test of another.) At the
// acknowledgement wait there is no such arm: `mismatched` returns
// ErrVersionMismatch outright, so the fence is the whole difference, with no
// select race to make the outcome a coin flip.
func TestSubmitDoesNotReportAMismatchAgainstItsOwnStandingClaim(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{DB: db, SubmitTimeout: 45 * time.Second}

	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })

	type result struct {
		v   gate.Verdict
		err error
	}
	done := make(chan result, 1)
	go func() {
		v, err := pcops.Submit(ctx, cfg, "g", "billing", "v2")
		done <- result{v, err}
	}()

	publish := func(m protocol.Message) {
		t.Helper()
		if err := b.Publish(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	coordinator := protocol.Address{Agent: "coordinator"}
	toBilling := protocol.Address{Agent: "billing"}
	toGate := protocol.Address{Topic: gate.Topic("g")}
	// A verdict for a round that tested billing at v1 — the version its
	// standing claim still holds, from before the Nack — while billing has
	// declared v2 and is waiting.
	staleRound := func() protocol.Message {
		return protocol.New(coordinator, toGate, protocol.IntentInform,
			map[string]any{"gate": "g", "passed": true, "text": "g PASSED",
				"versions": map[string]any{"billing": "v1", "gateway": "v1"}})
	}

	// waitForReadies waits until billing has published n IntentReady messages
	// — i.e. until Submit has re-declared readiness n-1 times. It watches the
	// durable log because Submit exposes no hook for "I am back at the
	// acknowledgement wait", and it fails fast if Submit has already returned,
	// so a deleted fence reports itself as the wrong error rather than as a
	// context timeout sixty seconds later.
	waitForReadies := func(n int) {
		t.Helper()
		for {
			select {
			case r := <-done:
				t.Fatalf("Submit returned early (verdict %+v, err %v) while waiting for it to re-declare readiness %d times", r.v, r.err, n)
			default:
			}
			recs, err := b.History(ctx, 0)
			if err != nil {
				t.Fatalf("history: %v", err)
			}
			seen := 0
			for _, r := range recs {
				if r.Msg.Intent == protocol.IntentReady && r.Msg.From.Agent == "billing" {
					seen++
				}
			}
			if seen >= n {
				return
			}
			select {
			case <-time.After(20 * time.Millisecond):
			case <-ctx.Done():
				t.Fatalf("billing published %d Ready messages, want %d", seen, n)
			}
		}
	}

	// Attempt 1: declared, then answered BOTH ways by the two coordinators.
	// The Ack is what sets recorded; the Nack is what must clear it again.
	waitForReadies(1)
	publish(protocol.New(coordinator, toBilling, protocol.IntentAck,
		map[string]any{"gate": "g", "outstanding": []string{"gateway"}, "running": false}))
	publish(protocol.New(coordinator, toBilling, protocol.IntentNack,
		map[string]any{"gate": "g", "testing": map[string]any{"billing": "v1", "gateway": "v1"}}))
	// The displacing round resolves, naming billing at its standing v1. This
	// one is masked by waitForRoundToResolve's mismatched arm; it is here to
	// carry the submit into attempt 2, which is where the fence is visible.
	publish(staleRound())

	// Attempt 2: re-declared at v2 and waiting for an acknowledgement, with
	// recorded false again because declareReady reset it.
	waitForReadies(2)
	// THE ASSERTION. A second round of the other coordinator's resolves,
	// naming billing at the same standing v1. With the fence this is a
	// foreign round and Submit keeps waiting for its own; without it, this is
	// offered as a mismatch and the acknowledgement wait returns
	// ErrVersionMismatch — against a branch that has not moved since v2 was
	// declared, and with the caller's only remedy being to resubmit at a HEAD
	// it is already submitting.
	publish(staleRound())

	select {
	case r := <-done:
		t.Fatalf("Submit returned %+v (err %v) on a verdict naming billing at its own standing v1, for a round it was never recorded in", r.v, r.err)
	case <-time.After(2 * time.Second):
		// Still waiting, correctly.
	}

	// Now answer attempt 2 properly, so the test also proves Submit is still
	// able to finish rather than merely still blocked.
	publish(protocol.New(coordinator, toBilling, protocol.IntentAck,
		map[string]any{"gate": "g", "outstanding": []string{}, "running": true}))
	publish(protocol.New(coordinator, toGate, protocol.IntentInform,
		map[string]any{"gate": "g", "passed": true, "text": "g PASSED",
			"versions": map[string]any{"billing": "v2", "gateway": "v2"}}))

	select {
	case r := <-done:
		if errors.Is(r.err, pcops.ErrVersionMismatch) {
			t.Fatalf("Submit err = %v, want the verdict for the version it declared", r.err)
		}
		if r.err != nil {
			t.Fatalf("Submit: %v", r.err)
		}
		if !r.v.Passed || r.v.Versions["billing"] != "v2" {
			t.Fatalf("Submit returned %+v, want a passing verdict over billing=v2", r.v)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Submit never returned after its own round resolved")
	}
}
