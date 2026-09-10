package pcops

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

// ErrNoVerdict means the spanning test never reported. Callers must keep this
// distinct from a failing verdict: "did not run" and "ran and failed" demand
// different responses from an agent.
var ErrNoVerdict = errors.New("pcops: no verdict before timeout")

// ErrNotAcknowledged means readiness was declared but nothing acknowledged
// it within AckTimeout. Callers must keep this distinct from ErrNoVerdict:
// "the gate hasn't run yet" (a live coordinator that just hasn't reached
// quorum) and "nothing is listening at all" are different failures that
// demand different responses — the F4 finding in
// docs/superpowers/specs/2026-09-02-live-fire-findings.md is exactly this
// confusion, observed live as an 8-minute silent block with a dead
// coordinator and, separately, a 7m45s wait for a peer that was never
// coming. gate.go's onReady now acks every readiness it records for exactly
// this reason.
var ErrNotAcknowledged = errors.New("pcops: gate did not acknowledge readiness")

// AckTimeout bounds how long Submit waits for the coordinator's IntentAck
// after declaring readiness, before concluding no coordinator is listening.
// This is deliberately much shorter than SubmitTimeout: it tolerates a
// coordinator that is merely slow to start, while still turning a genuinely
// missing one into a diagnosis in seconds rather than the minutes a live run
// actually lost to silence (see ErrNotAcknowledged and gate.go's onReady).
const AckTimeout = 10 * time.Second

// Submit declares readiness for a gate and blocks until the coordinator
// broadcasts a verdict for it.
//
// This is the whole harness-agnostic contract: a gate id, an opaque version, an
// agent name. Any tool that can run a shell command can participate.
func Submit(ctx context.Context, cfg Config, gateID, agentName, version string) (gate.Verdict, error) {
	// The headline correctness guard this branch exists to deliver is
	// versions[agentName] != version below. protocol.Versions returns "" for
	// a participant absent from a verdict's Versions map, so an empty
	// version here would compare "" against "" and PASS that guard for a
	// verdict that never tested this agent at all. `pc submit` cannot reach
	// this (resolveVersion errors rather than returning a placeholder), but
	// Submit is a package-level function any caller can reach directly, and
	// this is the one input that silently turns the guard off.
	if version == "" {
		return gate.Verdict{}, fmt.Errorf("pcops: version must not be empty")
	}
	// A zero SubmitTimeout means "unset", not "already expired": callers that
	// build a Config by hand (and every test that does) would otherwise get an
	// instantly cancelled context and ErrNoVerdict.
	timeout := cfg.SubmitTimeout
	if timeout <= 0 {
		timeout = DefaultSubmitTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	b, err := sqlite.Open(ctx, cfg.DB, sqlite.WithPollInterval(25*time.Millisecond))
	if err != nil {
		return gate.Verdict{}, fmt.Errorf("open bus: %w", err)
	}
	defer b.Close()

	a, err := agent.New(ctx, b, agentName, []string{gate.Topic(gateID)})
	if err != nil {
		return gate.Verdict{}, fmt.Errorf("join as %q: %w", agentName, err)
	}

	// readyAt is set immediately before each gate.Ready below and fences off
	// every verdict from a previous round. The gate id plus a `passed` bool
	// does not identify a round, and pkg/bus/sqlite resumes a subscription
	// from the STORED cursor whenever a row exists for this agent name — so
	// an earlier round's Inform can still be sitting unread in the log and
	// would be returned instantly as this round's answer. Two paths reach
	// that state: a PASSING verdict routes no blocks, so nothing advances the
	// courier past its Inform; and an agent with no in-process courier only
	// ever has short-lived submit processes, whose `defer b.Close()` makes
	// the poller skip saveCursor entirely. protocol.New stamps Timestamp, so
	// the round boundary is simply time.
	//
	// readyAt must be race-safe: the attempt loop below re-declares readiness
	// (and so re-assigns readyAt) on every retry after a Nack, from the same
	// goroutine that calls Submit, while the IntentInform handler reads it
	// from the agent's own dispatch goroutine. A bare variable written once
	// before go a.Run(ctx) was safe by construction (the goroutine start is
	// itself a happens-before edge); re-assigning it per attempt after that
	// point is a data race without a mutex.
	var (
		readyMu sync.Mutex
		readyAt time.Time
	)
	readAt := func() time.Time { readyMu.Lock(); defer readyMu.Unlock(); return readyAt }
	setReadyAt := func(t time.Time) { readyMu.Lock(); readyAt = t; readyMu.Unlock() }

	// declined fires when a verdict for this gate arrives that did NOT test
	// this agent's version. After a Nack, that is exactly the proof the
	// in-flight round that displaced our readiness has resolved — which is
	// when re-declaring readiness can succeed.
	declined := make(chan struct{}, 1)

	verdicts := make(chan gate.Verdict, 1)
	a.On(protocol.IntentInform, func(_ context.Context, _ *agent.Agent, m protocol.Message) *protocol.Message {
		if id, _ := m.Body["gate"].(string); id != gateID {
			return nil
		}
		passed, ok := m.Body["passed"].(bool)
		if !ok {
			return nil // not a verdict broadcast
		}
		if m.Timestamp.Before(readAt()) {
			return nil // a previous round's verdict, replayed from the cursor
		}
		// A gate id and a passed bool do not identify a round. Accept a verdict
		// only when it says it tested THIS agent at exactly the version submitted.
		// pkg/gate drops a readiness that arrives while a round is already in
		// flight, so without this guard an agent would accept the in-flight
		// round's verdict — computed entirely without its version.
		versions := protocol.Versions(m.Body["versions"])
		if versions[agentName] != version {
			// This verdict resolved the in-flight round that displaced our own
			// readiness (see the Nack handler below) — it is proof that round
			// is done, which is exactly what waitForRoundToResolve is waiting
			// for, so a re-declared readiness can now succeed.
			select {
			case declined <- struct{}{}:
			default:
			}
			return nil
		}
		// The broadcast carries one "text" line for both outcomes ("<gate>
		// PASSED" / "<gate> FAILED: <detail>"); Detail is documented as
		// empty on pass, so only surface it on failure.
		var detail string
		if !passed {
			detail, _ = m.Body["text"].(string)
		}
		select {
		case verdicts <- gate.Verdict{GateID: gateID, Passed: passed, Detail: detail, Versions: versions}:
		default:
		}
		return nil
	})
	// acked carries the coordinator's IntentAck for THIS gate, decoded to the
	// required participants it is still waiting on. See gate.go's onReady:
	// every readiness it records gets one of these back, which is F4's whole
	// fix — a blocked submit gets a prompt, positive signal instead of total
	// silence. Buffered 1 like verdicts, for the same reason: the handler
	// must never block the agent's dispatch loop on a send nobody is
	// reading yet.
	acked := make(chan []string, 1)
	a.On(protocol.IntentAck, func(_ context.Context, _ *agent.Agent, m protocol.Message) *protocol.Message {
		if id, _ := m.Body["gate"].(string); id != gateID {
			return nil
		}
		// An Ack is a direct reply, exactly like the Nack below, so it sits on
		// the same replay path the Inform handler guards against above: a
		// submit process that was killed after being acked (or nacked) but
		// before exiting cleanly leaves that reply sitting unread, and
		// pkg/bus/sqlite resumes the next submit under this agent name from
		// the stored cursor, handing it straight back as if it were fresh. An
		// unfenced stale Ack here would satisfy the acknowledgement wait for a
		// round that already ended, then block on the verdict wait until ctx
		// expires — the same ErrNoVerdict-instead-of-ErrNotAcknowledged
		// misdiagnosis Ruling 1 exists to prevent, just arriving from this
		// handler instead of the timer.
		if m.Timestamp.Before(readAt()) {
			return nil // a previous round's ack, replayed from the cursor
		}
		select {
		case acked <- protocol.Strings(m.Body["outstanding"]):
		default:
		}
		return nil
	})
	// nacked carries the coordinator's IntentNack: this readiness was dropped
	// because a round was already in flight, and the versions that round is
	// testing instead. IntentNack for the same reason as IntentAck — the
	// courier registers no handler for it, so it reaches this subscription
	// and is never forwarded into a live agent session.
	nacked := make(chan map[string]string, 1)
	a.On(protocol.IntentNack, func(_ context.Context, _ *agent.Agent, m protocol.Message) *protocol.Message {
		if id, _ := m.Body["gate"].(string); id != gateID {
			return nil
		}
		// A Nack is a direct reply too, so it is just as exposed to a lagging
		// cursor as the Inform and Ack handlers: a submit killed right after
		// being nacked (F2 measured models doing exactly this) leaves the
		// Nack unread, and the next submit under this agent name is handed it
		// back as its first message. Without this fence that stale Nack would
		// print a false "gate is mid-round" line and enter
		// waitForRoundToResolve for a round that is not this attempt's round
		// at all — and that wait has no AckTimeout bound, so with no
		// coordinator left to signal declined it would block the full
		// SubmitTimeout instead of failing fast with ErrNotAcknowledged.
		if m.Timestamp.Before(readAt()) {
			return nil // a previous round's nack, replayed from the cursor
		}
		select {
		case nacked <- protocol.Versions(m.Body["testing"]):
		default:
		}
		return nil
	})
	go a.Run(ctx)

	// Retry lives here rather than being returned to the caller. A Nack means
	// this readiness was dropped, so waiting is futile until the in-flight
	// round resolves — but handing that retry to a model is worse: F2
	// measured six minutes of redundant submits against a fourteen-second
	// loop. Keeping it inside the tool also keeps the three documented exit
	// codes meaningful instead of adding a fourth outcome for an agent to
	// mishandle.
	for {
		setReadyAt(time.Now())
		// declined, acked and nacked are all cross-attempt state: any of the
		// three can hold a stale signal from a previous attempt's reply,
		// buffered here (capacity 1) before this attempt's own Ready is even
		// declared. TestSubmitDeclinesAVerdictThatDidNotIncludeIt is exactly
		// this for declined: a foreign-version verdict can leave a stale
		// signal sitting here long before any Nack exists. The three
		// handlers apply the readAt() fence uniformly against replay from
		// the cursor, but a fence on arrival does not drain what an earlier
		// attempt already buffered — so drain all three before declaring a
		// fresh Ready, not just declined.
		select {
		case <-declined:
		default:
		}
		select {
		case <-acked:
		default:
		}
		select {
		case <-nacked:
		default:
		}
		if err := gate.Ready(ctx, a, gateID, version); err != nil {
			return gate.Verdict{}, fmt.Errorf("declare ready: %w", err)
		}

		// First wait for the acknowledgement — bounded by AckTimeout, not the
		// full submit timeout, so a missing coordinator is diagnosed in
		// seconds. A verdict is also accepted here and satisfies the wait
		// outright: the F2 cache path (see gate.go's gateState doc comment)
		// answers a redundant identical resubmit straight from a remembered
		// verdict without recording readiness at all, so no ack is ever sent
		// for it. Treating "verdict arrived" as "acknowledged" is what keeps
		// that path from regressing into a spurious ErrNotAcknowledged.
		select {
		case outstanding := <-acked:
			if len(outstanding) > 0 {
				// Surfaced directly here, not threaded back through the return
				// value: Submit's signature is a fixed public contract (just a
				// verdict and an error), and this is informational only — it
				// does not change what Submit ultimately returns. A caller
				// blocked on `pc submit` sees this on the process's own stderr
				// the moment the coordinator responds, which is exactly the
				// "waiting on a peer, not on nothing" signal F4 is about.
				fmt.Fprintf(os.Stderr, "pc submit: gate %q acknowledged; still waiting on %s\n",
					gateID, strings.Join(outstanding, ", "))
			}
		case testing := <-nacked:
			fmt.Fprintf(os.Stderr, "pc submit: gate %q is mid-round (testing %s); waiting for it to finish\n",
				gateID, describeVersions(testing))
			res := waitForRoundToResolve(ctx, declined, verdicts)
			if !res.resolved {
				return gate.Verdict{}, ErrNoVerdict
			}
			if res.hasVerdict {
				return res.verdict, nil
			}
			continue
		case v := <-verdicts:
			return v, nil
		case <-time.After(AckTimeout):
			return gate.Verdict{}, fmt.Errorf("gate %q: %w", gateID, ErrNotAcknowledged)
		case <-ctx.Done():
			return gate.Verdict{}, ErrNoVerdict
		}

		// Acknowledged: wait for the verdict that tested this version.
		select {
		case v := <-verdicts:
			return v, nil
		// A Nack arriving here, for an attempt that was already acked, looks
		// impossible from onReady's logic alone: it answers one Ready with
		// exactly one of {ack, nack}, never both. But that mutual exclusion
		// is a property of the coordinator's in-process logic, not of
		// delivery over a durable, replayable log — and delivery guarantees
		// are not this code's assumption to make. Two live paths reach here
		// regardless: a stale Nack replayed from a lagging cursor (the fence
		// above rejects a stale nack for THIS agent's own earlier attempt,
		// but not one whose timestamp happens to postdate readAt() while
		// still answering a round this attempt no longer cares about); and
		// two coordinators on one database, since nothing prevents a second
		// `pc up` from also joining as "coordinator" — both subscribe to the
		// same topic and share one cursor row, and either can answer the same
		// Ready differently. Keep this arm.
		case testing := <-nacked:
			fmt.Fprintf(os.Stderr, "pc submit: gate %q is mid-round (testing %s); waiting for it to finish\n",
				gateID, describeVersions(testing))
			res := waitForRoundToResolve(ctx, declined, verdicts)
			if !res.resolved {
				return gate.Verdict{}, ErrNoVerdict
			}
			if res.hasVerdict {
				return res.verdict, nil
			}
			continue
		case <-ctx.Done():
			return gate.Verdict{}, ErrNoVerdict
		}
	}
}

// roundResolution reports how waitForRoundToResolve concluded. resolved is
// false only when ctx ended before the in-flight round did. hasVerdict is
// set when the round resolved WITH a verdict that answers this attempt
// directly (the race-won case below) — the caller must return it as-is
// rather than looping back to re-declare readiness, which would publish a
// second, spurious gate.Ready at a version the round already resolved.
type roundResolution struct {
	verdict    gate.Verdict
	hasVerdict bool
	resolved   bool
}

// waitForRoundToResolve blocks until the in-flight round that displaced our
// readiness has resolved. Two things can report that: declined, signalled by
// a verdict for this gate that did not test our version (proof the round is
// done, with nothing further to hand back); or verdicts, when the verdict
// that resolves the round happens to be OUR OWN — a race this attempt wins
// outright, since there is nothing left to wait for. Unlike an earlier
// version of this function, that verdict is returned to the caller rather
// than re-buffered into verdicts and left for the loop to pick up on its next
// iteration: re-declaring readiness after a verdict already answered this
// attempt would publish a redundant gate.Ready at the same version, and in
// gate.go that hits the F2 cache and replays another resolve — another
// broadcast, another block fanout on failure, another OnVerdict, and (via
// pcops.Run's round counter) a run that can be failed a round early by a
// purely spurious cache replay.
func waitForRoundToResolve(ctx context.Context, declined <-chan struct{}, verdicts chan gate.Verdict) roundResolution {
	select {
	case <-declined:
		return roundResolution{resolved: true}
	case v := <-verdicts:
		return roundResolution{verdict: v, hasVerdict: true, resolved: true}
	case <-ctx.Done():
		return roundResolution{}
	}
}

// describeVersions renders a version set for one line of operator output,
// sorted because Go's map range order is randomised per iteration.
func describeVersions(vs map[string]string) string {
	if len(vs) == 0 {
		return "an unreported version set"
	}
	parts := make([]string, 0, len(vs))
	for name, v := range vs {
		parts = append(parts, name+"@"+abbrevVersion(v))
	}
	sort.Strings(parts)
	return strings.Join(parts, ", ")
}
