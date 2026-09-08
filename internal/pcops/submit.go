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
		versions := versionsFromBody(m.Body["versions"])
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
		select {
		case acked <- outstandingFromBody(m.Body["outstanding"]):
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
		select {
		case nacked <- versionsFromBody(m.Body["testing"]):
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
			if !waitForRoundToResolve(ctx, declined, verdicts) {
				return gate.Verdict{}, ErrNoVerdict
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
		case testing := <-nacked:
			fmt.Fprintf(os.Stderr, "pc submit: gate %q is mid-round (testing %s); waiting for it to finish\n",
				gateID, describeVersions(testing))
			if !waitForRoundToResolve(ctx, declined, verdicts) {
				return gate.Verdict{}, ErrNoVerdict
			}
			continue
		case <-ctx.Done():
			return gate.Verdict{}, ErrNoVerdict
		}
	}
}

// waitForRoundToResolve blocks until the in-flight round that displaced our
// readiness has resolved, reported by a verdict we declined. It returns false
// when the context ended first. A verdict that DOES match ours can still
// arrive here — a race we win — so it is drained into verdicts' buffer for the
// caller's next select rather than dropped.
func waitForRoundToResolve(ctx context.Context, declined <-chan struct{}, verdicts chan gate.Verdict) bool {
	select {
	case <-declined:
		return true
	case v := <-verdicts:
		select {
		case verdicts <- v:
		default:
		}
		return true
	case <-ctx.Done():
		return false
	}
}

// describeVersions renders a version set for one line of operator output,
// sorted because Go's map range order is randomised per iteration.
func describeVersions(vs map[string]string) string {
	if len(vs) == 0 {
		return "an unreported version set"
	}
	parts := make([]string, 0, len(vs))
	for agent, v := range vs {
		parts = append(parts, agent+"@"+abbrevVersion(v))
	}
	sort.Strings(parts)
	return strings.Join(parts, ", ")
}

// outstandingFromBody coerces a wire "outstanding" value into []string,
// accepting both the in-memory []string (pkg/gate builds it that way
// directly, and the in-memory bus passes values through unchanged) and a
// JSON []any (what pkg/bus/sqlite delivers after a round trip through the
// database). Mirrors gate.go's versionsFromBody for the same reason.
func outstandingFromBody(v any) []string {
	switch vv := v.(type) {
	case []string:
		return vv
	case []any:
		out := make([]string, 0, len(vv))
		for _, e := range vv {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}
