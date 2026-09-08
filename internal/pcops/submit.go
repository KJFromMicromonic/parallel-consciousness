package pcops

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
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

	// readyAt is captured before gate.Ready below and fences off every verdict
	// from a previous round. The gate id plus a `passed` bool does not identify
	// a round, and pkg/bus/sqlite resumes a subscription from the STORED cursor
	// whenever a row exists for this agent name — so an earlier round's Inform
	// can still be sitting unread in the log and would be returned instantly as
	// this round's answer. Two paths reach that state: a PASSING verdict routes
	// no blocks, so nothing advances the courier past its Inform; and an agent
	// with no in-process courier only ever has short-lived submit processes,
	// whose `defer b.Close()` makes the poller skip saveCursor entirely.
	// protocol.New stamps Timestamp, so the round boundary is simply time.
	var readyAt time.Time

	verdicts := make(chan gate.Verdict, 1)
	a.On(protocol.IntentInform, func(_ context.Context, _ *agent.Agent, m protocol.Message) *protocol.Message {
		if id, _ := m.Body["gate"].(string); id != gateID {
			return nil
		}
		passed, ok := m.Body["passed"].(bool)
		if !ok {
			return nil // not a verdict broadcast
		}
		if m.Timestamp.Before(readyAt) {
			return nil // a previous round's verdict, replayed from the cursor
		}
		// A gate id and a passed bool do not identify a round. Accept a verdict
		// only when it says it tested THIS agent at exactly the version submitted.
		// pkg/gate drops a readiness that arrives while a round is already in
		// flight, so without this guard an agent would accept the in-flight
		// round's verdict — computed entirely without its version.
		versions := versionsFromBody(m.Body["versions"])
		if versions[agentName] != version {
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
	// Set before a.Run starts, never after: handlers only run from that
	// goroutine, so starting it after the write is what publishes readyAt to
	// them without a mutex. It is still the instant before Ready, and the
	// coordinator cannot broadcast this round's verdict before it sees Ready.
	readyAt = time.Now()
	go a.Run(ctx)

	if err := gate.Ready(ctx, a, gateID, version); err != nil {
		return gate.Verdict{}, fmt.Errorf("declare ready: %w", err)
	}

	// First wait for the acknowledgement — bounded by AckTimeout, not the
	// full submit timeout, so a missing coordinator is diagnosed in seconds.
	// A verdict is also accepted here and satisfies the wait outright: the
	// F2 cache path (see gate.go's gateState doc comment) answers a
	// redundant identical resubmit straight from a remembered verdict
	// without recording readiness at all, so no ack is ever sent for it.
	// Treating "verdict arrived" as "acknowledged" is what keeps that path
	// from regressing into a spurious ErrNotAcknowledged.
	ackTimer := time.NewTimer(AckTimeout)
	defer ackTimer.Stop()
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
	case v := <-verdicts:
		return v, nil
	case <-ackTimer.C:
		return gate.Verdict{}, fmt.Errorf("gate %q: %w", gateID, ErrNotAcknowledged)
	case <-ctx.Done():
		return gate.Verdict{}, ErrNoVerdict
	}

	select {
	case v := <-verdicts:
		return v, nil
	case <-ctx.Done():
		return gate.Verdict{}, ErrNoVerdict
	}
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
