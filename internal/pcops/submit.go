package pcops

import (
	"context"
	"errors"
	"fmt"
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
		// The broadcast carries one "text" line for both outcomes ("<gate>
		// PASSED" / "<gate> FAILED: <detail>"); Detail is documented as
		// empty on pass, so only surface it on failure.
		var detail string
		if !passed {
			detail, _ = m.Body["text"].(string)
		}
		select {
		case verdicts <- gate.Verdict{GateID: gateID, Passed: passed, Detail: detail}:
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

	select {
	case v := <-verdicts:
		return v, nil
	case <-ctx.Done():
		return gate.Verdict{}, ErrNoVerdict
	}
}
