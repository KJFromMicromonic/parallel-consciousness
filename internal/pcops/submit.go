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
	ctx, cancel := context.WithTimeout(ctx, cfg.SubmitTimeout)
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

	verdicts := make(chan gate.Verdict, 1)
	a.On(protocol.IntentInform, func(_ context.Context, _ *agent.Agent, m protocol.Message) *protocol.Message {
		if id, _ := m.Body["gate"].(string); id != gateID {
			return nil
		}
		passed, ok := m.Body["passed"].(bool)
		if !ok {
			return nil // not a verdict broadcast
		}
		detail, _ := m.Body["text"].(string)
		select {
		case verdicts <- gate.Verdict{GateID: gateID, Passed: passed, Detail: detail}:
		default:
		}
		return nil
	})
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
