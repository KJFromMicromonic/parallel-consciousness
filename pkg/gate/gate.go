// Package gate coordinates cross-service integration tests over the Conclave
// conversation layer. Participants declare readiness for a named gate; when the
// full required set is ready, a Coordinator asks a designated runner to execute
// the spanning test and broadcasts the verdict. Conclave coordinates the
// handshake — it never runs a test itself.
package gate

import (
	"context"

	"github.com/yourname/conclave/pkg/agent"
	"github.com/yourname/conclave/pkg/protocol"
)

// Topic returns the bus topic a gate's signals ride on. The agent hosting a
// Coordinator must be subscribed to this topic for each gate it coordinates.
func Topic(gateID string) string { return "gate." + gateID }

// Spec defines one gate: a spanning test across services.
type Spec struct {
	ID       string   // e.g. "checkout"
	Required []string // participant agent names whose readiness is needed
	Runner   string   // agent designated to run the spanning test
}

// Verdict is what a runner reports back and what the coordinator broadcasts.
type Verdict struct {
	GateID   string
	Passed   bool
	Detail   string            // failing test name / error; empty on pass
	Versions map[string]string // participant -> version that was tested
}

// Ready declares that the calling agent is at a compatible state for a gate.
// Harness-agnostic by design: a gate id and an opaque version string, nothing
// more. A future `conclave ready --gate G --version V` CLI maps 1:1 onto this.
func Ready(ctx context.Context, a *agent.Agent, gateID, version string) error {
	return a.Send(ctx, protocol.New(
		protocol.Address{Agent: a.Name},
		protocol.Address{Topic: Topic(gateID)},
		protocol.IntentReady,
		map[string]any{"gate": gateID, "version": version},
	))
}

// ServeRunner registers fn as the test executor on a runner agent. When a gate
// opens, the coordinator sends the runner an IntentRequest carrying the gate id
// and the participating versions; fn runs the spanning test however it likes and
// returns a Verdict. This callback is the only place a test is executed —
// pkg/gate itself never shells out.
func ServeRunner(a *agent.Agent, fn func(ctx context.Context, gateID string, versions map[string]string) Verdict) {
	a.On(protocol.IntentRequest, func(ctx context.Context, ag *agent.Agent, m protocol.Message) *protocol.Message {
		gateID, _ := m.Body["gate"].(string)
		if gateID == "" {
			return nil // not a gate request; ignore
		}
		versions, _ := m.Body["versions"].(map[string]string)
		v := fn(ctx, gateID, versions)
		intent := protocol.IntentDone
		if !v.Passed {
			intent = protocol.IntentDisagree
		}
		reply := m.Reply(protocol.Address{Agent: ag.Name}, intent, map[string]any{
			"gate":   gateID,
			"detail": v.Detail,
		})
		return &reply
	})
}
