// Package gate coordinates cross-service integration tests over the Conclave
// conversation layer. Participants declare readiness for a named gate; when the
// full required set is ready, a Coordinator asks a designated runner to execute
// the spanning test and broadcasts the verdict. Conclave coordinates the
// handshake — it never runs a test itself.
package gate

import (
	"context"
	"fmt"
	"sync"

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

// Coordinator hosts one or more gates on top of an agent. One agent hosts it;
// that agent must be subscribed to Topic(id) for each registered gate so it
// receives participants' readiness signals.
type Coordinator struct {
	a         *agent.Agent
	mu        sync.Mutex
	gates     map[string]*gateState
	onVerdict func(Verdict)
}

type gateState struct {
	spec     Spec
	mu       sync.Mutex
	ready    map[string]string // participant -> version
	inflight bool              // a run is awaiting the runner's verdict
}

// NewCoordinator wires gate handlers onto an agent.
func NewCoordinator(a *agent.Agent) *Coordinator {
	c := &Coordinator{a: a, gates: make(map[string]*gateState)}
	a.On(protocol.IntentReady, c.onReady)
	a.On(protocol.IntentDone, c.onVerdictMsg)
	a.On(protocol.IntentDisagree, c.onVerdictMsg)
	a.On(protocol.IntentBlock, c.onVerdictMsg)
	return c
}

// Register declares a gate this coordinator will coordinate.
func (c *Coordinator) Register(spec Spec) {
	c.mu.Lock()
	c.gates[spec.ID] = &gateState{spec: spec, ready: make(map[string]string)}
	c.mu.Unlock()
}

// OnVerdict registers a hook called with every resolved verdict. Useful for
// logging and tests.
func (c *Coordinator) OnVerdict(fn func(Verdict)) { c.onVerdict = fn }

func (c *Coordinator) gate(id string) *gateState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gates[id]
}

func (c *Coordinator) onReady(ctx context.Context, _ *agent.Agent, m protocol.Message) *protocol.Message {
	gateID, _ := m.Body["gate"].(string)
	version, _ := m.Body["version"].(string)
	gs := c.gate(gateID)
	if gs == nil {
		return nil
	}
	gs.mu.Lock()
	required := contains(gs.spec.Required, m.From.Agent)
	if required && !gs.inflight {
		gs.ready[m.From.Agent] = version // dedup by participant; last write wins
	}
	full := required && !gs.inflight && len(gs.ready) == len(gs.spec.Required)
	gs.mu.Unlock()
	if full {
		c.open(ctx, gs)
	}
	return nil // ready is terminal
}

func (c *Coordinator) open(ctx context.Context, gs *gateState) {
	gs.mu.Lock()
	gs.inflight = true
	versions := copyMap(gs.ready)
	runner := gs.spec.Runner
	gateID := gs.spec.ID
	gs.mu.Unlock()

	_ = c.a.Send(ctx, protocol.New(
		protocol.Address{Agent: c.a.Name},
		protocol.Address{Agent: runner},
		protocol.IntentRequest,
		map[string]any{"gate": gateID, "versions": versions},
	))
}

func (c *Coordinator) onVerdictMsg(ctx context.Context, _ *agent.Agent, m protocol.Message) *protocol.Message {
	gateID, _ := m.Body["gate"].(string)
	gs := c.gate(gateID)
	if gs == nil {
		return nil
	}
	detail, _ := m.Body["detail"].(string)
	c.resolve(ctx, gs, Verdict{GateID: gateID, Passed: m.Intent == protocol.IntentDone, Detail: detail})
	return nil
}

// resolve finalizes one run: broadcasts the verdict, routes blocks to owners on
// failure, clears readiness (re-arm), and fires the OnVerdict hook. Idempotent
// per run via the inflight flag — a duplicate verdict is a no-op.
func (c *Coordinator) resolve(ctx context.Context, gs *gateState, v Verdict) {
	gs.mu.Lock()
	if !gs.inflight {
		gs.mu.Unlock()
		return
	}
	gs.inflight = false
	if v.Versions == nil {
		v.Versions = copyMap(gs.ready)
	}
	gs.ready = make(map[string]string) // re-arm for the next round
	owners := append([]string(nil), gs.spec.Required...)
	gateID := gs.spec.ID
	gs.mu.Unlock()

	text := fmt.Sprintf("%s PASSED", gateID)
	if !v.Passed {
		text = fmt.Sprintf("%s FAILED: %s", gateID, v.Detail)
	}
	_ = c.a.Send(ctx, protocol.New(
		protocol.Address{Agent: c.a.Name},
		protocol.Address{Topic: Topic(gateID)},
		protocol.IntentInform,
		map[string]any{"text": text, "gate": gateID, "passed": v.Passed},
	))

	if !v.Passed {
		for _, owner := range owners {
			_ = c.a.Send(ctx, protocol.New(
				protocol.Address{Agent: c.a.Name},
				protocol.Address{Agent: owner},
				protocol.IntentBlock,
				map[string]any{"text": fmt.Sprintf("%s gate failing: %s", gateID, v.Detail), "gate": gateID},
			))
		}
	}

	if c.onVerdict != nil {
		c.onVerdict(v)
	}
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

func copyMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
