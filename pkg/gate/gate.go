// Package gate coordinates cross-service integration tests over the Parallel Consciousness
// conversation layer. Participants declare readiness for a named gate; when the
// full required set is ready, a Coordinator asks a designated runner to execute
// the spanning test and broadcasts the verdict. Parallel Consciousness coordinates the
// handshake — it never runs a test itself.
package gate

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
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
// more. A future `pc ready --gate G --version V` CLI maps 1:1 onto this.
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
		// NOTE: in-memory bus only — it passes Body by reference, so versions is
		// a map[string]string. A serializing transport (JSON/Kafka) would deliver
		// map[string]any here; revisit when the transport adapter lands.
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
	timeout   time.Duration
	mu        sync.Mutex
	gates     map[string]*gateState
	onVerdict func(Verdict)
}

type gateState struct {
	spec     Spec
	mu       sync.Mutex
	ready    map[string]string // participant -> version
	inflight bool              // a run is awaiting the runner's verdict
	gen      int               // bumped on each resolution; invalidates stale timers
}

// NewCoordinator wires gate handlers onto an agent.
func NewCoordinator(a *agent.Agent) *Coordinator {
	c := &Coordinator{a: a, timeout: 5 * time.Second, gates: make(map[string]*gateState)}
	a.On(protocol.IntentReady, c.onReady)
	a.On(protocol.IntentDone, c.onVerdictMsg)
	a.On(protocol.IntentDisagree, c.onVerdictMsg)
	a.On(protocol.IntentBlock, c.onVerdictMsg)
	return c
}

// SetRunnerTimeout sets how long the coordinator waits for a runner's verdict
// before declaring the gate stalled. Default 5s.
func (c *Coordinator) SetRunnerTimeout(d time.Duration) {
	c.mu.Lock()
	c.timeout = d
	c.mu.Unlock()
}

func (c *Coordinator) runnerTimeout() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.timeout
}

// Register declares a gate this coordinator will coordinate.
func (c *Coordinator) Register(spec Spec) {
	c.mu.Lock()
	c.gates[spec.ID] = &gateState{spec: spec, ready: make(map[string]string)}
	c.mu.Unlock()
}

// OnVerdict registers a hook called with every resolved verdict. Useful for
// logging and tests. Guarded by c.mu so it is safe to set alongside the other
// configuration setters; the hook is read via verdictHook from resolve.
func (c *Coordinator) OnVerdict(fn func(Verdict)) {
	c.mu.Lock()
	c.onVerdict = fn
	c.mu.Unlock()
}

func (c *Coordinator) verdictHook() func(Verdict) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.onVerdict
}

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
	gen := gs.gen
	versions := copyMap(gs.ready)
	runner := gs.spec.Runner
	gateID := gs.spec.ID
	gs.mu.Unlock()

	timeout := c.runnerTimeout()
	req := protocol.New(
		protocol.Address{Agent: c.a.Name},
		protocol.Address{Agent: runner},
		protocol.IntentRequest,
		map[string]any{"gate": gateID, "versions": versions},
	)
	req.Deadline = req.Timestamp.Add(timeout)
	_ = c.a.Send(ctx, req)

	time.AfterFunc(timeout, func() {
		gs.mu.Lock()
		stale := gs.gen != gen || !gs.inflight
		gs.mu.Unlock()
		if stale {
			return // this run already resolved; ignore
		}
		c.resolve(ctx, gs, Verdict{GateID: gateID, Passed: false, Detail: "runner unresponsive"}, true)
	})
}

func (c *Coordinator) onVerdictMsg(ctx context.Context, _ *agent.Agent, m protocol.Message) *protocol.Message {
	gateID, _ := m.Body["gate"].(string)
	gs := c.gate(gateID)
	if gs == nil {
		return nil
	}
	detail, _ := m.Body["detail"].(string)
	c.resolve(ctx, gs, Verdict{GateID: gateID, Passed: m.Intent == protocol.IntentDone, Detail: detail}, false)
	return nil
}

func (c *Coordinator) resolve(ctx context.Context, gs *gateState, v Verdict, stalled bool) {
	gs.mu.Lock()
	if !gs.inflight {
		gs.mu.Unlock()
		return
	}
	gs.inflight = false
	gs.gen++ // invalidate any pending timer for this run
	if v.Versions == nil {
		v.Versions = copyMap(gs.ready)
	}
	gs.ready = make(map[string]string) // re-arm for the next round
	owners := append([]string(nil), gs.spec.Required...)
	gateID := gs.spec.ID
	gs.mu.Unlock()

	var text string
	switch {
	case stalled:
		text = fmt.Sprintf("%s STALLED: runner unresponsive", gateID)
	case v.Passed:
		text = fmt.Sprintf("%s PASSED", gateID)
	default:
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

	if hook := c.verdictHook(); hook != nil {
		hook(v)
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
