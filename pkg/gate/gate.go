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
		// Transport-safe: the in-memory bus passes versions as map[string]string;
		// a JSON transport (e.g. pkg/bus/sqlite) delivers map[string]any.
		versions := protocol.Versions(m.Body["versions"])
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

	// lastVerdict and lastStalled remember the most recently resolved round:
	// who was tested at which version (Verdict.Versions), whether it passed,
	// and whether that resolution came from a stalled runner. onReady
	// consults this before recording a new readiness signal: a participant
	// re-submitting at the identical version a completed round already
	// covered is asking "is my version good?", and the honest, immediate
	// answer is the recorded verdict — not silence until submit_timeout,
	// which a quorum that can never re-form would otherwise guarantee.
	//
	// The remembered round is replayed through resolve (see onReady), the
	// same path a freshly-computed verdict takes — not delivered by some
	// separate, bespoke message. An earlier version of this mechanism sent a
	// direct IntentInform straight to the resubmitting sender instead. That
	// looked correct in isolation, but internal/pcops's courier forwards any
	// non-block intent addressed to an agent into that agent's live session
	// (the same path that carries peer `pc send` traffic) — so the "answer"
	// was also delivered to the courier, which nudged the session, which
	// resubmitted, which produced another cached answer, forwarded again,
	// forever. Because that direct send bypassed resolve, the round-cap
	// counter (driven by OnVerdict) never advanced either, so nothing bounded
	// it: an unbounded busy loop, worse than the multi-minute hang it was
	// meant to fix. Routing through resolve keeps every existing consumer of
	// a round's outcome — the topic broadcast, the failure blocks, and the
	// OnVerdict hook that round-caps a stuck participant — exactly as they
	// are for a fresh round, so a repeat offender is still bounded.
	//
	// A second design mistake, also caught in a live run: the guard that
	// decides whether the cache still applies must not compare only the
	// asking participant's own version. An earlier version did exactly
	// that, and one participant's fresh, passing resubmit was ignored
	// because the OTHER, unchanged participant's resubmit was answered from
	// a now-stale FAILED verdict — the cache never noticed the first
	// participant had moved on, because nothing prompted it to look. See
	// onReady for the sticky-invalidation fix and the exact sequence that
	// exposed it.
	//
	// Known limitation: this means a gate cannot be deliberately re-run on an
	// unchanged version — e.g. to retry a flaky spanning test — since the
	// recorded verdict wins instead. That is the right default; a forced
	// re-run would need an explicit opt-in, which is not implemented here.
	lastVerdict *Verdict
	lastStalled bool
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
	// Not in flight, a verdict is remembered, and this required participant
	// is exactly where the remembered round left it: resolve a synthetic
	// round from the remembered verdict instead of recording new readiness.
	// Exact string equality on purpose — any change since (even an
	// uncommitted one that changed the version string) means this is a new
	// state that has never been tested.
	//
	// A remembered verdict is only a valid answer to ANY participant when
	// nothing relevant has changed since the round that produced it — not
	// merely when the asking participant's own version is unchanged. A live
	// run exposed the earlier, submitter-only comparison: participant A
	// fixed its half and resubmitted a new version, then participant B
	// resubmitted its own unchanged version and was wrongly answered with
	// the pre-fix FAILED verdict from cache, because the guard never
	// noticed A had already moved on. Since the cached path skips recording
	// readiness, A's new version was then simply never picked up — the gate
	// stayed stuck on a stale verdict until the participants worked around
	// it out of band. See gateState's doc comment for the full account.
	//
	// The fix is sticky invalidation: the moment ANY required participant's
	// version diverges from what the remembered round tested for it, the
	// remembered verdict is discarded outright (not just skipped for that
	// one sender), and stays discarded until the next genuine resolve
	// stores a fresh one. That is simpler than comparing whole version sets
	// on every submit, and it is what makes B's very next, unchanged
	// resubmit fall through to ordinary readiness recording instead of a
	// second stale answer.
	if !gs.inflight && gs.lastVerdict != nil && required {
		if tested, ok := gs.lastVerdict.Versions[m.From.Agent]; ok && tested == version {
			cached := *gs.lastVerdict
			stalled := gs.lastStalled
			// Mirror what open does to gs.inflight before handing off to
			// resolve: resolve requires it (its guard would otherwise
			// silently no-op this synthetic round) and it also blocks any
			// readiness recorded concurrently from folding into it.
			gs.inflight = true
			gs.mu.Unlock()
			c.resolve(ctx, gs, cached, stalled, true)
			// No IntentAck on this path, deliberately: the cache answers via
			// resolve's own IntentInform broadcast without ever recording
			// readiness, so there is nothing here for an ack to confirm. F4
			// (see below) only concerns itself with recorded readiness;
			// pcops.Submit treats a verdict's arrival as satisfying its ack
			// wait too, precisely so a redundant identical resubmit answered
			// from cache is never mistaken for an unacknowledged gate.
			return nil
		}
		gs.lastVerdict = nil
	}

	// recorded is whether THIS call adds m.From.Agent to the round's
	// readiness set — the same condition that used to gate the assignment
	// below, now also gating the F4 acknowledgement so the two can never
	// drift apart.
	recorded := required && !gs.inflight
	if recorded {
		gs.ready[m.From.Agent] = version // dedup by participant; last write wins
	}
	full := recorded && len(gs.ready) == len(gs.spec.Required)

	var reply *protocol.Message
	if recorded {
		// F4: acknowledge every recorded readiness with who the gate is
		// still waiting on. Without this, a blocked `pc submit` cannot tell
		// "the gate hasn't run yet" from "no coordinator is running at all"
		// from "a required peer is never coming" — all three looked
		// identical (total silence) in a live run against real coding
		// agents; see docs/superpowers/specs/2026-09-02-live-fire-findings.md,
		// findings F4 and "F4 reinforced". outstandingFor reads gs.ready,
		// which by construction already includes m.From.Agent, so a
		// quorum-completing submit correctly gets back an empty list.
		//
		// IntentAck, not e.g. IntentInform or a direct reply, is the
		// deliberate choice: internal/pcops's courier only registers
		// handlers for IntentBlock and the six sendable intents (inform,
		// request, propose, agree, disagree, done) — nothing forwards
		// IntentAck into a live coding-agent session, so this reaches only
		// pcops.Submit's own bus subscription. That matters because an
		// earlier design in this project answered a cached verdict with a
		// direct send instead of routing through resolve; the courier
		// forwarded THAT straight into the agent's session, the agent
		// reacted by resubmitting, and that produced an unbounded feedback
		// loop (2,589 goroutines in 8 seconds — see gateState's doc comment
		// above). Do not add a courier handler for IntentAck: that would
		// reopen exactly that loop for this acknowledgement.
		outstanding := outstandingFor(gs)
		ack := m.Reply(protocol.Address{Agent: c.a.Name}, protocol.IntentAck, map[string]any{
			"gate":        gateID,
			"outstanding": outstanding,
		})
		reply = &ack
	} else if required {
		// The readiness was dropped because a round is already in flight.
		// Saying so turns an indistinguishable silence into a signal: a
		// blocked `pc submit` can otherwise not tell "the gate has not run
		// yet" from "no coordinator is running" from "a required peer is
		// never coming".
		//
		// This deliberately does NOT carry outstandingFor(gs). open leaves
		// gs.ready intact for the whole round (only resolve clears it), so on
		// this path outstandingFor always returns an empty slice — which reads
		// as "waiting on nobody", the opposite of what is happening. What is
		// actually useful is the version set the in-flight round is testing
		// instead of this submitter's.
		//
		// IntentNack for the same reason as the IntentAck above: the courier
		// registers no handler for it, so it reaches pcops.Submit's own
		// subscription and never gets forwarded into a live agent session.
		// Do not add a courier handler for it.
		nack := m.Reply(protocol.Address{Agent: c.a.Name}, protocol.IntentNack, map[string]any{
			"gate":    gateID,
			"testing": copyMap(gs.ready),
		})
		reply = &nack
	}
	gs.mu.Unlock()
	if full {
		c.open(ctx, gs)
	}
	// Returning reply (rather than nil, and rather than sending it here
	// ourselves) lets the caller's normal dispatch mechanism publish it —
	// ready itself stays terminal from the sender's point of view; this is
	// just the coordinator choosing to speak up when it used to stay silent.
	return reply
}

// outstandingFor lists the required participants that have not yet declared
// readiness for gs's current round, in Spec.Required order. Callers must hold
// gs.mu.
func outstandingFor(gs *gateState) []string {
	outstanding := make([]string, 0, len(gs.spec.Required))
	for _, p := range gs.spec.Required {
		if _, ok := gs.ready[p]; !ok {
			outstanding = append(outstanding, p)
		}
	}
	return outstanding
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
		c.resolve(ctx, gs, Verdict{GateID: gateID, Passed: false, Detail: "runner unresponsive"}, true, false)
	})
}

func (c *Coordinator) onVerdictMsg(ctx context.Context, _ *agent.Agent, m protocol.Message) *protocol.Message {
	gateID, _ := m.Body["gate"].(string)
	gs := c.gate(gateID)
	if gs == nil {
		return nil
	}
	detail, _ := m.Body["detail"].(string)
	c.resolve(ctx, gs, Verdict{GateID: gateID, Passed: m.Intent == protocol.IntentDone, Detail: detail}, false, false)
	return nil
}

// resolve settles an in-flight round — real or, when fromCache is true,
// synthetic — and is the single place that broadcasts a verdict, routes
// failure blocks, remembers the round, and fires OnVerdict. Routing the
// cached-answer path through here (see onReady) rather than around it is
// what keeps a repeated identical resubmit visible to everything that
// already watches a round's outcome, the round cap included.
func (c *Coordinator) resolve(ctx context.Context, gs *gateState, v Verdict, stalled, fromCache bool) {
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

	var text string
	switch {
	case stalled:
		text = fmt.Sprintf("%s STALLED: runner unresponsive", gateID)
	case v.Passed:
		text = fmt.Sprintf("%s PASSED", gateID)
	default:
		text = fmt.Sprintf("%s FAILED: %s", gateID, v.Detail)
	}
	if fromCache {
		// Visible marker: an operator reading the log, or an agent reading
		// its own submit output, can tell this was answered from a
		// completed round rather than a freshly run spanning test.
		text += " (already tested at this version)"
	}
	// Remember this round so a participant that re-submits at the same
	// version gets it back immediately instead of hanging. See onReady.
	remembered := v
	gs.lastVerdict = &remembered
	gs.lastStalled = stalled
	gs.mu.Unlock()

	// versions makes the verdict self-describing: it says which participant was
	// tested at which version. A participant needs that to tell whether this
	// verdict covered its own submission — a gate id and a passed bool cannot.
	// Without it, a readiness dropped mid-round would silently accept the
	// in-flight round's verdict, one computed without its version at all.
	_ = c.a.Send(ctx, protocol.New(
		protocol.Address{Agent: c.a.Name},
		protocol.Address{Topic: Topic(gateID)},
		protocol.IntentInform,
		map[string]any{"text": text, "gate": gateID, "passed": v.Passed, "versions": copyMap(v.Versions)},
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
