# Cross-Service Integration-Test Gate — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reusable `pkg/gate` coordinator that gates a cross-service integration test: services declare readiness, the gate opens when the required set is complete, a designated runner executes, and Parallel Consciousness broadcasts the verdict.

**Architecture:** A new `pkg/gate` package layered on the existing `pkg/bus` + `pkg/agent` + `pkg/protocol`. Participants broadcast an `IntentReady` speech act to a per-gate topic; a `Coordinator` (hosted by one agent) accumulates readiness, sends the designated runner an `IntentRequest` when the required set is complete, collects the runner's verdict (`done` = pass, `disagree`/`block` = fail), broadcasts the verdict, and routes failures back to owners as `block`. Parallel Consciousness never runs a test itself — the `ServeRunner` callback is the only execution seam and is supplied by the caller.

**Tech Stack:** Go 1.22, standard library, `github.com/google/uuid` (already vendored via go.sum). In-memory bus only.

## Global Constraints

- Go version stays `go 1.22` (per `go.mod`); do not raise it.
- **No new dependencies.** Standard library + the existing `github.com/google/uuid` only.
- Module path remains the placeholder `github.com/KJFromMicromonic/parallel-consciousness` (rename deferred by decision). Use this exact path in every import.
- `pkg/gate` MUST NOT execute tests or shell out. Coordination only; the caller-supplied `ServeRunner` callback is the sole execution seam.
- The gate API stays **harness-agnostic**: it traffics only in plain data (a gate id, a version string, a `Verdict`) — nothing tied to any agent harness.
- **Single-process / in-memory bus.** Do not attempt cross-process correctness.
- **Additive only.** Do not rewrite existing protocol intents, the bus, or the agent runtime beyond adding one new intent constant. `cmd/demo` stays unchanged.
- TDD: write the failing test first; commit after each task is green.

## File Structure

- `pkg/protocol/protocol.go` — **modify**: add `IntentReady` constant; add it to `WantsResponse`'s terminal set.
- `pkg/protocol/protocol_test.go` — **create**: assert `IntentReady` value and terminality.
- `pkg/gate/gate.go` — **create**: `Topic`, `Spec`, `Verdict`, `Ready`, `ServeRunner`, `Coordinator` (the readiness state machine).
- `pkg/gate/gate_test.go` — **create**: unit tests for `Ready`, `ServeRunner`, and the coordinator state machine.
- `cmd/gatedemo/main.go` — **create**: runnable two-round demo (pass round, regression/fail round).
- `cmd/demo/main.go` — **unchanged**.

---

### Task 1: Add the `ready` speech act to the protocol

**Files:**
- Modify: `pkg/protocol/protocol.go`
- Test: `pkg/protocol/protocol_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `protocol.IntentReady` (value `"ready"`); `Intent.WantsResponse()` returns `false` for it.

- [ ] **Step 1: Write the failing test**

Create `pkg/protocol/protocol_test.go`:

```go
package protocol

import "testing"

func TestReadyIntentValue(t *testing.T) {
	if IntentReady != "ready" {
		t.Fatalf("IntentReady = %q, want %q", IntentReady, "ready")
	}
}

func TestReadyIntentIsTerminal(t *testing.T) {
	if IntentReady.WantsResponse() {
		t.Fatal("IntentReady must be terminal (WantsResponse should be false)")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/protocol/ -run TestReadyIntent -v`
Expected: FAIL — compile error `undefined: IntentReady`.

- [ ] **Step 3: Add the constant and make it terminal**

In `pkg/protocol/protocol.go`, add to the coordination/work-negotiation const block (after `IntentDone`):

```go
	IntentReady    Intent = "ready"    // "I'm at a compatible state for a gate"
```

Then add `IntentReady` to the terminal set in `WantsResponse`:

```go
func (i Intent) WantsResponse() bool {
	switch i {
	case IntentInform, IntentAck, IntentNack, IntentYield, IntentDone, IntentReady:
		return false
	default:
		return true
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/protocol/ -run TestReadyIntent -v`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add pkg/protocol/protocol.go pkg/protocol/protocol_test.go
git commit -m "feat(protocol): add 'ready' speech act for gate readiness"
```

---

### Task 2: Gate types, topic helper, and the `Ready` participant helper

**Files:**
- Create: `pkg/gate/gate.go`
- Test: `pkg/gate/gate_test.go`

**Interfaces:**
- Consumes: `protocol.IntentReady`, `protocol.New`, `agent.Agent.Send`, `agent.New`, `bus.NewInMemory`.
- Produces:
  - `gate.Topic(gateID string) string` → `"gate." + gateID`
  - `gate.Spec{ ID string; Required []string; Runner string }`
  - `gate.Verdict{ GateID string; Passed bool; Detail string; Versions map[string]string }`
  - `gate.Ready(ctx context.Context, a *agent.Agent, gateID, version string) error` — broadcasts `IntentReady` to `Topic(gateID)` with body `{"gate": gateID, "version": version}`.

- [ ] **Step 1: Write the failing test**

Create `pkg/gate/gate_test.go`:

```go
package gate_test

import (
	"context"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

// recvMsg reads one message or fails the test after 2s.
func recvMsg(t *testing.T, ch chan protocol.Message) protocol.Message {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
		return protocol.Message{}
	}
}

func TestTopic(t *testing.T) {
	if got := gate.Topic("checkout"); got != "gate.checkout" {
		t.Fatalf("Topic = %q, want %q", got, "gate.checkout")
	}
}

func TestReadyBroadcastsToGateTopic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := bus.NewInMemory(8)

	// Observer subscribed to the gate topic captures the readiness signal.
	obs, err := agent.New(ctx, b, "obs", []string{gate.Topic("checkout")})
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan protocol.Message, 1)
	obs.On(protocol.IntentReady, func(ctx context.Context, a *agent.Agent, m protocol.Message) *protocol.Message {
		got <- m
		return nil
	})
	go obs.Run(ctx)

	// Sender only publishes; it needs no Run loop.
	sender, err := agent.New(ctx, b, "billing", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := gate.Ready(ctx, sender, "checkout", "v1"); err != nil {
		t.Fatal(err)
	}

	m := recvMsg(t, got)
	if m.Intent != protocol.IntentReady {
		t.Fatalf("intent = %q, want ready", m.Intent)
	}
	if m.From.Agent != "billing" {
		t.Fatalf("from = %q, want billing", m.From.Agent)
	}
	if m.Body["gate"] != "checkout" || m.Body["version"] != "v1" {
		t.Fatalf("body = %v, want gate=checkout version=v1", m.Body)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/gate/ -v`
Expected: FAIL — `package github.com/KJFromMicromonic/parallel-consciousness/pkg/gate is not in std` / no Go files.

- [ ] **Step 3: Create `pkg/gate/gate.go` with types, `Topic`, and `Ready`**

```go
// Package gate coordinates cross-service integration tests over the Parallel Consciousness
// conversation layer. Participants declare readiness for a named gate; when the
// full required set is ready, a Coordinator asks a designated runner to execute
// the spanning test and broadcasts the verdict. Parallel Consciousness coordinates the
// handshake — it never runs a test itself.
package gate

import (
	"context"

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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/gate/ -v`
Expected: PASS (`TestTopic`, `TestReadyBroadcastsToGateTopic`).

- [ ] **Step 5: Commit**

```bash
git add pkg/gate/gate.go pkg/gate/gate_test.go
git commit -m "feat(gate): gate types, topic helper, and Ready participant helper"
```

---

### Task 3: `ServeRunner` — the execution seam

**Files:**
- Modify: `pkg/gate/gate.go`
- Test: `pkg/gate/gate_test.go`

**Interfaces:**
- Consumes: `agent.Agent.On`, `protocol.IntentRequest`, `protocol.IntentDone`, `protocol.IntentDisagree`, `Message.Reply`.
- Produces: `gate.ServeRunner(a *agent.Agent, fn func(ctx context.Context, gateID string, versions map[string]string) Verdict)`. Registers an `IntentRequest` handler that, for messages whose body has a non-empty `"gate"`, calls `fn` and replies `done` (pass) or `disagree` (fail) with body `{"gate": gateID, "detail": v.Detail}`. Messages without a `"gate"` body are ignored (returns nil).

- [ ] **Step 1: Write the failing test**

Append to `pkg/gate/gate_test.go`:

```go
// sendAndCaptureReply wires a caller that sends one request to `to` and
// captures the runner's reply (done or disagree).
func sendAndCaptureReply(t *testing.T, ctx context.Context, b *bus.InMemory, to string, body map[string]any) protocol.Message {
	t.Helper()
	caller, err := agent.New(ctx, b, "caller", nil)
	if err != nil {
		t.Fatal(err)
	}
	reply := make(chan protocol.Message, 1)
	capture := func(ctx context.Context, a *agent.Agent, m protocol.Message) *protocol.Message {
		reply <- m
		return nil
	}
	caller.On(protocol.IntentDone, capture)
	caller.On(protocol.IntentDisagree, capture)
	go caller.Run(ctx)

	if err := caller.Send(ctx, protocol.New(
		protocol.Address{Agent: "caller"},
		protocol.Address{Agent: to},
		protocol.IntentRequest,
		body,
	)); err != nil {
		t.Fatal(err)
	}
	return recvMsg(t, reply)
}

func TestServeRunnerRepliesDoneOnPass(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := bus.NewInMemory(8)

	runner, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.ServeRunner(runner, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		if versions["billing"] != "v1" {
			t.Errorf("runner got versions %v, want billing=v1", versions)
		}
		return gate.Verdict{GateID: gateID, Passed: true}
	})
	go runner.Run(ctx)

	m := sendAndCaptureReply(t, ctx, b, "runner", map[string]any{
		"gate":     "checkout",
		"versions": map[string]string{"billing": "v1"},
	})
	if m.Intent != protocol.IntentDone {
		t.Fatalf("intent = %q, want done", m.Intent)
	}
}

func TestServeRunnerRepliesDisagreeOnFail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := bus.NewInMemory(8)

	runner, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.ServeRunner(runner, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: false, Detail: "boom"}
	})
	go runner.Run(ctx)

	m := sendAndCaptureReply(t, ctx, b, "runner", map[string]any{"gate": "checkout"})
	if m.Intent != protocol.IntentDisagree {
		t.Fatalf("intent = %q, want disagree", m.Intent)
	}
	if m.Body["detail"] != "boom" {
		t.Fatalf("detail = %v, want boom", m.Body["detail"])
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/gate/ -run TestServeRunner -v`
Expected: FAIL — compile error `undefined: gate.ServeRunner`.

- [ ] **Step 3: Add `ServeRunner` to `pkg/gate/gate.go`**

Append:

```go
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/gate/ -run TestServeRunner -v`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add pkg/gate/gate.go pkg/gate/gate_test.go
git commit -m "feat(gate): ServeRunner execution seam (done/disagree reply)"
```

---

### Task 4: `Coordinator` — readiness accounting, open, pass/fail, block-routing, re-arm

**Files:**
- Modify: `pkg/gate/gate.go`
- Test: `pkg/gate/gate_test.go`

**Interfaces:**
- Consumes: everything from Tasks 2–3, plus `protocol.IntentDone`, `protocol.IntentDisagree`, `protocol.IntentBlock`, `protocol.IntentInform`, `protocol.New`.
- Produces:
  - `gate.NewCoordinator(a *agent.Agent) *Coordinator` — registers handlers for `ready`, `done`, `disagree`, `block` on `a`.
  - `(*Coordinator).Register(spec Spec)` — declare a gate to coordinate.
  - `(*Coordinator).OnVerdict(fn func(Verdict))` — hook called with each resolved verdict (pass or fail).
  - Behavior: when all `Required` participants have declared ready (deduped by participant, last-version-wins, order-independent), the coordinator sends `Runner` an `IntentRequest` with `{"gate", "versions"}`; on `done` it broadcasts `"<id> PASSED"` (inform) to `Topic(id)`; on `disagree`/`block` it broadcasts `"<id> FAILED: <detail>"` and sends each owner a direct `IntentBlock`. After any verdict it clears readiness (re-arm). The verdict's `Versions` are the versions that were ready when the gate opened.

- [ ] **Step 1: Write the failing tests (state machine + a shared harness)**

Append to `pkg/gate/gate_test.go`:

```go
// --- coordinator test harness ---

type harness struct {
	ctx     context.Context
	cancel  context.CancelFunc
	bus     *bus.InMemory
	gateID  string
	coord   *gate.Coordinator
	verdict chan gate.Verdict
	blocks  chan protocol.Message
	parts   map[string]*agent.Agent
}

// setupGate stands up a gatekeeper hosting a coordinator for spec, plus an
// optional runner. If runnerFn is nil, no runner is registered (the gate will
// later stall — used by the timeout test). Participants are created lazily and
// each captures blocks routed to it.
func setupGate(t *testing.T, spec gate.Spec, runnerFn func(string, map[string]string) gate.Verdict) *harness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	b := bus.NewInMemory(64)

	gk, err := agent.New(ctx, b, "gatekeeper", []string{gate.Topic(spec.ID)})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	coord := gate.NewCoordinator(gk)
	coord.Register(spec)
	h := &harness{
		ctx: ctx, cancel: cancel, bus: b, gateID: spec.ID, coord: coord,
		verdict: make(chan gate.Verdict, 8),
		blocks:  make(chan protocol.Message, 8),
		parts:   map[string]*agent.Agent{},
	}
	coord.OnVerdict(func(v gate.Verdict) { h.verdict <- v })
	go gk.Run(ctx)

	if runnerFn != nil {
		r, err := agent.New(ctx, b, spec.Runner, nil)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		gate.ServeRunner(r, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
			return runnerFn(gateID, versions)
		})
		go r.Run(ctx)
	}
	return h
}

// part returns (creating + running once) a participant agent that captures any
// IntentBlock routed to it.
func (h *harness) part(t *testing.T, name string) *agent.Agent {
	t.Helper()
	if a, ok := h.parts[name]; ok {
		return a
	}
	a, err := agent.New(h.ctx, h.bus, name, nil)
	if err != nil {
		t.Fatal(err)
	}
	a.On(protocol.IntentBlock, func(ctx context.Context, ag *agent.Agent, m protocol.Message) *protocol.Message {
		h.blocks <- m
		return nil
	})
	go a.Run(h.ctx)
	h.parts[name] = a
	return a
}

func (h *harness) ready(t *testing.T, who, version string) {
	t.Helper()
	if err := gate.Ready(h.ctx, h.part(t, who), h.gateID, version); err != nil {
		t.Fatal(err)
	}
}

func recvVerdict(t *testing.T, ch chan gate.Verdict) gate.Verdict {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for verdict")
		return gate.Verdict{}
	}
}

func checkoutSpec() gate.Spec {
	return gate.Spec{ID: "checkout", Required: []string{"billing", "gateway"}, Runner: "runner"}
}

func passRunner(gateID string, versions map[string]string) gate.Verdict {
	return gate.Verdict{GateID: gateID, Passed: true}
}

// --- tests ---

func TestPartialReadinessDoesNotOpen(t *testing.T) {
	h := setupGate(t, checkoutSpec(), passRunner)
	defer h.cancel()

	h.ready(t, "billing", "v1") // only one of two required

	select {
	case v := <-h.verdict:
		t.Fatalf("gate opened with partial readiness: %+v", v)
	case <-time.After(250 * time.Millisecond):
		// good: no verdict
	}
}

func TestFullReadinessOpensAndPasses(t *testing.T) {
	h := setupGate(t, checkoutSpec(), passRunner)
	defer h.cancel()

	// Out-of-order on purpose: gateway before billing still opens.
	h.ready(t, "gateway", "g1")
	h.ready(t, "billing", "b1")

	v := recvVerdict(t, h.verdict)
	if !v.Passed {
		t.Fatalf("verdict = %+v, want passed", v)
	}
	if v.Versions["billing"] != "b1" || v.Versions["gateway"] != "g1" {
		t.Fatalf("versions = %v, want billing=b1 gateway=g1", v.Versions)
	}
}

func TestDuplicateReadyLastVersionWins(t *testing.T) {
	h := setupGate(t, checkoutSpec(), passRunner)
	defer h.cancel()

	h.ready(t, "billing", "v1")
	h.ready(t, "billing", "v2") // same participant again; must not open alone

	select {
	case v := <-h.verdict:
		t.Fatalf("opened with a single participant: %+v", v)
	case <-time.After(200 * time.Millisecond):
	}

	h.ready(t, "gateway", "g1")
	v := recvVerdict(t, h.verdict)
	if v.Versions["billing"] != "v2" {
		t.Fatalf("billing version = %q, want last-write v2", v.Versions["billing"])
	}
}

func TestFailVerdictBroadcastsAndBlocksOwners(t *testing.T) {
	failRunner := func(gateID string, versions map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: false, Detail: "checkout_test: 402"}
	}
	h := setupGate(t, checkoutSpec(), failRunner)
	defer h.cancel()

	h.ready(t, "billing", "b1")
	h.ready(t, "gateway", "g1")

	v := recvVerdict(t, h.verdict)
	if v.Passed {
		t.Fatalf("verdict = %+v, want failed", v)
	}
	if v.Detail != "checkout_test: 402" {
		t.Fatalf("detail = %q", v.Detail)
	}

	owners := map[string]bool{}
	for i := 0; i < 2; i++ {
		m := recvMsg(t, h.blocks)
		if m.Intent != protocol.IntentBlock {
			t.Fatalf("intent = %q, want block", m.Intent)
		}
		owners[m.To.Agent] = true
	}
	if !owners["billing"] || !owners["gateway"] {
		t.Fatalf("blocked owners = %v, want billing+gateway", owners)
	}
}

func TestGateReArmsAfterVerdict(t *testing.T) {
	h := setupGate(t, checkoutSpec(), passRunner)
	defer h.cancel()

	h.ready(t, "billing", "b1")
	h.ready(t, "gateway", "g1")
	if v := recvVerdict(t, h.verdict); !v.Passed {
		t.Fatalf("round 1 verdict = %+v", v)
	}

	// Second full round must trigger a second run.
	h.ready(t, "billing", "b2")
	h.ready(t, "gateway", "g2")
	v := recvVerdict(t, h.verdict)
	if !v.Passed || v.Versions["billing"] != "b2" {
		t.Fatalf("round 2 verdict = %+v, want passed billing=b2", v)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/gate/ -run 'Coordinator|Readiness|Verdict|ReArm|Duplicate|Opens' -v`
Expected: FAIL — compile error `undefined: gate.NewCoordinator`.

- [ ] **Step 3: Implement the `Coordinator` in `pkg/gate/gate.go`**

Add `"fmt"` and `"sync"` to the import block, then append:

```go
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/gate/ -v`
Expected: PASS (all tests, including Task 2/3).

- [ ] **Step 5: Commit**

```bash
git add pkg/gate/gate.go pkg/gate/gate_test.go
git commit -m "feat(gate): coordinator with readiness quorum, pass/fail, block-routing, re-arm"
```

---

### Task 5: Runner-unresponsive timeout (gate stalls)

**Files:**
- Modify: `pkg/gate/gate.go`
- Test: `pkg/gate/gate_test.go`

**Interfaces:**
- Consumes: everything from Task 4, plus `time` and `protocol.Message.Deadline`.
- Produces:
  - `(*Coordinator).SetRunnerTimeout(d time.Duration)` — how long to wait for a runner's verdict before declaring the gate stalled. Default `5s`.
  - Behavior: when a gate opens, the coordinator sets the request's `Deadline` and arms a timer. If no verdict arrives before the timeout, it resolves the gate as a failure with `Detail == "runner unresponsive"` (broadcast `"<id> STALLED: runner unresponsive"`, block each owner, re-arm). A timer for a run that already resolved is a no-op (generation guard).

- [ ] **Step 1: Write the failing test**

Append to `pkg/gate/gate_test.go`:

```go
func TestRunnerTimeoutStalls(t *testing.T) {
	// nil runnerFn => no runner agent exists, so the request is never answered.
	h := setupGate(t, checkoutSpec(), nil)
	defer h.cancel()
	h.coord.SetRunnerTimeout(80 * time.Millisecond)

	h.ready(t, "billing", "b1")
	h.ready(t, "gateway", "g1")

	v := recvVerdict(t, h.verdict)
	if v.Passed {
		t.Fatalf("verdict = %+v, want stalled failure", v)
	}
	if v.Detail != "runner unresponsive" {
		t.Fatalf("detail = %q, want \"runner unresponsive\"", v.Detail)
	}

	owners := map[string]bool{}
	for i := 0; i < 2; i++ {
		m := recvMsg(t, h.blocks)
		owners[m.To.Agent] = true
	}
	if !owners["billing"] || !owners["gateway"] {
		t.Fatalf("blocked owners = %v, want billing+gateway", owners)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/gate/ -run TestRunnerTimeoutStalls -v`
Expected: FAIL — compile error `undefined: (*gate.Coordinator).SetRunnerTimeout` (and, once that compiles, the test would otherwise hang then fail at the 2s verdict timeout because nothing stalls the gate).

- [ ] **Step 3: Add the timeout. Edit `pkg/gate/gate.go`**

3a. Add `"time"` to the import block.

3b. Add a `timeout` field to `Coordinator` and a `gen` field to `gateState`:

```go
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
```

3c. Initialize the default timeout in `NewCoordinator` and add the setter:

```go
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
```

3d. Replace `open` so it sets the deadline and arms a generation-guarded timer:

```go
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
```

3e. Change `resolve` to take a `stalled bool`, bump `gen`, and word the stall broadcast. Update the `onVerdictMsg` call site to pass `false`:

```go
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

	if c.onVerdict != nil {
		c.onVerdict(v)
	}
}
```

- [ ] **Step 4: Run the full package with the race detector**

Run: `go test ./pkg/gate/ -race -v`
Expected: PASS (all tests, no data races). `TestRunnerTimeoutStalls` resolves within ~80ms.

- [ ] **Step 5: Commit**

```bash
git add pkg/gate/gate.go pkg/gate/gate_test.go
git commit -m "feat(gate): runner-unresponsive timeout stalls the gate"
```

---

### Task 6: `cmd/gatedemo` — runnable two-round demo

**Files:**
- Create: `cmd/gatedemo/main.go`

**Interfaces:**
- Consumes: `bus.NewInMemory`, `agent.New`, `agent.Agent.Run/Send`, `gate.{Topic,Spec,Verdict,NewCoordinator,Ready,ServeRunner}`, `protocol.IntentBlock`.
- Produces: a `main` that runs a passing round then a regression round and exits cleanly.

- [ ] **Step 1: Create `cmd/gatedemo/main.go`**

```go
// Command gatedemo shows a cross-service integration-test gate: two service
// agents declare readiness, a gatekeeper opens the gate, a runner executes the
// spanning test, and the verdict is broadcast — first a passing round, then a
// regression that fails and routes a block back to the owner.
//
//	go run ./cmd/gatedemo
package main

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

const gateID = "checkout"

func main() {
	log.SetFlags(0)
	log.SetOutput(os.Stdout)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := bus.NewInMemory(64)
	var wg sync.WaitGroup
	run := func(a *agent.Agent) { wg.Add(1); go func() { defer wg.Done(); a.Run(ctx) }() }

	// Gatekeeper hosts the coordinator; it must subscribe to the gate topic.
	gk := mustAgent(ctx, b, "gatekeeper", []string{gate.Topic(gateID)})
	coord := gate.NewCoordinator(gk)
	coord.Register(gate.Spec{ID: gateID, Required: []string{"billing", "gateway"}, Runner: "runner"})
	coord.OnVerdict(func(v gate.Verdict) {
		if v.Passed {
			log.Printf("  ✓ gate %q PASSED  versions=%v", v.GateID, v.Versions)
		} else {
			log.Printf("  ✗ gate %q FAILED: %s", v.GateID, v.Detail)
		}
	})

	// Runner: the spanning test. Fails if billing shipped the regression e5f6.
	runner := mustAgent(ctx, b, "runner", nil)
	gate.ServeRunner(runner, func(ctx context.Context, id string, versions map[string]string) gate.Verdict {
		if versions["billing"] == "e5f6" {
			return gate.Verdict{GateID: id, Passed: false, Detail: "checkout_test: 402 from billing"}
		}
		return gate.Verdict{GateID: id, Passed: true}
	})

	// Service agents subscribe to the gate topic to hear verdicts; billing also
	// reacts to a routed block.
	billing := mustAgent(ctx, b, "billing", []string{gate.Topic(gateID)})
	billing.On(protocol.IntentBlock, func(ctx context.Context, a *agent.Agent, m protocol.Message) *protocol.Message {
		log.Printf("  [billing] heard the gate is failing on my change — will investigate")
		return nil
	})
	gateway := mustAgent(ctx, b, "gateway", []string{gate.Topic(gateID)})

	for _, a := range []*agent.Agent{gk, runner, billing, gateway} {
		run(a)
	}

	log.Println("── parallel-consciousness: cross-service test gate ──")
	log.Println("Round 1 — both services at compatible versions:")
	_ = gate.Ready(ctx, gateway, gateID, "a1b2")
	_ = gate.Ready(ctx, billing, gateID, "c3d4")
	time.Sleep(500 * time.Millisecond)

	log.Println("Round 2 — billing ships a regression (e5f6):")
	_ = gate.Ready(ctx, billing, gateID, "e5f6")
	_ = gate.Ready(ctx, gateway, gateID, "a1b2")
	time.Sleep(500 * time.Millisecond)

	log.Println("── done ──")
	cancel()
	wg.Wait()
}

func mustAgent(ctx context.Context, b bus.Bus, name string, topics []string) *agent.Agent {
	a, err := agent.New(ctx, b, name, topics)
	if err != nil {
		log.Fatalf("create %s: %v", name, err)
	}
	return a
}
```

- [ ] **Step 2: Verify it builds and vets**

Run: `go build ./... && go vet ./...`
Expected: no output, exit 0.

- [ ] **Step 3: Run the demo and eyeball the narrative**

Run: `go run ./cmd/gatedemo`
Expected (order of the interleaved turn-logs may vary slightly):
- Round 1: `billing`/`gateway` `--ready--> #gate.checkout`, `gatekeeper --request--> runner`, `runner --done--> gatekeeper`, `gatekeeper --inform--> #gate.checkout`, and `✓ gate "checkout" PASSED`.
- Round 2: readies, `runner --disagree--> gatekeeper`, `gatekeeper --inform--> #gate.checkout` (FAILED), `gatekeeper --block--> billing`, the `[billing] … will investigate` line, and `✗ gate "checkout" FAILED: checkout_test: 402 from billing`.
- Ends with `── done ──`.

- [ ] **Step 4: Full test sweep**

Run: `go test ./... && go vet ./...`
Expected: all packages PASS, vet clean.

- [ ] **Step 5: Commit**

```bash
git add cmd/gatedemo/main.go
git commit -m "feat(demo): cross-service test-gate demo (pass round + regression)"
```

---

## Self-Review

**1. Spec coverage** (each spec section → task):
- New `IntentReady` intent → Task 1. ✓
- `pkg/gate` package, `Spec`/`Verdict`/`Coordinator`/`Ready`/`ServeRunner` → Tasks 2–4. ✓
- Quorum-not-compatibility (dedup, last-write, order-independent open) → Task 4 (`onReady`). ✓
- Pass path / verdict broadcast / `OnVerdict` hook → Task 4. ✓
- Fail path + `block` routed to owners → Task 4. ✓
- Re-arm by clearing → Task 4 (`resolve` clears `ready`). ✓
- Runner-timeout / STALLED (deadline-enforcement item) → Task 5. ✓
- Dropped-`ready` known limitation → mitigated by the buffered coordinator inbox (`NewInMemory(64)` in the demo/harness); documented in the spec, not a code task. ✓
- `cmd/gatedemo` (pass + regression rounds) → Task 6; `cmd/demo` unchanged → not touched. ✓
- Test matrix (accumulation, open, pass, fail, timeout, duplicate, out-of-order, re-arm) → Tasks 2–5. ✓

**2. Placeholder scan:** No TBD/TODO/"add error handling"/"similar to Task N". Every code step shows complete code; shared test helpers are defined once (Task 2 `recvMsg`, Task 4 harness) and referenced by name thereafter. ✓

**3. Type consistency:** `Spec{ID,Required,Runner}`, `Verdict{GateID,Passed,Detail,Versions}`, `Topic(string)string`, `Ready(ctx,*agent.Agent,string,string)error`, `ServeRunner(*agent.Agent, func(context.Context,string,map[string]string)Verdict)`, `NewCoordinator(*agent.Agent)*Coordinator`, `Register(Spec)`, `OnVerdict(func(Verdict))`, `SetRunnerTimeout(time.Duration)` — used identically across tasks and tests. The `resolve` signature gains a `stalled bool` in Task 5; the only call sites (`onVerdictMsg`, the timer) are both updated in Task 5. ✓
