# Phase B Part 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Parallel Consciousness installable by someone who did not write it — a committed fixture, one portable contract snippet, verified recipes for pi and Claude Code, `pc init`, config validation that fails fast, an honest README, and a reproducible live-run script — and pay off the three refactors part 1's reviews deferred.

**Architecture:** Three behaviour-free refactors land first, while part 1's test suite is at its most constraining: the wire-format decode helpers move to `pkg/protocol`, `ErrLeaseLost` becomes one error identity that carries its cause, and `Submit`'s two repetition-maintained invariants become structural in a `submitWaiter`. Then the packaging work, which adds no coordination logic at all — a fixture as its own Go module, documentation, a scaffolder, validation rules, and a shell script.

**Tech Stack:** Go 1.22 (stdlib plus `modernc.org/sqlite`, `gopkg.in/yaml.v3`, `github.com/google/uuid`), git worktrees, SQLite, POSIX shell.

**Spec:** `docs/superpowers/specs/2026-09-06-phase-b-design.md` — read the sections "The fixture", "Packaging" and "Testing", then the **Part 2 addendum** at the end, which resolves the spec's open questions, corrects the timeout-budget claim, and specs the three refactors. The addendum is authoritative wherever it and the original body differ.

## Global Constraints

- Go floor is `go 1.22`. Do not raise it.
- No new module dependencies. No `go get`. `modernc.org/sqlite` stays at `v1.33.1`.
- `bus.Bus` stays exactly two methods, `Publish` and `Subscribe`. `History` and `Tail` are concrete methods on the SQLite adapter and must not become contract additions.
- `pkg/` is public API and may not import `internal/` at all — `internal/arch/arch_test.go` enforces this with a compiler check. `pkg/runtime` stays stdlib-only.
- `cmd/pc` stays a thin skin over `internal/pcops`. No coordination logic in a command.
- `internal/pcops/courier.go` is not touched by this plan, and no courier handler may be registered for `IntentAck` or `IntentNack`. The courier forwards messages into live coding-agent sessions; an earlier design that routed a coordinator message that way produced an unbounded feedback loop — 2,589 goroutines in 8 seconds.
- No `time.Sleep` in tests. A bounded `time.After` inside a `select`, used as a failure deadline, is correct and expected. Do not use `time.After` as a poll interval in new code.
- No existing assertion may be weakened or deleted anywhere. Two tests in `internal/pcops/submit_test.go` were specifically repaired from vacuous versions during part 1; a failure there is a signal to stop, not a test to adjust.
- `gofmt -l pkg internal cmd` must report only `cmd/sqlitedemo/main.go`, a documented pre-existing violation. Anything else your work adds must be fixed before committing. Do not describe a file this plan creates as pre-existing — `git log --diff-filter=A -- <path>` settles provenance in one command.
- Recipes ship only for harnesses verified end to end: pi and Claude Code. For any other harness, document what it needs; claim nothing.

---

## File Structure

**Created:**

| Path | Responsibility |
|---|---|
| `pkg/protocol/body.go` | Coercing wire body values back to Go types after a JSON round trip. One home for a concern currently duplicated three times. |
| `pkg/protocol/body_test.go` | Table tests for both decoders, including the shapes each bus actually delivers. |
| `fixtures/two-service/go.mod` | Makes the fixture its own module so the root `./...` excludes it from build, vet and test. |
| `fixtures/two-service/billing/billing.go` | Renders an invoice line including its currency. Compiles alone. |
| `fixtures/two-service/gateway/gateway.go` | Builds the invoice and populates the currency. Seeded wrong (`"EUR"`). Compiles alone. |
| `fixtures/two-service/integration/currency_test.go` | The spanning test. Reads the agreed value from the environment and *skips* when it is absent. |
| `fixtures/two-service/README.md` | What the fixture is for and why each constraint exists, for whoever finds it next. |
| `internal/fixtures/twoservice_test.go` | Repo-side insurance: copies the fixture to a temp dir and asserts the baseline genuinely fails. |
| `docs/agent-contract.md` | The portable artifact — the instructions every participating agent receives, identical across harnesses. |
| `docs/recipes/pi.md` | How pi ingests the contract, plus the stdin trap. |
| `docs/recipes/claude-code.md` | How Claude Code ingests the contract, plus print-mode flags. |
| `docs/recipes/other-harnesses.md` | What any harness needs, claiming support for none. |
| `scripts/live-run.sh` | Operator tool: pre-flight, fixture reset, daemon, both agents, report. |

**Modified:**

| Path | Change |
|---|---|
| `pkg/gate/gate.go` | Delete local `versionsFromBody`; call `protocol.Versions`. |
| `internal/pcops/watch.go` | Delete local `versionsFromBody`; call `protocol.Versions` / `protocol.Strings`. |
| `internal/pcops/submit.go` | Delete local `outstandingFromBody`; call the protocol decoders. Extract `submitWaiter`. |
| `internal/pcops/run.go` | `ErrLeaseLost` wraps `workspace.ErrLeaseLost`; the loss channel carries `leaseLoss`. |
| `internal/pcops/config.go` | `RunnerTimeout` field, `budget.runner_timeout`, and a `validate` step in `LoadConfig`. |
| `internal/pcops/up.go` | Use `cfg.RunnerTimeout` instead of the hardcoded ten minutes. |
| `cmd/pc/main.go` | `pc init`; `--config` defaults to `./.pc.yaml` when present; usage lists six commands. |
| `README.md` | Restructure "What exists today"; add the quickstart and an honest limitations statement. |

---

## Three hazards this codebase has already been bitten by

Read these before Task 1. Each cost real time on this project.

**1. Tests that look like they test something and don't.** Seven instances found so far. Two were on part 1's own branch: one published a stale verdict *before* the call under test, where an existing fence swallowed it, so it passed with the fix deleted; the other was named `TestSubmitTreatsANackAsAcknowledgement` but declared a single required participant, so its readiness completed quorum and was *acked* and no Nack was ever produced. For every test you write, ask: what single-line change to production code makes this fail? If you cannot name one, the test is decoration.

**2. `pi -p` merges piped stdin into the prompt.** A backgrounded invocation with an inherited open stdin blocks forever. `< /dev/null` fixes it. This cost two nine-minute live runs and is invisible until two harnesses are driven side by side. It matters directly in Task 9.

**3. Building the binary from the wrong directory.** One live run silently tested a stale `pc` binary for nine minutes. Task 9's pre-flight exists because of this: it asserts the binary's provenance before anything else runs.

---

## Task 1: Wire-format decoders move to `pkg/protocol`

A body value that has crossed the SQLite bus arrives as `map[string]any` or `[]any` after its JSON round trip and must be coerced back. There are three definitions of that one concern and eight call sites. `pkg/gate` and `internal/pcops` cannot share today because `internal/arch` forbids `pkg/` importing `internal/` — but `pkg/protocol` imports only `time` and `uuid`, both packages already import it, and this is exactly a wire-format concern.

**Files:**
- Create: `pkg/protocol/body.go`
- Create: `pkg/protocol/body_test.go`
- Modify: `pkg/gate/gate.go` (delete `versionsFromBody` at the bottom; update its one caller)
- Modify: `internal/pcops/watch.go` (delete `versionsFromBody`; update callers)
- Modify: `internal/pcops/submit.go` (delete `outstandingFromBody`; update callers)

**Interfaces:**
- Consumes: nothing new.
- Produces: `protocol.Versions(v any) map[string]string` and `protocol.Strings(v any) []string`. Tasks 3 and 4 do not use these, but every later reader of a message body should.

- [ ] **Step 1: Write the failing test**

Create `pkg/protocol/body_test.go`:

```go
package protocol_test

import (
	"reflect"
	"testing"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

// Both shapes are real and neither is hypothetical: the in-memory bus passes
// a map[string]string straight through, while pkg/bus/sqlite stores the body
// as JSON and hands back map[string]any. A decoder that handled only one
// would work in unit tests and fail across processes.
func TestVersions(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   any
		want map[string]string
	}{
		{"in-memory shape", map[string]string{"billing": "abc"}, map[string]string{"billing": "abc"}},
		{"json shape", map[string]any{"billing": "abc"}, map[string]string{"billing": "abc"}},
		{"json shape skips non-strings", map[string]any{"billing": "abc", "n": 3}, map[string]string{"billing": "abc"}},
		{"absent key", nil, nil},
		{"wrong type entirely", "not a map", nil},
		{"empty json map", map[string]any{}, map[string]string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := protocol.Versions(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("Versions(%#v) = %#v, want %#v", tc.in, got, tc.want)
			}
		})
	}
}

func TestStrings(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   any
		want []string
	}{
		{"in-memory shape", []string{"billing", "gateway"}, []string{"billing", "gateway"}},
		{"json shape", []any{"billing", "gateway"}, []string{"billing", "gateway"}},
		{"json shape skips non-strings", []any{"billing", 7}, []string{"billing"}},
		{"absent key", nil, nil},
		{"wrong type entirely", 42, nil},
		{"empty json list", []any{}, []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := protocol.Strings(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("Strings(%#v) = %#v, want %#v", tc.in, got, tc.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/protocol/ -run 'TestVersions|TestStrings' -v`
Expected: FAIL to compile — `undefined: protocol.Versions`.

- [ ] **Step 3: Write the implementation**

Create `pkg/protocol/body.go`:

```go
package protocol

// A message body is a map[string]any, and what a given key holds depends on
// which transport delivered it. The in-memory bus passes Go values through
// untouched, so a map[string]string arrives as a map[string]string. The SQLite
// bus stores the body as JSON, so the same value comes back as a
// map[string]any with string elements — and a []string comes back as a []any.
//
// Every consumer of a structured body value therefore has to accept both
// shapes. These two functions are that acceptance, in one place: three
// byte-equivalent copies of this logic previously lived in pkg/gate and
// internal/pcops, which is one drifting copy away from a wire-format bug that
// only appears across processes.
//
// Anything that is not the expected element type is skipped rather than
// guessed at, and an absent or wrongly-typed value yields nil rather than an
// error: a body is data from another process, so a missing field is a normal
// condition for a consumer to handle, not an exceptional one.

// Versions coerces a wire participant→version map back to map[string]string.
func Versions(v any) map[string]string {
	switch m := v.(type) {
	case map[string]string:
		return m
	case map[string]any:
		out := make(map[string]string, len(m))
		for k, raw := range m {
			if s, ok := raw.(string); ok {
				out[k] = s
			}
		}
		return out
	}
	return nil
}

// Strings coerces a wire string list back to []string.
func Strings(v any) []string {
	switch l := v.(type) {
	case []string:
		return l
	case []any:
		out := make([]string, 0, len(l))
		for _, raw := range l {
			if s, ok := raw.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/protocol/ -run 'TestVersions|TestStrings' -v`
Expected: PASS, all twelve subtests.

- [ ] **Step 5: Replace the three local definitions**

In `pkg/gate/gate.go`: delete the `versionsFromBody` function and its doc comment near the bottom of the file, and change its one caller (inside the `protocol.IntentRequest` handler in `ServeRunner`) from `versionsFromBody(m.Body["versions"])` to `protocol.Versions(m.Body["versions"])`.

In `internal/pcops/watch.go`: delete the `versionsFromBody` function and its doc comment. Change all three callers to `protocol.Versions(...)` — the `versions` reads inside `summarise`'s `IntentInform` case (there are two, one for the request line and one for the verdict line) and the `testing` read in its `IntentNack` case. Change the `outstanding` read in its `IntentAck` case from `outstandingFromBody(...)` to `protocol.Strings(...)`.

In `internal/pcops/submit.go`: delete the `outstandingFromBody` function and its doc comment. Change `versionsFromBody(m.Body["versions"])` in the `IntentInform` handler and `versionsFromBody(m.Body["testing"])` in the `IntentNack` handler to `protocol.Versions(...)`, and `outstandingFromBody(m.Body["outstanding"])` in the `IntentAck` handler to `protocol.Strings(...)`.

Both `pkg/gate` and `internal/pcops` already import `pkg/protocol`, so no import lines change. Update the comment in `submit.go` line ~51 that names `versionsFromBody` to name `protocol.Versions` instead — it explains why an empty `version` would defeat the guard, and the reasoning depends on the decoder returning `""` for an absent participant.

- [ ] **Step 6: Verify nothing regressed**

Run: `go test ./... -count=1` then `go test ./... -race -count=1`
Expected: PASS everywhere. `grep -rn "versionsFromBody\|outstandingFromBody" --include=*.go .` should return only matches inside comments in test files, if any; no function definitions and no call sites.

- [ ] **Step 7: Commit**

```bash
git add pkg/protocol/body.go pkg/protocol/body_test.go pkg/gate/gate.go internal/pcops/watch.go internal/pcops/submit.go
git commit -m "refactor(protocol): one home for wire-format body decoding

Three byte-equivalent definitions of the same JSON round-trip coercion lived
in pkg/gate and internal/pcops, across eight call sites. This branch alone
added two producers of version maps; a fourth decoder drifting from the other
three is a wire-format bug that only shows up across processes."
```

---

## Task 2: `ErrLeaseLost` becomes one error identity that carries its cause

Two problems, both found by part 1's whole-branch review. `pcops.ErrLeaseLost` shadows `workspace.ErrLeaseLost` by name while not being reachable through it, so `errors.Is(runErr, workspace.ErrLeaseLost)` is false — a trap for a caller reaching for the name they already know. And `heartbeatLease` matches the workspace sentinel then reports only `l.Agent`, discarding the wrapped chain that carries the worktree path and any SQL context.

**Files:**
- Modify: `internal/pcops/run.go`
- Test: `internal/pcops/run_test.go`

**Interfaces:**
- Consumes: `workspace.ErrLeaseLost` (exists), `workspace.Lease.Agent`, `workspace.Lease.Path`.
- Produces: `pcops.ErrLeaseLost` now satisfies `errors.Is(pcops.ErrLeaseLost, workspace.ErrLeaseLost)`. Unexported `leaseLoss{agent string; err error}` and `startHeartbeat(ctx, l, lost chan<- leaseLoss)`.

- [ ] **Step 1: Write the failing test**

Add to `internal/pcops/run_test.go`:

```go
// One error identity, not two. A caller that already knows
// workspace.ErrLeaseLost — the sentinel the workspace package documents as
// the only way a holder learns it was reclaimed — must be able to match a run
// failure with it. Two identically named sentinels in adjacent packages, only
// one of them reachable, is a taxonomy trap.
func TestErrLeaseLostIsReachableThroughTheWorkspaceSentinel(t *testing.T) {
	if !errors.Is(pcops.ErrLeaseLost, workspace.ErrLeaseLost) {
		t.Fatal("pcops.ErrLeaseLost does not wrap workspace.ErrLeaseLost: a caller holding the workspace sentinel cannot match a run failure with it")
	}
}
```

And extend the existing `TestRunFailsWhenALeaseIsLost` with two assertions immediately after its existing `errors.Is(err, pcops.ErrLeaseLost)` and `strings.Contains(err.Error(), "billing")` checks. Do not remove or alter those two:

```go
		// The workspace sentinel must match too — same identity, one error.
		if !errors.Is(err, workspace.ErrLeaseLost) {
			t.Errorf("Run err = %v, want it to satisfy errors.Is(err, workspace.ErrLeaseLost) as well", err)
		}
		// The heartbeat saw a real error carrying the worktree path; dropping
		// it and reporting only the agent name throws away the one detail
		// that says WHICH directory was lost.
		if !strings.Contains(err.Error(), "worktrees") {
			t.Errorf("Run err = %v, want it to carry the underlying lease error (which names the worktree path)", err)
		}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/pcops/ -run 'TestErrLeaseLostIsReachable|TestRunFailsWhenALeaseIsLost' -v`
Expected: `TestErrLeaseLostIsReachableThroughTheWorkspaceSentinel` FAILS with the "does not wrap" message. `TestRunFailsWhenALeaseIsLost` FAILS on the `worktrees` assertion, because today's error is exactly `pcops: lease lost to another holder: billing`.

If `run_test.go` does not yet import `pkg/workspace`, it does after part 1's Task 7 — confirm with `grep -n workspace internal/pcops/run_test.go` before assuming an import is missing.

- [ ] **Step 3: Write the implementation**

In `internal/pcops/run.go`, replace the `ErrLeaseLost` declaration:

```go
// ErrLeaseLost means a lease this run holds was reclaimed by another holder.
// Continuing would mean working in a directory the run no longer owns, so it
// stops and reports which agent's lease was lost — the same attributed-failure
// shape as ErrSessionDied.
//
// It WRAPS workspace.ErrLeaseLost deliberately, rather than being a second
// sentinel with the same name. pkg/workspace documents its own ErrLeaseLost as
// the only way a holder learns it was reclaimed, so that is the name a caller
// already has; two identically named sentinels in adjacent packages, with the
// outer one not reachable through the inner, is a trap rather than a taxonomy.
// One identity: errors.Is matches either name.
var ErrLeaseLost = fmt.Errorf("pcops: lease lost to another holder: %w", workspace.ErrLeaseLost)

// leaseLoss carries a fenced lease out of its heartbeat goroutine: the
// PC-assigned agent name so the run can say who lost the workspace, and the
// error the heartbeat actually saw, which names the worktree path. Mirrors
// sessionFailure below, for the same reason — an attributed failure needs both
// the identity and the cause.
type leaseLoss struct {
	agent string
	err   error
}
```

Change the channel's type where `Run` creates it:

```go
	// Buffered to hold one loss per lease this run could ever hold (the
	// runner's plus one per agent) so a fenced heartbeat goroutine's
	// non-blocking send never has to be discarded.
	lost := make(chan leaseLoss, 1+len(cfg.Agents))
```

Change `Run`'s select arm:

```go
		case l := <-lost:
			// Not transient: another holder now owns this workspace.
			// Continuing would mean working in a directory this run no
			// longer owns, so stop and attribute the failure the same
			// way ErrSessionDied does. ErrLeaseLost already wraps
			// workspace.ErrLeaseLost, so both names match; l.err is
			// included because it is what names the worktree path.
			return gate.Verdict{GateID: cfg.GateID},
				fmt.Errorf("agent %q: %w (%v)", l.agent, ErrLeaseLost, l.err)
```

Change both heartbeat signatures and the fenced branch:

```go
func startHeartbeat(ctx context.Context, l *workspace.Lease, lost chan<- leaseLoss) (stop func()) {
	hctx, cancel := context.WithCancel(ctx)
	go heartbeatLease(hctx, l, lost)
	return cancel
}
```

```go
func heartbeatLease(ctx context.Context, l *workspace.Lease, lost chan<- leaseLoss) {
```

and inside its error branch:

```go
				if errors.Is(err, workspace.ErrLeaseLost) {
					// Not transient: another holder owns this workspace
					// now. Stop heartbeating a lease we do not hold, and
					// tell Run — with the error, not just the name: it
					// carries the worktree path and any SQL context, and
					// that is the detail that says which directory was
					// lost.
					select {
					case lost <- leaseLoss{agent: l.Agent, err: err}:
					default:
					}
					return
				}
```

Leave the transient `log.Printf` branch exactly as it is: a heartbeat failure that is not `ErrLeaseLost` must still only be logged, because the lease may outlive one blip before the next tick renews it.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/pcops/ -run 'TestErrLeaseLostIsReachable|TestRunFailsWhenALeaseIsLost' -v`
Expected: PASS. Then `go test ./... -count=1` and `go test ./... -race -count=1`, both clean.

- [ ] **Step 5: Commit**

```bash
git add internal/pcops/run.go internal/pcops/run_test.go
git commit -m "fix(pcops): one lease-lost identity, carrying its cause

pcops.ErrLeaseLost shadowed workspace.ErrLeaseLost by name without being
reachable through it, so a caller holding the name the workspace package
documents could not match a run failure. The fenced-lease path also dropped
the underlying error, discarding the one detail that says which worktree
was lost."
```

---
## Task 3: `submitWaiter` — two invariants become structural

This is the highest-risk task in the plan and it adds no behaviour. `Submit` is correct: three reviews walked it and none could construct a wrong outcome. What this fixes is how that correctness is *maintained* — two invariants are enforced by copy-paste, and each has already been got wrong once:

1. **Fence every arriving message against the round boundary.** Three handlers carry three byte-identical copies of `if m.Timestamp.Before(readAt()) { return nil }`. The `IntentNack` handler did not carry it — part 1's whole-branch review, first Important finding — and a stale replayed Nack sent `Submit` into the one wait with no `AckTimeout` bound, turning a dead coordinator into five minutes of silence reported as the wrong error class.
2. **Drain cross-attempt state before declaring readiness.** Three near-identical non-blocking drains. The first version drained one of three.

**Read this before you start:** the only outcomes available here are "unchanged" and "worse". Every existing test in `internal/pcops/submit_test.go` must pass **unmodified**. If one fails, your refactor is wrong — stop and report it rather than adjusting the test. Two of those tests were repaired from vacuous versions during part 1, and `TestSubmitDeclinesAVerdictThatDidNotIncludeIt` asserts a *negative* (that `Submit` refuses to return while only a foreign-version verdict is available), which is exactly what a botched refactor trips.

**Files:**
- Create: `internal/pcops/submitwaiter.go`
- Create: `internal/pcops/submitwaiter_test.go`
- Modify: `internal/pcops/submit.go`

**Interfaces:**
- Consumes: `protocol.Versions` / `protocol.Strings` (Task 1), `gate.Ready`, `agent.Agent`.
- Produces: `submitWaiter` with `newSubmitWaiter()`, `fresh(protocol.Message) bool`, `declareReady(ctx, *agent.Agent, gateID, version string) error`, `offerVerdict`, `offerAck`, `offerNack`, `offerDeclined`, `waitForRoundToResolve(ctx) roundResolution`. `roundResolution` moves to this file unchanged.

- [ ] **Step 1: Write the failing test**

Create `internal/pcops/submitwaiter_test.go`:

```go
package pcops

import (
	"context"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

// The round boundary is the only thing standing between a replayed message
// from a previous attempt and this attempt treating it as an answer. Three
// handlers used to carry three copies of this comparison and one of them was
// written without it; this pins the rule at the one place it now lives.
func TestSubmitWaiterFreshFencesMessagesFromBeforeTheBoundary(t *testing.T) {
	w := newSubmitWaiter()

	stale := protocol.New(protocol.Address{Agent: "coordinator"},
		protocol.Address{Agent: "billing"}, protocol.IntentAck, map[string]any{"gate": "g"})

	// Anything at all, published before a boundary exists, is fresh: the zero
	// Time predates every message, which is what makes the very first attempt
	// accept its own reply.
	if !w.fresh(stale) {
		t.Fatal("fresh() rejected a message before any boundary was stamped; the first attempt would never see its own reply")
	}

	w.stampReadyAt(time.Now())

	if w.fresh(stale) {
		t.Fatal("fresh() accepted a message stamped before the current round boundary; a replayed reply from a previous attempt would be treated as this attempt's answer")
	}

	current := protocol.New(protocol.Address{Agent: "coordinator"},
		protocol.Address{Agent: "billing"}, protocol.IntentAck, map[string]any{"gate": "g"})
	if !w.fresh(current) {
		t.Fatal("fresh() rejected a message stamped after the boundary; this attempt would ignore its own reply")
	}
}

// Each of the four channels holds one buffered signal, and any of them can be
// left full by a previous attempt. A fence on arrival does not empty a buffer
// something already got into, so declaring a fresh readiness has to drain all
// four — the first version of this drained one of three.
func TestSubmitWaiterDeclareReadyDrainsEveryChannel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	w := newSubmitWaiter()

	// Fill all four, as a previous attempt's replies would have.
	w.offerVerdict(gateVerdictForTest())
	w.offerAck([]string{"gateway"})
	w.offerNack(map[string]string{"gateway": "v1"})
	w.offerDeclined()

	b := bus.NewInMemory(8)
	a, err := agent.New(ctx, b, "billing", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.declareReady(ctx, a, "g", "v2"); err != nil {
		t.Fatalf("declareReady: %v", err)
	}

	for _, tc := range []struct {
		name  string
		empty bool
	}{
		{"verdicts", len(w.verdicts) == 0},
		{"acked", len(w.acked) == 0},
		{"nacked", len(w.nacked) == 0},
		{"declined", len(w.declined) == 0},
	} {
		if !tc.empty {
			t.Errorf("%s still held a buffered signal after declareReady; a stale reply from a previous attempt would answer this one", tc.name)
		}
	}
}
```

Add this helper at the bottom of the same file — `gate.Verdict` is imported for it, so keep the import list accurate:

```go
func gateVerdictForTest() gate.Verdict {
	return gate.Verdict{GateID: "g", Passed: true, Versions: map[string]string{"billing": "v1"}}
}
```

with `"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"` added to the imports.

Note the package clause: this test is `package pcops`, not `package pcops_test`, because it exercises unexported behaviour. Every other test file in this directory is `pcops_test`; that is correct for them and this one is the deliberate exception.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/pcops/ -run TestSubmitWaiter -v`
Expected: FAIL to compile — `undefined: newSubmitWaiter`.

- [ ] **Step 3: Write the waiter**

Create `internal/pcops/submitwaiter.go`:

```go
package pcops

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

// submitWaiter owns the state Submit's message handlers and its attempt loop
// share: the round boundary, and the four channels the handlers signal on.
//
// It exists because two invariants of that shared state were previously
// maintained by repetition, and each was got wrong once:
//
//  1. Every handler must fence an arriving message against the round
//     boundary, or a reply replayed from a lagging cursor answers an attempt
//     it was never about. Three handlers carried three identical copies of
//     that comparison; the third was written without it. Now they call fresh.
//  2. Every attempt must drain cross-attempt state before declaring
//     readiness, or a signal buffered by a previous attempt answers this one.
//     Three near-identical drains; the first version drained one of three.
//     Now declareReady does all four.
//
// What deliberately stays outside: the bus, the agent, the config, the gate id
// and the version. This owns the round boundary and the four channels, and
// nothing whose lifetime differs from those.
type submitWaiter struct {
	mu      sync.Mutex
	readyAt time.Time

	// All four are buffered 1 and all four are written by non-blocking sends:
	// a handler runs on the agent's dispatch goroutine and must never block
	// it on a send nobody is reading yet.
	verdicts chan gate.Verdict
	acked    chan []string
	nacked   chan map[string]string
	declined chan struct{}
}

func newSubmitWaiter() *submitWaiter {
	return &submitWaiter{
		verdicts: make(chan gate.Verdict, 1),
		acked:    make(chan []string, 1),
		nacked:   make(chan map[string]string, 1),
		declined: make(chan struct{}, 1),
	}
}

// fresh reports whether m belongs to the current attempt rather than having
// been replayed from the durable cursor.
//
// pkg/bus/sqlite resumes a subscription from the STORED cursor whenever a row
// exists for this agent name, and it saves that cursor only after a batch has
// been handed to the channel — so a submit process that exits or is killed
// routinely leaves the tail of its own inbox unread, for the NEXT submit under
// the same name to receive as if it were fresh. Two paths reach that state: a
// passing verdict routes no blocks, so nothing advances a courier past its
// Inform; and an agent with no in-process courier only ever has short-lived
// submit processes, whose deferred bus Close makes the poller skip saving the
// cursor entirely.
//
// protocol.New stamps Timestamp, so the round boundary is simply time. Every
// handler must call this — the whole point of the method is that a fourth
// handler cannot forget a rule it has to invoke.
func (w *submitWaiter) fresh(m protocol.Message) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return !m.Timestamp.Before(w.readyAt)
}

// stampReadyAt moves the round boundary. Exported within the package only for
// declareReady and the waiter's own tests; callers in Submit go through
// declareReady, which is what keeps the stamp/drain/publish order intact.
func (w *submitWaiter) stampReadyAt(t time.Time) {
	w.mu.Lock()
	w.readyAt = t
	w.mu.Unlock()
}

// declareReady stamps a new round boundary, drains every cross-attempt
// channel, and publishes readiness — in that order, which is load-bearing.
//
// Stamping BEFORE draining means any handler that runs from here on fences
// correctly on arrival, so the drain has only to clear what was buffered
// before the stamp. Draining BEFORE publishing means no reply to THIS attempt
// can exist yet, so the drain cannot discard a legitimate signal.
//
// Reverse either and a bug returns. Drain-then-stamp leaves a window in which
// a pre-stamp message survives in a buffer and is then read as this attempt's
// answer. Publish-then-drain throws away this attempt's own acknowledgement.
//
// The publish lives inside this method rather than beside its call site
// precisely so that ordering is unreachable from outside: a caller cannot get
// the order wrong because it no longer has an order to get right.
func (w *submitWaiter) declareReady(ctx context.Context, a *agent.Agent, gateID, version string) error {
	w.stampReadyAt(time.Now())

	// Non-blocking drains: an empty channel must not block the attempt.
	select {
	case <-w.verdicts:
	default:
	}
	select {
	case <-w.acked:
	default:
	}
	select {
	case <-w.nacked:
	default:
	}
	select {
	case <-w.declined:
	default:
	}

	if err := gate.Ready(ctx, a, gateID, version); err != nil {
		return fmt.Errorf("declare ready: %w", err)
	}
	return nil
}

// The four offer* methods are the handlers' only way to signal. Each is a
// non-blocking send for the reason given on the struct's channel fields.

func (w *submitWaiter) offerVerdict(v gate.Verdict) {
	select {
	case w.verdicts <- v:
	default:
	}
}

func (w *submitWaiter) offerAck(outstanding []string) {
	select {
	case w.acked <- outstanding:
	default:
	}
}

func (w *submitWaiter) offerNack(testing map[string]string) {
	select {
	case w.nacked <- testing:
	default:
	}
}

func (w *submitWaiter) offerDeclined() {
	select {
	case w.declined <- struct{}{}:
	default:
	}
}

// roundResolution reports how waitForRoundToResolve concluded. resolved is
// false only when ctx ended before the in-flight round did. hasVerdict is
// set when the round resolved WITH a verdict that answers this attempt
// directly (the race-won case) — the caller must return it as-is rather than
// looping back to re-declare readiness, which would publish a second,
// spurious gate.Ready at a version the round already resolved.
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
// outright, since there is nothing left to wait for.
//
// That verdict is returned to the caller rather than re-buffered and left for
// the loop's next iteration: re-declaring readiness after a verdict already
// answered this attempt would publish a redundant gate.Ready at the same
// version, and in gate.go that hits the F2 cache and replays another resolve
// — another broadcast, another block fanout on failure, another OnVerdict,
// and (via pcops.Run's round counter) a run that can be failed a round early
// by a purely spurious cache replay.
func (w *submitWaiter) waitForRoundToResolve(ctx context.Context) roundResolution {
	select {
	case <-w.declined:
		return roundResolution{resolved: true}
	case v := <-w.verdicts:
		return roundResolution{verdict: v, hasVerdict: true, resolved: true}
	case <-ctx.Done():
		return roundResolution{}
	}
}
```

- [ ] **Step 4: Run the waiter's own tests**

Run: `go test ./internal/pcops/ -run TestSubmitWaiter -v`
Expected: PASS, both tests.

- [ ] **Step 5: Rewrite `Submit` to use it**

In `internal/pcops/submit.go`: delete the `readyMu`/`readyAt` var block and the `readAt`/`setReadyAt` closures, the four `make(chan ...)` lines, the `roundResolution` type and the `waitForRoundToResolve` function (both moved to `submitwaiter.go` in Step 3 — do not leave duplicates). Keep `describeVersions` where it is: `watch.go` uses it too.

Replace them with `w := newSubmitWaiter()` immediately after the `agent.New` block, then rewrite the three handlers and the loop. Every explanatory comment already in this function stays — they record findings from live runs, and the reasoning is not superseded by the refactor. The three handlers become:

```go
	a.On(protocol.IntentInform, func(_ context.Context, _ *agent.Agent, m protocol.Message) *protocol.Message {
		if id, _ := m.Body["gate"].(string); id != gateID {
			return nil
		}
		passed, ok := m.Body["passed"].(bool)
		if !ok {
			return nil // not a verdict broadcast
		}
		if !w.fresh(m) {
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
			w.offerDeclined()
			return nil
		}
		// The broadcast carries one "text" line for both outcomes ("<gate>
		// PASSED" / "<gate> FAILED: <detail>"); Detail is documented as
		// empty on pass, so only surface it on failure.
		var detail string
		if !passed {
			detail, _ = m.Body["text"].(string)
		}
		w.offerVerdict(gate.Verdict{GateID: gateID, Passed: passed, Detail: detail, Versions: versions})
		return nil
	})
	a.On(protocol.IntentAck, func(_ context.Context, _ *agent.Agent, m protocol.Message) *protocol.Message {
		if id, _ := m.Body["gate"].(string); id != gateID {
			return nil
		}
		if !w.fresh(m) {
			return nil // a previous round's ack, replayed from the cursor
		}
		w.offerAck(protocol.Strings(m.Body["outstanding"]))
		return nil
	})
	a.On(protocol.IntentNack, func(_ context.Context, _ *agent.Agent, m protocol.Message) *protocol.Message {
		if id, _ := m.Body["gate"].(string); id != gateID {
			return nil
		}
		if !w.fresh(m) {
			return nil // a previous round's nack, replayed from the cursor
		}
		w.offerNack(protocol.Versions(m.Body["testing"]))
		return nil
	})
	go a.Run(ctx)
```

Move the long doc comments that currently sit above each fence — the one on the Ack handler explaining the replay path, and the one on the Nack handler explaining why a stale Nack is worse than a stale Ack — up to `fresh`'s own doc comment in `submitwaiter.go` if they are not already covered there, rather than deleting them. The Inform handler's `readyAt` rationale block above the deleted var block moves to `fresh` too. Losing that reasoning is the one way this refactor makes things worse.

The loop becomes:

```go
	for {
		if err := w.declareReady(ctx, a, gateID, version); err != nil {
			return gate.Verdict{}, err
		}

		select {
		case outstanding := <-w.acked:
			if len(outstanding) > 0 {
				fmt.Fprintf(os.Stderr, "pc submit: gate %q acknowledged; still waiting on %s\n",
					gateID, strings.Join(outstanding, ", "))
			}
		case testing := <-w.nacked:
			fmt.Fprintf(os.Stderr, "pc submit: gate %q is mid-round (testing %s); waiting for it to finish\n",
				gateID, describeVersions(testing))
			res := w.waitForRoundToResolve(ctx)
			if !res.resolved {
				return gate.Verdict{}, ErrNoVerdict
			}
			if res.hasVerdict {
				return res.verdict, nil
			}
			continue
		case v := <-w.verdicts:
			return v, nil
		case <-time.After(AckTimeout):
			return gate.Verdict{}, fmt.Errorf("gate %q: %w", gateID, ErrNotAcknowledged)
		case <-ctx.Done():
			return gate.Verdict{}, ErrNoVerdict
		}

		select {
		case v := <-w.verdicts:
			return v, nil
		case testing := <-w.nacked:
			fmt.Fprintf(os.Stderr, "pc submit: gate %q is mid-round (testing %s); waiting for it to finish\n",
				gateID, describeVersions(testing))
			res := w.waitForRoundToResolve(ctx)
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
```

Both long comments currently attached to the loop stay: the "retry lives here rather than being returned to the caller" block above the `for`, the "first wait for the acknowledgement" block above the first select, the "surfaced directly here, not threaded back through the return value" block on the acked arm, and — most importantly — the "looks impossible from onReady's logic alone" block above the second select's nack arm. That last one is the record of a reviewed judgment call about delivery guarantees and must not be lost.

`declareReady` already wraps its error as `declare ready: %w`, so the call site returns `err` directly rather than wrapping again.

- [ ] **Step 6: Verify every existing test still passes, unmodified**

Run: `go test ./internal/pcops/ -count=1 -v 2>&1 | grep -E "^(=== RUN|--- (PASS|FAIL))"`
Expected: every test PASSes. Confirm by name that these four ran and passed: `TestSubmitDeclinesAVerdictThatDidNotIncludeIt`, `TestSubmitRedeclaresAfterNackAndReturnsTheReDeclaredVerdict`, `TestSubmitIgnoresAVerdictFromAPreviousRound`, `TestSubmitRedundantResubmitAnsweredFromCacheDoesNotErrorAsUnacknowledged`.

Then confirm you changed no test: `git diff --stat internal/pcops/submit_test.go` must show no changes. If it shows any, revert them.

Then: `go test ./... -count=1`, `go test ./... -race -count=1`, and `go test ./internal/pcops/ -count=3` — the last because this package is timing-sensitive and a refactor of its retry loop is exactly what makes it flaky.

- [ ] **Step 7: Commit**

```bash
git add internal/pcops/submitwaiter.go internal/pcops/submitwaiter_test.go internal/pcops/submit.go
git commit -m "refactor(pcops): submitWaiter makes Submit's invariants structural

Two rules were maintained by copy-paste and each was got wrong once: fence
every arriving message against the round boundary (the Nack handler shipped
without it), and drain cross-attempt state before re-declaring (the first
version drained one of three channels). declareReady owns the stamp/drain/
publish order so a caller cannot get it wrong, because it no longer has an
order to get right. No behaviour change; every existing test unmodified."
```

---

## Task 4: `budget.runner_timeout`, and `LoadConfig` fails fast

The spec's Packaging section says validation moves into `LoadConfig` so every command fails fast, and enumerates the rules. It also implies the timeout budgets are coherent, and they are not: `budget.submit_timeout` is a config field defaulting to five minutes, while the coordinator's runner timeout is hardcoded at ten in `up.go` and unreachable from a scenario file. So the shipped default pair is inverted — a submit nacked by a genuinely long round exhausts its own context before the round it is waiting for can finish. Part 1 made that survivable; it did not make the retry usable.

**Files:**
- Modify: `internal/pcops/config.go`
- Modify: `internal/pcops/up.go`
- Test: `internal/pcops/config_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces: `Config.RunnerTimeout time.Duration`, `DefaultRunnerTimeout = 10 * time.Minute`, the `budget.runner_timeout` YAML key, and a `validate() error` method on `Config` that `LoadConfig` calls last. Task 7's `pc init` scaffolds a file that must satisfy `validate`.

- [ ] **Step 1: Write the failing test**

Add to `internal/pcops/config_test.go` (create the file if it does not exist, `package pcops_test`, importing `os`, `path/filepath`, `strings`, `testing`, `time` and the `pcops` package):

```go
// One table, one invalid shape per row, each mapping to a specific error. A
// mismatch here used to produce a silent hang to the wall budget: pc up would
// start, no readiness would ever satisfy a gate that named a participant not
// in agents, and an operator would wait out the whole scenario to learn it.
func TestLoadConfigRejectsIncoherentScenarios(t *testing.T) {
	const valid = `
repo: /tmp/repo
db: /tmp/bus.db
gate:
  id: currency
  required: [billing]
  runner: integrator
  run: "go test ./integration/..."
agents:
  - name: billing
    branch: agent/billing
runner:
  name: integrator
  branch: agent/integration
budget:
  wall: 10m
  submit_timeout: 12m
  runner_timeout: 10m
`
	for _, tc := range []struct {
		name    string
		mutate  func(string) string
		wantErr string
	}{
		{
			"gate id empty",
			func(s string) string { return strings.Replace(s, "  id: currency", `  id: ""`, 1) },
			"gate.id",
		},
		{
			"required names an agent that does not exist",
			func(s string) string { return strings.Replace(s, "required: [billing]", "required: [nosuch]", 1) },
			"gate.required",
		},
		{
			"runner does not match runner.name",
			func(s string) string { return strings.Replace(s, "runner: integrator", "runner: nosuch", 1) },
			"gate.runner",
		},
		{
			"duplicate agent name",
			func(s string) string {
				return strings.Replace(s, "  - name: billing\n    branch: agent/billing",
					"  - name: billing\n    branch: agent/billing\n  - name: billing\n    branch: agent/other", 1)
			},
			"duplicate agent name",
		},
		{
			"duplicate branch",
			func(s string) string {
				return strings.Replace(s, "  - name: billing\n    branch: agent/billing",
					"  - name: billing\n    branch: agent/billing\n  - name: gateway\n    branch: agent/billing", 1)
			},
			"duplicate branch",
		},
		{
			"runner reuses an agent's branch",
			func(s string) string { return strings.Replace(s, "  branch: agent/integration", "  branch: agent/billing", 1) },
			"duplicate branch",
		},
		{
			"submit timeout below runner timeout",
			func(s string) string { return strings.Replace(s, "submit_timeout: 12m", "submit_timeout: 5m", 1) },
			"budget.submit_timeout",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "pc.yaml")
			if err := os.WriteFile(path, []byte(tc.mutate(valid)), 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := pcops.LoadConfig(path)
			if err == nil {
				t.Fatalf("LoadConfig accepted an invalid scenario (%s)", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("LoadConfig error = %q, want it to name %q so an operator knows which key to fix", err, tc.wantErr)
			}
		})
	}

	// The control: the unmutated document must load, or every row above
	// could be passing for the wrong reason.
	path := filepath.Join(t.TempDir(), "pc.yaml")
	if err := os.WriteFile(path, []byte(valid), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := pcops.LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig rejected the valid control document: %v", err)
	}
	if cfg.RunnerTimeout != 10*time.Minute {
		t.Errorf("RunnerTimeout = %v, want 10m from budget.runner_timeout", cfg.RunnerTimeout)
	}
	if cfg.SubmitTimeout != 12*time.Minute {
		t.Errorf("SubmitTimeout = %v, want 12m", cfg.SubmitTimeout)
	}
}

// Both budgets must have working defaults, because a scenario file is allowed
// to omit the whole budget block — and the defaults must not be the inverted
// pair that made the Nack retry unusable.
func TestLoadConfigDefaultsLeaveTheRetryUsable(t *testing.T) {
	const minimal = `
repo: /tmp/repo
db: /tmp/bus.db
gate:
  id: currency
  required: [billing]
  runner: integrator
  run: "go test ./integration/..."
agents:
  - name: billing
    branch: agent/billing
runner:
  name: integrator
  branch: agent/integration
`
	path := filepath.Join(t.TempDir(), "pc.yaml")
	if err := os.WriteFile(path, []byte(minimal), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := pcops.LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.RunnerTimeout <= 0 {
		t.Fatalf("RunnerTimeout = %v, want a working default", cfg.RunnerTimeout)
	}
	// The whole point: a submit nacked by a long round has to be able to
	// outlast that round, or the retry it was built for cannot complete.
	if cfg.SubmitTimeout <= cfg.RunnerTimeout {
		t.Fatalf("default SubmitTimeout %v <= default RunnerTimeout %v: a nacked submit exhausts its own context before the round it waits for can finish",
			cfg.SubmitTimeout, cfg.RunnerTimeout)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/pcops/ -run TestLoadConfig -v`
Expected: FAIL. `TestLoadConfigRejectsIncoherentScenarios` fails on its first row — `LoadConfig` accepts everything structurally parseable today. `TestLoadConfigDefaultsLeaveTheRetryUsable` fails to compile (`cfg.RunnerTimeout` undefined).

- [ ] **Step 3: Add the field and the default**

In `internal/pcops/config.go`, beside `DefaultSubmitTimeout`:

```go
// DefaultSubmitTimeout is how long `pc submit` waits for a verdict before
// reporting that none was obtained.
//
// It must exceed DefaultRunnerTimeout. A readiness that lands while a round is
// already in flight is nacked, and the submit then waits for that round to
// resolve before re-declaring — so a submit whose own budget is shorter than a
// round's exhausts its context before the round it is waiting for can finish,
// and reports ErrNoVerdict for a gate that was working correctly. LoadConfig
// enforces the same relationship for configured values.
const DefaultSubmitTimeout = 12 * time.Minute

// DefaultRunnerTimeout bounds how long the coordinator waits for the runner's
// spanning test before declaring the round stalled. Ten minutes is sized for a
// real integration suite, not a unit test.
const DefaultRunnerTimeout = 10 * time.Minute
```

Add to `Config`:

```go
	SubmitTimeout time.Duration
	RunnerTimeout time.Duration
	Wall          time.Duration // zero means unbounded
```

Add to `rawConfig`'s `Budget` struct:

```go
	Budget struct {
		Wall          string `yaml:"wall"`
		SubmitTimeout string `yaml:"submit_timeout"`
		RunnerTimeout string `yaml:"runner_timeout"`
	} `yaml:"budget"`
```

In `LoadConfig`, seed the default alongside `SubmitTimeout`:

```go
		SubmitTimeout: DefaultSubmitTimeout,
		RunnerTimeout: DefaultRunnerTimeout,
```

and parse the new key beside the existing two:

```go
	if raw.Budget.RunnerTimeout != "" {
		if cfg.RunnerTimeout, err = time.ParseDuration(raw.Budget.RunnerTimeout); err != nil {
			return Config{}, fmt.Errorf("budget.runner_timeout: %w", err)
		}
	}
```

- [ ] **Step 4: Add validation**

Still in `internal/pcops/config.go`, add below `LoadConfig`:

```go
// validate rejects a scenario that parses but cannot work.
//
// Every rule here was a silent hang before it was an error. A gate naming a
// participant absent from agents waits for a readiness nobody will ever
// declare; a gate.runner that matches no runner sends its spanning-test
// request to an agent that does not exist; two agents sharing a branch fight
// over one worktree. In each case `pc up` started cleanly and the operator
// waited out the wall budget to learn nothing. Failing at load costs one line
// of output instead.
func (c Config) validate() error {
	if c.GateID == "" {
		return fmt.Errorf("gate.id must not be empty")
	}
	if c.Gate.Runner == "" {
		return fmt.Errorf("gate.runner must name the runner agent")
	}
	if c.Gate.Runner != c.Runner.Name {
		return fmt.Errorf("gate.runner is %q but runner.name is %q: the spanning-test request would go to an agent that does not exist",
			c.Gate.Runner, c.Runner.Name)
	}
	if len(c.Gate.Required) == 0 {
		return fmt.Errorf("gate.required must name at least one participant")
	}

	names := make(map[string]bool, len(c.Agents)+1)
	branches := make(map[string]bool, len(c.Agents)+1)
	for _, a := range append(append([]AgentDef(nil), c.Agents...), c.Runner) {
		if a.Name == "" {
			return fmt.Errorf("every agent needs a name")
		}
		if a.Branch == "" {
			return fmt.Errorf("agent %q needs a branch", a.Name)
		}
		if names[a.Name] {
			return fmt.Errorf("duplicate agent name %q: one active owner per participant", a.Name)
		}
		if branches[a.Branch] {
			return fmt.Errorf("duplicate branch %q: two participants would contend for one worktree", a.Branch)
		}
		names[a.Name] = true
		branches[a.Branch] = true
	}
	for _, r := range c.Gate.Required {
		if !names[r] {
			return fmt.Errorf("gate.required names %q, which is not in agents: the gate would wait forever for a readiness nobody declares", r)
		}
	}

	if c.SubmitTimeout <= c.RunnerTimeout {
		return fmt.Errorf("budget.submit_timeout (%v) must exceed budget.runner_timeout (%v): a readiness that lands mid-round is nacked and waits for that round to resolve, so a shorter submit budget expires before the round it is waiting for can finish",
			c.SubmitTimeout, c.RunnerTimeout)
	}
	return nil
}
```

and call it as `LoadConfig`'s last act, replacing the bare `return cfg, nil`:

```go
	if err := cfg.validate(); err != nil {
		return Config{}, fmt.Errorf("invalid scenario %s: %w", path, err)
	}
	return cfg, nil
```

- [ ] **Step 5: Use the configured runner timeout**

In `internal/pcops/up.go`, replace the hardcoded call:

```go
	c.SetRunnerTimeout(cfg.RunnerTimeout)
```

`StartCoordinator` already receives `cfg`. If `cfg.RunnerTimeout` is zero — a `Config` built by hand in a test rather than loaded from a file — fall back rather than setting a zero timeout, which would stall every round instantly:

```go
	runnerTimeout := cfg.RunnerTimeout
	if runnerTimeout <= 0 {
		runnerTimeout = DefaultRunnerTimeout
	}
	c.SetRunnerTimeout(runnerTimeout)
```

- [ ] **Step 6: Run the tests**

Run: `go test ./internal/pcops/ -run TestLoadConfig -v`
Expected: PASS, all eight subtests plus both control assertions.

Then `go test ./... -count=1`. **Expect fallout here and read it carefully.** Several existing tests build a `Config` by hand with a short `SubmitTimeout` and no `RunnerTimeout`; they do not go through `LoadConfig`, so `validate` never runs and they are unaffected. But `TestSubmitTimesOutWithoutACoordinator` and any test asserting a timeout duration may depend on `DefaultSubmitTimeout` being five minutes. If one fails because the default changed from 5m to 12m, fix the test's expectation — that is a legitimate consequence of the change. If one fails for any other reason, stop and report it.

Then `go test ./... -race -count=1`.

- [ ] **Step 7: Commit**

```bash
git add internal/pcops/config.go internal/pcops/config_test.go internal/pcops/up.go
git commit -m "feat(pcops): both timeout budgets are the operator's, and LoadConfig fails fast

The runner timeout was hardcoded at ten minutes while submit_timeout defaulted
to five, so the retry part 1 built could not complete under shipped defaults:
a nacked submit exhausted its own context before the round it waited for
finished. Both are now config keys with a validated relationship.

Every other rule here was a silent hang before it was an error — a gate naming
an absent participant, a runner matching no agent, two participants sharing a
branch. pc up would start cleanly and the operator would wait out the wall
budget to learn nothing."
```

---
## Task 5: `fixtures/two-service` — a committed artifact a human drives

There is no fixture in this repository. The live runs used a throwaway that no longer exists, and rebuilding it from memory each time is why two of the four design constraints below were learned twice.

It is **its own Go module**, so the root `./...` excludes it from the project's build, vet and test surface and agents can mutate it freely during a demo without touching project source.

**Files:**
- Create: `fixtures/two-service/go.mod`
- Create: `fixtures/two-service/billing/billing.go`
- Create: `fixtures/two-service/gateway/gateway.go`
- Create: `fixtures/two-service/integration/currency_test.go`
- Create: `fixtures/two-service/README.md`
- Create: `internal/fixtures/twoservice_test.go`

**Interfaces:**
- Consumes: nothing from this repo — the fixture must not import it.
- Produces: a fixture whose gate command is `EXPECTED_CURRENCY=USD go test ./integration/...`. Tasks 6 and 9 reference that command string and the path `fixtures/two-service`.

- [ ] **Step 1: Create the module and both services**

`fixtures/two-service/go.mod`:

```
module example.com/twoservice

go 1.22
```

`fixtures/two-service/billing/billing.go`:

```go
// Package billing renders invoice lines for a customer statement.
package billing

import "fmt"

// Invoice is one line on a statement.
//
// Currency exists here from the start, deliberately. An earlier arrangement of
// this fixture gave billing the job of ADDING this field and gateway the job
// of setting it — so gateway could not compile until billing's change merged,
// and the two agents deadlocked rather than coordinated. Each half must build
// alone; the coordination this fixture is meant to exercise is about the
// agreed VALUE, not about whether the code compiles.
type Invoice struct {
	Customer    string
	AmountMinor int64
	Currency    string
}

// Render formats one invoice line. The currency is last so a failing spanning
// test can assert on the suffix and quote something readable.
func Render(inv Invoice) string {
	return fmt.Sprintf("%s: %d.%02d %s", inv.Customer, inv.AmountMinor/100, inv.AmountMinor%100, inv.Currency)
}
```

`fixtures/two-service/gateway/gateway.go`:

```go
// Package gateway builds invoices from incoming payment events.
package gateway

import "example.com/twoservice/billing"

// defaultCurrency is what this service stamps on every invoice it builds.
//
// It is seeded WRONG on purpose. Round one of a gate run has to fail for a
// realistic reason, and this is the most realistic one there is: an agent
// inherits existing code, has no reason to doubt it, and the gate teaches it
// otherwise. The correct value is written nowhere in this module — see
// integration/currency_test.go for why.
const defaultCurrency = "EUR"

// Build turns a payment event into an invoice.
func Build(customer string, amountMinor int64) billing.Invoice {
	return billing.Invoice{Customer: customer, AmountMinor: amountMinor, Currency: defaultCurrency}
}
```

- [ ] **Step 2: Create the spanning test**

`fixtures/two-service/integration/currency_test.go`:

```go
package integration_test

import (
	"os"
	"strings"
	"testing"

	"example.com/twoservice/billing"
	"example.com/twoservice/gateway"
)

// The spanning test: the only place the two services meet, and so the only
// thing that can fail when they disagree.
//
// Two constraints are encoded here, both learned from live runs.
//
// The agreed value arrives from the ENVIRONMENT, not from any file in this
// module. Capable models simply read a hardcoded expectation out of a test and
// fix the code to match it, first try — which meant the failure branch of a
// live run was never reached and the interesting behaviour was never observed.
// Supplying it from the gate command is also the realistic arrangement: the
// integration environment owns the contract between services, not either
// service's own source.
//
// And it SKIPS rather than fails when the variable is absent, so an agent
// running `go test ./...` inside its own worktree is not misled into thinking
// it has broken something. That also reinforces what the agent contract tells
// it directly: you cannot run the spanning test yourself, only the gate can.
func TestGatewayStampsTheAgreedCurrency(t *testing.T) {
	want := os.Getenv("EXPECTED_CURRENCY")
	if want == "" {
		t.Skip("EXPECTED_CURRENCY is not set: this spanning test runs only from the gate command, which owns the contract between these services")
	}

	line := billing.Render(gateway.Build("acme", 125_00))
	if !strings.HasSuffix(line, " "+want) {
		t.Fatalf("gateway stamped the wrong currency: rendered %q, want a line ending in %q", line, want)
	}
}
```

The failure message quotes the wanted value on purpose. That is the mechanism the whole fixture exists to exercise: the gate's failure detail is the only channel through which an agent can learn a value absent from its own worktree, and a live run confirmed an agent doing exactly that.

- [ ] **Step 3: Document the fixture for whoever finds it next**

`fixtures/two-service/README.md`:

```markdown
# two-service fixture

Two services that must agree on one value, and a spanning test that only the
gate can run. It exists to be driven by real coding agents through
`pc up` / `pc submit`, not by CI — `scripts/live-run.sh` is its usual driver.

It is a separate Go module so the parent project's `go build ./...`,
`go vet ./...` and `go test ./...` do not see it. Agents mutate it freely
during a run; nothing here is project source.

## The gate command

    EXPECTED_CURRENCY=USD go test ./integration/...

## Four constraints, each learned the hard way

1. **Each half compiles independently.** `billing.Invoice.Currency` exists up
   front. An earlier version made adding the field one agent's job and setting
   it the other's, so one could not compile until the other merged, and they
   deadlocked instead of coordinating.
2. **The agreed value is undiscoverable from either worktree.** `USD` appears
   nowhere in this module. It reaches the run only through the gate command's
   environment. Without this, capable models read the expectation out of the
   test and the failure branch is never exercised.
3. **One side is seeded wrong.** `gateway` stamps `"EUR"`, so round one fails
   for a reason agents actually encounter: inherited code that looks fine.
4. **The spanning test skips when the variable is absent**, so an agent running
   the suite locally is not misled, and the contract's claim that it cannot run
   the spanning test itself stays true.

`internal/fixtures/twoservice_test.go` in the parent module enforces all four.
```

- [ ] **Step 4: Write the repo-side test that enforces all four constraints**

Create `internal/fixtures/twoservice_test.go`:

```go
package fixtures_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// fixtureRoot is the committed fixture. It is a separate Go module, so nothing
// in the parent module's ./... ever compiles or runs it — which is exactly why
// it needs a test here: an artifact excluded from the build surface rots
// silently, and a fixture that has rotted into a PASSING state is worse than a
// missing one, because a live run then proves nothing while appearing to work.
func fixtureRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", "fixtures", "two-service"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("fixture module not found at %s: %v", root, err)
	}
	return root
}

// copyTree copies src to dst so a test can run the fixture's own suite without
// mutating the committed copy.
func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, b, info.Mode())
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Constraint 3, and the fixture's whole purpose: the baseline must FAIL under
// the gate command. If this ever passes, round one of a live run succeeds
// immediately, the failure-detail path is never exercised, and the
// demonstration silently stops demonstrating anything.
func TestFixtureBaselineFailsUnderTheGateCommand(t *testing.T) {
	dst := t.TempDir()
	copyTree(t, fixtureRoot(t), dst)

	cmd := exec.Command("go", "test", "./integration/...")
	cmd.Dir = dst
	cmd.Env = append(os.Environ(), "EXPECTED_CURRENCY=USD")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("fixture baseline PASSED under EXPECTED_CURRENCY=USD; round one of a live run would succeed immediately and the failure-detail path would never be exercised:\n%s", out)
	}
	if !strings.Contains(string(out), "EUR") {
		t.Errorf("baseline failed, but the failure detail does not mention the wrong value it found; an agent learns the right value from this text:\n%s", out)
	}
	if !strings.Contains(string(out), "USD") {
		t.Errorf("baseline failed, but the failure detail does not name the wanted value; that text is the ONLY channel through which an agent can learn a value absent from its worktree:\n%s", out)
	}
}

// Constraint 4: without the variable the spanning test skips, so an agent
// running the suite in its own worktree is not misled into thinking it broke
// something.
func TestFixtureSpanningTestSkipsWithoutTheVariable(t *testing.T) {
	dst := t.TempDir()
	copyTree(t, fixtureRoot(t), dst)

	cmd := exec.Command("go", "test", "./integration/...")
	cmd.Dir = dst
	// Explicitly cleared rather than merely unset in this process's env.
	cmd.Env = append(os.Environ(), "EXPECTED_CURRENCY=")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("spanning test did not pass-by-skipping with no EXPECTED_CURRENCY; an agent running the suite locally would think it had broken something:\n%s", out)
	}
}

// Constraint 1: each half builds alone. If one service cannot compile without
// the other's change, the two agents deadlock instead of coordinating — which
// is what an earlier arrangement of this fixture actually caused.
func TestFixtureHalvesCompileIndependently(t *testing.T) {
	root := fixtureRoot(t)
	for _, pkg := range []string{"./billing/...", "./gateway/..."} {
		cmd := exec.Command("go", "build", pkg)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Errorf("go build %s failed, so this half cannot compile alone:\n%s", pkg, out)
		}
	}
}

// Constraint 2: the agreed value must be undiscoverable from either worktree.
// A model that can read USD out of the fixture fixes the code first try, and
// the failure branch — the interesting one — is never reached.
func TestFixtureDoesNotContainTheAgreedValue(t *testing.T) {
	root := fixtureRoot(t)
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if strings.Contains(string(b), "USD") {
			rel, _ := filepath.Rel(root, path)
			t.Errorf("%s contains the agreed value: an agent can read it instead of learning it from a failing gate", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
```

Note the fixture's own README does not contain the literal `USD` outside the gate-command line — which it does. Put the gate command in the fixture README as `EXPECTED_CURRENCY=USD go test ./integration/...`, then **exclude `README.md` from the constraint-2 walk**, because documentation for a human maintainer is not a file an agent's worktree exposes as source. Add this to the walk, before the read:

```go
		if filepath.Base(path) == "README.md" {
			return nil // maintainer documentation, not agent-visible source
		}
```

- [ ] **Step 5: Run the fixture tests**

Run: `go test ./internal/fixtures/ -v`
Expected: all four PASS. `TestFixtureBaselineFailsUnderTheGateCommand` passing means the fixture correctly fails; read its output once by hand to confirm the failure text names both `EUR` and `USD`.

Then confirm the fixture really is excluded from the parent module: `go test ./... -count=1` must not compile anything under `fixtures/`, and `go vet ./...` must stay clean. `go list ./... | grep twoservice` must return nothing.

- [ ] **Step 6: Commit**

```bash
git add fixtures/two-service internal/fixtures/twoservice_test.go
git commit -m "feat(fixtures): committed two-service fixture with its four constraints enforced

Two services that must agree on one value, and a spanning test only the gate
can run. Its own Go module, so the project's build surface excludes it and
agents can mutate it freely.

Each of the four design constraints cost a live run to learn, and each now has
a test: both halves compile alone, USD appears nowhere in the module, gateway
is seeded EUR so round one fails realistically, and the spanning test skips
rather than fails when the variable is absent."
```

---

## Task 6: The contract snippet and the two verified recipes

The contract snippet is the portable artifact and the project's strongest evidence for its harness-agnostic claim: the same ~20 lines worked verbatim across pi and Claude Code with zero prompt revisions. It exists in no file today — it lived only inside the live-run sessions.

It documents `pc send`. The live-fire findings record that it did not, and that agents worked out peer messaging anyway, three times unprompted — the best evidence the project has that the protocol is legible rather than merely documented. Documenting it forfeits observing that again. That trade is made deliberately: part 2's job is that someone else can install this, and withholding a working tool to preserve a research observation is wrong once shipping. The observation is recorded and dated in the live-fire findings.

**Files:**
- Create: `docs/agent-contract.md`
- Create: `docs/recipes/pi.md`
- Create: `docs/recipes/claude-code.md`
- Create: `docs/recipes/other-harnesses.md`

**Interfaces:**
- Consumes: the gate command from Task 5; `pc submit` / `pc send` / `pc watch` as they exist.
- Produces: `docs/agent-contract.md` as the single path Task 9's script and both recipes point at.

- [ ] **Step 1: Write the contract snippet**

`docs/agent-contract.md`. Keep it short — every line is context an agent pays for on every turn, and its brevity is part of why it ported across harnesses unchanged:

```markdown
# You are one agent in a coordinated change

You own one service in a shared repository. Another agent owns the other. You
each work in your own git worktree and cannot see each other's files.

Your task is in `$PC_TASK`. Your agent name is `$PC_AGENT`.

## Committing and submitting

When your change is ready:

1. Commit it in your worktree.
2. Run: `pc submit --gate <gate-id> --agent "$PC_AGENT"`

`pc submit` blocks until a cross-service test gate has run and returns a
verdict. Exit code 0 means the gate passed. A non-zero exit means it failed or
could not decide; read its output.

**You cannot run the spanning test yourself.** It lives outside both worktrees
and only the gate can run it. Running your own tests locally is still useful
for checking your half compiles and behaves.

## Reading a failure

A failing gate tells you what it found and what it expected. That text may be
the only place a value you need appears — if your worktree does not contain
it, the failure detail is where to look.

## Talking to the other agent

`pc send --to <agent> "<message>"` delivers a message to another participant.
Use it when you need something only they can answer — an agreed value, a
choice of representation, whether they have already handled a case.

## Rules

- Change only your own service. Do not edit the other agent's files.
- Do not edit the spanning test to make it pass.
- Do not weaken assertions. A gate that passes for the wrong reason is worse
  than one that fails.
- Re-submit after each fix. The gate re-runs on every submission.
```

- [ ] **Step 2: Write the pi recipe**

`docs/recipes/pi.md`:

```markdown
# Recipe: pi

Verified end to end against a real gate run. `@earendil-works/pi-coding-agent`.

## Loading the contract

    pi --skill docs/agent-contract.md -p "$PC_TASK" < /dev/null

`--skill` is the right channel: CLI-provided resources load **before** project
trust is resolved, and non-interactive modes never prompt for that trust — so a
contract supplied any other way may simply not be present when the agent
starts.

## The stdin trap — read this before backgrounding anything

`pi -p` merges piped stdin into the prompt. A backgrounded invocation that
inherits an open stdin therefore **blocks forever**, with no error and no
output. Always redirect:

    pi --skill docs/agent-contract.md -p "$PC_TASK" < /dev/null &

This cost two nine-minute live runs before it was understood. It is invisible
until you drive two harnesses side by side, because the other one does not
behave this way.

## Environment

| Variable | Purpose |
|---|---|
| `PC_AGENT` | This agent's name, as it appears in `gate.required` |
| `PC_TASK` | The task text |
| `PC_DB` | The coordination database, so `pc` finds the same bus as the daemon |

`pc` must be on `PATH`. Build it with `go build -o "$BINDIR/pc" ./cmd/pc` from
the project root — not from inside a worktree or the fixture, which is how one
live run silently tested a stale binary for nine minutes.

## What pi does not have

No MCP support, by design, and no background bash. Neither is needed: the
contract only requires running a shell command and reading its exit code.
```

- [ ] **Step 3: Write the Claude Code recipe**

`docs/recipes/claude-code.md`:

```markdown
# Recipe: Claude Code

Verified end to end against a real gate run.

## Loading the contract

Claude Code reads `CLAUDE.md` from the working directory, so the contract is
copied into the agent's worktree as part of setting the run up:

    cp docs/agent-contract.md "$WORKTREE/CLAUDE.md"

## Print mode

A non-interactive run needs its permissions declared up front, or it stops to
ask and a backgrounded process simply waits:

    claude -p "$PC_TASK" \
      --permission-mode acceptEdits \
      --allowedTools Read Edit Write Bash \
      < /dev/null

`Bash` is not optional — `pc submit` is a shell command, and without it the
agent can edit code but never reach the gate.

## Environment

| Variable | Purpose |
|---|---|
| `PC_AGENT` | This agent's name, as it appears in `gate.required` |
| `PC_TASK` | The task text |
| `PC_DB` | The coordination database, so `pc` finds the same bus as the daemon |

`pc` must be on `PATH`, built from the project root.
```

- [ ] **Step 4: Write the honest statement for everything else**

`docs/recipes/other-harnesses.md`:

```markdown
# Other harnesses

There are no recipes here for other harnesses, deliberately. This project
claims support only for what it has driven end to end through a real gate,
which today is pi and Claude Code.

## What a harness needs

Any coding agent can participate if it can do four things:

1. **Run a shell command** and let the agent see its output. `pc submit` and
   `pc send` are shell commands; nothing else is required.
2. **Surface exit codes**, or at least let the agent read them. `pc submit`
   distinguishes pass, fail and could-not-decide by exit status.
3. **Accept an instruction file** at startup — `docs/agent-contract.md`, by
   whatever mechanism the harness offers.
4. **Work in a directory you choose**, so it can be pointed at a git worktree.

No MCP server, no plugin, and no API integration is needed.

## If you get one working

The two existing recipes are short because there was little to say once the
contract loaded. If you drive a third harness through a full run, a recipe of
the same shape is welcome — and please record what surprised you, not just what
worked. Both existing recipes carry a trap that cost real time.
```

- [ ] **Step 5: Commit**

```bash
git add docs/agent-contract.md docs/recipes
git commit -m "docs: the contract snippet, and recipes for the two verified harnesses

The snippet is the portable artifact — the same ~20 lines ran verbatim on pi
and Claude Code with no revisions — and it existed in no file until now.

It documents pc send. The live-fire runs deliberately withheld it and agents
discovered peer messaging anyway, three times; that is the best evidence this
project has that the protocol is legible. Shipping the tool forfeits observing
that again, which is the right trade for an artifact someone else installs.

Recipes for pi and Claude Code only, each carrying the trap that cost a live
run. For anything else: what a harness needs, and no claim of support."
```

---
## Task 7: `pc init`, and `--config` finds `./.pc.yaml`

The original CLI design described both and neither shipped. Today every command that needs a scenario requires `--config` explicitly, and there is no way to produce a valid scenario file except by reading the loader's struct tags.

**Files:**
- Modify: `cmd/pc/main.go`
- Test: `cmd/pc/main_test.go`

**Interfaces:**
- Consumes: `pcops.LoadConfig` with Task 4's `validate`, and the fixture path from Task 5.
- Produces: `pc init [--force]`; a shared `resolveConfig(flagValue, command string) (pcops.Config, error)` replacing the three per-command helpers; `usage` listing six commands. Task 9's script relies on `--config .pc.yaml` working and on `pc init` producing a file that loads.

- [ ] **Step 1: Write the failing tests**

Add to `cmd/pc/main_test.go`:

```go
// Round-trip, not byte comparison: what matters is that the scaffold is a
// document this project's own loader accepts and validates. Asserting exact
// bytes would break on every comment edit while proving less.
func TestInitWritesAConfigThatLoadsAndValidates(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)

	if got := run(context.Background(), []string{"init"}); got != 0 {
		t.Fatalf("pc init = %d, want 0", got)
	}
	if _, err := os.Stat(".pc.yaml"); err != nil {
		t.Fatalf("pc init did not write .pc.yaml: %v", err)
	}
	if _, err := pcops.LoadConfig(".pc.yaml"); err != nil {
		t.Fatalf("pc init wrote a scenario its own loader rejects: %v", err)
	}
}

// Overwriting a scenario someone has edited is destructive and silent. It
// needs an explicit flag.
func TestInitRefusesToClobberWithoutForce(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)

	if err := os.WriteFile(".pc.yaml", []byte("# mine\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := run(context.Background(), []string{"init"}); got == 0 {
		t.Fatal("pc init overwrote an existing .pc.yaml and exited 0")
	}
	b, err := os.ReadFile(".pc.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "# mine\n" {
		t.Fatalf("pc init modified an existing file without --force; contents now %q", b)
	}
	if got := run(context.Background(), []string{"init", "--force"}); got != 0 {
		t.Fatalf("pc init --force = %d, want 0", got)
	}
	if _, err := pcops.LoadConfig(".pc.yaml"); err != nil {
		t.Fatalf("pc init --force wrote a scenario its own loader rejects: %v", err)
	}
}

// The whole point of a default: an operator in a directory with a .pc.yaml
// should not have to name it on every command.
func TestConfigDefaultsToDotPcYamlWhenPresent(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)

	if got := run(context.Background(), []string{"init"}); got != 0 {
		t.Fatal("pc init failed")
	}
	// The scaffold puts the database under ./.pc/, and cmdWatch reports a bus
	// failure with the SAME exit code 2 it uses for a missing config — so
	// without this directory the test would fail for a reason that has
	// nothing to do with config defaulting.
	if err := os.MkdirAll(".pc", 0o755); err != nil {
		t.Fatal(err)
	}
	// watch --no-follow must now get past config resolution. Exit 2 here can
	// only mean the default was not applied.
	code := run(context.Background(), []string{"watch", "--no-follow"})
	if code == 2 {
		t.Errorf("pc watch with no --config exited 2 in a directory containing .pc.yaml; the default was not applied")
	}
}

// And the inverse, so the default cannot silently mask a genuine mistake.
func TestConfigStillRequiredWhenNoDotPcYaml(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)

	if got := run(context.Background(), []string{"watch", "--no-follow"}); got != 2 {
		t.Errorf("pc watch with no --config and no .pc.yaml = %d, want 2", got)
	}
}
```

`t.Chdir` requires Go 1.24; this project's floor is 1.22. Use this helper instead, added once at the bottom of `main_test.go`:

```go
// chdir moves into dir for the duration of the test. t.Chdir would do this
// but arrived in Go 1.24 and this module's floor is 1.22.
func chdir(t *testing.T, dir string) {
	t.Helper()
	prev, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chdir(prev) })
}
```

and replace every `t.Chdir(dir)` above with `chdir(t, dir)`. These four tests must not run in parallel with anything else in the package, since they change the process's working directory — do not add `t.Parallel()` to them.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/pc/ -run 'TestInit|TestConfig' -v`
Expected: FAIL. The two `init` tests exit 2 with `unknown command "init"`; `TestConfigDefaultsToDotPcYamlWhenPresent` exits 2 because `--config` is still mandatory.

- [ ] **Step 3: Add `pc init`**

In `cmd/pc/main.go`, add the scaffold as a package-level constant. It points at the committed fixture so `pc init` followed by `scripts/live-run.sh` works with no editing:

```go
// initScaffold is what `pc init` writes. It is a working scenario against the
// committed fixture rather than a skeleton of empty keys: the fastest way to
// understand a scenario file is to run one, and a scaffold that fails
// validation teaches the wrong first lesson. Every value here satisfies
// pcops.LoadConfig's validation — TestInitWritesAConfigThatLoadsAndValidates
// asserts exactly that.
const initScaffold = `# Parallel Consciousness scenario.
#
# A scenario is a hand-written loop definition: who participates, what gate
# they must pass, and what bounds the run. This one drives the committed
# two-service fixture; point repo/agents at your own code to adapt it.

# The repository the agents work in. Each agent gets its own git worktree of it.
repo: ./fixtures/two-service

# The coordination database. Every pc command must agree on this path, so
# either keep it here or set $PC_DB (which overrides this).
db: ./.pc/bus.db

gate:
  # Gate id. Agents pass this to 'pc submit --gate'.
  id: currency
  # Every participant that must declare readiness before the gate runs.
  # Each name must appear in 'agents' below.
  required: [billing, gateway]
  # Which agent runs the spanning test. Must match runner.name below.
  runner: integrator
  # The spanning test itself, run in the runner's worktree after every
  # participant's branch is merged into it.
  #
  # EXPECTED_CURRENCY lives here on purpose: the value the two services must
  # agree on is supplied by the integration environment, so neither agent can
  # read it out of its own worktree. They learn it from a failing gate.
  run: "EXPECTED_CURRENCY=USD go test ./integration/..."

agents:
  - name: billing
    branch: agent/billing
    role: implementer
    task: "Render the invoice currency correctly. You own billing/ only."
  - name: gateway
    branch: agent/gateway
    role: implementer
    task: "Stamp the agreed currency on invoices you build. You own gateway/ only."

runner:
  name: integrator
  branch: agent/integration

budget:
  # Total wall clock for one run. Omit for unbounded.
  wall: 20m
  # How long 'pc submit' waits for a verdict. Must exceed runner_timeout: a
  # readiness that lands mid-round is nacked and then waits for that round to
  # finish, so a shorter budget expires before the round it is waiting for.
  submit_timeout: 12m
  # How long the coordinator waits for the spanning test before calling the
  # round stalled.
  runner_timeout: 10m
`

// defaultConfigPath is what every command that needs a scenario falls back to
// when --config is not given.
const defaultConfigPath = ".pc.yaml"

func cmdInit(_ context.Context, args []string) int {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	force := fs.Bool("force", false, "overwrite an existing "+defaultConfigPath)
	fs.Parse(args)

	if _, err := os.Stat(defaultConfigPath); err == nil && !*force {
		// Refusing is the whole feature: a scenario file is hand-edited, and
		// silently replacing one is destructive in a way no other pc command is.
		fmt.Fprintf(os.Stderr, "pc init: %s already exists (use --force to overwrite)\n", defaultConfigPath)
		return 2
	}
	if err := os.WriteFile(defaultConfigPath, []byte(initScaffold), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "pc init: %v\n", err)
		return 2
	}
	fmt.Printf("wrote %s\n", defaultConfigPath)
	return 0
}
```

Register it in `run`'s dispatch, before the `default` arm:

```go
	case "init":
		return cmdInit(ctx, args[1:])
```

and update the usage string:

```go
const usage = "usage: pc <init|submit|send|up|run-gate|watch> [flags]"
```

- [ ] **Step 4: Make `--config` default to `./.pc.yaml`**

Replace the three helpers' shared prologue with one function, and have each call it:

```go
// resolveConfig loads the scenario a command needs, defaulting to ./.pc.yaml
// when --config was not given.
//
// The default exists because an operator working in one scenario's directory
// should not name the same file on every command. It is a fallback and not a
// silent one: with neither the flag nor the file, the error names both, and
// the error still explains WHY a scenario is mandatory — a command cannot
// filter a feed, open a gate or merge branches without a gate definition, and
// a missing one previously surfaced as a hang rather than a message.
func resolveConfig(configPath, command string) (pcops.Config, error) {
	if configPath == "" {
		if _, err := os.Stat(defaultConfigPath); err != nil {
			return pcops.Config{}, fmt.Errorf("pc %s: --config is required, or run in a directory containing %s (no gate definition without one; `pc init` writes one)",
				command, defaultConfigPath)
		}
		configPath = defaultConfigPath
	}
	return pcops.LoadConfig(configPath)
}
```

Then: `resolveUpConfig(path)` becomes `resolveConfig(path, "up")` at its call site and the old function is deleted; `resolveWatchConfig(path)` becomes `resolveConfig(path, "watch")` and is deleted; and `resolveRunGateConfig` keeps its own function — it also derives branches and the workdir default — but its first lines change from the explicit `--config` check to `cfg, err := resolveConfig(configPath, "run-gate")`, returning the error unchanged.

Update the three flag help strings from `"scenario file (required)"` to `"scenario file (default: ./.pc.yaml if present)"`.

- [ ] **Step 5: Run the tests**

Run: `go test ./cmd/pc/ -v -count=1`
Expected: PASS, including the four new tests and the existing `TestUpRequiresConfig`, `TestRunGateRequiresConfig` and `TestWatchRequiresConfig`.

**Those three existing tests will now fail** if they run in a directory that happens to contain a `.pc.yaml` — they assert exit 2 with no `--config`. They currently pass because the package's test working directory has no such file, and that remains true. Do not change them. If any does fail, check whether one of your new tests leaked a `.pc.yaml` into the package directory rather than a temp dir; that is the bug, not the assertion.

Also update `TestUsageListsAllFiveCommands`: rename it to `TestUsageListsEveryCommand` and add `init` to the list it checks. That test exists precisely so a command added to dispatch cannot go unadvertised.

Then `go test ./... -count=1` and `go run ./cmd/pc` (usage must list six commands).

- [ ] **Step 6: Commit**

```bash
git add cmd/pc/main.go cmd/pc/main_test.go
git commit -m "feat(pc): pc init, and --config defaults to ./.pc.yaml

The original CLI design described both and neither shipped, so producing a
valid scenario meant reading the loader's struct tags. init writes a working
scenario against the committed fixture rather than a skeleton of empty keys —
a scaffold that fails its own validation teaches the wrong first lesson — and
refuses to clobber an edited file without --force.

The three per-command config helpers collapse into one that carries the same
rationale text, since a missing gate definition used to surface as a hang."
```

---

## Task 8: README — stop describing a previous version of the project

The vision sections are still accurate as intent and are left alone. One section is a phase behind: "What exists today" lists `pkg/protocol`, `pkg/bus`, `pkg/agent`, `pkg/gate` and three demo binaries, and mentions `cmd/pc` zero times and `pc watch` zero times. The CLI is now the primary interface, and a README that omits it misleads exactly the design partners it is addressed to.

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: everything the previous seven tasks produced — the recipes' paths, `pc init`, the fixture path, the gate command.
- Produces: nothing code depends on.

- [ ] **Step 1: Replace the "What exists today" section**

Replace everything from the `## What exists today` heading up to (but not including) `## Principles` with the following. The three demo `### Run the ...` subsections are folded in: they still work and are worth keeping, but below the thing an operator actually wants first.

```markdown
## What exists today

A working coordination kernel and a CLI over it. Two agents from different
vendors have converged through one shared test gate on one machine.

```text
cmd/pc              the operator and agent interface: init, submit, send,
                    up, run-gate, watch
internal/pcops      composition root: the one place that knows about the
                    gate, the bus, workspace leases and the agent runtime
                    at the same time
pkg/protocol        typed messages, addressing, threading, deadlines
pkg/bus             pluggable transport contract (Publish + Subscribe) and
                    in-memory implementation
pkg/bus/sqlite      durable cross-process transport with replayable cursors,
                    plus read-only History/Tail for observability
pkg/agent           conversation loop, intent dispatch, acknowledgements,
                    cooperative interruption
pkg/gate            cross-agent readiness quorum, test execution, verdicts,
                    version-matched answers, failure routing
pkg/workspace       git-worktree leases with holder-token fencing
pkg/runtime         harness-neutral agent contract (stdlib only)
fixtures/two-service  a demo scenario: two services that must agree on one
                    value, and a spanning test only the gate can run
```

### Quickstart

You need Go 1.22+, git, and at least one coding agent CLI — see the recipes
below. Two terminals.

```bash
# Build the CLI. Do this from the project root: building from elsewhere is how
# one live run silently tested a stale binary for nine minutes.
go build -o ./bin/pc ./cmd/pc
export PATH="$PWD/bin:$PATH"

# Write a scenario. The default one drives the committed fixture.
pc init

# Terminal 1 — the coordinator, and the runner that executes the gate.
pc up &
pc run-gate --workdir /path/to/integration-worktree

# Terminal 2 — watch everything, including agent-to-agent messages.
pc watch --all
```

Then launch your agents in their worktrees, following the recipe for your
harness. Or run the whole thing at once:

```bash
scripts/live-run.sh
```

### Recipes

The contract every agent receives is one file: [docs/agent-contract.md](./docs/agent-contract.md).
It is the same text for every harness — the strongest evidence for this
project's agnostic claim is that it ported between two vendors with no edits.

- [pi](./docs/recipes/pi.md) — verified end to end
- [Claude Code](./docs/recipes/claude-code.md) — verified end to end
- [other harnesses](./docs/recipes/other-harnesses.md) — what a harness needs;
  no support claimed

### What is proven, and what is not

Proven: two agents from different vendors, launched separately, converging
through one gate on one machine — including a failing round whose detail
taught one agent a value that was verifiably absent from its own worktree.

Not proven, and not claimed:

- More than one machine, one repository, or one gate per coordinator.
- Agent supervision. A human launches the agents; PC does not manage
  processes.
- Deliberately re-running a gate on an unchanged version.
- Containment. Worktrees bound visibility, not capability — an agent can
  still write outside its own tree.
- Enforced token budgets. They are observed, not enforced.

### The demos

Three older binaries still exercise the kernel directly, without the CLI:

```bash
go run ./cmd/demo       # planner/researcher/writer negotiate a dependency
go run ./cmd/gatedemo   # two service owners, a runner, and a failing round
go run ./cmd/sqlitedemo # the same protocol across processes
```

### Run the tests

```bash
go test ./...
```

See [PROTOCOL.md](./PROTOCOL.md) for the wire contract,
[the product design](./docs/superpowers/specs/2026-07-21-agent-coordination-loop-studio-design.md)
for the full direction, and `docs/superpowers/specs/` for the phase designs and
the live-fire findings behind them.
```

- [ ] **Step 2: Update the Roadmap's checkboxes**

In the `## Roadmap` section, tick the items this and the preceding phases delivered, leaving the rest untouched:

```markdown
- [x] Intent-typed conversation protocol
- [x] In-memory transport
- [x] Durable SQLite transport
- [x] Cooperative interruption
- [x] Cross-agent integration-test gates
- [x] Local `pc` CLI over a shared domain core
- [x] Worktree registration and exclusive leases
- [x] Generic CLI contract for externally launched agents
- [x] Codex and Claude Code setup recipes
```

Change that last line to read `- [x] pi and Claude Code setup recipes` — Codex is not verified and the roadmap must not claim it. Leave every unticked item exactly as it is.

- [ ] **Step 3: Verify the links resolve**

Run: `for f in docs/agent-contract.md docs/recipes/pi.md docs/recipes/claude-code.md docs/recipes/other-harnesses.md scripts/live-run.sh PROTOCOL.md; do test -e "$f" && echo "ok $f" || echo "MISSING $f"; done`
Expected: every line `ok`. `scripts/live-run.sh` does not exist until Task 9 — if you are doing tasks in order, expect that one to be MISSING here and re-run this check after Task 9.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs(readme): describe the project that exists

'What exists today' listed the kernel packages and three demo binaries and
mentioned cmd/pc zero times — the CLI that is now the primary interface. It
also had no quickstart, no recipes, and no statement of what is actually
proven versus intended, which is the first thing a design partner needs.

The vision sections are unchanged; they remain accurate as intent. The roadmap
now claims pi rather than Codex, which is not verified."
```

---

## Task 9: `scripts/live-run.sh`

Phase B's real proof is a live run, and live runs have their own flake rate: two failed for reasons entirely outside the code — one because a stale binary was silently under test, one because a harness blocked on inherited stdin. This script is what would have prevented both, and it makes the demonstration reproducible by someone other than its author.

It is an operator tool, not CI. It launches both vendors itself, backgrounded, each with `< /dev/null`.

**Files:**
- Create: `scripts/live-run.sh`

**Interfaces:**
- Consumes: `pc init` and `--config` defaulting (Task 7), the fixture and its gate command (Task 5), both recipes (Task 6).
- Produces: nothing code depends on.

- [ ] **Step 1: Write the script**

Create `scripts/live-run.sh`, executable:

```bash
#!/usr/bin/env bash
# Drive one live run of the two-service fixture with two real coding agents.
#
# This is an operator tool, not CI. Its pre-flight exists because two live runs
# failed for reasons outside the code entirely: one silently tested a stale pc
# binary for nine minutes, and one hung forever because a backgrounded `pi -p`
# inherited an open stdin. Both are checked below before anything starts.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"

RUN_DIR="${RUN_DIR:-$ROOT/.pc/live-$(date +%Y%m%d-%H%M%S)}"
BIN_DIR="$RUN_DIR/bin"
LOG_DIR="$RUN_DIR/logs"
WORK_DIR="$RUN_DIR/worktrees"
FIXTURE="$ROOT/fixtures/two-service"

mkdir -p "$BIN_DIR" "$LOG_DIR" "$WORK_DIR"

say() { printf '\n=== %s\n' "$*"; }
die() { printf '\nlive-run: %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------- pre-flight

say "pre-flight"

[ -f "$ROOT/go.mod" ] || die "not at the project root (no go.mod at $ROOT)"
[ -d "$FIXTURE" ] || die "fixture missing at $FIXTURE"

# Binary provenance. Built here, now, from this checkout — not found on PATH,
# where it may be any age. This is the check that a stale binary defeated.
say "building pc from $ROOT"
go build -o "$BIN_DIR/pc" ./cmd/pc || die "go build ./cmd/pc failed"
export PATH="$BIN_DIR:$PATH"
command -v pc >/dev/null || die "pc not on PATH after build"
resolved="$(command -v pc)"
[ "$resolved" = "$BIN_DIR/pc" ] || die "pc resolves to $resolved, not the binary just built at $BIN_DIR/pc"
printf 'pc: %s\n' "$resolved"

# Harness smoke test. A harness that cannot even report its version will not
# survive a nine-minute run, and finding that out now costs seconds.
have_pi=0
have_claude=0
if command -v pi >/dev/null && pi --version >/dev/null 2>&1; then
  have_pi=1
  printf 'pi: %s (%s)\n' "$(command -v pi)" "$(pi --version 2>&1 | head -1)"
fi
if command -v claude >/dev/null && claude --version >/dev/null 2>&1; then
  have_claude=1
  printf 'claude: %s (%s)\n' "$(command -v claude)" "$(claude --version 2>&1 | head -1)"
fi
if [ "$have_pi" -eq 0 ] && [ "$have_claude" -eq 0 ]; then
  die "no verified harness found: install pi or claude (see docs/recipes/)"
fi
if [ "$have_pi" -eq 0 ] || [ "$have_claude" -eq 0 ]; then
  printf '\nlive-run: only one harness available; running both agents on it.\n'
  printf 'The two-vendor configuration is what the agnostic claim rests on.\n'
fi

# ------------------------------------------------------------- fixture reset

say "resetting the fixture"

# Each agent gets its own worktree of the fixture repo. The fixture is its own
# git repository so a run never touches project source; initialise it once.
if [ ! -d "$FIXTURE/.git" ]; then
  git -C "$FIXTURE" init -q
  git -C "$FIXTURE" add -A
  git -C "$FIXTURE" -c user.email=live-run@local -c user.name=live-run commit -qm "fixture baseline"
fi

for spec in "billing:agent/billing" "gateway:agent/gateway" "integrator:agent/integration"; do
  name="${spec%%:*}"; branch="${spec##*:}"
  path="$WORK_DIR/$name"
  git -C "$FIXTURE" worktree remove --force "$path" 2>/dev/null || true
  git -C "$FIXTURE" branch -D "$branch" 2>/dev/null || true
  git -C "$FIXTURE" worktree add -q -b "$branch" "$path" HEAD
  printf '%-11s %s (%s)\n' "$name" "$path" "$branch"
done

# --------------------------------------------------------------- the scenario

say "writing the scenario"

export PC_DB="$RUN_DIR/bus.db"
CONFIG="$RUN_DIR/pc.yaml"
( cd "$RUN_DIR" && pc init --force >/dev/null )
sed -e "s|^repo: .*|repo: $FIXTURE|" -e "s|^db: .*|db: $PC_DB|" \
  "$RUN_DIR/.pc.yaml" > "$CONFIG"
pc watch --config "$CONFIG" --no-follow >/dev/null || die "the scenario at $CONFIG does not load"
printf 'scenario: %s\n' "$CONFIG"

# ----------------------------------------------------------------- the daemons

say "starting the coordinator and the runner"

pc up --config "$CONFIG" >"$LOG_DIR/up.log" 2>&1 < /dev/null &
UP_PID=$!
pc run-gate --config "$CONFIG" --workdir "$WORK_DIR/integrator" >"$LOG_DIR/run-gate.log" 2>&1 < /dev/null &
GATE_PID=$!
pc watch --config "$CONFIG" --all --full >"$LOG_DIR/watch.log" 2>&1 < /dev/null &
WATCH_PID=$!

cleanup() {
  kill "$UP_PID" "$GATE_PID" "$WATCH_PID" 2>/dev/null || true
  wait "$UP_PID" "$GATE_PID" "$WATCH_PID" 2>/dev/null || true
}
trap cleanup EXIT

# StartCoordinator returns only after its subscription is live, but these are
# separate processes — give them a moment to reach that point before any agent
# can declare readiness into a log nobody is watching.
sleep 2
kill -0 "$UP_PID" 2>/dev/null || die "pc up exited immediately; see $LOG_DIR/up.log"
kill -0 "$GATE_PID" 2>/dev/null || die "pc run-gate exited immediately; see $LOG_DIR/run-gate.log"

# ------------------------------------------------------------------ the agents

say "launching the agents"

launch() {
  local name="$1" harness="$2" task="$3" wt="$WORK_DIR/$1"
  # < /dev/null on BOTH harnesses. `pi -p` merges piped stdin into the prompt,
  # so a backgrounded invocation with an inherited stdin blocks forever with no
  # output. This cost two nine-minute runs.
  case "$harness" in
    pi)
      ( cd "$wt" && PC_AGENT="$name" PC_TASK="$task" PC_DB="$PC_DB" \
        pi --skill "$ROOT/docs/agent-contract.md" -p "$task" < /dev/null ) \
        >"$LOG_DIR/$name.log" 2>&1 &
      ;;
    claude)
      cp "$ROOT/docs/agent-contract.md" "$wt/CLAUDE.md"
      ( cd "$wt" && PC_AGENT="$name" PC_TASK="$task" PC_DB="$PC_DB" \
        claude -p "$task" --permission-mode acceptEdits \
        --allowedTools Read Edit Write Bash < /dev/null ) \
        >"$LOG_DIR/$name.log" 2>&1 &
      ;;
  esac
  # Set explicitly rather than leaving the caller to read $!: a background
  # job started inside a function does set $! in the calling shell, but that
  # is subtle enough to be worth not depending on.
  LAST_PID=$!
  printf '%-11s %s (pid %d, log %s)\n' "$name" "$harness" "$LAST_PID" "$LOG_DIR/$name.log"
}

# Two vendors when both are available — that pairing is the configuration the
# harness-agnostic claim actually rests on.
if [ "$have_pi" -eq 1 ]; then billing_harness=pi; else billing_harness=claude; fi
if [ "$have_claude" -eq 1 ]; then gateway_harness=claude; else gateway_harness=pi; fi

launch billing "$billing_harness" \
  "Render the invoice currency correctly. You own billing/ only. Submit with: pc submit --gate currency --agent billing"
AGENT_PIDS=("$LAST_PID")
launch gateway "$gateway_harness" \
  "Stamp the agreed currency on invoices you build. You own gateway/ only. Submit with: pc submit --gate currency --agent gateway"
AGENT_PIDS+=("$LAST_PID")

# --------------------------------------------------------------------- report

say "waiting for the agents"
for pid in "${AGENT_PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done

say "report"
printf 'run directory: %s\n\n' "$RUN_DIR"
printf 'gate activity:\n'
grep -E "ready|ack|nack|request|inform|block" "$LOG_DIR/watch.log" | tail -40 || true
printf '\nverdicts seen by the coordinator:\n'
grep -E "PASSED|FAILED|STALLED" "$LOG_DIR/up.log" || printf '  (none)\n'
printf '\nlogs: %s\n' "$LOG_DIR"

if grep -q "PASSED" "$LOG_DIR/up.log" 2>/dev/null; then
  printf '\nlive-run: the gate PASSED.\n'
  exit 0
fi
printf '\nlive-run: no passing verdict. Read %s and %s.\n' "$LOG_DIR/watch.log" "$LOG_DIR/up.log"
exit 1
```

- [ ] **Step 2: Make it executable and check it**

Run:
```bash
chmod +x scripts/live-run.sh
bash -n scripts/live-run.sh          # parse check
command -v shellcheck >/dev/null && shellcheck scripts/live-run.sh || echo "shellcheck not installed; skipping"
```
Expected: `bash -n` silent. If `shellcheck` is available, fix everything it reports at error or warning level. Do not add it as a project dependency.

- [ ] **Step 3: Verify the pre-flight actually pre-flights**

This is the only part of the script that can be checked without two coding agents installed, and it is the part that earns the script's existence. Verify each failure path by hand:

```bash
# Wrong directory: must die on the go.mod check rather than proceeding.
( cd /tmp && bash "$OLDPWD/scripts/live-run.sh" ; echo "exit=$?" )
```
Expected: it resolves ROOT from its own location and does NOT die on that check — confirm it instead reaches the build step. If it dies at `not at the project root`, the `ROOT` resolution is wrong; fix it.

```bash
# No harness on PATH: must die in pre-flight, not after starting daemons.
( PATH="/usr/bin:/bin" bash scripts/live-run.sh ; echo "exit=$?" )
```
Expected: dies with `no verified harness found`, and `$RUN_DIR/logs` contains no `up.log` — proving it failed before starting anything.

Record both outcomes in your report. If you have pi or Claude Code installed, run the script fully once and report what happened; if you do not, say so explicitly rather than implying it was exercised.

- [ ] **Step 4: Commit**

```bash
git add scripts/live-run.sh
git commit -m "feat(scripts): reproducible live run with a pre-flight that catches both known traps

Two live runs failed for reasons outside the code: one silently tested a stale
pc binary for nine minutes, one hung forever on a backgrounded pi -p that
inherited an open stdin. The pre-flight asserts binary provenance by building
it here and checking PATH resolves to that build, and every harness invocation
redirects stdin from /dev/null.

Launches both vendors when both are available, because that pairing is what
the harness-agnostic claim rests on. An operator tool, not CI."
```

---

## Final verification

Run all of these before considering the plan complete:

- `gofmt -l pkg internal cmd` — reports only `cmd/sqlitedemo/main.go`.
- `go vet ./...` and `go build ./...` — clean.
- `go test ./... -count=1` — all pass.
- `go test ./... -race -count=1` — clean.
- `go test ./internal/pcops/ ./pkg/gate/ ./pkg/protocol/ ./cmd/pc/ -count=3` — deterministic.
- `go list ./... | grep twoservice` — empty, proving the fixture stays out of the module.
- `go run ./cmd/pc` — usage lists six commands including `init`.
- `git diff --stat internal/pcops/submit_test.go` against the branch point — **no changes**. Task 3 was a refactor; a modified test there means it changed behaviour.
- `bash -n scripts/live-run.sh` — silent.
- Both `live-run.sh` pre-flight failure paths from Task 9 Step 3, by hand.

**Not verified by any of the above, and not to be claimed as such:** whether a real harness ingests either recipe correctly, and whether the full live run works end to end. Those need two coding agents and a human watching. The exit gate is the automated suite plus the pre-flight; the live run itself is a separate step taken after the branch is green.
