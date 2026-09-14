package pcops_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime/fake"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/workspace"
)

// buildPC compiles cmd/pc so the fake agents can invoke the real CLI, exactly
// as a coding agent's bash tool would.
func buildPC(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "pc")
	out, err := exec.Command("go", "build", "-o", bin,
		"github.com/KJFromMicromonic/parallel-consciousness/cmd/pc").CombinedOutput()
	if err != nil {
		t.Fatalf("build pc: %v: %s", err, out)
	}
	return bin
}

// diagnosticMarker is the distinctive text the spanning check emits when it
// fails, so the test can prove that failure detail actually survives the
// runner-verdict -> coordinator -> block round trip and reaches the blocked
// agent, not just that the round eventually converges.
const diagnosticMarker = "MISSING_CURRENCY_FIELD"

// twoServiceRepo builds the fixture: a spanning check that passes only when
// BOTH files carry the currency field. Neither agent can satisfy it alone.
// On failure it names which file(s) are missing the field, using
// diagnosticMarker, so a block message carries a non-empty, checkable detail
// instead of the silence grep -q alone would produce.
func twoServiceRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	git(t, dir, "init", "-q", "-b", "main")
	os.WriteFile(filepath.Join(dir, "billing.txt"), []byte("amount\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "gateway.txt"), []byte("amount\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "check.sh"), []byte(`#!/bin/sh
missing=""
grep -q currency billing.txt || missing="$missing billing.txt"
grep -q currency gateway.txt || missing="$missing gateway.txt"
if [ -n "$missing" ]; then
  echo "`+diagnosticMarker+`:$missing"
  exit 1
fi
exit 0
`), 0o755)
	git(t, dir, "add", ".")
	git(t, dir, "commit", "-q", "-m", "init")
	return dir
}

func TestRunConvergesAfterAFailingRound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	repo := twoServiceRepo(t)
	pc := buildPC(t)
	db := filepath.Join(t.TempDir(), "bus.db")

	cfg := pcops.Config{
		Repo:   repo,
		DB:     db,
		GateID: "checkout",
		Gate: pcops.GateDef{
			Required: []string{"billing", "gateway"},
			Runner:   "integrator",
			Run:      "sh check.sh",
		},
		Agents: []pcops.AgentDef{
			{Name: "billing", Branch: "agent/billing", Role: "implementer", Task: "add currency"},
			{Name: "gateway", Branch: "agent/gateway", Role: "implementer", Task: "send currency"},
		},
		Runner:        pcops.AgentDef{Name: "integrator", Branch: "agent/integration"},
		SubmitTimeout: 60 * time.Second,
		Wall:          90 * time.Second,
	}

	// Round one: billing does its half, gateway does NOT. The spanning check
	// must fail, routing a block to both owners. On being steered, each agent
	// writes the currency field and re-submits.
	//
	// fixAndResubmit also records the exact steer text it was given, to
	// "steer-received.txt", and commits it alongside the fix. That file
	// travels with the branch, so it survives the worktree being released
	// when Run returns, and lets the assertions below prove the runner's
	// diagnostic actually reached the blocked agent — not merely that some
	// steer happened.
	submit := fake.Exec{Args: []string{pc, "submit", "--gate", "checkout"}}
	fixAndResubmit := func(path string) func(string) []fake.Action {
		return func(text string) []fake.Action {
			return []fake.Action{
				fake.Write{Path: path, Content: "amount currency\n"},
				fake.Write{Path: "steer-received.txt", Content: text},
				commitAll,
				submit,
			}
		}
	}

	r := fake.New(map[string]fake.Script{
		"billing": {
			OnStart: []fake.Action{fake.Write{Path: "billing.txt", Content: "amount currency\n"}, commitAll, submit},
			OnSteer: fixAndResubmit("billing.txt"),
		},
		"gateway": {
			OnStart: []fake.Action{fake.Write{Path: "gateway.txt", Content: "amount\n"}, commitAll, submit},
			OnSteer: fixAndResubmit("gateway.txt"),
		},
	})

	v, err := pcops.Run(ctx, cfg, r)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !v.Passed {
		t.Fatalf("loop never converged: %+v", v)
	}

	// Prove the failure detail from round one's spanning check actually
	// reached the blocked agents, rather than just trusting that the loop
	// eventually converged. Both branches still exist in repo even though
	// Run has released their worktrees, so the committed record is read back
	// with `git show <branch>:<path>`.
	for _, branch := range []string{"agent/billing", "agent/gateway"} {
		got := gitShow(t, repo, branch, "steer-received.txt")
		if !strings.Contains(got, diagnosticMarker) {
			t.Fatalf("steer text recorded on %s = %q, want it to contain %q", branch, got, diagnosticMarker)
		}
		if !strings.Contains(got, "gateway.txt") {
			t.Fatalf("steer text recorded on %s = %q, want it to name gateway.txt as the missing field", branch, got)
		}
	}
}

// gitShow reads path as committed on branch, without needing a checkout —
// the worktree it was written in may already be gone by the time this runs.
func gitShow(t *testing.T, repo, branch, path string) string {
	t.Helper()
	out, err := exec.Command("git", "-C", repo, "show", branch+":"+path).CombinedOutput()
	if err != nil {
		t.Fatalf("git show %s:%s: %v: %s", branch, path, err, out)
	}
	return string(out)
}

// commitAll is how a fake agent publishes its work to its branch.
var commitAll = fake.Exec{Args: []string{"sh", "-c",
	"git add -A && git -c user.email=t@example.com -c user.name=t commit -q -m work"}}

// scenario builds a one-agent scenario over the fixture repo, so the tests
// below differ only in the script they give the agent.
func scenario(t *testing.T, repo string, agents []pcops.AgentDef) pcops.Config {
	t.Helper()
	required := make([]string, 0, len(agents))
	for _, a := range agents {
		required = append(required, a.Name)
	}
	return pcops.Config{
		Repo:   repo,
		DB:     filepath.Join(t.TempDir(), "bus.db"),
		GateID: "checkout",
		Gate: pcops.GateDef{
			Required: required,
			Runner:   "integrator",
			Run:      "sh check.sh",
		},
		Agents:        agents,
		Runner:        pcops.AgentDef{Name: "integrator", Branch: "agent/integration"},
		SubmitTimeout: 45 * time.Second,
		Wall:          120 * time.Second,
	}
}

// A session that dies mid-task must fail the run promptly and WITH
// ATTRIBUTION. Before this, drainEvents discarded every event including
// exited, so Run waited out the entire wall budget for a verdict that could
// never arrive and then blamed the budget.
//
// The wall budget here is 120s and the assertion is that Run returns inside
// 30s: waiting the budget out is exactly the old behaviour, so the deadline is
// the point of the test, not incidental.
func TestRunFailsWithAttributionWhenASessionDies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg := scenario(t, twoServiceRepo(t), []pcops.AgentDef{
		{Name: "billing", Branch: "agent/billing", Role: "implementer", Task: "add currency"},
	})
	r := fake.New(map[string]fake.Script{
		// Dies before submitting anything: no verdict can ever arrive.
		"billing": {OnStart: []fake.Action{fake.Exit{}}},
	})

	start := time.Now()
	_, err := pcops.Run(ctx, cfg, r)
	elapsed := time.Since(start)

	if !errors.Is(err, pcops.ErrSessionDied) {
		t.Fatalf("Run err = %v, want ErrSessionDied", err)
	}
	if !strings.Contains(err.Error(), "billing") {
		t.Fatalf("Run err = %v, want it to name the agent that died", err)
	}
	if elapsed > 30*time.Second {
		t.Fatalf("Run took %v with a %v wall budget: it waited the budget out instead of detecting the death", elapsed, cfg.Wall)
	}
}

// A gate that fails every round must stop at the cap rather than retry until
// the wall budget — and cfg.Wall == 0 is documented as unbounded, so without a
// cap a scenario file with no budget.wall retried forever. The spec's Negative
// Result section makes three attempts the answer, not a number to retry past.
func TestRunStopsAfterTheRoundCap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pc := buildPC(t)
	cfg := scenario(t, twoServiceRepo(t), []pcops.AgentDef{
		{Name: "billing", Branch: "agent/billing", Role: "implementer", Task: "add currency"},
	})
	submit := fake.Exec{Args: []string{pc, "submit", "--gate", "checkout"}}
	r := fake.New(map[string]fake.Script{
		// Submits, is blocked, resubmits unchanged: gateway.txt never gains the
		// currency field, so the spanning check can never pass.
		"billing": {
			OnStart: []fake.Action{submit},
			OnSteer: func(string) []fake.Action { return []fake.Action{submit} },
		},
	})

	v, err := pcops.Run(ctx, cfg, r)
	if err == nil {
		t.Fatal("Run returned nil error for a gate that never passes")
	}
	if v.Passed {
		t.Fatalf("verdict = %+v, want a failing one", v)
	}
	// "fix cycles", not "rounds". Since readiness became a standing claim the
	// two differ: N participants fixing in sequence produce N failing verdicts
	// per cycle, so an operator reading "3 rounds" after watching five spanning
	// test runs go by would reasonably conclude the tool was lying to them. The
	// assertion is on the count and the unit together, because naming the count
	// in the wrong unit is the actual defect being guarded here.
	if !strings.Contains(err.Error(), "3 fix cycles") {
		t.Fatalf("Run err = %v, want it to name the count of fix cycles it stopped after", err)
	}
}

// budget.submit_timeout was dead configuration: Run injected only PC_AGENT and
// PC_DB, so every `pc submit` a spawned agent ran parked for the 5-minute
// default whatever the scenario said. The agent here records the environment it
// was given, outside its worktree so the record survives the lease being
// released.
func TestRunInjectsTheConfiguredSubmitTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	record := filepath.Join(t.TempDir(), "env.txt")
	cfg := scenario(t, twoServiceRepo(t), []pcops.AgentDef{
		{Name: "billing", Branch: "agent/billing", Role: "implementer", Task: "add currency"},
	})
	r := fake.New(map[string]fake.Script{
		"billing": {OnStart: []fake.Action{
			fake.Exec{Args: []string{"sh", "-c", "printenv PC_SUBMIT_TIMEOUT > " + record}},
			fake.Exit{},
		}},
	})

	if _, err := pcops.Run(ctx, cfg, r); !errors.Is(err, pcops.ErrSessionDied) {
		t.Fatalf("Run err = %v, want ErrSessionDied after the scripted exit", err)
	}
	got, err := os.ReadFile(record)
	if err != nil {
		t.Fatalf("the session never recorded its environment: %v", err)
	}
	if strings.TrimSpace(string(got)) != cfg.SubmitTimeout.String() {
		t.Fatalf("PC_SUBMIT_TIMEOUT = %q, want %q", strings.TrimSpace(string(got)), cfg.SubmitTimeout)
	}
}

// A fenced lease means this run no longer owns its worktree. Continuing to
// work in it is worse than stopping: the heartbeat must stop and the run must
// fail with attribution, the same shape as the session-death path.
func TestRunFailsWhenALeaseIsLost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	repo := twoServiceRepo(t)
	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{
		Repo:   repo,
		DB:     db,
		GateID: "checkout",
		Gate: pcops.GateDef{
			Required: []string{"billing"},
			Runner:   "integrator",
			Run:      "sh check.sh",
		},
		Agents:        []pcops.AgentDef{{Name: "billing", Branch: "agent/billing", Role: "implementer", Task: "x"}},
		Runner:        pcops.AgentDef{Name: "integrator", Branch: "agent/integration"},
		SubmitTimeout: 20 * time.Second,
		Wall:          50 * time.Second,
	}

	// An agent that never submits, so Run stays in its wait while the lease
	// is stolen underneath it.
	r := fake.New(map[string]fake.Script{
		"billing": {OnStart: []fake.Action{fake.Emit{Tool: runtime.ToolUse{Name: "read", Target: "x", Ok: true}}}},
	})

	errs := make(chan error, 1)
	go func() { _, err := pcops.Run(ctx, cfg, r); errs <- err }()

	// Acquire commits the lease row BEFORE worktreeAdd creates the directory,
	// so this directory appearing is proof Run's row already exists — the
	// precondition for a steal to be a RECLAIM (rotating the holder token and
	// fencing Run) rather than a plain first INSERT. Without this wait the
	// stealer can win the race, Run then fails with ErrLeased during setup,
	// and the test never reaches the heartbeat path it exists to exercise.
	root := filepath.Join(filepath.Dir(db), "worktrees")
	appeared := time.After(30 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(root, "billing")); err == nil {
			break
		}
		select {
		case <-appeared:
			t.Fatal("Run never acquired billing's worktree")
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Steal billing's lease: a second Manager with a very short TTL reclaims
	// it, which rotates the holder token and fences the original holder.
	stealer, err := workspace.New(ctx, repo, root, db)
	if err != nil {
		t.Fatal(err)
	}
	defer stealer.Close()
	stealer.SetTTL(1 * time.Nanosecond)
	deadline := time.After(30 * time.Second)
	for {
		if _, err := stealer.Acquire(ctx, "billing", "agent/billing"); err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("never managed to reclaim billing's lease")
		case <-time.After(200 * time.Millisecond):
		}
	}

	select {
	case err := <-errs:
		if !errors.Is(err, pcops.ErrLeaseLost) {
			t.Fatalf("Run err = %v, want ErrLeaseLost", err)
		}
		if !strings.Contains(err.Error(), "billing") {
			t.Fatalf("error does not name the agent: %v", err)
		}
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
	case <-time.After(45 * time.Second):
		t.Fatal("Run did not fail after its lease was lost")
	}
}

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

// threeServiceRepo is twoServiceRepo with a third half, so the fixture can
// exercise what changed when readiness became a standing claim: with three
// required participants, one fix cycle is three separate rounds rather than
// one. The spanning check still passes only when EVERY file carries the
// currency field, so no participant can satisfy it alone.
func threeServiceRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	git(t, dir, "init", "-q", "-b", "main")
	for _, name := range []string{"billing.txt", "gateway.txt", "ledger.txt"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("amount\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "check.sh"), []byte(`#!/bin/sh
missing=""
for f in billing.txt gateway.txt ledger.txt; do
  grep -q currency "$f" || missing="$missing $f"
done
if [ -n "$missing" ]; then
  echo "`+diagnosticMarker+`:$missing"
  exit 1
fi
exit 0
`), 0o755); err != nil {
		t.Fatal(err)
	}
	git(t, dir, "add", ".")
	git(t, dir, "commit", "-q", "-m", "init")
	return dir
}

// TestRunConvergesWithThreeParticipantsEachFixingOnce pins what the round cap
// means now that readiness is a standing claim.
//
// Three required participants each fix their own half exactly once, in
// sequence. Before standing readiness that was TWO runner invocations: a round
// could not open until every participant had re-declared, so the three fixes
// arrived as one round. Now a single re-declaration completes the standing
// quorum, so the same three fixes open four rounds — FAIL(b1,g1,l1),
// FAIL(b2,g1,l1), FAIL(b2,g2,l1), PASS(b2,g2,l2) — and `rounds++` on every
// failing verdict reaches a cap of 3 on the THIRD failing verdict, which is
// the moment the second participant finishes fixing. Run then reports "still
// failing after 3 rounds" before the third participant has submitted its fix
// at all, with the gate about to pass. Each of those rounds is also a full run
// of the spanning test — ten minutes of budget apiece in the live config — so
// the cost of the miscount is not only the wrong verdict.
//
// The staggering (fix on your first, second, third steer respectively) is what
// makes the trace deterministic, and it is load-bearing rather than
// decorative. Every failing round blocks all three owners, so agents that all
// fixed on the first steer raced the runner: this branch made the runner merge
// and report the commit it ACTUALLY merged, so a fix committed after a round
// opened is still picked up by that round's merge, and the run then converges
// in one or two rounds instead of three. Measured: the same scenario resolved
// in 3 failing rounds on one execution and 1 on the next. A test that
// sometimes produces one failing round is a test that sometimes asserts
// nothing about a cap of three.
//
// The steer-count assertion is there for the same reason. Convergence alone
// would still "pass" against a trace that never reached three failing rounds,
// and that is precisely the trace against which the defect is invisible.
func TestRunConvergesWithThreeParticipantsEachFixingOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	pc := buildPC(t)
	cfg := scenario(t, threeServiceRepo(t), []pcops.AgentDef{
		{Name: "billing", Branch: "agent/billing", Role: "implementer", Task: "add currency to billing"},
		{Name: "gateway", Branch: "agent/gateway", Role: "implementer", Task: "add currency to gateway"},
		{Name: "ledger", Branch: "agent/ledger", Role: "implementer", Task: "add currency to ledger"},
	})
	cfg.Wall = 150 * time.Second

	submit := fake.Exec{Args: []string{pc, "submit", "--gate", "checkout"}}
	// Submit at once; fix on the nth steer and on no other, so exactly one
	// participant's half is repaired between consecutive rounds.
	var steers sync.Map // agent -> *int32, how many blocks it was steered with
	fixOnSteer := func(agent string, n int32, path string) func(string) []fake.Action {
		seen := new(int32)
		steers.Store(agent, seen)
		return func(string) []fake.Action {
			if atomic.AddInt32(seen, 1) != n {
				return nil
			}
			return []fake.Action{
				fake.Write{Path: path, Content: "amount currency\n"},
				commitAll,
				submit,
			}
		}
	}
	r := fake.New(map[string]fake.Script{
		"billing": {OnStart: []fake.Action{submit}, OnSteer: fixOnSteer("billing", 1, "billing.txt")},
		"gateway": {OnStart: []fake.Action{submit}, OnSteer: fixOnSteer("gateway", 2, "gateway.txt")},
		"ledger":  {OnStart: []fake.Action{submit}, OnSteer: fixOnSteer("ledger", 3, "ledger.txt")},
	})

	v, err := pcops.Run(ctx, cfg, r)
	if err != nil {
		t.Fatalf("Run: %v — three participants each fixing their own half once is one fix cycle, and the cap must not end the run inside it", err)
	}
	if !v.Passed {
		t.Fatalf("verdict = %+v, want a passing one once all three halves carry the field", v)
	}
	// Three failing rounds actually happened: ledger is steered once per
	// failing verdict and only fixes on the third, so the run could not have
	// passed with fewer. Asserted rather than assumed, because a trace with
	// fewer failing rounds never reaches a cap of 3 and would make this test
	// vacuous without changing its outcome.
	seen, ok := steers.Load("ledger")
	if !ok {
		t.Fatal("ledger was never scripted")
	}
	if got := atomic.LoadInt32(seen.(*int32)); got < 3 {
		t.Fatalf("ledger was steered %d times, want at least 3: the run converged without ever reaching three failing rounds, so it never exercised the cap", got)
	}
}
