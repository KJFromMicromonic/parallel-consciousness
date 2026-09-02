package pcops_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime/fake"
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
	if !strings.Contains(err.Error(), "3 rounds") {
		t.Fatalf("Run err = %v, want it to name the round count", err)
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
