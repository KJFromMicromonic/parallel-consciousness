package pcops_test

import (
	"context"
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
