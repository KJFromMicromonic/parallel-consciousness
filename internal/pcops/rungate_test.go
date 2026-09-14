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
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
)

func git(t *testing.T, dir string, args ...string) {
	t.Helper()
	c := exec.Command("git", append([]string{"-C", dir}, args...)...)
	c.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com",
		"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com")
	if out, err := c.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
}

// TestRunGateMergesBranchesBeforeTesting exercises the full flow: two
// branches each add a distinct file, and the spanning test only passes if
// both files are present in the runner's worktree — proof that the runner
// merged both branches rather than testing either alone.
//
// It uses StartCoordinator and StartRunner (not the blocking Up/RunGate)
// because both return only once their agent is subscribed to the bus. The
// durable bus starts new subscribers at HEAD, so a `go Up(...)` / `go
// RunGate(...)` pair races Submit: if either goroutine has not yet reached
// agent.New when Submit publishes, the message is missed and the gate stalls
// until the coordinator's 10-minute runner timeout, hanging this test's
// 30-second context with no useful signal. Here, the two Start* calls
// returning IS the synchronization — no sleeps, no retries.
func TestRunGateMergesBranchesBeforeTesting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	repo := t.TempDir()
	git(t, repo, "init", "-q", "-b", "main")
	os.WriteFile(filepath.Join(repo, "seed.txt"), []byte("seed\n"), 0o644)
	git(t, repo, "add", ".")
	git(t, repo, "commit", "-q", "-m", "init")

	// Two branches, each adding a distinct file.
	for _, b := range []string{"a", "b"} {
		git(t, repo, "checkout", "-q", "-b", b, "main")
		os.WriteFile(filepath.Join(repo, b+".txt"), []byte(b), 0o644)
		git(t, repo, "add", ".")
		git(t, repo, "commit", "-q", "-m", b)
	}
	git(t, repo, "checkout", "-q", "main")

	work := filepath.Join(t.TempDir(), "integrator")
	git(t, repo, "worktree", "add", "-B", "agent/integration", work)

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{
		DB:     db,
		GateID: "g",
		Gate:   pcops.GateDef{Required: []string{"x"}, Runner: "integrator", Run: "test -f a.txt && test -f b.txt"},
	}

	cstop, err := pcops.StartCoordinator(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("StartCoordinator: %v", err)
	}
	defer cstop()

	rstop, err := pcops.StartRunner(ctx, cfg, work, []string{"a", "b"})
	if err != nil {
		t.Fatalf("StartRunner: %v", err)
	}
	defer rstop()

	cfg.SubmitTimeout = 20 * time.Second
	v, err := pcops.Submit(ctx, cfg, "g", "x", "v1")
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if !v.Passed {
		t.Fatalf("verdict = %+v; the runner should have merged both branches", v)
	}
}

// The verdict must name what the runner MERGED, not what a participant
// declared. Those are the same value in the ordinary case, which is precisely
// why the first version of this test could not detect anything: it declared the
// branch tip, so the coordinator's backfill — which echoes the declaration —
// produced the right answer for the wrong reason.
//
// This opens a deliberate gap. billing declares a version that is NOT its branch
// tip, so the two candidate answers differ and only a runner that reports what
// it actually merged yields the tip.
//
// It declares readiness with gate.Ready rather than pcops.Submit on purpose.
// Submit carries a version guard that DECLINES any verdict naming this agent at
// a version other than the one it declared — so a Submit here would correctly
// refuse the very verdict this test needs to read, and then block until its
// timeout. The coordinator's OnVerdict callback sees the broadcast regardless.
func TestRunGateVerdictNamesWhatWasMergedNotWhatWasDeclared(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	repo := t.TempDir()
	git(t, repo, "init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(repo, "seed.txt"), []byte("seed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, repo, "add", ".")
	git(t, repo, "commit", "-q", "-m", "init")

	git(t, repo, "checkout", "-q", "-b", "agent/billing", "main")
	if err := os.WriteFile(filepath.Join(repo, "a.txt"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, repo, "add", ".")
	git(t, repo, "commit", "-q", "-m", "a")
	tipOut, err := exec.Command("git", "-C", repo, "rev-parse", "agent/billing").Output()
	if err != nil {
		t.Fatal(err)
	}
	tip := strings.TrimSpace(string(tipOut))
	git(t, repo, "checkout", "-q", "main")

	work := filepath.Join(t.TempDir(), "integrator")
	git(t, repo, "worktree", "add", "-B", "agent/integration", work)

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{
		DB:     db,
		GateID: "g",
		Gate:   pcops.GateDef{Required: []string{"billing"}, Runner: "integrator", Run: "test -f a.txt"},
		Agents: []pcops.AgentDef{{Name: "billing", Branch: "agent/billing"}},
	}

	verdicts := make(chan gate.Verdict, 4)
	cstop, err := pcops.StartCoordinator(ctx, cfg, func(v gate.Verdict) {
		select {
		case verdicts <- v:
		default:
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cstop()

	rstop, err := pcops.StartRunner(ctx, cfg, work, []string{"agent/billing"})
	if err != nil {
		t.Fatal(err)
	}
	defer rstop()

	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	billing, err := agent.New(ctx, b, "billing", []string{gate.Topic("g")})
	if err != nil {
		t.Fatal(err)
	}
	go billing.Run(ctx)

	const declared = "declared-not-the-tip"
	if err := gate.Ready(ctx, billing, "g", declared); err != nil {
		t.Fatal(err)
	}

	select {
	case v := <-verdicts:
		if v.Versions["billing"] == declared {
			t.Fatalf("verdict echoed the DECLARED version %q; it must name the commit the runner merged", declared)
		}
		if v.Versions["billing"] != tip {
			t.Fatalf("verdict versions = %v, want billing at the merged tip %q", v.Versions, tip)
		}
		if _, branchKeyed := v.Versions["agent/billing"]; branchKeyed {
			t.Errorf("verdict versions are keyed by BRANCH rather than participant: %v", v.Versions)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("no verdict broadcast")
	}
}
