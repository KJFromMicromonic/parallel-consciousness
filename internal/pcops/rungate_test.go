package pcops_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
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
