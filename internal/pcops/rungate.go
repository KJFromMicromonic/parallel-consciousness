package pcops

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
)

// StartRunner performs all runner setup synchronously and returns only once
// the runner agent is subscribed and its run loop is started.
//
// This split exists for the same reason StartCoordinator has it: the durable
// bus starts a new subscriber at the CURRENT HEAD of the message log, never
// replaying messages published before the subscription began. The
// coordinator sends its IntentRequest directly to the runner as soon as
// quorum is reached; if the runner's agent.New call has not yet completed at
// that moment, the request lands in the log with nobody watching, and the
// gate stalls until the coordinator's runner timeout — which this project
// sets to 10 minutes — rather than failing fast. By returning only after
// agent.New has completed, a caller that gets a nil error here knows the
// runner cannot miss a subsequent request.
func StartRunner(ctx context.Context, cfg Config, workdir string, branches []string) (stop func(), err error) {
	b, err := sqlite.Open(ctx, cfg.DB, sqlite.WithPollInterval(25*time.Millisecond))
	if err != nil {
		return nil, fmt.Errorf("open bus: %w", err)
	}

	a, err := agent.New(ctx, b, cfg.Gate.Runner, nil)
	if err != nil {
		b.Close()
		return nil, fmt.Errorf("join as %q: %w", cfg.Gate.Runner, err)
	}
	gate.ServeRunner(a, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		if detail, err := mergeAll(workdir, branches); err != nil {
			return gate.Verdict{GateID: gateID, Passed: false, Detail: detail, Versions: versions}
		}
		out, err := runShell(ctx, workdir, cfg.Gate.Run)
		if err != nil {
			return gate.Verdict{GateID: gateID, Passed: false, Detail: trim(out), Versions: versions}
		}
		return gate.Verdict{GateID: gateID, Passed: true, Versions: versions}
	})
	go a.Run(ctx)

	var once sync.Once
	stop = func() {
		once.Do(func() { b.Close() })
	}
	return stop, nil
}

// RunGate serves the spanning test for a gate. On each gate opening it resets
// its worktree, merges every participating branch, and runs the configured
// command. Merge conflicts are reported as an ordinary failing verdict, because
// two agents editing one contract file is the expected case, not an error.
func RunGate(ctx context.Context, cfg Config, workdir string, branches []string) error {
	stop, err := StartRunner(ctx, cfg, workdir, branches)
	if err != nil {
		return err
	}
	defer stop()

	<-ctx.Done()
	return ctx.Err()
}

// mergeAll resets the runner's worktree to main and merges each branch in turn.
// The reset makes every round independent of the last.
func mergeAll(workdir string, branches []string) (string, error) {
	// Deliberately no `checkout main`: main is checked out in the primary
	// worktree, and git refuses to check out a branch twice. Resetting the
	// runner's own branch to main achieves the same clean baseline.
	for _, args := range [][]string{
		{"reset", "--hard", "-q", "main"},
		{"clean", "-qfd"},
	} {
		if out, err := gitIn(workdir, args...); err != nil {
			return trim(out), err
		}
	}
	for _, br := range branches {
		out, err := gitIn(workdir, "merge", "--no-edit", "-q", br)
		if err != nil {
			_, _ = gitIn(workdir, "merge", "--abort")
			return fmt.Sprintf("merge conflict on %s: %s", br, trim(out)), err
		}
	}
	return "", nil
}

func gitIn(dir string, args ...string) (string, error) {
	out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput()
	return string(out), err
}

func runShell(ctx context.Context, dir, command string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// trim keeps verdict details small enough to travel in a message body.
func trim(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 2000 {
		return s[len(s)-2000:]
	}
	return s
}
