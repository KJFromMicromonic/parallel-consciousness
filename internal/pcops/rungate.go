package pcops

import (
	"context"
	"errors"
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
	// cfg.RunnerTimeout is zero for a Config built by hand rather than loaded
	// via LoadConfig; fall back rather than passing a zero timeout, which
	// would make runShell's context expire before the command even starts.
	// This mirrors StartCoordinator's own fallback in up.go — the two used to
	// diverge, with the coordinator's wait bound operator-settable via
	// runner_timeout and this shell's own timeout hardcoded to 10 minutes
	// regardless, so a runner_timeout of 30m gave the coordinator half an
	// hour while this killed the command at ten, and a runner_timeout of 2m
	// let the shell burn on for eight minutes after the coordinator had
	// already declared the round stalled.
	runnerTimeout := cfg.RunnerTimeout
	if runnerTimeout <= 0 {
		runnerTimeout = DefaultRunnerTimeout
	}
	// mergeAll reports per branch; the coordinator keys readiness by
	// participant. Build the translation from cfg directly rather than zipping
	// it against the branches slice: that slice is produced by cmd/pc's
	// branchesFromConfig, whose ordering is an implementation detail of another
	// package, and a mismatch would mis-attribute every version silently.
	participantOf := make(map[string]string, len(cfg.Agents))
	for _, ag := range cfg.Agents {
		participantOf[ag.Branch] = ag.Name
	}
	gate.ServeRunner(a, func(ctx context.Context, gateID string, _ map[string]string) gate.Verdict {
		merged, detail, err := mergeAll(ctx, workdir, branches)
		if err != nil {
			// No versions: the merge was aborted, so nothing was tested and the
			// coordinator's backfill should describe the round instead.
			return gate.Verdict{GateID: gateID, Passed: false, Detail: detail}
		}
		tested := make(map[string]string, len(merged))
		for br, sha := range merged {
			if name, ok := participantOf[br]; ok {
				tested[name] = sha
			}
		}
		out, err := runShell(ctx, workdir, cfg.Gate.Run, runnerTimeout)
		if err != nil {
			return gate.Verdict{GateID: gateID, Passed: false, Detail: trim(out), Versions: tested}
		}
		return gate.Verdict{GateID: gateID, Passed: true, Versions: tested}
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

// mergeAll resets the runner's worktree to main and merges each participant's
// branch into it, returning the commit it merged per branch.
//
// The returned map is what makes a verdict truthful. The coordinator otherwise
// backfills a verdict's Versions from the readiness it recorded — the versions
// participants DECLARED — and those diverge from what was tested the moment an
// agent commits again after submitting. Reporting the tip that actually went in
// is the only way the verdict can describe the run rather than the intent.
//
// On any failure it returns a nil map: the merge is aborted, so nothing
// coherent was tested and there is no honest SHA to report.
func mergeAll(ctx context.Context, workdir string, branches []string) (map[string]string, string, error) {
	// Deliberately no `checkout main`: main is checked out in the primary
	// worktree, and git refuses to check out a branch twice. Resetting the
	// runner's own branch to main achieves the same clean baseline.
	for _, args := range [][]string{
		{"reset", "--hard", "-q", "main"},
		{"clean", "-qfd"},
	} {
		if out, err := gitIn(ctx, workdir, args...); err != nil {
			return nil, trim(out), err
		}
	}
	merged := make(map[string]string, len(branches))
	for _, br := range branches {
		// Resolve BEFORE merging, and merge the resolved commit rather than
		// the branch name. Merging `br` and then asking what `br` points at
		// re-reads a mutable ref: an agent committing in the gap between those
		// two git invocations would have us record a commit this run never
		// merged — the very defect truthful versions exist to remove, in
		// miniature. Merging the sha closes the window: what we merge and what
		// we report are the same value by construction.
		//
		// This comment is the ONLY thing holding that ordering. Reverting to
		// `merge br` followed by `rev-parse br` leaves every test in this
		// package green, because reproducing the defect needs a commit landing
		// inside the gap between two git invocations — a race no test here can
		// open deterministically, which is why there is no test for it and why
		// there is unlikely ever to be one. So do not "simplify" the two-step
		// resolve-then-merge back into merging the branch name: nothing but
		// this paragraph will stop you, and nothing will tell you afterwards.
		shaOut, shaErr := gitIn(ctx, workdir, "rev-parse", br)
		if shaErr != nil {
			// Same "merge failed on %s" prefix as the ordinary merge-failure
			// path below: a branch that cannot even be resolved is a plain
			// merge failure, not a conflict, and existing callers already key
			// off that phrasing to tell the two apart.
			return nil, fmt.Sprintf("merge failed on %s: cannot resolve: %v: %s", br, shaErr, trim(shaOut)), shaErr
		}
		sha := trim(shaOut)

		out, err := gitIn(ctx, workdir, "merge", "--no-edit", "-q", sha)
		if err == nil {
			merged[br] = sha
			continue
		}
		// Classify before aborting: `merge --abort` clears the unmerged index
		// this reads. Calling every non-zero exit a conflict sent both owners
		// hunting a conflict that did not exist whenever the real cause was a
		// missing branch or transient ref/index.lock contention — the detail an
		// agent is steered with has to name what actually happened.
		conflict := isMergeConflict(err, out) || hasUnmergedPaths(ctx, workdir)
		_, _ = gitIn(ctx, workdir, "merge", "--abort")
		if conflict {
			return nil, fmt.Sprintf("merge conflict on %s: %s", br, trim(out)), err
		}
		return nil, fmt.Sprintf("merge failed on %s: %v: %s", br, err, trim(out)), err
	}
	return merged, "", nil
}

// isMergeConflict reports whether a failed `git merge` failed because of a
// content conflict. git exits 1 and prints CONFLICT for that case, and uses
// other statuses for "not something we can merge", a locked index, and the rest.
func isMergeConflict(err error, out string) bool {
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 1 {
		return false
	}
	return strings.Contains(out, "CONFLICT")
}

// hasUnmergedPaths asks git directly, so a conflict is still classified as one
// if a future git release changes its wording or its exit status.
func hasUnmergedPaths(ctx context.Context, workdir string) bool {
	out, err := gitIn(ctx, workdir, "ls-files", "-u")
	return err == nil && strings.TrimSpace(out) != ""
}

// gitIn runs one git command in dir. exec.CommandContext, not exec.Command: a
// stale index.lock makes git wait forever, and the caller's wall budget has to
// bound that instead of the run hanging past it. runShell below already did
// this correctly.
func gitIn(ctx context.Context, dir string, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...).CombinedOutput()
	return string(out), err
}

func runShell(ctx context.Context, dir, command string, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
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
