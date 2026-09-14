package pcops

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/workspace"
)

// leaseTTL mirrors workspace.Manager's default lease TTL (see
// workspace.New). Run never calls SetTTL, so this is the TTL every lease it
// acquires actually has.
const leaseTTL = 30 * time.Second

// heartbeatInterval is how often Run renews a lease it still holds. It must
// stay well under leaseTTL so a single missed tick can never let a live
// lease go stale before the next one; a third of the TTL leaves two full
// cycles of margin.
const heartbeatInterval = leaseTTL / 3

// maxRounds caps how many failing gate rounds one scenario will run.
//
// The spec's Negative Result section makes three attempts the ANSWER to the
// project's central empirical question — whether an N-agent loop beats one good
// agent — not a number to retry past. Without a cap, a gate that fails every
// round loops until the wall budget, and cfg.Wall == 0 is documented as
// unbounded, so a scenario file with no `budget.wall` retried forever.
const maxRounds = 3

// fixCycle decides which failing verdicts count against maxRounds, so the cap
// bounds FIX CYCLES rather than raw failing verdicts.
//
// The cap's meaning changed underneath this file when readiness became a
// standing claim, without a line of this file being touched. Before that, a
// round could only open once EVERY required participant had re-declared, so
// one failing verdict WAS one full fix cycle: three participants each fixing
// their own half produced two runner invocations (one FAIL, then a PASS once
// all three had re-declared). After it, one participant's re-declaration
// completes the standing quorum on its own, so the same three fixes produce
// four — FAIL(b1,g1,d1), FAIL(b2,g1,d1), FAIL(b2,g2,d1), PASS(b2,g2,d2) — and
// `rounds++` on every failing verdict trips a cap of 3 the moment the SECOND
// participant finishes fixing, before the third has submitted its fix at all.
// Each of those rounds is also a full run of the spanning test, a ten-minute
// budget in the live config.
//
// A failing verdict therefore advances the counter only when it ENDS a cycle,
// which is either of two things:
//
//   - every required participant has moved since the round that last counted.
//     That is the post-branch spelling of "the whole quorum re-declared",
//     which is exactly what one round used to mean.
//
//   - nothing new was contributed: either nobody has moved since the round
//     that last counted, or the only participants that moved had already
//     moved earlier in this same cycle. Waiting for the rest of a cycle that
//     is no longer accumulating anything is waiting for nothing.
//
//     This half is load-bearing, and it is why "an unchanged re-run is free"
//     is NOT the rule here. pkg/gate answers a resubmit at a version set it
//     already tested from its remembered verdict WITHOUT running the spanning
//     test, and routes that answer through resolve — broadcast, failure
//     blocks and OnVerdict included — so a steered agent that resubmits
//     unchanged is steered again, and busy-loops. pkg/gate's gateState doc
//     comment records that exact loop being observed live, and names this
//     counter as what bounds it. cfg.Wall == 0 is documented as unbounded, so
//     a rule that never counted an unchanged re-run would restore an infinite
//     loop rather than remove one. TestRunStopsAfterTheRoundCap is that case:
//     one participant, resubmitting the same version forever.
//
// A verdict where SOME but not all required participants have newly moved is
// one cycle still in progress, and does not advance the counter.
type fixCycle struct {
	required []string
	// started is false until the first failing verdict, which always counts:
	// there is no earlier round to have moved since.
	started bool
	// base is the version set of the round that last counted — the state this
	// cycle is measured against, not the previous verdict's state. Comparing
	// against the previous verdict would make each participant's fix look like
	// fresh movement forever, since a participant that fixed two rounds ago
	// still differs from base and must not be counted as moving again.
	base map[string]string
	// moved is which required participants have already moved since base,
	// accumulated across the verdicts of this cycle.
	moved map[string]bool
}

func newFixCycle(required []string) *fixCycle {
	return &fixCycle{required: required, moved: make(map[string]bool, len(required))}
}

// completes reports whether v ends a fix cycle and so advances the round cap.
// It must be called exactly once per failing verdict, in arrival order: it
// carries the cycle's state forward.
func (fc *fixCycle) completes(v gate.Verdict) bool {
	if !fc.started {
		fc.start(v)
		return true
	}
	// A required participant counts as moved only when the verdict actually
	// names it at a version differing from base. An absent participant is not
	// movement: a merge failure produces a verdict with no versions at all,
	// and reading "absent" as "changed" would call that a completed cycle.
	movedNow := make([]string, 0, len(fc.required))
	for _, p := range fc.required {
		if tested, named := v.Versions[p]; named && tested != fc.base[p] {
			movedNow = append(movedNow, p)
		}
	}
	if len(movedNow) == len(fc.required) {
		fc.start(v) // the whole quorum has re-declared: one cycle, as before
		return true
	}
	fresh := false
	for _, p := range movedNow {
		if !fc.moved[p] {
			fresh = true
		}
		fc.moved[p] = true
	}
	if !fresh {
		fc.start(v) // nothing new is arriving; this cycle is not progressing
		return true
	}
	return false
}

// start makes v the state the next cycle is measured against.
func (fc *fixCycle) start(v gate.Verdict) {
	fc.started = true
	fc.base = make(map[string]string, len(v.Versions))
	for name, version := range v.Versions {
		fc.base[name] = version
	}
	fc.moved = make(map[string]bool, len(fc.required))
}

// ErrSessionDied reports that a spawned session ended or errored before the
// gate reached a verdict. It is deliberately terminal: the spec requires the
// lease released and the run failed WITH ATTRIBUTION, and explicitly no silent
// retry, because a crash loop spends real money.
var ErrSessionDied = errors.New("pcops: agent session ended before a verdict")

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

// sessionFailure carries a terminal session event out of the drainer, with the
// PC-assigned agent name attached so the run can name who died. The name comes
// from the Spec rather than from Event.Agent: identity is assigned by the
// control plane, and an adapter is not required to echo it back.
type sessionFailure struct {
	agent string
	kind  runtime.EventKind
	err   string
}

// leaseLoss carries a fenced lease out of its heartbeat goroutine: the
// PC-assigned agent name so the run can say who lost the workspace, and the
// error the heartbeat actually saw, which names the worktree path. Mirrors
// sessionFailure above, for the same reason — an attributed failure needs both
// the identity and the cause.
type leaseLoss struct {
	agent string
	err   error
}

// Run executes one scenario: lease a workspace per participant, launch each
// agent, bridge them onto the bus, host the gate, and wait for a verdict.
//
// Identity is assigned here rather than negotiated with a model: PC_AGENT,
// PC_DB and PC_SUBMIT_TIMEOUT are injected into each session's environment, so
// the durable cursor key is always correct and a spawned agent's `pc submit`
// parks for the scenario's configured timeout rather than the default.
//
// The coordinator and the runner are started with StartCoordinator and
// StartRunner — not the blocking Up/RunGate — because both return only once
// their agent is actually subscribed to the bus. pkg/bus/sqlite starts a new
// subscriber at the CURRENT HEAD of the message log, so a bare `go Up(...)` /
// `go RunGate(...)` pair races every agent session about to be spawned below:
// a readiness or request message published before the subscription exists is
// missed forever, silently stalling the gate (in the runner's case, for the
// full 10-minute runner timeout instead of failing fast). Starting both
// synchronously, and before any session is spawned, closes that race.
func Run(ctx context.Context, cfg Config, r runtime.Runtime) (gate.Verdict, error) {
	if cfg.Wall > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Wall)
		defer cancel()
	}

	root := filepath.Join(filepath.Dir(cfg.DB), "worktrees")
	wm, err := workspace.New(ctx, cfg.Repo, root, cfg.DB)
	if err != nil {
		return gate.Verdict{}, err
	}
	defer wm.Close()

	// Buffered to hold one loss per lease this run could ever hold (the
	// runner's plus one per agent) so a fenced heartbeat goroutine's
	// non-blocking send never has to be discarded.
	lost := make(chan leaseLoss, 1+len(cfg.Agents))

	verdicts := make(chan gate.Verdict, 4)
	cstop, err := StartCoordinator(ctx, cfg, func(v gate.Verdict) {
		// Non-blocking: a slow or absent consumer must never stall the
		// coordinator's own dispatch goroutine, which is what calls this
		// hook. submit.go's verdict delivery uses the same shape for the
		// same reason.
		select {
		case verdicts <- v:
		default:
		}
	})
	if err != nil {
		return gate.Verdict{}, fmt.Errorf("start coordinator: %w", err)
	}
	defer cstop()

	// The runner gets its own worktree so it can merge every branch.
	runnerLease, err := wm.Acquire(ctx, cfg.Runner.Name, cfg.Runner.Branch)
	if err != nil {
		return gate.Verdict{}, fmt.Errorf("lease runner workspace: %w", err)
	}
	defer runnerLease.Release(context.Background())
	// See heartbeatLease: the runner holds its lease for the life of the
	// scenario too, so it must be kept fresh for just as long.
	stopRunnerHeartbeat := startHeartbeat(ctx, runnerLease, lost)
	defer stopRunnerHeartbeat()

	branches := make([]string, 0, len(cfg.Agents))
	for _, a := range cfg.Agents {
		branches = append(branches, a.Branch)
	}
	rstop, err := StartRunner(ctx, cfg, runnerLease.Path, branches)
	if err != nil {
		return gate.Verdict{}, fmt.Errorf("start runner: %w", err)
	}
	defer rstop()

	b, err := sqlite.Open(ctx, cfg.DB, sqlite.WithPollInterval(25*time.Millisecond))
	if err != nil {
		return gate.Verdict{}, fmt.Errorf("open bus: %w", err)
	}
	defer b.Close()

	// Buffered and never read more than once: Run returns on the first
	// failure, and the extra slot keeps a late drainer's send from blocking
	// after that.
	failures := make(chan sessionFailure, len(cfg.Agents)+1)

	// The submit timeout has to travel to the spawned agent, or every `pc
	// submit` it runs parks for DefaultSubmitTimeout regardless of the scenario
	// file — the loop test's 60s SubmitTimeout had no effect at all, and only
	// terminated because Wall bounded it. cmd/pc reads PC_SUBMIT_TIMEOUT back.
	submitTimeout := cfg.SubmitTimeout
	if submitTimeout <= 0 {
		submitTimeout = DefaultSubmitTimeout
	}

	for _, def := range cfg.Agents {
		lease, err := wm.Acquire(ctx, def.Name, def.Branch)
		if err != nil {
			return gate.Verdict{}, fmt.Errorf("lease workspace for %s: %w", def.Name, err)
		}
		defer lease.Release(context.Background())
		stopHeartbeat := startHeartbeat(ctx, lease, lost)
		defer stopHeartbeat()

		sess, err := r.Start(ctx, runtime.Spec{
			Agent:   def.Name,
			Workdir: lease.Path,
			Role:    def.Role,
			Task:    def.Task,
			Env: map[string]string{
				"PC_AGENT":          def.Name,
				"PC_DB":             cfg.DB,
				"PC_SUBMIT_TIMEOUT": submitTimeout.String(),
			},
			Budget: runtime.Budget{Wall: cfg.Wall},
			// A subdirectory of the run's own directory, named for the
			// agent, so each session's raw adapter frames land somewhere
			// findable without agents colliding with each other's transcripts.
			TranscriptDir: filepath.Join(filepath.Dir(cfg.DB), "transcripts", def.Name),
		})
		if err != nil {
			return gate.Verdict{}, fmt.Errorf("start %s: %w", def.Name, err)
		}
		defer sess.Close(context.Background())

		if err := StartCourier(ctx, b, def.Name, sess, nil); err != nil {
			return gate.Verdict{}, err
		}
		go drainEvents(def.Name, sess, failures)
	}

	rounds := 0
	cycles := newFixCycle(cfg.Gate.Required)
	for {
		select {
		case v := <-verdicts:
			if v.Passed {
				return v, nil
			}
			// A failing verdict already routed blocks to the owners; their
			// couriers steer them into a fix. Keep waiting for the next round,
			// up to the cap — which counts FIX CYCLES rather than failing
			// verdicts, because standing readiness turned one cycle into as
			// many verdicts as there are participants. See fixCycle.
			if !cycles.completes(v) {
				continue
			}
			rounds++
			if rounds >= maxRounds {
				return v, fmt.Errorf("gate %s still failing after %d rounds (cap %d): %s",
					cfg.GateID, rounds, maxRounds, v.Detail)
			}
		case f := <-failures:
			// Attribution, and no retry. Every lease and session is released by
			// the deferred cleanup above as this returns.
			if f.err != "" {
				return gate.Verdict{GateID: cfg.GateID},
					fmt.Errorf("agent %q reported %s (%s): %w", f.agent, f.kind, f.err, ErrSessionDied)
			}
			return gate.Verdict{GateID: cfg.GateID},
				fmt.Errorf("agent %q reported %s: %w", f.agent, f.kind, ErrSessionDied)
		case l := <-lost:
			// Not transient: another holder now owns this workspace.
			// Continuing would mean working in a directory this run no
			// longer owns, so stop and attribute the failure the same
			// way ErrSessionDied does. ErrLeaseLost already wraps
			// workspace.ErrLeaseLost, so both names match; l.err is
			// included because it is what names the worktree path.
			return gate.Verdict{GateID: cfg.GateID},
				fmt.Errorf("agent %q: %w (%v)", l.agent, ErrLeaseLost, l.err)
		case <-ctx.Done():
			return gate.Verdict{GateID: cfg.GateID}, fmt.Errorf("scenario budget exhausted: %w", ctx.Err())
		}
	}
}

// drainEvents keeps a session's event channel moving and forwards the terminal
// ones. Evidence collection lands with the domain store; for now the rest of
// the events must simply not back up.
//
// Discarding every event made a dead session invisible: Run waited out the
// whole wall budget for a verdict that could never arrive and then reported
// "scenario budget exhausted" with no attribution.
//
// Any exited or errored event counts as a death, without also checking for a
// preceding idle as the spec's detection column suggests. While a scenario is
// live a participant is either working or parked inside `pc submit`; Phase A
// has no orderly per-session finish to distinguish, because the run ends when
// the gate passes, not when a session ends. Sessions do emit exited during the
// deferred teardown after Run has returned — the non-blocking send below is
// what keeps that from mattering.
func drainEvents(agent string, s runtime.Session, failures chan<- sessionFailure) {
	for ev := range s.Events() {
		switch ev.Kind {
		case runtime.KindExited, runtime.KindErrored:
			select {
			case failures <- sessionFailure{agent: agent, kind: ev.Kind, err: ev.Err}:
			default:
			}
		}
	}
}

// startHeartbeat begins renewing l on a ticker and returns a stop func that
// ends it. The returned func must be deferred AFTER (so it runs BEFORE, since
// defers are LIFO) the lease's own Release, so the heartbeat goroutine is
// guaranteed to have stopped issuing renewals before the lease row is deleted.
func startHeartbeat(ctx context.Context, l *workspace.Lease, lost chan<- leaseLoss) (stop func()) {
	hctx, cancel := context.WithCancel(ctx)
	go heartbeatLease(hctx, l, lost)
	return cancel
}

// heartbeatLease renews l on a ticker until ctx ends.
//
// This exists because workspace.Manager reclaims a lease that has gone stale
// — no heartbeat within its TTL, 30s by default — and Run holds every lease
// it acquires for the entire scenario: this package's own whole-loop test
// sets a 90s wall budget, and StartCoordinator configures a 10-minute runner
// timeout for real gate checks. Without a heartbeat, every lease Run holds
// goes stale while still in active use.
//
// The consequence is worse than a passive expiry. When the on-disk worktree
// still matches the branch being requested — the ordinary case, since Run's
// own holder is still using it — Manager.Acquire's reclaim path does not
// merely fail on a stale row: a second Acquire for the same agent and branch
// SILENTLY SUCCEEDS and hands back a live *Lease pointing at the very
// directory the original session is still working in, with no signal to
// either holder. That reattach-on-match behaviour is deliberate (it is how
// crash recovery works), which is exactly why a holder that is genuinely
// still alive must keep renewing for as long as it holds the lease.
func heartbeatLease(ctx context.Context, l *workspace.Lease, lost chan<- leaseLoss) {
	t := time.NewTicker(heartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := l.Heartbeat(ctx); err != nil {
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
				// A transient heartbeat failure must not kill the run —
				// the lease may simply outlive one blip before the next
				// tick renews it. Reporting it is enough; pkg/agent uses
				// the same log.Printf style for its own message trace.
				log.Printf("pcops: heartbeat lease %s: %v", l.Path, err)
			}
		}
	}
}
