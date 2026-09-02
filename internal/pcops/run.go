package pcops

import (
	"context"
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

// Run executes one scenario: lease a workspace per participant, launch each
// agent, bridge them onto the bus, host the gate, and wait for a verdict.
//
// Identity is assigned here rather than negotiated with a model: PC_AGENT and
// PC_DB are injected into each session's environment, so the durable cursor key
// is always correct.
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
	stopRunnerHeartbeat := startHeartbeat(ctx, runnerLease)
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

	for _, def := range cfg.Agents {
		lease, err := wm.Acquire(ctx, def.Name, def.Branch)
		if err != nil {
			return gate.Verdict{}, fmt.Errorf("lease workspace for %s: %w", def.Name, err)
		}
		defer lease.Release(context.Background())
		stopHeartbeat := startHeartbeat(ctx, lease)
		defer stopHeartbeat()

		sess, err := r.Start(ctx, runtime.Spec{
			Agent:   def.Name,
			Workdir: lease.Path,
			Role:    def.Role,
			Task:    def.Task,
			Env:     map[string]string{"PC_AGENT": def.Name, "PC_DB": cfg.DB},
			Budget:  runtime.Budget{Wall: cfg.Wall},
		})
		if err != nil {
			return gate.Verdict{}, fmt.Errorf("start %s: %w", def.Name, err)
		}
		defer sess.Close(context.Background())

		if err := StartCourier(ctx, b, def.Name, sess, nil); err != nil {
			return gate.Verdict{}, err
		}
		go drainEvents(sess)
	}

	for {
		select {
		case v := <-verdicts:
			if v.Passed {
				return v, nil
			}
			// A failing verdict already routed blocks to the owners; their
			// couriers steer them into a fix. Keep waiting for the next round.
		case <-ctx.Done():
			return gate.Verdict{GateID: cfg.GateID}, fmt.Errorf("scenario budget exhausted: %w", ctx.Err())
		}
	}
}

// drainEvents keeps a session's event channel moving. Evidence collection lands
// with the domain store; for now the events must simply not back up.
func drainEvents(s runtime.Session) {
	for range s.Events() {
	}
}

// startHeartbeat begins renewing l on a ticker and returns a stop func that
// ends it. The returned func must be deferred AFTER (so it runs BEFORE, since
// defers are LIFO) the lease's own Release, so the heartbeat goroutine is
// guaranteed to have stopped issuing renewals before the lease row is deleted.
func startHeartbeat(ctx context.Context, l *workspace.Lease) (stop func()) {
	hctx, cancel := context.WithCancel(ctx)
	go heartbeatLease(hctx, l)
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
func heartbeatLease(ctx context.Context, l *workspace.Lease) {
	t := time.NewTicker(heartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// A transient heartbeat failure must not kill the run — the
			// lease may simply outlive one blip before the next tick
			// renews it. Reporting it is enough; pkg/agent uses the same
			// log.Printf style for its own message trace.
			if err := l.Heartbeat(ctx); err != nil {
				log.Printf("pcops: heartbeat lease %s: %v", l.Path, err)
			}
		}
	}
}
