package pcops

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/workspace"
)

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
	cstop, err := StartCoordinator(ctx, cfg, func(v gate.Verdict) { verdicts <- v })
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
