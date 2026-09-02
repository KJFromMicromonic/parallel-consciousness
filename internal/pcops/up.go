package pcops

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
)

// StartCoordinator performs all coordinator setup synchronously and returns
// only once the coordinator agent is subscribed and its run loop is started.
//
// This split exists because the durable bus starts a new subscriber at the
// CURRENT HEAD of the message log: it never replays messages published
// before the subscription began. If a caller only had `Up`, which blocks
// forever, there would be no way to know when the coordinator's
// subscription actually took effect. A participant that declares readiness
// before that point has its message land in the log with nobody watching,
// the gate never opens, and any Submit for it hangs until it times out —
// silently, and in production, not just in tests. By returning only after
// agent.New has completed (which performs the subscription), a caller that
// gets a nil error here knows it is now safe for participants to submit.
func StartCoordinator(ctx context.Context, cfg Config, onVerdict func(gate.Verdict)) (stop func(), err error) {
	b, err := sqlite.Open(ctx, cfg.DB, sqlite.WithPollInterval(25*time.Millisecond))
	if err != nil {
		return nil, fmt.Errorf("open bus: %w", err)
	}

	a, err := agent.New(ctx, b, "coordinator", []string{gate.Topic(cfg.GateID)})
	if err != nil {
		b.Close()
		return nil, fmt.Errorf("join as coordinator: %w", err)
	}
	c := gate.NewCoordinator(a)
	// The runner shells out to a real test command, which is far slower than
	// the in-process default of 5s.
	c.SetRunnerTimeout(10 * time.Minute)
	c.Register(gate.Spec{ID: cfg.GateID, Required: cfg.Gate.Required, Runner: cfg.Gate.Runner})
	if onVerdict != nil {
		c.OnVerdict(onVerdict)
	}
	go a.Run(ctx)

	var once sync.Once
	stop = func() {
		once.Do(func() { b.Close() })
	}
	return stop, nil
}

// Up hosts the coordinator for the configured gate and blocks until ctx ends.
// It adds no coordination semantics of its own; pkg/gate owns all of them.
func Up(ctx context.Context, cfg Config, onVerdict func(gate.Verdict)) error {
	stop, err := StartCoordinator(ctx, cfg, onVerdict)
	if err != nil {
		return err
	}
	defer stop()

	<-ctx.Done()
	return ctx.Err()
}
