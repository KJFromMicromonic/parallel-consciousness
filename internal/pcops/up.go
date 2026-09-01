package pcops

import (
	"context"
	"fmt"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
)

// Up hosts the coordinator for the configured gate and blocks until ctx ends.
// It adds no coordination semantics of its own; pkg/gate owns all of them.
func Up(ctx context.Context, cfg Config, onVerdict func(gate.Verdict)) error {
	b, err := sqlite.Open(ctx, cfg.DB, sqlite.WithPollInterval(25*time.Millisecond))
	if err != nil {
		return fmt.Errorf("open bus: %w", err)
	}
	defer b.Close()

	a, err := agent.New(ctx, b, "coordinator", []string{gate.Topic(cfg.GateID)})
	if err != nil {
		return fmt.Errorf("join as coordinator: %w", err)
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

	<-ctx.Done()
	return ctx.Err()
}
