package pcops_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
)

// A single-participant gate whose runner always passes: submit must return a
// passing verdict rather than time out.
func TestSubmitReturnsThePassingVerdict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{DB: db, SubmitTimeout: 10 * time.Second}

	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })

	coord, err := agent.New(ctx, b, "coordinator", []string{gate.Topic("g")})
	if err != nil {
		t.Fatal(err)
	}
	c := gate.NewCoordinator(coord)
	c.Register(gate.Spec{ID: "g", Required: []string{"billing"}, Runner: "runner"})
	go coord.Run(ctx)

	run, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.ServeRunner(run, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: true}
	})
	go run.Run(ctx)

	v, err := pcops.Submit(ctx, cfg, "g", "billing", "v1")
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if !v.Passed {
		t.Fatalf("verdict = %+v, want passed", v)
	}
}

func TestSubmitTimesOutWithoutACoordinator(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cfg := pcops.Config{
		DB:            filepath.Join(t.TempDir(), "bus.db"),
		SubmitTimeout: 300 * time.Millisecond,
	}
	if _, err := pcops.Submit(ctx, cfg, "g", "billing", "v1"); err == nil {
		t.Fatal("want a timeout error when nothing coordinates the gate")
	}
}
