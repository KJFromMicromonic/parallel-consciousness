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

func TestUpCoordinatesUntilQuorum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{
		DB:            db,
		GateID:        "g",
		Gate:          pcops.GateDef{Required: []string{"billing"}, Runner: "runner"},
		SubmitTimeout: 10 * time.Second,
	}

	verdicts := make(chan gate.Verdict, 1)
	go pcops.Up(ctx, cfg, func(v gate.Verdict) { verdicts <- v })

	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	run, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.ServeRunner(run, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: true}
	})
	go run.Run(ctx)

	if _, err := pcops.Submit(ctx, cfg, "g", "billing", "v1"); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	select {
	case v := <-verdicts:
		if !v.Passed {
			t.Fatalf("verdict = %+v", v)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("coordinator never resolved the gate")
	}
}
