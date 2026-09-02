package pcops_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
)

// TestStartCoordinatorReachesQuorumWithoutRace is the regression test for the
// defect that made the original Up-based test flaky: the durable bus starts a
// subscriber at HEAD, so a participant that declares readiness before the
// coordinator's subscription exists has its message missed forever.
// StartCoordinator returning is the only synchronization used here — no
// sleeps, no retries — which is exactly the property that proves the race is
// closed. It also exercises a full quorum-to-verdict round: readiness,
// runner request, and a passing verdict delivered both to Submit's caller
// and to the onVerdict hook.
func TestStartCoordinatorReachesQuorumWithoutRace(t *testing.T) {
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
	stop, err := pcops.StartCoordinator(ctx, cfg, func(v gate.Verdict) { verdicts <- v })
	if err != nil {
		t.Fatalf("StartCoordinator: %v", err)
	}
	defer stop()

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

	// StartCoordinator has already returned above, so the coordinator is
	// guaranteed to be subscribed before this readiness declaration is
	// published — no sleep required to make that true.
	v, err := pcops.Submit(ctx, cfg, "g", "billing", "v1")
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if !v.Passed {
		t.Fatalf("Submit verdict = %+v, want passed", v)
	}

	select {
	case hv := <-verdicts:
		if !hv.Passed {
			t.Fatalf("onVerdict hook verdict = %+v, want passed", hv)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("onVerdict hook never fired")
	}
}

// TestUpPropagatesContextCancellation covers Up's own wrapper contract, which
// is otherwise unexercised by tests that call StartCoordinator directly:
// that Up blocks until ctx ends and then reports why via ctx.Err(), rather
// than returning nil or hanging. Coordination itself is StartCoordinator's
// responsibility and is covered by TestStartCoordinatorReachesQuorumWithoutRace;
// this test only proves the thin wrapper around it — defer stop(), block on
// ctx.Done(), return ctx.Err() — actually does what it claims.
func TestUpPropagatesContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{
		DB:     db,
		GateID: "g",
		Gate:   pcops.GateDef{Required: []string{"billing"}, Runner: "runner"},
	}

	errs := make(chan error, 1)
	go func() { errs <- pcops.Up(ctx, cfg, nil) }()

	cancel()

	select {
	case err := <-errs:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Up err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Up never returned after ctx was canceled")
	}
}
