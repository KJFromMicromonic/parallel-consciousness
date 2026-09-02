package pcops_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
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
	if v.Detail != "" {
		t.Fatalf("verdict.Detail = %q, want empty on a passing verdict", v.Detail)
	}
}

// A single-participant gate whose runner always fails: submit must return a
// nil error alongside a non-passing verdict that carries the failure detail —
// distinct from ErrNoVerdict, which means no verdict arrived at all.
func TestSubmitReturnsAFailingVerdict(t *testing.T) {
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
		return gate.Verdict{GateID: gateID, Passed: false, Detail: "spanning test failed"}
	})
	go run.Run(ctx)

	v, err := pcops.Submit(ctx, cfg, "g", "billing", "v1")
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if v.Passed {
		t.Fatalf("verdict = %+v, want not passed", v)
	}
	if v.Detail == "" || !strings.Contains(v.Detail, "failed") {
		t.Fatalf("verdict.Detail = %q, want it to mention the failure", v.Detail)
	}
}

func TestSubmitTimesOutWithoutACoordinator(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cfg := pcops.Config{
		DB:            filepath.Join(t.TempDir(), "bus.db"),
		SubmitTimeout: 300 * time.Millisecond,
	}
	if _, err := pcops.Submit(ctx, cfg, "g", "billing", "v1"); !errors.Is(err, pcops.ErrNoVerdict) {
		t.Fatalf("Submit err = %v, want ErrNoVerdict", err)
	}
}

// A verdict already in the log must not be mistaken for this round's answer.
// pkg/bus/sqlite resumes a subscription from the STORED cursor whenever a row
// exists for that agent name, so a `pc submit` under a name whose cursor sits
// behind a previous round's Inform used to be handed that Inform and return it
// instantly — with the wrong outcome.
//
// The setup reproduces exactly that state rather than simulating it: a warmup
// subscription under the agent's own name advances and persists its cursor, the
// bus holding it is closed, and only then is the stale PASSING verdict
// published. Submit then runs against a coordinator whose runner FAILS, so the
// two rounds are distinguishable by outcome, not just by content.
func TestSubmitIgnoresAVerdictFromAPreviousRound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{DB: db, SubmitTimeout: 20 * time.Second}

	warm, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })

	// Persist a cursor for "billing" that sits behind the stale verdict. Two
	// warmup messages, not one: the poller saves the cursor for a batch only
	// after delivering it, so receiving the SECOND message is what proves the
	// first one's position was already written.
	in, err := warm.Subscribe(ctx, "billing", []string{gate.Topic("g")})
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"warmup-1", "warmup-2"} {
		if err := b.Publish(ctx, protocol.New(
			protocol.Address{Agent: "warmup"}, protocol.Address{Agent: "billing"},
			protocol.IntentInform, map[string]any{"text": text})); err != nil {
			t.Fatal(err)
		}
		select {
		case <-in:
		case <-time.After(15 * time.Second):
			t.Fatalf("warmup message %q never arrived", text)
		}
	}
	// Closing the bus stops that poller for good, so nothing can advance the
	// stored cursor past the stale verdict published next.
	warm.Close()

	stale := protocol.New(
		protocol.Address{Agent: "coordinator"}, protocol.Address{Topic: gate.Topic("g")},
		protocol.IntentInform,
		map[string]any{"text": "g PASSED", "gate": "g", "passed": true})
	if err := b.Publish(ctx, stale); err != nil {
		t.Fatal(err)
	}

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
		return gate.Verdict{GateID: gateID, Passed: false, Detail: "this round failed"}
	})
	go run.Run(ctx)

	v, err := pcops.Submit(ctx, cfg, "g", "billing", "v2")
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if v.Passed {
		t.Fatal("Submit returned the previous round's passing verdict")
	}
	if !strings.Contains(v.Detail, "this round failed") {
		t.Fatalf("verdict.Detail = %q, want this round's failure detail", v.Detail)
	}
}
