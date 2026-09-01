// Package runtimetest holds the conformance suite every runtime.Runtime adapter
// must pass. Properties are about lifecycle only, never about what a model
// says, so both the fake and a real harness can satisfy them.
package runtimetest

import (
	"context"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime"
)

// Run executes the suite. newRuntime must return a fresh Runtime; spec must be
// a task the adapter can actually complete.
//
// This certifies lifecycle behaviour only: that a session reaches idle, that
// Close ends the event stream and unblocks Wait, and that a steer is accepted
// and provably causes another turn to run and settle. It does NOT certify the
// semantic correctness of what an adapter does with a steered message, or of
// anything else the agent under the adapter chooses to do — that is out of
// scope by design, so the suite works unchanged for both the fake and a real
// harness.
func Run(t *testing.T, newRuntime func(t *testing.T) runtime.Runtime, spec runtime.Spec) {
	t.Run("ReachesIdle", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s, err := newRuntime(t).Start(ctx, spec)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close(ctx)

		var sawStarted bool
		for ev := range s.Events() {
			switch ev.Kind {
			case runtime.KindStarted:
				sawStarted = true
			case runtime.KindIdle:
				if !sawStarted {
					t.Fatal("idle before started")
				}
				return
			}
		}
		t.Fatal("event stream closed before idle")
	})

	t.Run("CloseEndsTheStreamAndWaitReturns", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s, err := newRuntime(t).Start(ctx, spec)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Close(ctx); err != nil {
			t.Fatalf("Close: %v", err)
		}
		for range s.Events() { // must drain and close, not block
		}
		if _, err := s.Wait(ctx); err != nil {
			t.Fatalf("Wait after Close: %v", err)
		}
	})

	t.Run("SteerIsAccepted", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s, err := newRuntime(t).Start(ctx, spec)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close(ctx)

		waitForIdle(t, ctx, s) // let the session settle before steering

		if err := s.Steer(ctx, "noted"); err != nil {
			t.Fatalf("Steer: %v", err)
		}

		// A steer that is silently discarded would still pass a check that
		// only inspects Steer's return value. Require observable proof: a
		// second idle, meaning the steer opened a new turn that then settled.
		// This checks lifecycle only — never the content of any event or
		// message — so it holds for any honest adapter, not just the fake.
		waitForIdle(t, ctx, s)
	})
}

// waitForIdle drains events until KindIdle arrives, failing the test if the
// stream closes or ctx expires first.
func waitForIdle(t *testing.T, ctx context.Context, s runtime.Session) {
	t.Helper()
	for {
		select {
		case ev, ok := <-s.Events():
			if !ok {
				t.Fatal("event stream closed before idle")
			}
			if ev.Kind == runtime.KindIdle {
				return
			}
		case <-ctx.Done():
			t.Fatalf("context done before idle: %v", ctx.Err())
		}
	}
}
