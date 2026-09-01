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
		if err := s.Steer(ctx, "noted"); err != nil {
			t.Fatalf("Steer: %v", err)
		}
	})
}
