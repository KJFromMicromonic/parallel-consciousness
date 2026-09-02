// Package runtimetest holds the conformance suite every runtime.Runtime adapter
// must pass. Properties are about lifecycle only, never about what a model
// says, so both the fake and a real harness can satisfy them.
package runtimetest

import (
	"context"
	"io/fs"
	"path/filepath"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime"
)

// Options carries adapter capabilities the newer properties need but a single
// Spec cannot express. The base Spec passed to Run stays a long-lived session
// that reaches idle and stays controllable — most of the suite reuses it
// across subtests — so a session that ends entirely on its own needs to be
// named separately.
type Options struct {
	// Completes is a Spec whose session reaches its own natural end — no
	// Close, no Interrupt required — so a property about ExitCompleted has
	// something to observe that is provably not an externally-terminated
	// session.
	Completes runtime.Spec
}

// Run executes the suite. newRuntime must return a fresh Runtime; spec must be
// a task the adapter can actually complete; opts.Completes must be a task
// whose session ends on its own.
//
// This certifies lifecycle behaviour only: that a session reaches idle, that
// Close ends the event stream and unblocks Wait, and that a steer is accepted
// and provably causes another turn to run and settle. It does NOT certify the
// semantic correctness of what an adapter does with a steered message, or of
// anything else the agent under the adapter chooses to do — that is out of
// scope by design, so the suite works unchanged for both the fake and a real
// harness.
func Run(t *testing.T, newRuntime func(t *testing.T) runtime.Runtime, spec runtime.Spec, opts Options) {
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

	t.Run("WaitAfterANaturalEndReturnsCompleted", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s, err := newRuntime(t).Start(ctx, opts.Completes)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close(ctx)

		for range s.Events() { // drain until the session ends on its own
		}

		out, err := s.Wait(ctx)
		if err != nil {
			t.Fatalf("Wait after a natural end: %v", err)
		}
		if out.Reason != runtime.ExitCompleted {
			t.Fatalf("Outcome.Reason = %q, want %q", out.Reason, runtime.ExitCompleted)
		}
	})

	t.Run("CloseThenWaitReturnsInterrupted", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s, err := newRuntime(t).Start(ctx, spec)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Close(ctx); err != nil {
			t.Fatalf("Close: %v", err)
		}
		for range s.Events() {
		}

		out, err := s.Wait(ctx)
		if err != nil {
			t.Fatalf("Wait after Close: %v", err)
		}
		if out.Reason != runtime.ExitInterrupted {
			t.Fatalf("Outcome.Reason = %q, want %q", out.Reason, runtime.ExitInterrupted)
		}
	})

	t.Run("SteerProducesAQueueReceiptThatDrains", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s, err := newRuntime(t).Start(ctx, spec)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close(ctx)

		waitForIdle(t, ctx, s)

		if err := s.Steer(ctx, "receipt check"); err != nil {
			t.Fatalf("Steer: %v", err)
		}

		var sawReceipt bool
		deadline := time.After(30 * time.Second)
		for {
			select {
			case ev, ok := <-s.Events():
				if !ok {
					t.Fatal("event stream closed before the queue drained")
				}
				if ev.Kind != runtime.KindQueueChanged {
					continue
				}
				if ev.Pending == nil {
					t.Fatal("KindQueueChanged event with a nil Pending")
				}
				if !sawReceipt {
					if ev.Pending.Steering < 1 {
						// Not the receipt for our steer; keep scanning.
						continue
					}
					sawReceipt = true
					continue
				}
				if ev.Pending.Steering == 0 && ev.Pending.FollowUp == 0 {
					return // queue drained back to zero: property satisfied
				}
			case <-deadline:
				t.Fatalf("timed out waiting for the queue to drain (receipt seen=%v)", sawReceipt)
			case <-ctx.Done():
				t.Fatalf("context done waiting for the queue to drain: %v", ctx.Err())
			}
		}
	})

	t.Run("InterruptSettlesPromptlyAndIsInterrupted", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s, err := newRuntime(t).Start(ctx, spec)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close(ctx)

		waitForIdle(t, ctx, s)

		if err := s.Interrupt(ctx); err != nil {
			t.Fatalf("Interrupt: %v", err)
		}

		type result struct {
			out runtime.Outcome
			err error
		}
		done := make(chan result, 1)
		go func() {
			out, err := s.Wait(ctx)
			done <- result{out, err}
		}()

		select {
		case r := <-done:
			if r.err != nil {
				t.Fatalf("Wait after Interrupt: %v", r.err)
			}
			if r.out.Reason != runtime.ExitInterrupted {
				t.Fatalf("Outcome.Reason = %q, want %q", r.out.Reason, runtime.ExitInterrupted)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("session did not settle within 10s of Interrupt")
		}
	})

	t.Run("AnAlreadyCancelledCtxOnSteerReturnsPromptly", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s, err := newRuntime(t).Start(ctx, spec)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close(ctx)

		waitForIdle(t, ctx, s)

		cancelled, cancelNow := context.WithCancel(context.Background())
		cancelNow()

		errCh := make(chan error, 1)
		go func() { errCh <- s.Steer(cancelled, "should not be delivered") }()

		select {
		case err := <-errCh:
			if err == nil {
				t.Fatal("Steer with an already-cancelled ctx returned a nil error")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Steer with an already-cancelled ctx did not return promptly")
		}
	})

	t.Run("TranscriptDirGetsWritten", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		ts := spec
		ts.TranscriptDir = t.TempDir()

		s, err := newRuntime(t).Start(ctx, ts)
		if err != nil {
			t.Fatal(err)
		}

		waitForIdle(t, ctx, s)
		if err := s.Close(ctx); err != nil {
			t.Fatalf("Close: %v", err)
		}
		for range s.Events() {
		}
		if _, err := s.Wait(ctx); err != nil {
			t.Fatalf("Wait: %v", err)
		}

		if !dirHasContent(t, ts.TranscriptDir) {
			t.Fatalf("TranscriptDir %s has no content by the time the session settled", ts.TranscriptDir)
		}
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

// dirHasContent reports whether dir contains at least one non-empty regular
// file, without assuming anything about an adapter's transcript naming — the
// suite only certifies that something was written, not what it is called.
func dirHasContent(t *testing.T, dir string) bool {
	t.Helper()
	var found bool
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.Size() > 0 {
			found = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	return found
}
