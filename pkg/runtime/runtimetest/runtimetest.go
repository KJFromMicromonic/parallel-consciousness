// Package runtimetest holds the conformance suite every runtime.Runtime adapter
// must pass. Properties are about lifecycle only, never about what a model
// says, so both the fake and a real harness can satisfy them.
package runtimetest

import (
	"context"
	"io/fs"
	"os"
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

	// LongRunning is a Spec whose session, once started, spends tens of
	// seconds doing one thing — long enough that a property can reliably
	// catch it mid-flight and assert Interrupt cuts it off in a few seconds,
	// nowhere near its natural duration. Without a genuinely long action
	// there is no way to tell "Interrupt preempted this" apart from "this
	// happened to finish on its own before Interrupt was even checked."
	LongRunning runtime.Spec

	// InFlightWork is used by AnInFlightSteerDoesNotPreempt and
	// AnInFlightFollowDoesNotPreempt to prove the mirror image of
	// InterruptPreemptsInFlightWork: that Steer and Follow do NOT cut off
	// work already in flight. Its session's OnStart behaviour must be a
	// several-second action followed by one KindToolUsed event whose
	// Tool.Target equals WorkDoneMarker, reporting that the in-flight work
	// has completed. It needs to be its own short spec — not opts.LongRunning
	// reused — because LongRunning's own action deliberately runs tens of
	// seconds (room for InterruptPreemptsInFlightWork's few-second deadline
	// to mean something); forcing these ordering properties onto that same
	// duration would mean waiting out tens of seconds of real time, several
	// times over under -count=5, just to observe an event order that a
	// several-second action already proves.
	InFlightWork runtime.Spec

	// WorkDoneMarker is the Tool.Target of InFlightWork's completion event.
	WorkDoneMarker string
	// SteerAppliedMarker is the Tool.Target InFlightWork's OnSteer path must
	// report once its own action runs, at the next turn boundary.
	SteerAppliedMarker string
	// FollowAppliedMarker is the Tool.Target InFlightWork's OnFollow path
	// must report once its own action runs, at the next turn boundary.
	FollowAppliedMarker string
}

// Run executes the suite. newRuntime must return a fresh Runtime; spec must be
// a task the adapter can actually complete; opts.Completes must be a task
// whose session ends on its own; opts.LongRunning must be a task that stays
// busy for tens of seconds so InterruptPreemptsInFlightWork has something
// genuinely in flight to interrupt.
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

	t.Run("TranscriptDirThatCannotBeCreatedFailsStart", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		// A regular file where a directory needs to go: os.MkdirAll cannot
		// create anything under it, so this reliably reproduces "the
		// requested directory cannot be honoured" without depending on
		// filesystem permissions, which behave inconsistently across CI
		// environments (and not at all for a process running as root).
		blocker := filepath.Join(t.TempDir(), "blocker")
		if err := os.WriteFile(blocker, []byte("not a directory"), 0o644); err != nil {
			t.Fatal(err)
		}

		ts := spec
		ts.TranscriptDir = filepath.Join(blocker, "transcripts")

		if _, err := newRuntime(t).Start(ctx, ts); err == nil {
			t.Fatal("Start succeeded with a TranscriptDir that cannot be created, want an error")
		}
	})

	t.Run("FollowProducesAQueueReceiptThatDrains", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s, err := newRuntime(t).Start(ctx, spec)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close(ctx)

		waitForIdle(t, ctx, s)

		if err := s.Follow(ctx, "follow receipt check"); err != nil {
			t.Fatalf("Follow: %v", err)
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
					if ev.Pending.FollowUp < 1 {
						// Not the receipt for our follow; keep scanning.
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

	t.Run("InterruptPreemptsInFlightWork", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s, err := newRuntime(t).Start(ctx, opts.LongRunning)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close(ctx)

		// Wait for genuine evidence the long-running action is in flight —
		// KindTurnBegan, not KindIdle. Waiting for idle here would prove
		// nothing: idle means the action already finished, which is exactly
		// the case a preemption property must NOT exercise.
		inFlight := time.After(10 * time.Second)
	waitInFlight:
		for {
			select {
			case ev, ok := <-s.Events():
				if !ok {
					t.Fatal("event stream closed before the long-running turn began")
				}
				if ev.Kind == runtime.KindTurnBegan {
					break waitInFlight
				}
			case <-inFlight:
				t.Fatal("timed out waiting for the long-running turn to begin")
			}
		}

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

		// Bounded well inside the long-running action's own duration: a fake
		// (or adapter) that let the action run to completion instead of
		// actually preempting it would blow this deadline.
		select {
		case r := <-done:
			if r.err != nil {
				t.Fatalf("Wait after Interrupt: %v", r.err)
			}
			if r.out.Reason != runtime.ExitInterrupted {
				t.Fatalf("Outcome.Reason = %q, want %q", r.out.Reason, runtime.ExitInterrupted)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("session did not settle within 5s of Interrupt while a long-running action was in flight: Interrupt did not preempt it")
		}
	})

	t.Run("AnInFlightSteerDoesNotPreempt", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s, err := newRuntime(t).Start(ctx, opts.InFlightWork)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close(ctx)

		waitForTurnBegan(t, ctx, s)

		if err := s.Steer(ctx, "steer while busy"); err != nil {
			t.Fatalf("Steer: %v", err)
		}

		assertVerbDoesNotPreempt(t, ctx, s, "Steer", opts.WorkDoneMarker, opts.SteerAppliedMarker,
			func(q runtime.Queue) int { return q.Steering })
	})

	t.Run("AnInFlightFollowDoesNotPreempt", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s, err := newRuntime(t).Start(ctx, opts.InFlightWork)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close(ctx)

		waitForTurnBegan(t, ctx, s)

		if err := s.Follow(ctx, "follow while busy"); err != nil {
			t.Fatalf("Follow: %v", err)
		}

		assertVerbDoesNotPreempt(t, ctx, s, "Follow", opts.WorkDoneMarker, opts.FollowAppliedMarker,
			func(q runtime.Queue) int { return q.FollowUp })
	})

	t.Run("AnAlreadyCancelledCtxOnInterruptReturnsPromptly", func(t *testing.T) {
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
		go func() { errCh <- s.Interrupt(cancelled) }()

		select {
		case err := <-errCh:
			if err == nil {
				t.Fatal("Interrupt with an already-cancelled ctx returned a nil error")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Interrupt with an already-cancelled ctx did not return promptly")
		}
	})

	t.Run("AnAlreadyCancelledCtxOnWaitReturnsPromptly", func(t *testing.T) {
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

		type result struct {
			out runtime.Outcome
			err error
		}
		resCh := make(chan result, 1)
		go func() {
			out, err := s.Wait(cancelled)
			resCh <- result{out, err}
		}()

		select {
		case r := <-resCh:
			if r.err == nil {
				t.Fatal("Wait with an already-cancelled ctx returned a nil error")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Wait with an already-cancelled ctx did not return promptly")
		}
	})
}

// waitForTurnBegan drains events until KindTurnBegan arrives, failing the
// test if the stream closes or ctx expires first. Unlike waitForIdle, this is
// evidence a turn has genuinely STARTED — used by properties that need to
// catch a session while its current turn's action is still in flight, where
// waiting for idle would prove nothing (idle means that action already
// finished, which is exactly the case such a property must not exercise).
func waitForTurnBegan(t *testing.T, ctx context.Context, s runtime.Session) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case ev, ok := <-s.Events():
			if !ok {
				t.Fatal("event stream closed before the turn began")
			}
			if ev.Kind == runtime.KindTurnBegan {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for the turn to begin")
		case <-ctx.Done():
			t.Fatalf("context done before the turn began: %v", ctx.Err())
		}
	}
}

// assertVerbDoesNotPreempt is shared by AnInFlightSteerDoesNotPreempt and
// AnInFlightFollowDoesNotPreempt: everything about the property is identical
// between the two verbs except which Queue counter counts as that verb's
// accept receipt, so callers pass queueField to select it.
//
// The assertion is ordering-based, not timing-based, on purpose — a timing
// threshold here would be flaky against a real adapter. It requires BOTH:
//
//  1. the accept receipt (KindQueueChanged with queueField(Pending) >= 1)
//     arrives before workDoneMarker — proving the message really was
//     accepted while the in-flight action was still running, not after it
//     happened to finish; and
//  2. appliedMarker (the verb's own scripted action, run once loop() reaches
//     the next turn boundary) arrives after workDoneMarker — proving it did
//     NOT cut the in-flight action off early.
//
// Either check alone would be meaningless: without (1), a verb that silently
// waited for idle before even accepting the message would pass; without (2),
// nothing distinguishes non-preemption from preemption at all.
func assertVerbDoesNotPreempt(t *testing.T, ctx context.Context, s runtime.Session, verb, workDoneMarker, appliedMarker string, queueField func(runtime.Queue) int) {
	t.Helper()
	var receiptSeenBeforeCompletion, workDone bool
	deadline := time.After(30 * time.Second)
	for {
		select {
		case ev, ok := <-s.Events():
			if !ok {
				t.Fatalf("event stream closed before both %s markers arrived", verb)
			}
			switch ev.Kind {
			case runtime.KindQueueChanged:
				if !workDone && ev.Pending != nil && queueField(*ev.Pending) >= 1 {
					receiptSeenBeforeCompletion = true
				}
			case runtime.KindToolUsed:
				if ev.Tool == nil {
					continue
				}
				switch ev.Tool.Target {
				case workDoneMarker:
					workDone = true
				case appliedMarker:
					if !workDone {
						t.Fatalf("%s's marker arrived before the in-flight work's completion marker: %s preempted work in flight", verb, verb)
					}
					if !receiptSeenBeforeCompletion {
						t.Fatalf("the %s queue receipt never arrived before the in-flight work completed: cannot prove the message was accepted while work was in flight", verb)
					}
					return
				}
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s non-preemption evidence (workDone=%v)", verb, workDone)
		case <-ctx.Done():
			t.Fatalf("context done waiting for %s non-preemption evidence: %v", verb, ctx.Err())
		}
	}
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
