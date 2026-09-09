// Package fake is a scripted runtime.Runtime for tests. It exists so the whole
// coordination loop can be exercised deterministically in CI without an API
// key, a network, or a single token of spend.
package fake

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime"
)

// ErrSessionEnding is returned by Steer/Follow when the session ended — via
// Close, Interrupt, or a scripted Exit — before the message could be handed
// to loop() at all. Returning nil in that race would tell the caller
// "accepted" for a message that will now never run and never drain; a caller
// checking errors.Is(err, ErrSessionEnding) can tell that apart from a real
// delivery.
var ErrSessionEnding = errors.New("fake: session ended before the message could be accepted")

// Action is one scripted thing a fake agent does.
type Action interface{ isAction() }

// Write writes Content to Path, relative to the session's workdir.
type Write struct{ Path, Content string }

// Exec runs a command in the session's workdir with the session's env. This is
// how a fake agent calls `pc submit`. It runs under the session's internal
// workCtx, so an Interrupt or Close arriving while it is in flight actually
// kills it rather than waiting it out — see session.workCtx.
type Exec struct{ Args []string }

// Emit reports a tool use without doing anything, for evidence assertions.
type Emit struct{ Tool runtime.ToolUse }

// Exit ends the session where it stands, so a script can play the failure mode
// the spec cares about: a session that dies mid-task and never reaches a
// verdict. The session emits its terminal event exactly as a real adapter does
// when its process goes away. Because this is a scripted natural end, Wait
// reports runtime.ExitCompleted for it — as opposed to Close or Interrupt,
// which report runtime.ExitInterrupted.
type Exit struct{}

func (Write) isAction() {}
func (Exec) isAction()  {}
func (Emit) isAction()  {}
func (Exit) isAction()  {}

// Script is one agent's behaviour: what it does on start, and what it does when
// a message is delivered.
//
// OnSteer and OnFollow are kept separate because the courier's central policy —
// IntentBlock takes the steer path, every other intent takes the follow path —
// is otherwise unassertable: aliasing Follow to Steer made the whole suite pass
// with that mapping inverted. A script that only cares THAT a message arrived
// may set either hook alone; each verb prefers its own hook and falls back to
// the other when it is nil, so existing steer-only scripts keep working. A
// script asserting the routing sets BOTH, and then the two paths are
// distinguishable. Both may be nil, in which case delivery is a no-op.
type Script struct {
	OnStart  []Action
	OnSteer  func(text string) []Action
	OnFollow func(text string) []Action
}

// Runtime is the fake. Scripts are keyed by Spec.Agent.
type Runtime struct{ scripts map[string]Script }

func New(scripts map[string]Script) *Runtime { return &Runtime{scripts: scripts} }

func (r *Runtime) Name() string { return "fake" }

func (r *Runtime) Start(ctx context.Context, spec runtime.Spec) (runtime.Session, error) {
	var transcript *os.File
	if spec.TranscriptDir != "" {
		// A real adapter that cannot honour TranscriptDir must fail Start
		// rather than write elsewhere; the fake honours it by actually
		// creating the directory, so a failure here is the same kind of
		// failure a real adapter would report.
		if err := os.MkdirAll(spec.TranscriptDir, 0o755); err != nil {
			return nil, fmt.Errorf("fake: create transcript dir %s: %w", spec.TranscriptDir, err)
		}
		f, err := os.OpenFile(filepath.Join(spec.TranscriptDir, "transcript.log"),
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, fmt.Errorf("fake: open transcript in %s: %w", spec.TranscriptDir, err)
		}
		transcript = f
	}

	workCtx, cancelWork := context.WithCancel(context.Background())
	s := &session{
		spec:       spec,
		script:     r.scripts[spec.Agent],
		events:     make(chan runtime.Event, 256),
		work:       make(chan queuedWork, 8),
		done:       make(chan struct{}),
		stopped:    make(chan struct{}),
		workCtx:    workCtx,
		cancelWork: cancelWork,
		start:      time.Now(),
		transcript: transcript,
	}
	go s.loop()
	s.work <- queuedWork{actions: s.script.OnStart, kind: kindInitial}
	return s, nil
}

// workKind distinguishes the initial OnStart script, which carries no queue
// receipt, from a Steer- or Follow-triggered turn, which does.
type workKind int

const (
	kindInitial workKind = iota
	kindSteer
	kindFollow
)

// queuedWork is one turn's worth of scripted actions, tagged with which queue
// (if any) it came from. The accept receipt for a Steer or Follow message is
// NOT carried in here — see queue()'s doc comment for why it is emitted
// directly by the caller instead of by loop() processing this struct.
type queuedWork struct {
	actions []Action
	kind    workKind
}

type session struct {
	spec   runtime.Spec
	script Script

	events chan runtime.Event
	work   chan queuedWork

	once sync.Once
	done chan struct{}

	// stopped is closed by loop() itself, only once loop() has actually
	// returned — after any in-flight action has genuinely stopped running,
	// not merely after a termination signal was issued. done closing tells
	// loop() to stop; stopped closing is loop() reporting that it has.
	// Wait() blocks on stopped, not done: a Wait that returned the instant
	// Interrupt was called would be true regardless of whether the fake
	// actually killed anything in flight, which is exactly the gap
	// InterruptPreemptsInFlightWork exists to catch — it needs "the session
	// has ended" to mean the goroutine (and the process it may still be
	// running) has actually stopped, not just that a signal was accepted.
	stopped chan struct{}

	// workCtx is cancelled by Interrupt or Close (via closeWithReason) so
	// that an in-flight Exec action can actually be killed instead of run to
	// completion, and so loop() can abandon the rest of a turn's scripted
	// actions once cancellation is observed. Steer and Follow deliberately
	// never touch workCtx: the Session contract documents Steer as NOT
	// preemption ("delivered at the next turn boundary"), so only Interrupt
	// — and Close, which ends everything — may cut work off early.
	workCtx    context.Context
	cancelWork context.CancelFunc

	mu        sync.Mutex
	endReason runtime.ExitReason
	pending   runtime.Queue

	// evMu serializes emit() against closeEvents(), the one place s.events is
	// closed. emit() is no longer called only from loop(): queue() (called
	// from whatever arbitrary goroutine invokes Steer or Follow) now emits
	// the accept receipt itself, synchronously, rather than leaving that to
	// loop() — see queue()'s doc comment for why. That makes s.events a
	// channel with concurrent senders, and Go panics on a send to an already
	// closed channel regardless of which select branch would otherwise have
	// been chosen. evMu plus evClosed close that race: closeEvents holds the
	// lock across setting evClosed and closing the channel, so any emit()
	// already past its own closed-check either finishes its send under the
	// lock before the close can proceed, or (checked after acquiring the
	// lock) sees evClosed and safely no-ops instead of touching a channel
	// that might already be gone.
	evMu     sync.Mutex
	evClosed bool

	// transcript is the raw-frame record for Spec.TranscriptDir. Only loop()
	// (a single goroutine) ever writes through it, so no lock is needed.
	transcript *os.File

	start time.Time
}

func (s *session) loop() {
	// LIFO: closeTranscript runs first, then closeEvents, then
	// close(stopped) last — so a caller unblocked by stopped sees a fully
	// wound-down session: transcript flushed and closed, events channel
	// already closed too.
	defer close(s.stopped)
	defer s.closeEvents()
	defer s.closeTranscript()
	s.emit(runtime.Event{Kind: runtime.KindStarted})
	for {
		select {
		case <-s.done:
			s.emitFinal(runtime.Event{Kind: runtime.KindExited})
			return
		case qw := <-s.work:
			s.emit(runtime.Event{Kind: runtime.KindTurnBegan})
		actions:
			for _, a := range qw.actions {
				select {
				case <-s.workCtx.Done():
					// Interrupt or Close fired mid-turn: abandon the rest of
					// this turn's scripted actions instead of running them to
					// completion. This — plus Exec running under workCtx — is
					// what makes Interrupt an actual preemption primitive,
					// rather than "wait for the current turn, then stop,"
					// which is already what Steer/Follow give you for free.
					break actions
				default:
					s.run(a)
				}
			}
			// The initial OnStart turn carries no queue receipt: it was never
			// accepted via Steer or Follow, so there is nothing to drain.
			if qw.kind != kindInitial {
				q := s.dequeue(qw.kind)
				s.emit(runtime.Event{Kind: runtime.KindQueueChanged, Pending: &q})
			}
			s.emit(runtime.Event{Kind: runtime.KindTurnEnded})
			s.emit(runtime.Event{Kind: runtime.KindIdle})
		}
	}
}

func (s *session) closeTranscript() {
	if s.transcript != nil {
		s.transcript.Close()
	}
}

// closeEvents is the one place s.events is closed. See evMu's doc comment on
// session for why closing it plainly (as loop() used to, when it was the
// channel's only writer) would race a concurrent emit() from queue().
func (s *session) closeEvents() {
	s.evMu.Lock()
	defer s.evMu.Unlock()
	s.evClosed = true
	close(s.events)
}

func (s *session) run(a Action) {
	switch v := a.(type) {
	case Write:
		path := filepath.Join(s.spec.Workdir, v.Path)
		err := os.WriteFile(path, []byte(v.Content), 0o644)
		s.emit(runtime.Event{Kind: runtime.KindToolUsed, Tool: &runtime.ToolUse{
			Name: "write", Target: v.Path, Ok: err == nil,
		}})
		s.trace("write", v.Path, err == nil)
	case Exec:
		// CommandContext, not Command: workCtx is cancelled by Interrupt or
		// Close, and this is the one place a scripted action can genuinely
		// block for a long time, so it is the one place that needs killing
		// rather than just not-being-started next.
		cmd := exec.CommandContext(s.workCtx, v.Args[0], v.Args[1:]...)
		cmd.Dir = s.spec.Workdir
		cmd.Env = os.Environ()
		for k, val := range s.spec.Env {
			cmd.Env = append(cmd.Env, k+"="+val)
		}
		out, err := cmd.CombinedOutput()
		s.emit(runtime.Event{Kind: runtime.KindToolUsed, Tool: &runtime.ToolUse{
			Name: "bash", Target: v.Args[0], Detail: string(out), Ok: err == nil,
		}})
		s.trace("exec", fmt.Sprint(v.Args), err == nil)
	case Emit:
		t := v.Tool
		s.emit(runtime.Event{Kind: runtime.KindToolUsed, Tool: &t})
		s.trace("emit", t.Name+" "+t.Target, t.Ok)
	case Exit:
		s.trace("exit", "", true)
		// closeWithReason, not a direct close(s.done), so the once guard
		// still holds when the owner Closes or Interrupts the session
		// afterwards too. ExitCompleted (not ExitInterrupted, which Close and
		// Interrupt record) is what lets Wait tell a scripted natural end
		// apart from an externally terminated one.
		s.closeWithReason(runtime.ExitCompleted)
	}
}

// trace appends one line to Spec.TranscriptDir's transcript file, giving that
// field a real consumer instead of leaving it speculative. No-op when no
// TranscriptDir was requested.
func (s *session) trace(kind, detail string, ok bool) {
	if s.transcript == nil {
		return
	}
	fmt.Fprintf(s.transcript, "%s %s %s ok=%t\n", time.Now().Format(time.RFC3339Nano), kind, detail, ok)
}

func (s *session) emit(ev runtime.Event) {
	ev.At = time.Now()
	ev.Agent = s.spec.Agent
	s.evMu.Lock()
	defer s.evMu.Unlock()
	if s.evClosed {
		return
	}
	select {
	case s.events <- ev:
	case <-s.done:
	}
}

// emitFinal sends the exit-path event with a non-blocking send instead of
// racing it against s.done via the ordinary emit select. By the time loop()
// takes its <-s.done branch, done is already closed, so an ordinary select
// {send, <-done} would have both cases ready and Go would pick uniformly at
// random between them — dropping the terminal event roughly half the time
// even with an active reader. A non-blocking send delivers it whenever the
// buffer has room (with capacity 256, effectively always) while still never
// blocking or leaking loop() if the buffer is somehow full.
func (s *session) emitFinal(ev runtime.Event) {
	ev.At = time.Now()
	ev.Agent = s.spec.Agent
	select {
	case s.events <- ev:
	default:
	}
}

func (s *session) Events() <-chan runtime.Event { return s.events }

// Steer queues the steer script. Like a real adapter it does not preempt: the
// actions run after whatever turn is currently in flight.
func (s *session) Steer(ctx context.Context, text string) error {
	return s.queue(ctx, pick(s.script.OnSteer, s.script.OnFollow), text, kindSteer)
}

// Follow queues the follow-up script. A real adapter queues this behind pending
// work rather than at the next turn boundary; the fake's single work queue makes
// both arrive in send order, which is enough to observe WHICH path was taken.
func (s *session) Follow(ctx context.Context, text string) error {
	return s.queue(ctx, pick(s.script.OnFollow, s.script.OnSteer), text, kindFollow)
}

// pick returns the verb's own hook, or the other one when it is unset.
func pick(own, fallback func(string) []Action) func(string) []Action {
	if own != nil {
		return own
	}
	return fallback
}

// queue accepts a steer or follow message: it checks ctx first (an
// already-cancelled ctx must return promptly regardless of whether a hook is
// even set), then emits the accept receipt itself — synchronously, on this
// call's own goroutine — before ever handing the scripted actions to loop()
// via s.work.
//
// That ordering is deliberate and structural, not incidental. The day-0
// spike (Q3) measured a real adapter's queue_update firing immediately on
// injection, regardless of what tool call was in flight at the time; loop()
// only reaches the top of its select — and so only notices a newly queued
// item at all — between turns, so an earlier version that instead bundled
// the receipt into the queuedWork value and let loop() emit it on dequeue
// silently reintroduced exactly the coupling Steer/Follow's contract
// forbids: with a prior turn's action still running, that receipt would not
// appear until that action finished, indistinguishable from Steer having
// waited to be accepted rather than merely waited to be applied.
//
// Emitting here, before the s.work send, also gives receipt-before-drain
// ordering for free without depending on scheduling: this goroutine's send
// to s.events happens fully before its subsequent send to s.work, which in
// turn happens before loop() can receive that item and eventually emit its
// drain — so the receipt is guaranteed to reach s.events before that
// message's own drain event, and before the completion of whatever turn was
// already in flight.
func (s *session) queue(ctx context.Context, hook func(string) []Action, text string, kind workKind) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("fake: queue message: %w", ctx.Err())
	default:
	}
	if hook == nil {
		return nil
	}
	actions := hook(text)
	q := s.enqueue(kind)
	s.emit(runtime.Event{Kind: runtime.KindQueueChanged, Pending: &q})
	select {
	case s.work <- queuedWork{actions: actions, kind: kind}:
		return nil
	case <-s.done:
		// The counter was already incremented, and the receipt above already
		// emitted, but nothing will ever dequeue and drain this message now —
		// the session is ending. That stale receipt is harmless the same way
		// the leaked counter increment is: no further receipt will ever be
		// observed either way, and the caller must NOT be told "accepted" by
		// this method's return value, which is what a caller actually acts
		// on: this message will never run.
		return fmt.Errorf("fake: queue message: %w", ErrSessionEnding)
	case <-ctx.Done():
		return fmt.Errorf("fake: queue message: %w", ctx.Err())
	}
}

// enqueue records one more accepted message of kind and returns the resulting
// snapshot for the receipt event.
func (s *session) enqueue(kind workKind) runtime.Queue {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch kind {
	case kindSteer:
		s.pending.Steering++
	case kindFollow:
		s.pending.FollowUp++
	}
	return s.pending
}

// dequeue records that one message of kind has now been applied (its turn
// ran) and returns the resulting snapshot for the drain event.
func (s *session) dequeue(kind workKind) runtime.Queue {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch kind {
	case kindSteer:
		if s.pending.Steering > 0 {
			s.pending.Steering--
		}
	case kindFollow:
		if s.pending.FollowUp > 0 {
			s.pending.FollowUp--
		}
	}
	return s.pending
}

func (s *session) Interrupt(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("fake: interrupt: %w", ctx.Err())
	default:
	}
	s.closeWithReason(runtime.ExitInterrupted)
	return nil
}

// Close never blocks — closeWithReason only takes a mutex, cancels workCtx,
// and closes a channel — so there is no operation here for a ctx deadline to
// interrupt. That is why, unlike Interrupt, Close does not select on
// ctx.Done(): honouring cancellation on a call that already returns
// immediately would add a branch that could never fire, not real ctx
// support. This is deliberate, not an inconsistency with the contract's
// "every method MUST honour ctx" — there is nothing here to honour it against.
func (s *session) Close(ctx context.Context) error {
	s.closeWithReason(runtime.ExitInterrupted)
	return nil
}

// closeWithReason records why the session ended, cancels workCtx so any
// in-flight Exec is killed and loop() abandons the rest of its turn, and
// closes s.done — all exactly once. The once-guard means the FIRST caller to
// reach here wins the reason: an external Close or Interrupt racing a
// scripted Exit does not overwrite a natural end already recorded, and a
// scripted Exit that has already fired leaves a later Close's
// ExitInterrupted attempt a no-op, matching Close's documented idempotency.
func (s *session) closeWithReason(reason runtime.ExitReason) {
	s.once.Do(func() {
		s.mu.Lock()
		s.endReason = reason
		s.mu.Unlock()
		s.cancelWork()
		close(s.done)
	})
}

func (s *session) Wait(ctx context.Context) (runtime.Outcome, error) {
	select {
	case <-s.stopped:
		s.mu.Lock()
		reason := s.endReason
		s.mu.Unlock()
		return runtime.Outcome{Reason: reason, Duration: time.Since(s.start)}, nil
	case <-ctx.Done():
		return runtime.Outcome{}, fmt.Errorf("fake: wait: %w", ctx.Err())
	}
}
