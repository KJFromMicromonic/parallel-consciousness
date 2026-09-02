// Package fake is a scripted runtime.Runtime for tests. It exists so the whole
// coordination loop can be exercised deterministically in CI without an API
// key, a network, or a single token of spend.
package fake

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime"
)

// Action is one scripted thing a fake agent does.
type Action interface{ isAction() }

// Write writes Content to Path, relative to the session's workdir.
type Write struct{ Path, Content string }

// Exec runs a command in the session's workdir with the session's env. This is
// how a fake agent calls `pc submit`.
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

	s := &session{
		spec:       spec,
		script:     r.scripts[spec.Agent],
		events:     make(chan runtime.Event, 256),
		work:       make(chan queuedWork, 8),
		receipts:   make(chan runtime.Event, 8),
		done:       make(chan struct{}),
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
// (if any) it came from so the loop can emit an accurate drain receipt.
type queuedWork struct {
	actions []Action
	kind    workKind
}

type session struct {
	spec   runtime.Spec
	script Script

	events chan runtime.Event
	work   chan queuedWork

	// receipts carries KindQueueChanged events from Steer/Follow — called
	// from the caller's own goroutine — into loop(), which is the only
	// goroutine allowed to call emit(). emit() assumes it is never called
	// concurrently with loop()'s own shutdown; routing external events
	// through this channel instead of emitting them directly from Steer/
	// Follow's goroutine preserves that invariant rather than reintroducing
	// the send-on-a-closing-channel race emitFinal's doc comment already
	// warns about.
	receipts chan runtime.Event

	once sync.Once
	done chan struct{}

	mu        sync.Mutex
	endReason runtime.ExitReason
	pending   runtime.Queue

	// transcript is the raw-frame record for Spec.TranscriptDir. Only loop()
	// (a single goroutine) ever writes through it, so no lock is needed.
	transcript *os.File

	start time.Time
}

func (s *session) loop() {
	defer close(s.events)
	defer s.closeTranscript()
	s.emit(runtime.Event{Kind: runtime.KindStarted})
	for {
		select {
		case <-s.done:
			s.emitFinal(runtime.Event{Kind: runtime.KindExited})
			return
		case ev := <-s.receipts:
			s.emit(ev)
		case qw := <-s.work:
			s.emit(runtime.Event{Kind: runtime.KindTurnBegan})
			for _, a := range qw.actions {
				s.run(a)
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
		cmd := exec.Command(v.Args[0], v.Args[1:]...)
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
// even set), then — mirroring the day-0 spike's queue_update, which fired
// immediately on injection — hands the KindQueueChanged receipt to loop() via
// s.receipts before handing the scripted actions to loop() via s.work. The
// receipt travels through a channel rather than a direct s.emit call because
// this method runs on the CALLER's goroutine, and only loop() may call emit
// safely (see the receipts field doc).
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
	select {
	case s.receipts <- runtime.Event{Kind: runtime.KindQueueChanged, Pending: &q}:
	case <-s.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("fake: queue message: %w", ctx.Err())
	}
	select {
	case s.work <- queuedWork{actions: actions, kind: kind}:
		return nil
	case <-s.done:
		return nil
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

func (s *session) Close(ctx context.Context) error {
	s.closeWithReason(runtime.ExitInterrupted)
	return nil
}

// closeWithReason records why the session ended and closes s.done exactly
// once. The once-guard means the FIRST caller to reach here wins the reason:
// an external Close or Interrupt racing a scripted Exit does not overwrite a
// natural end already recorded, and a scripted Exit that has already fired
// leaves a later Close's ExitInterrupted attempt a no-op, matching Close's
// documented idempotency.
func (s *session) closeWithReason(reason runtime.ExitReason) {
	s.once.Do(func() {
		s.mu.Lock()
		s.endReason = reason
		s.mu.Unlock()
		close(s.done)
	})
}

func (s *session) Wait(ctx context.Context) (runtime.Outcome, error) {
	select {
	case <-s.done:
		s.mu.Lock()
		reason := s.endReason
		s.mu.Unlock()
		return runtime.Outcome{Reason: reason, Duration: time.Since(s.start)}, nil
	case <-ctx.Done():
		return runtime.Outcome{}, fmt.Errorf("fake: wait: %w", ctx.Err())
	}
}
