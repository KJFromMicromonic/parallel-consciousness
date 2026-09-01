// Package fake is a scripted runtime.Runtime for tests. It exists so the whole
// coordination loop can be exercised deterministically in CI without an API
// key, a network, or a single token of spend.
package fake

import (
	"context"
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

func (Write) isAction() {}
func (Exec) isAction()  {}
func (Emit) isAction()  {}

// Script is one agent's behaviour: what it does on start, and what it does when
// steered. OnSteer may be nil.
type Script struct {
	OnStart []Action
	OnSteer func(text string) []Action
}

// Runtime is the fake. Scripts are keyed by Spec.Agent.
type Runtime struct{ scripts map[string]Script }

func New(scripts map[string]Script) *Runtime { return &Runtime{scripts: scripts} }

func (r *Runtime) Name() string { return "fake" }

func (r *Runtime) Start(ctx context.Context, spec runtime.Spec) (runtime.Session, error) {
	s := &session{
		spec:   spec,
		script: r.scripts[spec.Agent],
		events: make(chan runtime.Event, 256),
		work:   make(chan []Action, 8),
		done:   make(chan struct{}),
		start:  time.Now(),
	}
	go s.loop()
	s.work <- s.script.OnStart
	return s, nil
}

type session struct {
	spec   runtime.Spec
	script Script

	events chan runtime.Event
	work   chan []Action

	once sync.Once
	done chan struct{}

	start time.Time
}

func (s *session) loop() {
	defer close(s.events)
	s.emit(runtime.Event{Kind: runtime.KindStarted})
	for {
		select {
		case <-s.done:
			s.emitFinal(runtime.Event{Kind: runtime.KindExited})
			return
		case actions := <-s.work:
			s.emit(runtime.Event{Kind: runtime.KindTurnBegan})
			for _, a := range actions {
				s.run(a)
			}
			s.emit(runtime.Event{Kind: runtime.KindTurnEnded})
			s.emit(runtime.Event{Kind: runtime.KindIdle})
		}
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
	case Emit:
		t := v.Tool
		s.emit(runtime.Event{Kind: runtime.KindToolUsed, Tool: &t})
	}
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
	if s.script.OnSteer == nil {
		return nil
	}
	select {
	case s.work <- s.script.OnSteer(text):
	case <-s.done:
	}
	return nil
}

func (s *session) Follow(ctx context.Context, text string) error { return s.Steer(ctx, text) }

func (s *session) Interrupt(ctx context.Context) error { return nil }

func (s *session) Close(ctx context.Context) error {
	s.once.Do(func() { close(s.done) })
	return nil
}

func (s *session) Wait(ctx context.Context) (runtime.Outcome, error) {
	<-s.done
	return runtime.Outcome{Reason: runtime.ExitCompleted, Duration: time.Since(s.start)}, nil
}
