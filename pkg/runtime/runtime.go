// Package runtime is the harness-neutral contract for launching and supervising
// a coding agent. It imports only the standard library, and deliberately names
// nothing after any particular harness: adapters live behind this interface so
// the control plane never grows a dependency on one vendor's event model.
package runtime

import (
	"context"
	"time"
)

// Runtime launches agent sessions.
type Runtime interface {
	// Name identifies the adapter, e.g. "pi". Recorded in evidence.
	Name() string
	Start(ctx context.Context, spec Spec) (Session, error)
}

// Spec describes one agent session to launch.
type Spec struct {
	Agent   string            // control-plane-assigned identity
	Workdir string            // the lease path
	Role    string            // role framing, prefixed onto the task
	Task    string            // initial instruction
	Env     map[string]string // e.g. PC_AGENT, PC_DB, provider credentials
	Model   string            // opaque; the adapter maps it
	Budget  Budget

	// TranscriptDir is the directory where the adapter writes its raw native
	// frames. Event deliberately carries no raw adapter payload — that would
	// be the seam through which harness specifics leak into callers — so the
	// transcript is where debuggability actually lives, and without a field
	// naming it a caller has no way to find it. An adapter that cannot honour
	// the requested directory MUST fail at Start rather than silently write
	// somewhere else: a debugging aid that might not be where it claims is
	// worse than an explicit error. An empty TranscriptDir means the caller
	// does not want a transcript.
	TranscriptDir string
}

// Budget bounds a session. Wall is enforced by the caller; Tokens is reported
// only, because enforcement needs a policy decision this layer should not make.
type Budget struct {
	Wall   time.Duration
	Tokens int64 // 0 means unbounded
}

// Session is one running agent.
//
// Steer is NOT preemption. Adapters deliver it at the next turn boundary, so a
// message sent while a tool call is in flight waits for that call to finish.
// Interrupt is the only verb that preempts in-flight work.
//
// Every method MUST honour context cancellation and return promptly with a
// non-nil error rather than blocking. Every method here already takes a ctx;
// a contract where it is decorative — accepted but never selected on — is
// worse than one that never took it at all, because a caller who times out or
// shuts down has no way to trust that the call will ever return.
type Session interface {
	Events() <-chan Event
	Steer(ctx context.Context, s string) error
	Follow(ctx context.Context, s string) error
	Interrupt(ctx context.Context) error
	Close(ctx context.Context) error

	// Wait returns once the session has ended — by natural completion,
	// budget exhaustion, an interrupt, or Close — whichever happens first.
	// Outcome.Reason says which. Wait does not itself end anything; it only
	// reports an ending that happens by one of those means.
	Wait(ctx context.Context) (Outcome, error)
}

// EventKind is the control plane's vocabulary for what an agent is doing. It is
// deliberately smaller than any adapter's native event set.
type EventKind string

const (
	KindStarted   EventKind = "started"
	KindTurnBegan EventKind = "turn_began"
	KindTurnEnded EventKind = "turn_ended"
	KindToolUsed  EventKind = "tool_used"
	// KindQueueChanged is REQUIRED, not optional: an adapter MUST emit it
	// when it accepts a steer or a follow, and again when the queue drains,
	// with Event.Pending set both times. Queue{0,0} is a valid, honest
	// answer for a harness with no introspectable queue — the requirement is
	// the receipt, not a non-zero count. This exists because the day-0 spike
	// (Q3) measured delivery and action diverging by as much as the length
	// of an in-flight tool call (23s behind a sleep 25 in the measured
	// case); this event is the only thing that makes that gap observable to
	// the control plane instead of a silent black box between "accepted"
	// and "acted on."
	KindQueueChanged EventKind = "queue_changed"
	KindIdle         EventKind = "idle"
	KindErrored      EventKind = "errored"
	KindExited       EventKind = "exited"
)

// Event carries no adapter-native payload on purpose: a raw field would be the
// seam through which harness specifics reach callers, and no import test would
// catch it. Adapters write raw frames to a transcript file instead.
type Event struct {
	Kind    EventKind
	At      time.Time
	Agent   string
	Tool    *ToolUse // set when Kind == KindToolUsed
	Pending *Queue   // set when Kind == KindQueueChanged
	Err     string
}

// ToolUse is observed evidence: what the agent actually did, rather than what
// it reported doing.
type ToolUse struct {
	Name   string // read, write, edit, bash, grep, find, ls
	Target string // path or command
	Detail string
	Ok     bool
}

// Queue reports what an adapter has accepted but not yet applied — the receipt
// that separates "delivered" from "acted on".
type Queue struct {
	Steering int
	FollowUp int
}

// ExitReason explains why a session ended.
type ExitReason string

const (
	ExitCompleted   ExitReason = "completed"
	ExitInterrupted ExitReason = "interrupted"
	ExitBudget      ExitReason = "budget"
	ExitError       ExitReason = "error"
)

// Outcome summarises a finished session.
type Outcome struct {
	Reason   ExitReason
	Tokens   int64
	Duration time.Duration
}
