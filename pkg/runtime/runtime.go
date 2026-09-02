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
type Session interface {
	Events() <-chan Event
	Steer(ctx context.Context, s string) error
	Follow(ctx context.Context, s string) error
	Interrupt(ctx context.Context) error
	Close(ctx context.Context) error
	Wait(ctx context.Context) (Outcome, error)
}

// EventKind is the control plane's vocabulary for what an agent is doing. It is
// deliberately smaller than any adapter's native event set.
type EventKind string

const (
	KindStarted      EventKind = "started"
	KindTurnBegan    EventKind = "turn_began"
	KindTurnEnded    EventKind = "turn_ended"
	KindToolUsed     EventKind = "tool_used"
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
