// Package agent provides the runtime that turns a Bus connection into a
// coworker-like participant: it runs a conversation loop, dispatches messages
// by intent, auto-acks where the protocol expects it, and exposes cooperative
// interruption so an agent deep in a task can still notice an urgent ping at a
// safe boundary.
package agent

import (
	"context"
	"log"
	"sync"

	"github.com/yourname/conclave/pkg/bus"
	"github.com/yourname/conclave/pkg/protocol"
)

// Handler reacts to one inbound message. Returning a non-nil message sends it
// (typically built with msg.Reply(...)). Return nil to stay silent.
type Handler func(ctx context.Context, a *Agent, msg protocol.Message) *protocol.Message

// Agent is a single participant on the bus.
type Agent struct {
	Name   string
	bus    bus.Bus
	in     <-chan protocol.Message
	topics []string

	mu       sync.Mutex
	handlers map[protocol.Intent]Handler

	// urgent carries messages flagged for cooperative interruption so a busy
	// agent can poll Interrupted() at a safe point mid-task.
	urgent chan protocol.Message
}

// New connects an agent to the bus and subscribes to its inbox plus topics.
func New(ctx context.Context, b bus.Bus, name string, topics []string) (*Agent, error) {
	in, err := b.Subscribe(ctx, name, topics)
	if err != nil {
		return nil, err
	}
	return &Agent{
		Name:     name,
		bus:      b,
		in:       in,
		topics:   topics,
		handlers: make(map[protocol.Intent]Handler),
		urgent:   make(chan protocol.Message, 8),
	}, nil
}

// On registers a handler for an intent.
func (a *Agent) On(intent protocol.Intent, h Handler) {
	a.mu.Lock()
	a.handlers[intent] = h
	a.mu.Unlock()
}

// Send publishes a message and logs the turn for demo visibility.
func (a *Agent) Send(ctx context.Context, m protocol.Message) error {
	logTurn(m)
	return a.bus.Publish(ctx, m)
}

// Interrupted returns any message that arrived flagged urgent while the agent
// was working. Agents call this at safe checkpoints — cooperative, not
// preemptive, which is far easier to reason about than interrupting a tool
// call mid-flight.
func (a *Agent) Interrupted() (protocol.Message, bool) {
	select {
	case m := <-a.urgent:
		return m, true
	default:
		return protocol.Message{}, false
	}
}

// Run is the conversation loop. It dispatches each inbound message to the
// registered handler, auto-acking intents that expect a response when no
// handler chooses to reply, so a peer is never left waiting silently.
func (a *Agent) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-a.in:
			if !ok {
				return
			}
			a.dispatch(ctx, msg)
		}
	}
}

func (a *Agent) dispatch(ctx context.Context, msg protocol.Message) {
	// Route urgent blocks into the interruption channel as well, so a busy
	// agent can react at its next checkpoint.
	if msg.Intent == protocol.IntentBlock {
		select {
		case a.urgent <- msg:
		default:
		}
	}

	a.mu.Lock()
	h := a.handlers[msg.Intent]
	a.mu.Unlock()

	var reply *protocol.Message
	if h != nil {
		reply = h(ctx, a, msg)
	}

	if reply != nil {
		_ = a.Send(ctx, *reply)
		return
	}

	// No explicit reply but the intent expected one: auto-ack so the sender's
	// deadline logic resolves instead of escalating on a false timeout.
	if msg.Intent.WantsResponse() && msg.From.Agent != "" {
		ack := msg.Reply(protocol.Address{Agent: a.Name}, protocol.IntentAck, nil)
		_ = a.Send(ctx, ack)
	}
}

func logTurn(m protocol.Message) {
	to := m.To.Agent
	if to == "" {
		to = "#" + m.To.Topic
	}
	log.Printf("  %-9s --%-9s--> %-9s  %v", m.From.Agent, m.Intent, to, compact(m.Body))
}

func compact(b map[string]any) string {
	if len(b) == 0 {
		return ""
	}
	if t, ok := b["text"]; ok {
		return "“" + toStr(t) + "”"
	}
	return "{…}"
}

func toStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
