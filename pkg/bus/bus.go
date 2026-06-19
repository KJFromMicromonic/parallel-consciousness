// Package bus defines the pluggable transport abstraction and ships an
// in-memory implementation as the zero-dependency default.
//
// The contract is deliberately tiny: Publish a message, Subscribe to receive
// the ones addressed to you (directly or via a topic you've joined). A
// Redpanda/Kafka adapter implements the same interface so swapping transports
// never touches agent code.
package bus

import (
	"context"
	"sync"

	"github.com/yourname/conclave/pkg/protocol"
)

// Bus is the transport contract. Implementations: InMemory (default),
// Redpanda (adapter, see pkg/bus/redpanda).
type Bus interface {
	// Publish delivers a message. Direct messages route by To.Agent;
	// topic messages fan out to all subscribers of To.Topic.
	Publish(ctx context.Context, m protocol.Message) error

	// Subscribe registers an agent and the topics it listens on, returning a
	// channel of inbound messages. The channel closes when ctx is cancelled.
	Subscribe(ctx context.Context, agent string, topics []string) (<-chan protocol.Message, error)
}

// InMemory is a single-process bus backed by goroutines and channels. It's the
// default so `go run` works with zero infrastructure. Semantics intentionally
// mirror what the Kafka adapter guarantees: ordered per-recipient delivery.
type InMemory struct {
	mu     sync.RWMutex
	agents map[string]chan protocol.Message // agent -> inbox
	topics map[string]map[string]struct{}   // topic -> set of subscriber agents
	buf    int
}

// NewInMemory creates an in-memory bus. buf is the per-agent inbox buffer.
func NewInMemory(buf int) *InMemory {
	if buf <= 0 {
		buf = 64
	}
	return &InMemory{
		agents: make(map[string]chan protocol.Message),
		topics: make(map[string]map[string]struct{}),
		buf:    buf,
	}
}

func (b *InMemory) Subscribe(ctx context.Context, agent string, topics []string) (<-chan protocol.Message, error) {
	b.mu.Lock()
	ch := make(chan protocol.Message, b.buf)
	b.agents[agent] = ch
	for _, t := range topics {
		if b.topics[t] == nil {
			b.topics[t] = make(map[string]struct{})
		}
		b.topics[t][agent] = struct{}{}
	}
	b.mu.Unlock()

	// Tear down on context cancellation so demos exit cleanly.
	go func() {
		<-ctx.Done()
		b.mu.Lock()
		delete(b.agents, agent)
		for _, subs := range b.topics {
			delete(subs, agent)
		}
		close(ch)
		b.mu.Unlock()
	}()

	return ch, nil
}

func (b *InMemory) Publish(_ context.Context, m protocol.Message) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Direct delivery.
	if m.To.Agent != "" {
		if ch, ok := b.agents[m.To.Agent]; ok {
			b.deliver(ch, m)
		}
		return nil
	}

	// Topic fan-out. Don't echo a broadcast back to its sender.
	if m.To.Topic != "" {
		for agent := range b.topics[m.To.Topic] {
			if agent == m.From.Agent {
				continue
			}
			if ch, ok := b.agents[agent]; ok {
				b.deliver(ch, m)
			}
		}
	}
	return nil
}

// deliver is non-blocking: a full inbox drops rather than stalling the whole
// bus. The Kafka adapter relies on broker-side buffering instead; this is the
// in-memory tradeoff and is documented for adopters.
func (b *InMemory) deliver(ch chan protocol.Message, m protocol.Message) {
	select {
	case ch <- m:
	default:
		// inbox full; dropped. Real deployments use the Kafka adapter.
	}
}
