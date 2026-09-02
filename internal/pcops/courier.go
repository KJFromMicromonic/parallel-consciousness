package pcops

import (
	"context"
	"fmt"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime"
)

// StartCourier bridges the bus into one agent session. It runs a pkg/agent.Agent
// as the session's proxy, so a spawned coding agent appears on the bus as an
// ordinary protocol participant and pkg/gate never learns a model is involved.
//
// Neither delivery path preempts. A block arriving while the agent runs its test
// suite waits for that suite to finish, which is the right trade: aborting the
// tool would destroy work the agent is seconds from reporting.
func StartCourier(ctx context.Context, b bus.Bus, name string, sess runtime.Session, topics []string) error {
	a, err := agent.New(ctx, b, name, topics)
	if err != nil {
		return fmt.Errorf("courier for %q: %w", name, err)
	}

	deliver := func(steer bool) agent.Handler {
		return func(ctx context.Context, _ *agent.Agent, m protocol.Message) *protocol.Message {
			// The sqlite bus filters senders on the topic path only, so a
			// self-addressed direct message arrives here. Drop it, or the agent
			// gets steered by its own outbound send.
			if m.From.Agent == name {
				return nil
			}
			text, _ := m.Body["text"].(string)
			if text == "" {
				return nil
			}
			if steer {
				_ = sess.Steer(ctx, text)
			} else {
				_ = sess.Follow(ctx, text)
			}
			return nil
		}
	}

	// Blocks are urgent, so they take the steer path; everything else queues
	// behind pending work.
	a.On(protocol.IntentBlock, deliver(true))
	for _, in := range []protocol.Intent{
		protocol.IntentInform, protocol.IntentRequest, protocol.IntentPropose,
		protocol.IntentAgree, protocol.IntentDisagree, protocol.IntentDone,
	} {
		a.On(in, deliver(false))
	}

	go a.Run(ctx)
	return nil
}
