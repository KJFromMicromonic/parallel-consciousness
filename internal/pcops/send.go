package pcops

import (
	"context"
	"fmt"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

// sendableIntents is the allowlist an agent may use from the shell. Control
// intents such as ready and ack belong to the gate and the runtime, not to a
// model deciding what to type.
var sendableIntents = map[string]protocol.Intent{
	"inform":   protocol.IntentInform,
	"request":  protocol.IntentRequest,
	"propose":  protocol.IntentPropose,
	"agree":    protocol.IntentAgree,
	"disagree": protocol.IntentDisagree,
	"block":    protocol.IntentBlock,
	"done":     protocol.IntentDone,
}

// Send publishes one message from one agent to another.
func Send(ctx context.Context, cfg Config, from, to, intent, text string) error {
	in, ok := sendableIntents[intent]
	if !ok {
		return fmt.Errorf("unknown intent %q", intent)
	}
	b, err := sqlite.Open(ctx, cfg.DB, sqlite.WithPollInterval(25*time.Millisecond))
	if err != nil {
		return fmt.Errorf("open bus: %w", err)
	}
	defer b.Close()

	m := protocol.New(
		protocol.Address{Agent: from},
		protocol.Address{Agent: to},
		in,
		map[string]any{"text": text},
	)
	if err := b.Publish(ctx, m); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	return nil
}
