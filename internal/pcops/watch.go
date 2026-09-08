package pcops

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

// maxSummary bounds a formatted line's body summary. Long test output and
// merge conflicts routinely run to thousands of characters, which makes a
// feed unreadable; --full opts out.
const maxSummary = 120

// abbrevVersion shortens a git SHA to the conventional short form. Versions
// are opaque strings by contract, so a shorter one is returned unchanged.
func abbrevVersion(v string) string {
	if len(v) > 8 {
		return v[:8]
	}
	return v
}

// FormatRecord renders one log record as one readable line.
//
// Pure by design: it takes a record and returns a string, so the format can
// be asserted exhaustively in table tests without a database, a bus, or a
// running gate anywhere in sight.
func FormatRecord(r sqlite.Record, full bool) string {
	to := r.Msg.To.Agent
	if to == "" {
		to = "#" + r.Msg.To.Topic
	}
	line := fmt.Sprintf("%s  %-11s → %-15s %-10s %s",
		r.Msg.Timestamp.Format("15:04:05"), r.Msg.From.Agent, to,
		string(r.Msg.Intent), summarise(r.Msg, full))
	return strings.TrimRight(line, " ")
}

// summarise picks the most informative field for each intent.
func summarise(m protocol.Message, full bool) string {
	switch m.Intent {
	case protocol.IntentReady:
		if v, ok := m.Body["version"].(string); ok {
			return "v=" + abbrevVersion(v)
		}
	case protocol.IntentRequest:
		if vs := versionsFromBody(m.Body["versions"]); len(vs) > 0 {
			names := make([]string, 0, len(vs))
			for n := range vs {
				names = append(names, n)
			}
			sort.Strings(names) // stable output; map order is not
			parts := make([]string, 0, len(names))
			for _, n := range names {
				parts = append(parts, n+"="+abbrevVersion(vs[n]))
			}
			return strings.Join(parts, " ")
		}
	case protocol.IntentAck:
		if out := outstandingFromBody(m.Body["outstanding"]); len(out) > 0 {
			return "waiting on " + strings.Join(out, ", ")
		}
	case protocol.IntentNack:
		// Unlike the Ack above, a Nack carries "testing" (the version set the
		// in-flight round is actually testing), not "outstanding" — see
		// gate.go's onReady: open leaves gs.ready intact for a round's whole
		// lifetime, only resolve clears it, so outstandingFor(gs) always
		// returns an empty slice on this path. Rendering with describeVersions
		// is deliberate: it is the exact same rendering pcops.Submit's own
		// stderr line uses for this field, so the CLI and the log read
		// identically for the same event.
		if testing := versionsFromBody(m.Body["testing"]); len(testing) > 0 {
			return "mid-round, testing " + describeVersions(testing)
		}
	}
	// Everything else: whichever human-readable field is present.
	for _, k := range []string{"text", "detail"} {
		if s, ok := m.Body[k].(string); ok && s != "" {
			return clip(s, full)
		}
	}
	return ""
}

func clip(s string, full bool) string {
	s = strings.ReplaceAll(strings.TrimSpace(s), "\n", " ")
	if full || len(s) <= maxSummary {
		return s
	}
	return s[:maxSummary] + "…"
}

// Watch renders the gate's activity feed to out: everything already recorded,
// then — when follow is set — everything that arrives afterwards.
//
// It reads the durable log directly rather than subscribing, for two reasons.
// A new subscriber starts at the log's HEAD, so it would see no history at
// all; and Subscribe filters to messages addressed to the subscriber, so an
// observer would miss the routed failure blocks and agent-to-agent traffic
// that are the most useful things to watch.
//
// gateID filters to one gate when non-empty; a message belongs to a gate if
// it rides that gate's topic or names it in its body.
func Watch(ctx context.Context, cfg Config, gateID string, full, follow bool, out io.Writer) error {
	// Tail's output channel closes for three separate reasons: ctx ending, the
	// bus closing, or a mid-stream History error inside its poll loop — and
	// only the first two leave anything in ctx.Err(). The third reaches the
	// caller only through this hook, so it is captured here and checked after
	// the follow loop, ahead of ctx.Err(): otherwise a live database failure
	// closes the channel exactly like a clean stop, and Watch would report
	// success for a feed that silently died. Reading tailErr after the range
	// over ch ends is race-free — Tail's goroutine writes it (if at all)
	// strictly before closing the channel via its deferred close(out), and a
	// channel close is a happens-before edge for every receive that observes it.
	var tailErr error
	b, err := sqlite.Open(ctx, cfg.DB,
		sqlite.WithPollInterval(250*time.Millisecond),
		sqlite.WithErrorHook(func(err error) { tailErr = err }),
	)
	if err != nil {
		return fmt.Errorf("open bus: %w", err)
	}
	defer b.Close()

	write := func(r sqlite.Record) error {
		if !matchesGate(r, gateID) {
			return nil
		}
		_, err := fmt.Fprintln(out, FormatRecord(r, full))
		return err
	}

	if !follow {
		recs, err := b.History(ctx, 0)
		if err != nil {
			return fmt.Errorf("history: %w", err)
		}
		for _, r := range recs {
			if err := write(r); err != nil {
				return err
			}
		}
		return nil
	}

	ch, err := b.Tail(ctx, 0)
	if err != nil {
		return fmt.Errorf("tail: %w", err)
	}
	for r := range ch {
		if err := write(r); err != nil {
			return err
		}
	}
	if tailErr != nil {
		return fmt.Errorf("watch: %w", tailErr)
	}
	return ctx.Err()
}

func matchesGate(r sqlite.Record, gateID string) bool {
	if gateID == "" {
		return true
	}
	if r.Msg.To.Topic == gate.Topic(gateID) {
		return true
	}
	id, _ := r.Msg.Body["gate"].(string)
	return id == gateID
}

// versionsFromBody decodes a participant→version map that has crossed the
// bus. Over pkg/bus/sqlite a body is JSON, so a map[string]string is
// delivered as map[string]any; pkg/gate carries its own copy of this for
// the same reason. Anything that is not a string is skipped rather than
// guessed at.
func versionsFromBody(v any) map[string]string {
	switch m := v.(type) {
	case map[string]string:
		return m
	case map[string]any:
		out := make(map[string]string, len(m))
		for k, raw := range m {
			if s, ok := raw.(string); ok {
				out[k] = s
			}
		}
		return out
	}
	return nil
}
