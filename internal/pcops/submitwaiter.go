package pcops

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

// submitWaiter owns the state Submit's message handlers and its attempt loop
// share: the round boundary, and the four channels the handlers signal on.
//
// It exists because two invariants of that shared state were previously
// maintained by repetition, and each was got wrong once:
//
//  1. Every handler must fence an arriving message against the round
//     boundary, or a reply replayed from a lagging cursor answers an attempt
//     it was never about. Three handlers carried three identical copies of
//     that comparison; the third was written without it. Now they call fresh.
//  2. Every attempt must drain cross-attempt state before declaring
//     readiness, or a signal buffered by a previous attempt answers this one.
//     Three near-identical drains; the first version drained one of three.
//     Now declareReady does all four.
//
// What deliberately stays outside: the bus, the agent, the config, the gate id
// and the version. This owns the round boundary and the four channels, and
// nothing whose lifetime differs from those.
type submitWaiter struct {
	mu sync.Mutex
	// readyAt is written once per attempt by declareReady, called from
	// Submit's own goroutine, and read by fresh from the agent's dispatch
	// goroutine — which is why this field is guarded by mu while the four
	// channels below it are not: a channel is already safe for concurrent
	// use without one.
	readyAt time.Time

	// All four are buffered 1 and all four are written by non-blocking sends:
	// a handler runs on the agent's dispatch goroutine and must never block
	// it on a send nobody is reading yet.
	verdicts chan gate.Verdict
	acked    chan []string
	nacked   chan map[string]string
	declined chan struct{}
}

func newSubmitWaiter() *submitWaiter {
	return &submitWaiter{
		verdicts: make(chan gate.Verdict, 1),
		acked:    make(chan []string, 1),
		nacked:   make(chan map[string]string, 1),
		declined: make(chan struct{}, 1),
	}
}

// fresh reports whether m belongs to the current attempt rather than having
// been replayed from the durable cursor.
//
// pkg/bus/sqlite resumes a subscription from the STORED cursor whenever a row
// exists for this agent name, and it saves that cursor only after a batch has
// been handed to the channel — so a submit process that exits or is killed
// routinely leaves the tail of its own inbox unread, for the NEXT submit under
// the same name to receive as if it were fresh. Two paths reach that state: a
// passing verdict routes no blocks, so nothing advances a courier past its
// Inform; and an agent with no in-process courier only ever has short-lived
// submit processes, whose deferred bus Close makes the poller skip saving the
// cursor entirely.
//
// An Ack is a direct reply, exactly like a Nack, so it sits on the same
// replay path: a submit process that was killed after being acked (or
// nacked) but before exiting cleanly leaves that reply sitting unread, and
// the next submit under this agent name is handed it straight back as if it
// were fresh. An unfenced stale Ack would satisfy the acknowledgement wait
// for a round that already ended, then block on the verdict wait until ctx
// expires — the same ErrNoVerdict-instead-of-ErrNotAcknowledged
// misdiagnosis Ruling 1 exists to prevent, just arriving from the Ack
// handler instead of the timer.
//
// A stale Nack is worse than a stale Ack: it is just as exposed to a lagging
// cursor as the Inform and Ack cases above (a submit killed right after
// being nacked — F2 measured models doing exactly this — leaves that Nack
// unread for the next submit under the same name to receive as its first
// message), but an unfenced stale Nack would print a false "gate is
// mid-round" line and enter waitForRoundToResolve for a round that is not
// this attempt's round at all — and that wait has no AckTimeout bound, so
// with no coordinator left to signal declined it would block the full
// SubmitTimeout instead of failing fast with ErrNotAcknowledged.
//
// protocol.New stamps Timestamp, so the round boundary is simply time. Every
// handler must call this — the whole point of the method is that a fourth
// handler cannot forget a rule it has to invoke.
func (w *submitWaiter) fresh(m protocol.Message) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return !m.Timestamp.Before(w.readyAt)
}

// stampReadyAt moves the round boundary. Exported within the package only for
// declareReady and the waiter's own tests; callers in Submit go through
// declareReady, which is what keeps the stamp/drain/publish order intact.
func (w *submitWaiter) stampReadyAt(t time.Time) {
	w.mu.Lock()
	w.readyAt = t
	w.mu.Unlock()
}

// declareReady stamps a new round boundary, drains every cross-attempt
// channel, and publishes readiness — in that order, which is load-bearing.
//
// Stamping BEFORE draining means any handler that runs from here on fences
// correctly on arrival, so the drain has only to clear what was buffered
// before the stamp. Draining BEFORE publishing means no reply to THIS attempt
// can exist yet, so the drain cannot discard a legitimate signal.
//
// Reverse either and a bug returns. Drain-then-stamp leaves a window in which
// a pre-stamp message survives in a buffer and is then read as this attempt's
// answer. Publish-then-drain throws away this attempt's own acknowledgement.
//
// The publish lives inside this method rather than beside its call site
// precisely so that ordering is unreachable from outside: a caller cannot get
// the order wrong because it no longer has an order to get right.
//
// Draining verdicts is a deliberate tightening beyond the three channels the
// pre-refactor loop drained (declined, acked, nacked): every signal sitting
// in verdicts at the instant of the stamp was necessarily published before
// the new boundary, so this fourth drain makes that channel obey on
// re-declaration exactly the rule fresh already applies to it on arrival.
//
// declined, acked and nacked are all cross-attempt state too: any of the
// three can hold a stale signal from a previous attempt's reply, buffered
// before this attempt's own Ready is even declared.
// TestSubmitDeclinesAVerdictThatDidNotIncludeIt is exactly this for
// declined: a foreign-version verdict can leave a stale signal sitting there
// long before any Nack exists — a fence on arrival does not drain what an
// earlier attempt already buffered, which is what this drain is for.
func (w *submitWaiter) declareReady(ctx context.Context, a *agent.Agent, gateID, version string) error {
	w.stampReadyAt(time.Now())

	// Non-blocking drains: an empty channel must not block the attempt.
	select {
	case <-w.verdicts:
	default:
	}
	select {
	case <-w.acked:
	default:
	}
	select {
	case <-w.nacked:
	default:
	}
	select {
	case <-w.declined:
	default:
	}

	if err := gate.Ready(ctx, a, gateID, version); err != nil {
		return fmt.Errorf("declare ready: %w", err)
	}
	return nil
}

// The four offer* methods are the handlers' only way to signal. Each is a
// non-blocking send for the reason given on the struct's channel fields.

func (w *submitWaiter) offerVerdict(v gate.Verdict) {
	select {
	case w.verdicts <- v:
	default:
	}
}

func (w *submitWaiter) offerAck(outstanding []string) {
	select {
	case w.acked <- outstanding:
	default:
	}
}

func (w *submitWaiter) offerNack(testing map[string]string) {
	select {
	case w.nacked <- testing:
	default:
	}
}

func (w *submitWaiter) offerDeclined() {
	select {
	case w.declined <- struct{}{}:
	default:
	}
}

// roundResolution reports how waitForRoundToResolve concluded. resolved is
// false only when ctx ended before the in-flight round did. hasVerdict is
// set when the round resolved WITH a verdict that answers this attempt
// directly (the race-won case) — the caller must return it as-is rather than
// looping back to re-declare readiness, which would publish a second,
// spurious gate.Ready at a version the round already resolved.
type roundResolution struct {
	verdict    gate.Verdict
	hasVerdict bool
	resolved   bool
}

// waitForRoundToResolve blocks until the in-flight round that displaced our
// readiness has resolved. Two things can report that: declined, signalled by
// a verdict for this gate that did not test our version (proof the round is
// done, with nothing further to hand back); or verdicts, when the verdict
// that resolves the round happens to be OUR OWN — a race this attempt wins
// outright, since there is nothing left to wait for.
//
// That verdict is returned to the caller rather than re-buffered and left for
// the loop's next iteration: re-declaring readiness after a verdict already
// answered this attempt would publish a redundant gate.Ready at the same
// version, and in gate.go that hits the F2 cache and replays another resolve
// — another broadcast, another block fanout on failure, another OnVerdict,
// and (via pcops.Run's round counter) a run that can be failed a round early
// by a purely spurious cache replay.
//
// verdicts is checked first, on its own, before the select that also watches
// declined: both can be buffered at once (the round that displaced us
// resolved with a verdict that also happens to test our own version), and
// Go's select picks uniformly among ready cases when more than one is ready.
// Without this non-blocking first look, the correct answer — a verdict that
// tested our version is a final answer, where declined only reports that the
// displacing round finished — would be discarded half the time, sending the
// attempt back to declareReady to re-declare readiness for a round that had
// already answered it, and losing that verdict to the same F2 cache-replay
// consequence chain described above.
func (w *submitWaiter) waitForRoundToResolve(ctx context.Context) roundResolution {
	select {
	case v := <-w.verdicts:
		return roundResolution{verdict: v, hasVerdict: true, resolved: true}
	default:
	}
	select {
	case <-w.declined:
		return roundResolution{resolved: true}
	case v := <-w.verdicts:
		return roundResolution{verdict: v, hasVerdict: true, resolved: true}
	case <-ctx.Done():
		return roundResolution{}
	}
}
