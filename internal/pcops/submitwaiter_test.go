package pcops

import (
	"context"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

// The round boundary is the only thing standing between a replayed message
// from a previous attempt and this attempt treating it as an answer. Three
// handlers used to carry three copies of this comparison and one of them was
// written without it; this pins the rule at the one place it now lives.
func TestSubmitWaiterFreshFencesMessagesFromBeforeTheBoundary(t *testing.T) {
	w := newSubmitWaiter()

	stale := protocol.New(protocol.Address{Agent: "coordinator"},
		protocol.Address{Agent: "billing"}, protocol.IntentAck, map[string]any{"gate": "g"})

	// Anything at all, published before a boundary exists, is fresh: the zero
	// Time predates every message, which is what makes the very first attempt
	// accept its own reply.
	if !w.fresh(stale) {
		t.Fatal("fresh() rejected a message before any boundary was stamped; the first attempt would never see its own reply")
	}

	// The boundary and the "current" message are stamped as explicit offsets
	// from stale's own timestamp rather than back-to-back time.Now() calls:
	// on this machine consecutive time.Now() reads tie (measured ~95% of the
	// time), which would make this assertion flake on nothing more than
	// clock granularity rather than on fresh()'s actual boundary logic.
	boundary := stale.Timestamp.Add(time.Second)
	w.stampReadyAt(boundary)

	if w.fresh(stale) {
		t.Fatal("fresh() accepted a message stamped before the current round boundary; a replayed reply from a previous attempt would be treated as this attempt's answer")
	}

	current := protocol.New(protocol.Address{Agent: "coordinator"},
		protocol.Address{Agent: "billing"}, protocol.IntentAck, map[string]any{"gate": "g"})
	current.Timestamp = boundary.Add(time.Second)
	if !w.fresh(current) {
		t.Fatal("fresh() rejected a message stamped after the boundary; this attempt would ignore its own reply")
	}
}

// Each of the four channels holds one buffered signal, and any of them can be
// left full by a previous attempt. A fence on arrival does not empty a buffer
// something already got into, so declaring a fresh readiness has to drain all
// four — the first version of this drained one of three.
func TestSubmitWaiterDeclareReadyDrainsEveryChannel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	w := newSubmitWaiter()

	// Fill all four, as a previous attempt's replies would have.
	w.offerVerdict(gateVerdictForTest())
	w.offerAck([]string{"gateway"})
	w.offerNack(map[string]string{"gateway": "v1"})
	w.offerDeclined()

	b := bus.NewInMemory(8)
	a, err := agent.New(ctx, b, "billing", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.declareReady(ctx, a, "g", "v2"); err != nil {
		t.Fatalf("declareReady: %v", err)
	}

	for _, tc := range []struct {
		name  string
		empty bool
	}{
		{"verdicts", len(w.verdicts) == 0},
		{"acked", len(w.acked) == 0},
		{"nacked", len(w.nacked) == 0},
		{"declined", len(w.declined) == 0},
	} {
		if !tc.empty {
			t.Errorf("%s still held a buffered signal after declareReady; a stale reply from a previous attempt would answer this one", tc.name)
		}
	}
}

func gateVerdictForTest() gate.Verdict {
	return gate.Verdict{GateID: "g", Passed: true, Versions: map[string]string{"billing": "v1"}}
}
