package pcops

import (
	"testing"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
)

// fails builds the failing verdict a round over these versions produces. Only
// Versions is read by fixCycle, but the rest is filled in so a reader is not
// left wondering whether Passed matters here (Run never offers a passing
// verdict to the counter: it returns on one).
func fails(versions map[string]string) gate.Verdict {
	return gate.Verdict{GateID: "g", Passed: false, Detail: "spanning test failed", Versions: versions}
}

// count replays a whole sequence of failing verdicts through one fixCycle and
// reports which of them advanced the cap, so each case below reads as the
// round-by-round trace it is describing.
func count(required []string, rounds ...map[string]string) []bool {
	fc := newFixCycle(required)
	got := make([]bool, 0, len(rounds))
	for _, versions := range rounds {
		got = append(got, fc.completes(fails(versions)))
	}
	return got
}

func same(got, want []bool) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// The defect item 2 reports, at the unit level: three required participants
// each fixing their own half once. Standing readiness turns that single fix
// cycle into three failing verdicts, and counting each of them exhausts a cap
// of 3 before the third participant has submitted its fix at all.
func TestFixCycleDoesNotCountPartialProgressAsAWholeCycle(t *testing.T) {
	got := count([]string{"billing", "gateway", "db"},
		map[string]string{"billing": "b1", "gateway": "g1", "db": "d1"},
		map[string]string{"billing": "b2", "gateway": "g1", "db": "d1"},
		map[string]string{"billing": "b2", "gateway": "g2", "db": "d1"},
	)
	want := []bool{true, false, false}
	if !same(got, want) {
		t.Errorf("counted rounds = %v, want %v: only the first round ends a cycle; the other two are one cycle in progress", got, want)
	}
}

// The other half of the rule: once every required participant has moved, the
// cycle is complete and the next partial round starts a new one. This is the
// post-branch spelling of what one round used to mean — the whole quorum
// re-declared.
func TestFixCycleCountsARoundWhereEveryParticipantHasMoved(t *testing.T) {
	got := count([]string{"billing", "gateway"},
		map[string]string{"billing": "b1", "gateway": "g1"},
		map[string]string{"billing": "b2", "gateway": "g1"}, // billing fixed
		map[string]string{"billing": "b2", "gateway": "g2"}, // gateway fixed: cycle done
		map[string]string{"billing": "b3", "gateway": "g2"}, // next cycle begins
	)
	want := []bool{true, false, true, false}
	if !same(got, want) {
		t.Errorf("counted rounds = %v, want %v", got, want)
	}
}

// A participant resubmitting an unchanged version is answered from pkg/gate's
// remembered verdict without the spanning test running at all, and that answer
// still routes blocks — so it is steered and resubmits again. This counter is
// the only thing bounding that loop (see fixCycle's doc comment), so every
// unchanged round must count. TestRunStopsAfterTheRoundCap is the same case
// through the whole loop.
func TestFixCycleCountsAnUnchangedResubmit(t *testing.T) {
	got := count([]string{"billing"},
		map[string]string{"billing": "b1"},
		map[string]string{"billing": "b1"},
		map[string]string{"billing": "b1"},
	)
	want := []bool{true, true, true}
	if !same(got, want) {
		t.Errorf("counted rounds = %v, want %v: an unchanged re-run must still burn a cap slot", got, want)
	}
}

// One participant committing on every steer while its peers never move again
// is the case that makes "count only rounds where nobody moved" insufficient:
// the version set differs every round, so no round would ever be unchanged,
// and with cfg.Wall == 0 documented as unbounded the loop would never end.
// Re-movement by a participant that already moved in this cycle contributes
// nothing new, so it closes the cycle.
func TestFixCycleBoundsOneParticipantChurningAlone(t *testing.T) {
	got := count([]string{"billing", "gateway"},
		map[string]string{"billing": "b1", "gateway": "g1"},
		map[string]string{"billing": "b2", "gateway": "g1"}, // billing moves: progress
		map[string]string{"billing": "b3", "gateway": "g1"}, // billing again: nothing new
		map[string]string{"billing": "b4", "gateway": "g1"},
		map[string]string{"billing": "b5", "gateway": "g1"},
	)
	want := []bool{true, false, true, false, true}
	if !same(got, want) {
		t.Errorf("counted rounds = %v, want %v: a lone churning participant must still reach the cap", got, want)
	}
}

// A merge failure produces a verdict the coordinator can only partly backfill,
// and a runner that reports nothing produces one with no versions at all.
// Absent is not "changed": reading it that way would call a merge failure a
// completed cycle, and reading it as "moved" for every participant would count
// every such round twice over. Absent means not moved, so a gate that fails to
// merge every round still reaches the cap rather than looping forever.
func TestFixCycleCountsVerdictsThatNameNobody(t *testing.T) {
	got := count([]string{"billing", "gateway"},
		map[string]string{"billing": "b1", "gateway": "g1"},
		nil,
		nil,
	)
	want := []bool{true, true, true}
	if !same(got, want) {
		t.Errorf("counted rounds = %v, want %v", got, want)
	}
}
