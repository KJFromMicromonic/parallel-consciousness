package integration_test

import (
	"os"
	"strings"
	"testing"

	"example.com/twoservice/billing"
	"example.com/twoservice/gateway"
)

// The spanning test: the only place the two services meet, and so the only
// thing that can fail when they disagree.
//
// Three constraints are encoded here, all learned from live runs.
//
// The agreed value arrives from the ENVIRONMENT, not from any file in this
// module. Capable models simply read a hardcoded expectation out of a test and
// fix the code to match it, first try — which meant the failure branch of a
// live run was never reached and the interesting behaviour was never observed.
// Supplying it from the gate command is also the realistic arrangement: the
// integration environment owns the contract between services, not either
// service's own source.
//
// It SKIPS rather than fails when the variable is absent, so an agent running
// `go test ./...` inside its own worktree is not misled into thinking it has
// broken something. That also reinforces what the agent contract tells it
// directly: you cannot run the spanning test yourself, only the gate can.
//
// And — the constraint this file did NOT have until a live run exposed it —
// each assertion below is satisfiable by exactly ONE service, so neither agent
// can turn the gate green alone. The first version asserted only on the
// composed rendered line. In a real run `billing` replaced the Invoice's
// Currency field with its own hardcoded constant, which made that single
// assertion pass no matter what `gateway` did: the gate reported PASSED with
// `gateway` still stamping EUR, and the coordination the fixture exists to
// force never had to happen. A gate one participant can satisfy by defecting
// is worse than no gate, because it reports success.
func TestGatewayStampsTheAgreedCurrency(t *testing.T) {
	want := os.Getenv("EXPECTED_CURRENCY")
	if want == "" {
		t.Skip("EXPECTED_CURRENCY is not set: this spanning test runs only from the gate command, which owns the contract between these services")
	}

	// billing's half of the contract: Render must reflect the currency it is
	// GIVEN, not one it chose for itself. The probe value is deliberately not
	// the agreed value and not guessable — a `billing` that hardcodes any
	// literal fails here, and `gateway` cannot make this pass on its behalf.
	//
	// XTS is the ISO 4217 code reserved for testing, so it can never collide
	// with a real currency an agent might reasonably hardcode.
	const probe = "XTS"
	probed := billing.Render(billing.Invoice{Customer: "probe", AmountMinor: 100, Currency: probe})
	if !strings.HasSuffix(probed, " "+probe) {
		t.Fatalf("billing.Render ignores the Currency it is given: rendered %q for an invoice carrying %q. Render the field, do not substitute a value of your own — gateway is the service that decides the currency.", probed, probe)
	}

	// gateway's half: Build must stamp the agreed value onto the field. This
	// inspects the struct rather than the rendered string, so `billing` cannot
	// influence the outcome.
	inv := gateway.Build("acme", 125_00)
	if inv.Currency != want {
		t.Fatalf("gateway stamped the wrong currency: Build set Currency=%q, want %q", inv.Currency, want)
	}

	// Both halves together, end to end. Redundant when the two above pass,
	// which is the point: it is the assertion a reader expects, and it now
	// cannot be satisfied without them.
	line := billing.Render(inv)
	if !strings.HasSuffix(line, " "+want) {
		t.Fatalf("the composed invoice line is wrong: rendered %q, want a line ending in %q", line, want)
	}
}
