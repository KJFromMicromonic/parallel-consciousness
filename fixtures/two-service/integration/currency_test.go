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
// Two constraints are encoded here, both learned from live runs.
//
// The agreed value arrives from the ENVIRONMENT, not from any file in this
// module. Capable models simply read a hardcoded expectation out of a test and
// fix the code to match it, first try — which meant the failure branch of a
// live run was never reached and the interesting behaviour was never observed.
// Supplying it from the gate command is also the realistic arrangement: the
// integration environment owns the contract between services, not either
// service's own source.
//
// And it SKIPS rather than fails when the variable is absent, so an agent
// running `go test ./...` inside its own worktree is not misled into thinking
// it has broken something. That also reinforces what the agent contract tells
// it directly: you cannot run the spanning test yourself, only the gate can.
func TestGatewayStampsTheAgreedCurrency(t *testing.T) {
	want := os.Getenv("EXPECTED_CURRENCY")
	if want == "" {
		t.Skip("EXPECTED_CURRENCY is not set: this spanning test runs only from the gate command, which owns the contract between these services")
	}

	line := billing.Render(gateway.Build("acme", 125_00))
	if !strings.HasSuffix(line, " "+want) {
		t.Fatalf("gateway stamped the wrong currency: rendered %q, want a line ending in %q", line, want)
	}
}
