package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
)

// pcops.Run injects PC_SUBMIT_TIMEOUT into every session it spawns. Ignoring it
// here is what made budget.submit_timeout dead configuration: a spawned agent's
// `pc submit` parked for the 5-minute default whatever the scenario said.
func TestSubmitTimeoutComesFromTheEnvironment(t *testing.T) {
	t.Setenv("PC_DB", "/tmp/pc-test.db")
	t.Setenv("PC_SUBMIT_TIMEOUT", "90s")

	cfg, err := loadConfig("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SubmitTimeout != 90*time.Second {
		t.Errorf("SubmitTimeout = %v, want 90s from $PC_SUBMIT_TIMEOUT", cfg.SubmitTimeout)
	}
}

// An absent or unusable value must fall back to the default, never to zero: a
// zero timeout builds an already-expired context and reports "no verdict"
// instantly, which an agent cannot distinguish from a real stall.
func TestSubmitTimeoutFallsBackToTheDefault(t *testing.T) {
	wantDefault := func(value string) {
		t.Helper()
		t.Setenv("PC_SUBMIT_TIMEOUT", value)
		if got := submitTimeoutFromEnv(); got != pcops.DefaultSubmitTimeout {
			t.Errorf("submitTimeoutFromEnv() with %q = %v, want the %v default",
				value, got, pcops.DefaultSubmitTimeout)
		}
	}
	wantDefault("")     // unset
	wantDefault("soon") // unparseable
	wantDefault("0s")   // zero expires instantly
	wantDefault("-5s")
}

// sampleScenario is a minimal two-agent scenario file, enough to exercise
// config resolution without touching a real bus or git worktree.
const sampleScenario = `
db: .pc/poc.db
gate:
  id: checkout
  required: [billing, gateway]
  runner: integrator
  run: go test ./...
agents:
  - name: billing
    branch: agent/billing
  - name: gateway
    branch: agent/gateway
runner:
  name: integrator
  branch: agent/integration
`

func writeScenario(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "poc.yaml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// pc up hosts the coordinator, which needs the gate definition — required
// participants, runner name — that only a scenario file carries. Falling back
// to an env-only config here would silently start a coordinator for no gate.
func TestUpRequiresConfig(t *testing.T) {
	_, err := resolveUpConfig("")
	if err == nil {
		t.Fatal("resolveUpConfig(\"\") = nil error, want one naming --config")
	}
	if !strings.Contains(err.Error(), "--config") {
		t.Errorf("error %q does not name the missing --config flag", err.Error())
	}

	if got := cmdUp(context.Background(), nil); got != 2 {
		t.Errorf("cmdUp with no --config = %d, want 2", got)
	}
}

func TestUpLoadsTheGateFromConfig(t *testing.T) {
	cfg, err := resolveUpConfig(writeScenario(t, sampleScenario))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GateID != "checkout" || cfg.Gate.Runner != "integrator" {
		t.Errorf("cfg = %+v, want the checkout gate with runner integrator", cfg)
	}
}

// run-gate needs the same gate definition as up, for the same reason.
func TestRunGateRequiresConfig(t *testing.T) {
	_, _, _, err := resolveRunGateConfig("", "")
	if err == nil {
		t.Fatal("resolveRunGateConfig(\"\", \"\") = nil error, want one naming --config")
	}
	if !strings.Contains(err.Error(), "--config") {
		t.Errorf("error %q does not name the missing --config flag", err.Error())
	}

	if got := cmdRunGate(context.Background(), nil); got != 2 {
		t.Errorf("cmdRunGate with no --config = %d, want 2", got)
	}
}

// A runner with nothing to merge is a silent hang waiting to happen, so an
// agents list with no branches must fail fast at startup, not stall inside
// pcops.RunGate waiting for a gate opening that will never resolve anything.
func TestRunGateRejectsAnEmptyAgentsList(t *testing.T) {
	path := writeScenario(t, `
db: .pc/poc.db
gate:
  id: checkout
  runner: integrator
  run: go test ./...
runner:
  name: integrator
  branch: agent/integration
`)
	if _, _, _, err := resolveRunGateConfig(path, ""); err == nil {
		t.Fatal("resolveRunGateConfig with no agents = nil error, want one rejecting it")
	}
}

func TestRunGateRejectsABlankBranch(t *testing.T) {
	path := writeScenario(t, `
db: .pc/poc.db
gate:
  id: checkout
  required: [billing]
  runner: integrator
  run: go test ./...
agents:
  - name: billing
    branch: ""
runner:
  name: integrator
  branch: agent/integration
`)
	if _, _, _, err := resolveRunGateConfig(path, ""); err == nil {
		t.Fatal("resolveRunGateConfig with a blank branch = nil error, want one rejecting it")
	}
}

// --workdir is the runner's own git worktree, and defaults to the process's
// current directory when the operator does not name one explicitly.
func TestRunGateDefaultsWorkdirToCWD(t *testing.T) {
	wantWd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	_, gotWd, _, err := resolveRunGateConfig(writeScenario(t, sampleScenario), "")
	if err != nil {
		t.Fatal(err)
	}
	if gotWd != wantWd {
		t.Errorf("workdir = %q, want cwd %q", gotWd, wantWd)
	}
}

func TestRunGateHonorsExplicitWorkdir(t *testing.T) {
	_, gotWd, _, err := resolveRunGateConfig(writeScenario(t, sampleScenario), "/some/explicit/dir")
	if err != nil {
		t.Fatal(err)
	}
	if gotWd != "/some/explicit/dir" {
		t.Errorf("workdir = %q, want the explicit --workdir", gotWd)
	}
}

// Branches are derived from the scenario file, not a separate flag, so the
// scenario stays the single source of truth for who the runner merges.
func TestBranchesFromConfigPreservesConfigOrder(t *testing.T) {
	_, _, branches, err := resolveRunGateConfig(writeScenario(t, sampleScenario), "")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"agent/billing", "agent/gateway"}
	if len(branches) != len(want) || branches[0] != want[0] || branches[1] != want[1] {
		t.Errorf("branches = %v, want %v in config order", branches, want)
	}
}

// The usage string is the operator's map of what pc can do; advertising a
// command that does not exist (or omitting one that does) was already flagged
// once in review, so pin the full, accurate list down with a test.
func TestUsageListsAllFourCommands(t *testing.T) {
	for _, cmd := range []string{"submit", "send", "up", "run-gate"} {
		if !strings.Contains(usage, cmd) {
			t.Errorf("usage %q does not mention %q", usage, cmd)
		}
	}
}

func TestUnknownSubcommandExits2(t *testing.T) {
	if got := run(context.Background(), []string{"bogus"}); got != 2 {
		t.Errorf("run with unknown subcommand = %d, want 2", got)
	}
}

func TestNoSubcommandExits2(t *testing.T) {
	if got := run(context.Background(), nil); got != 2 {
		t.Errorf("run with no subcommand = %d, want 2", got)
	}
}
