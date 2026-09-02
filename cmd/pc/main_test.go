package main

import (
	"context"
	"io"
	"os"
	"os/exec"
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

// chdir points the process's cwd at dir and returns a func that restores the
// original directory. resolveVersion's git step deliberately reads the
// process cwd (not a path derived from config), so exercising it means
// actually moving the process there — which is why the tests that use this
// cannot run in parallel with anything else that depends on cwd.
func chdir(t *testing.T, dir string) {
	t.Helper()
	orig, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(orig); err != nil {
			t.Fatal(err)
		}
	})
}

// An explicit --version wins outright: it must not consult git at all, which
// this proves by resolving it from a directory that is not a git repository.
func TestResolveVersionExplicitWinsWithoutGit(t *testing.T) {
	chdir(t, t.TempDir())
	got, err := resolveVersion(context.Background(), "v1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	if got != "v1.2.3" {
		t.Errorf("resolveVersion(explicit) = %q, want %q", got, "v1.2.3")
	}
}

// With no --version, resolveVersion falls back to the cwd's git HEAD — the
// committed state the gate will actually merge and test, not whatever is
// sitting uncommitted in the tree.
func TestResolveVersionFallsBackToGitHEAD(t *testing.T) {
	dir := t.TempDir()
	runGit := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=test@example.com",
			"GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=test@example.com",
		)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	runGit("init")
	if err := os.WriteFile(filepath.Join(dir, "f.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit("add", "f.txt")
	runGit("commit", "-m", "initial")

	wantOut, err := exec.Command("git", "-C", dir, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	want := strings.TrimSpace(string(wantOut))

	chdir(t, dir)
	got, err := resolveVersion(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("resolveVersion(\"\") = %q, want HEAD %q", got, want)
	}
}

// Outside a git repository and with no --version, there is nothing to
// attribute a verdict to, so resolveVersion must error rather than fall back
// to a placeholder like "unversioned" — a made-up version is the defect this
// fixes, not an acceptable degraded mode.
func TestResolveVersionErrorsOutsideGitRepo(t *testing.T) {
	chdir(t, t.TempDir())
	_, err := resolveVersion(context.Background(), "")
	if err == nil {
		t.Fatal("resolveVersion(\"\") outside a git repo = nil error, want one")
	}
	if !strings.Contains(err.Error(), "--version") {
		t.Errorf("error %q does not tell the caller they can pass --version explicitly", err.Error())
	}
}

// A submit that cannot identify its own version is an identity error, not a
// gate verdict, so it must exit 2 — never 0 or 1, which are reserved for an
// actual verdict from the gate.
func TestCmdSubmitExits2WhenVersionCannotBeResolved(t *testing.T) {
	chdir(t, t.TempDir())
	t.Setenv("PC_DB", filepath.Join(t.TempDir(), "pc.db"))
	t.Setenv("PC_AGENT", "billing")

	got := cmdSubmit(context.Background(), []string{"--gate", "checkout"})
	if got != 2 {
		t.Errorf("cmdSubmit with unresolvable version = %d, want 2", got)
	}
}

// captureStderr redirects os.Stderr for the duration of fn and returns
// whatever was written to it. cmdSubmit's diagnostics are otherwise
// invisible to a plain exit-code assertion.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w
	fn()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	os.Stderr = orig
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

// TestCmdSubmitExits2AndNamesTheGateWhenNotAcknowledged is F4's CLI-facing
// regression test: no coordinator is running at all — pcops.Submit returns
// pcops.ErrNotAcknowledged — and cmdSubmit must map that to exit 2 (an
// operational error, never confusable with exit 1's "the gate failed") with
// an actionable message naming the gate and pointing at `pc up`, distinct
// from the CLI's generic "pc submit: %v" fallback used for every other
// error. See docs/superpowers/specs/2026-09-02-live-fire-findings.md, F4:
// this is exactly the case that used to be an 8-minute silent block.
func TestCmdSubmitExits2AndNamesTheGateWhenNotAcknowledged(t *testing.T) {
	t.Setenv("PC_DB", filepath.Join(t.TempDir(), "pc.db"))
	t.Setenv("PC_AGENT", "billing")

	var got int
	stderr := captureStderr(t, func() {
		got = cmdSubmit(context.Background(), []string{"--gate", "checkout", "--version", "v1"})
	})
	if got != 2 {
		t.Errorf("cmdSubmit with no coordinator = %d, want 2", got)
	}
	if !strings.Contains(stderr, "checkout") || !strings.Contains(stderr, "pc up") {
		t.Errorf("stderr = %q, want it to name the gate %q and mention `pc up`", stderr, "checkout")
	}
}
