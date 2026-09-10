package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
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
	_, err := resolveConfig("", "up")
	if err == nil {
		t.Fatal("resolveConfig(\"\", \"up\") = nil error, want one naming --config")
	}
	if !strings.Contains(err.Error(), "--config") {
		t.Errorf("error %q does not name the missing --config flag", err.Error())
	}

	if got := cmdUp(context.Background(), nil); got != 2 {
		t.Errorf("cmdUp with no --config = %d, want 2", got)
	}
}

func TestUpLoadsTheGateFromConfig(t *testing.T) {
	cfg, err := resolveConfig(writeScenario(t, sampleScenario), "up")
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

// watch needs the same gate definition as up and run-gate, for the same
// reason: a feed without one cannot filter, and a bare database does not say
// which gates exist.
func TestWatchRequiresConfig(t *testing.T) {
	_, err := resolveConfig("", "watch")
	if err == nil {
		t.Fatal("resolveConfig(\"\", \"watch\") = nil error, want one naming --config")
	}
	if !strings.Contains(err.Error(), "--config") {
		t.Errorf("error %q does not name the missing --config flag", err.Error())
	}

	if got := cmdWatch(context.Background(), nil); got != 2 {
		t.Errorf("cmdWatch with no --config = %d, want 2", got)
	}
}

// A cancelled context must not read as a failure at the CLI boundary: main
// wires ctx to SIGINT/SIGTERM, so cmdWatch must map context.Canceled to exit
// 0 rather than the generic error path, mirroring exitForDaemon's treatment
// of the same case for up and run-gate. Passing an already-cancelled context
// surfaces the cancellation through sqlite.Open, before Watch ever reaches
// its follow loop — that loop's own cancellation handling is exercised by
// TestWatchStopsCleanlyWhenCancelledMidFollow in internal/pcops/watch_test.go.
// This test pins only the CLI's exit-code mapping.
func TestCmdWatchExitsCleanlyOnCancelledContext(t *testing.T) {
	t.Setenv("PC_DB", filepath.Join(t.TempDir(), "bus.db"))
	path := writeScenario(t, sampleScenario)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if got := cmdWatch(ctx, []string{"--config", path}); got != 0 {
		t.Errorf("cmdWatch with an already-cancelled context = %d, want 0 (a clean stop, not a failure)", got)
	}
}

// notifyingWriter is a thread-safe io.Writer that closes notify on its first
// write, used below to detect that cmdWatch's follow loop has actually
// produced output before the test cancels it — rather than guessing at how
// long that takes with a fixed delay.
type notifyingWriter struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	notify chan struct{}
	once   sync.Once
}

func newNotifyingWriter() *notifyingWriter {
	return &notifyingWriter{notify: make(chan struct{})}
}

func (w *notifyingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.buf.Write(p)
	w.once.Do(func() { close(w.notify) })
	return n, err
}

func (w *notifyingWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// FIX 1's CLI half: the DEFAULT invocation — no --gate, no --all — is the one
// an operator actually types (`pc watch --config scenario.yaml`), and it was
// exactly the invocation that silently dropped every peer pc send message,
// because cmdWatch used to default --gate to cfg.GateID with no way back to
// the unfiltered path. A direct, gate-less peer message must render under
// this default, unaided by any flag.
func TestCmdWatchDefaultInvocationRendersPeerTraffic(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "bus.db")
	scenario := fmt.Sprintf(`
db: %s
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
`, dbPath)
	path := writeScenario(t, scenario)

	ctx := context.Background()
	b, err := sqlite.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	peer := protocol.New(protocol.Address{Agent: "billing"}, protocol.Address{Agent: "gateway"},
		protocol.IntentInform, map[string]any{"text": "PEER MESSAGE FROM A SHELL"})
	if err := b.Publish(ctx, peer); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	out := newNotifyingWriter()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	copyDone := make(chan struct{})
	go func() {
		io.Copy(out, r)
		close(copyDone)
	}()

	runCtx, cancel := context.WithCancel(context.Background())
	exitCode := make(chan int, 1)
	go func() {
		// No --gate, no --all: the default invocation under test.
		exitCode <- cmdWatch(runCtx, []string{"--config", path})
	}()

	select {
	case <-out.notify:
		// cmdWatch has rendered its first line; safe to stop it now.
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("timed out waiting for cmdWatch to produce output before cancelling")
	}
	cancel()

	select {
	case <-exitCode:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for cmdWatch to return after cancellation")
	}

	os.Stdout = orig
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	<-copyDone

	if got := out.String(); !strings.Contains(got, "PEER MESSAGE FROM A SHELL") {
		t.Errorf("pc watch's default invocation did not render peer traffic:\n%s", got)
	}
}

// The usage string is the operator's map of what pc can do; advertising a
// command that does not exist (or omitting one that does) was already flagged
// once in review, so pin the full, accurate list down with a test.
func TestUsageListsEveryCommand(t *testing.T) {
	for _, cmd := range []string{"init", "submit", "send", "up", "run-gate", "watch"} {
		if !strings.Contains(usage, cmd) {
			t.Errorf("usage %q does not mention %q", usage, cmd)
		}
	}
}

// Round-trip, not byte comparison: what matters is that the scaffold is a
// document this project's own loader accepts and validates. Asserting exact
// bytes would break on every comment edit while proving less.
func TestInitWritesAConfigThatLoadsAndValidates(t *testing.T) {
	dir := t.TempDir()
	chdir(t, dir)

	if got := run(context.Background(), []string{"init"}); got != 0 {
		t.Fatalf("pc init = %d, want 0", got)
	}
	if _, err := os.Stat(".pc.yaml"); err != nil {
		t.Fatalf("pc init did not write .pc.yaml: %v", err)
	}
	if _, err := pcops.LoadConfig(".pc.yaml"); err != nil {
		t.Fatalf("pc init wrote a scenario its own loader rejects: %v", err)
	}
}

// Overwriting a scenario someone has edited is destructive and silent. It
// needs an explicit flag.
func TestInitRefusesToClobberWithoutForce(t *testing.T) {
	dir := t.TempDir()
	chdir(t, dir)

	if err := os.WriteFile(".pc.yaml", []byte("# mine\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := run(context.Background(), []string{"init"}); got == 0 {
		t.Fatal("pc init overwrote an existing .pc.yaml and exited 0")
	}
	b, err := os.ReadFile(".pc.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "# mine\n" {
		t.Fatalf("pc init modified an existing file without --force; contents now %q", b)
	}
	if got := run(context.Background(), []string{"init", "--force"}); got != 0 {
		t.Fatalf("pc init --force = %d, want 0", got)
	}
	if _, err := pcops.LoadConfig(".pc.yaml"); err != nil {
		t.Fatalf("pc init --force wrote a scenario its own loader rejects: %v", err)
	}
}

// The whole point of a default: an operator in a directory with a .pc.yaml
// should not have to name it on every command.
func TestConfigDefaultsToDotPcYamlWhenPresent(t *testing.T) {
	dir := t.TempDir()
	chdir(t, dir)

	if got := run(context.Background(), []string{"init"}); got != 0 {
		t.Fatal("pc init failed")
	}
	// watch --no-follow must now get past config resolution. Exit 2 here can
	// only mean the default was not applied — cmdWatch reports a bus failure
	// with the SAME exit code 2 it uses for a missing config, so this test
	// depends on pc init having also created the scaffold's database
	// directory; if it stops doing that, this goes back to failing here for
	// a reason unrelated to config defaulting.
	code := run(context.Background(), []string{"watch", "--no-follow"})
	if code == 2 {
		t.Errorf("pc watch with no --config exited 2 in a directory containing .pc.yaml; the default was not applied")
	}
}

// And the inverse, so the default cannot silently mask a genuine mistake.
func TestConfigStillRequiredWhenNoDotPcYaml(t *testing.T) {
	dir := t.TempDir()
	chdir(t, dir)

	if got := run(context.Background(), []string{"watch", "--no-follow"}); got != 2 {
		t.Errorf("pc watch with no --config and no .pc.yaml = %d, want 2", got)
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
