// Command pc is the harness-agnostic surface: any coding agent that can run a
// shell command can join a Parallel Consciousness gate.
//
// Exit codes for `pc submit` are load-bearing:
//
//	0  the gate passed
//	1  the gate failed or stalled — the spanning test ran and did not pass
//	2  no verdict — config, identity, connection error, or timeout
//
// pcops.Run's own sentinel errors (ErrSessionDied, ErrLeaseLost) have no exit
// code here at all: nothing in this package dispatches Run, so they never
// reach a CLI boundary to map. Documented so a reader comparing the sentinel
// list above against Run's doesn't go looking for a mapping that does not
// exist.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
)

// usage lists every subcommand main actually dispatches. An earlier review
// flagged advertising a command that did not exist; keep this list exact in
// both directions as commands are added.
const usage = "usage: pc <init|submit|send|up|run-gate|watch> [flags]"

// initScaffold is what `pc init` writes. It is a working scenario against the
// committed fixture rather than a skeleton of empty keys: the fastest way to
// understand a scenario file is to run one, and a scaffold that fails
// validation teaches the wrong first lesson. Every value here satisfies
// pcops.LoadConfig's validation — TestInitWritesAConfigThatLoadsAndValidates
// asserts exactly that.
const initScaffold = `# Parallel Consciousness scenario.
#
# A scenario is a hand-written loop definition: who participates, what gate
# they must pass, and what bounds the run. This one drives the committed
# two-service fixture; point repo/agents at your own code to adapt it.

# The repository the agents work in. Each agent gets its own git worktree of it.
repo: ./fixtures/two-service

# The coordination database. Every pc command must agree on this path, so
# either keep it here or set $PC_DB (which overrides this).
db: ./.pc/bus.db

gate:
  # Gate id. Agents pass this to 'pc submit --gate'.
  id: currency
  # Every participant that must declare readiness before the gate runs.
  # Each name must appear in 'agents' below.
  required: [billing, gateway]
  # Which agent runs the spanning test. Must match runner.name below.
  runner: integrator
  # The spanning test itself, run in the runner's worktree after every
  # participant's branch is merged into it.
  #
  # EXPECTED_CURRENCY lives here on purpose: the value the two services must
  # agree on is supplied by the integration environment, so neither agent can
  # read it out of its own worktree. They learn it from a failing gate.
  run: "EXPECTED_CURRENCY=USD go test ./integration/..."

agents:
  - name: billing
    branch: agent/billing
    role: implementer
    task: "Render the invoice currency correctly. You own billing/ only."
  - name: gateway
    branch: agent/gateway
    role: implementer
    task: "Stamp the agreed currency on invoices you build. You own gateway/ only."

runner:
  name: integrator
  branch: agent/integration

budget:
  # Total wall clock for one run. Omit for unbounded.
  wall: 20m
  # How long 'pc submit' waits for a verdict. Must exceed runner_timeout: a
  # readiness that lands mid-round is nacked and then waits for that round to
  # finish, so a shorter budget expires before the round it is waiting for.
  submit_timeout: 12m
  # How long the coordinator waits for the spanning test before calling the
  # round stalled.
  runner_timeout: 10m
`

// defaultConfigPath is what every command that needs a scenario falls back to
// when --config is not given.
const defaultConfigPath = ".pc.yaml"

func cmdInit(_ context.Context, args []string) int {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	force := fs.Bool("force", false, "overwrite an existing "+defaultConfigPath)
	fs.Parse(args)

	if _, err := os.Stat(defaultConfigPath); err == nil && !*force {
		// Refusing is the whole feature: a scenario file is hand-edited, and
		// silently replacing one is destructive in a way no other pc command is.
		fmt.Fprintf(os.Stderr, "pc init: %s already exists (use --force to overwrite)\n", defaultConfigPath)
		return 2
	}
	if err := os.WriteFile(defaultConfigPath, []byte(initScaffold), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "pc init: %v\n", err)
		return 2
	}
	fmt.Printf("wrote %s\n", defaultConfigPath)
	return 0
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:]))
}

// run dispatches one subcommand. It is separate from main so tests can drive
// dispatch — including the no-args and unknown-command exit paths — without
// going through os.Exit.
func run(ctx context.Context, args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, usage)
		return 2
	}
	switch args[0] {
	case "init":
		return cmdInit(ctx, args[1:])
	case "submit":
		return cmdSubmit(ctx, args[1:])
	case "send":
		return cmdSend(ctx, args[1:])
	case "up":
		return cmdUp(ctx, args[1:])
	case "run-gate":
		return cmdRunGate(ctx, args[1:])
	case "watch":
		return cmdWatch(ctx, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", args[0])
		return 2
	}
}

func cmdSubmit(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	gateID := fs.String("gate", "", "gate id")
	as := fs.String("as", "", "agent identity (defaults to $PC_AGENT)")
	version := fs.String("version", "", "opaque version string")
	config := fs.String("config", "", "scenario file (optional when $PC_DB is set)")
	fs.Parse(args)

	cfg, err := loadConfig(*config)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	name := *as
	if name == "" {
		name = os.Getenv("PC_AGENT")
	}
	if name == "" || *gateID == "" {
		fmt.Fprintln(os.Stderr, "pc submit: --gate and an identity (--as or $PC_AGENT) are required")
		return 2
	}
	v, err := resolveVersion(ctx, *version)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pc submit: %v\n", err)
		// Cannot identify what state is being submitted — an identity error,
		// not a gate verdict, so it must not be conflated with exit 1.
		return 2
	}

	verdict, err := pcops.Submit(ctx, cfg, *gateID, name, v)
	if err != nil {
		if errors.Is(err, pcops.ErrNotAcknowledged) {
			// F4: this is an operational error — nothing acknowledged the
			// readiness declaration within pcops.AckTimeout — not a gate
			// verdict, so it must not read like exit 1. Named separately
			// from the generic branch below so the message is actionable:
			// a blocked agent that sees this should go check `pc up`, not
			// keep waiting or start improvising a peer message the way a
			// live run's agent did after 7m45s of silence (see
			// docs/superpowers/specs/2026-09-02-live-fire-findings.md, F4
			// and "F4 reinforced").
			fmt.Fprintf(os.Stderr, "pc submit: gate %q was never acknowledged — is `pc up` running for this gate?\n", *gateID)
			return 2
		}
		fmt.Fprintf(os.Stderr, "pc submit: %v\n", err)
		// Every other error Submit can return — opening the bus, joining as
		// the named agent, declaring readiness, or timing out waiting for a
		// verdict — means no verdict was obtained, so they all map to the
		// same exit code as ErrNotAcknowledged above: 2, not 1. Exit 1 is
		// reserved for a verdict that actually arrived and failed.
		return 2
	}
	// Detail is documented as empty on a pass, and printing it unconditionally
	// put a bare blank line on stdout for every passing submit.
	if verdict.Detail != "" {
		fmt.Println(verdict.Detail)
	}
	if verdict.Passed {
		return 0
	}
	return 1
}

func cmdSend(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("send", flag.ExitOnError)
	to := fs.String("to", "", "recipient agent")
	intent := fs.String("intent", "inform", "intent")
	from := fs.String("as", "", "sender identity (defaults to $PC_AGENT)")
	config := fs.String("config", "", "scenario file (optional when $PC_DB is set)")
	fs.Parse(args)

	cfg, err := loadConfig(*config)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	name := *from
	if name == "" {
		name = os.Getenv("PC_AGENT")
	}
	if name == "" || *to == "" || fs.NArg() == 0 {
		fmt.Fprintln(os.Stderr, `usage: pc send --to <agent> [--intent inform] "message"`)
		return 2
	}
	// Join every argument: an unquoted `pc send --to x hello world` used to send
	// just "hello", silently dropping the rest of the message.
	text := strings.Join(fs.Args(), " ")
	if err := pcops.Send(ctx, cfg, name, *to, *intent, text); err != nil {
		fmt.Fprintf(os.Stderr, "pc send: %v\n", err)
		return 2
	}
	return 0
}

func cmdUp(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("up", flag.ExitOnError)
	config := fs.String("config", "", "scenario file (default: ./.pc.yaml if present)")
	fs.Parse(args)

	cfg, err := resolveConfig(*config, "up")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	return exitForDaemon(pcops.Up(ctx, cfg, printVerdict))
}

// resolveConfig loads the scenario a command needs, defaulting to ./.pc.yaml
// when --config was not given.
//
// The default exists because an operator working in one scenario's directory
// should not name the same file on every command. It is a fallback and not a
// silent one: with neither the flag nor the file, the error names both, and
// the error still explains WHY a scenario is mandatory — a command cannot
// filter a feed, open a gate or merge branches without a gate definition, and
// a missing one previously surfaced as a hang rather than a message.
func resolveConfig(configPath, command string) (pcops.Config, error) {
	if configPath == "" {
		if _, err := os.Stat(defaultConfigPath); err != nil {
			return pcops.Config{}, fmt.Errorf("pc %s: --config is required, or run in a directory containing %s (no gate definition without one; `pc init` writes one)",
				command, defaultConfigPath)
		}
		configPath = defaultConfigPath
	}
	return pcops.LoadConfig(configPath)
}

// printVerdict is the operator's only view of a live `pc up` run, so it prints
// one readable line per resolved verdict rather than a struct dump.
func printVerdict(v gate.Verdict) {
	status := "FAIL"
	if v.Passed {
		status = "PASS"
	}
	if v.Detail != "" {
		fmt.Printf("gate %s: %s — %s\n", v.GateID, status, v.Detail)
		return
	}
	fmt.Printf("gate %s: %s\n", v.GateID, status)
}

func cmdRunGate(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("run-gate", flag.ExitOnError)
	config := fs.String("config", "", "scenario file (default: ./.pc.yaml if present)")
	workdir := fs.String("workdir", "", "runner's git worktree (default: current directory)")
	fs.Parse(args)

	cfg, wd, branches, err := resolveRunGateConfig(*config, *workdir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	return exitForDaemon(pcops.RunGate(ctx, cfg, wd, branches))
}

// resolveRunGateConfig loads the scenario, derives the branches to merge from
// cfg.Agents, and resolves the workdir default — all before anything touches
// the bus or a git worktree, so a bad config fails fast instead of hanging
// inside RunGate waiting for a gate opening that will never resolve.
func resolveRunGateConfig(configPath, workdir string) (cfg pcops.Config, wd string, branches []string, err error) {
	cfg, err = resolveConfig(configPath, "run-gate")
	if err != nil {
		return pcops.Config{}, "", nil, err
	}
	branches, err = branchesFromConfig(cfg)
	if err != nil {
		return pcops.Config{}, "", nil, err
	}
	wd = workdir
	if wd == "" {
		wd, err = os.Getwd()
		if err != nil {
			return pcops.Config{}, "", nil, fmt.Errorf("pc run-gate: getwd: %w", err)
		}
	}
	return cfg, wd, branches, nil
}

// branchesFromConfig is the branches a runner merges, taken from the scenario
// file in agent order rather than a separate flag: the scenario is the single
// source of truth for who participates. A config with no agents, or an agent
// with no branch, is rejected here rather than left to become a runner that
// merges nothing and hangs waiting for a gate opening forever.
func branchesFromConfig(cfg pcops.Config) ([]string, error) {
	if len(cfg.Agents) == 0 {
		return nil, fmt.Errorf("pc run-gate: config has no agents to merge")
	}
	branches := make([]string, 0, len(cfg.Agents))
	for _, a := range cfg.Agents {
		if a.Branch == "" {
			return nil, fmt.Errorf("pc run-gate: agent %q has no branch configured", a.Name)
		}
		branches = append(branches, a.Branch)
	}
	return branches, nil
}

// cmdWatch streams the gate's activity feed. --config is required for the
// same reason as up and run-gate: a feed without a gate definition cannot
// filter, and a coordinator's database alone does not say which gates exist.
func cmdWatch(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	config := fs.String("config", "", "scenario file (default: ./.pc.yaml if present)")
	gateID := fs.String("gate", "", "which gate to show (default: the scenario's gate.id; use --all to see everything)")
	all := fs.Bool("all", false, "show every gate and all peer traffic, not just the scenario's gate")
	full := fs.Bool("full", false, "do not truncate long details")
	noFollow := fs.Bool("no-follow", false, "print recorded history and exit")
	fs.Parse(args)

	cfg, err := resolveConfig(*config, "watch")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	// --gate "" is deliberately not magic: it re-defaults to cfg.GateID just
	// like omitting the flag, so the only way to reach the unfiltered path
	// (matchesGate short-circuiting on an empty gate id) is the explicit
	// --all flag. An empty string is not discoverable the way a flag is.
	id := *gateID
	if id == "" {
		id = cfg.GateID
	}
	if *all {
		id = ""
	}
	// A signal-cancelled ctx blocking inside --follow is a clean Ctrl-C, not a
	// failure: main wires ctx to SIGINT/SIGTERM, so context.Canceled here means
	// the operator asked to stop, exactly like exitForDaemon treats it for the
	// other long-running commands.
	if err := pcops.Watch(ctx, cfg, id, *full, !*noFollow, os.Stdout); err != nil &&
		!errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "pc watch: %v\n", err)
		return 2
	}
	return 0
}

// exitForDaemon maps a daemon's terminal error to an exit code. Up and
// RunGate return ctx.Err() by design once ctx ends, and main wires ctx to
// SIGINT/SIGTERM via signal.NotifyContext — so a context cancellation here is
// a normal, requested shutdown, not a failure to report as one.
func exitForDaemon(err error) int {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return 0
	}
	fmt.Fprintln(os.Stderr, err)
	return 2
}

// resolveVersion implements the design spec's resolution order for `pc
// submit`: --version, when the caller passed one, wins outright and git is
// never consulted. Otherwise fall back to `git rev-parse HEAD` run in the
// process's current working directory — deliberately the cwd, not the repo
// root and not a path derived from config, because an agent runs `pc submit`
// from inside its own git worktree and that worktree's HEAD is precisely the
// version being declared. HEAD is the right answer even with uncommitted
// changes in the tree: the gate merges committed branches, so uncommitted
// work is invisible to it regardless, and HEAD is what the gate will
// actually test. Reporting anything else would overstate what was submitted.
// With no explicit version and no git repository to fall back to, there is
// nothing left to attribute a verdict to, so this returns an error rather
// than a placeholder constant — an unattributable "unversioned" readiness
// declaration was the defect this replaces.
func resolveVersion(ctx context.Context, explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	out, err := exec.CommandContext(ctx, "git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "", fmt.Errorf("no version: not in a git repository and --version was not passed; pass --version explicitly: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

// loadConfig prefers an explicit scenario file and otherwise synthesises the
// minimum from the environment, so an agent needs only $PC_DB to participate.
//
// $PC_SUBMIT_TIMEOUT applies only on the env-only path, and deliberately so: a
// scenario file already carries budget.submit_timeout, and pcops.Run injects
// that same value into the session's environment, so the two agree. Hardcoding
// DefaultSubmitTimeout here was what made budget.submit_timeout dead
// configuration — every spawned agent parked for five minutes whatever the
// scenario said.
func loadConfig(path string) (pcops.Config, error) {
	if path != "" {
		return pcops.LoadConfig(path)
	}
	db := os.Getenv("PC_DB")
	if db == "" {
		return pcops.Config{}, fmt.Errorf("no config: pass --config or set $PC_DB")
	}
	return pcops.Config{DB: db, SubmitTimeout: submitTimeoutFromEnv()}, nil
}

// submitTimeoutFromEnv reads $PC_SUBMIT_TIMEOUT, falling back to the default
// when it is absent or unparseable. An unusable value must not turn into a
// zero timeout, which would report "no verdict" instantly.
func submitTimeoutFromEnv() time.Duration {
	v := os.Getenv("PC_SUBMIT_TIMEOUT")
	if v == "" {
		return pcops.DefaultSubmitTimeout
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		fmt.Fprintf(os.Stderr, "pc: ignoring unusable $PC_SUBMIT_TIMEOUT %q, using %v\n", v, pcops.DefaultSubmitTimeout)
		return pcops.DefaultSubmitTimeout
	}
	return d
}
