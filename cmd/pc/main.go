// Command pc is the harness-agnostic surface: any coding agent that can run a
// shell command can join a Parallel Consciousness gate.
//
// Exit codes for `pc submit` are load-bearing:
//
//	0  the gate passed
//	1  the gate failed or stalled — the spanning test ran and did not pass
//	2  no verdict — config, identity, connection error, or timeout
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
)

func main() {
	if len(os.Args) < 2 {
		// Only the commands main actually dispatches: up and run-gate are
		// library functions in pcops, not CLI subcommands, until Phase B wires
		// them, and advertising them here only earns an exit 2.
		fmt.Fprintln(os.Stderr, "usage: pc <submit|send> [flags]")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch os.Args[1] {
	case "submit":
		os.Exit(cmdSubmit(ctx, os.Args[2:]))
	case "send":
		os.Exit(cmdSend(ctx, os.Args[2:]))
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		os.Exit(2)
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
	v := *version
	if v == "" {
		v = "unversioned"
	}

	verdict, err := pcops.Submit(ctx, cfg, *gateID, name, v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pc submit: %v\n", err)
		// Every error Submit can return — opening the bus, joining as the
		// named agent, declaring readiness, or timing out — means no
		// verdict was obtained, so they all map to the same exit code.
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
