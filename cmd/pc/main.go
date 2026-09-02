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
	"syscall"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: pc <submit|send|up|run-gate> [flags]")
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
	fmt.Println(verdict.Detail)
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
	if err := pcops.Send(ctx, cfg, name, *to, *intent, fs.Arg(0)); err != nil {
		fmt.Fprintf(os.Stderr, "pc send: %v\n", err)
		return 2
	}
	return 0
}

// loadConfig prefers an explicit scenario file and otherwise synthesises the
// minimum from the environment, so an agent needs only $PC_DB to participate.
func loadConfig(path string) (pcops.Config, error) {
	if path != "" {
		return pcops.LoadConfig(path)
	}
	db := os.Getenv("PC_DB")
	if db == "" {
		return pcops.Config{}, fmt.Errorf("no config: pass --config or set $PC_DB")
	}
	return pcops.Config{DB: db, SubmitTimeout: pcops.DefaultSubmitTimeout}, nil
}
