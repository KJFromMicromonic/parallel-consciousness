// Command sqlitedemo runs the cross-service test gate over the SQLite bus so
// agents in separate OS processes coordinate through one shared database file.
//
// Terminal 1 (long-running coordinator + runner):
//	go run ./cmd/sqlitedemo --role gatekeeper --db /tmp/team.db
//
// Terminals 2 and 3 (services declaring readiness):
//	go run ./cmd/sqlitedemo --role service --name gateway --version a1b2 --db /tmp/team.db
//	go run ./cmd/sqlitedemo --role service --name billing --version c3d4 --db /tmp/team.db
//
// A billing version of "e5f6" makes the runner fail, demonstrating the gate
// routing a block back to the owner.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

const gateID = "checkout"

func main() {
	log.SetFlags(log.Ltime)
	role := flag.String("role", "", "gatekeeper | service")
	dbPath := flag.String("db", "", "path to the shared SQLite file")
	name := flag.String("name", "", "service agent name (service role)")
	version := flag.String("version", "", "service version token (service role)")
	flag.Parse()

	if *dbPath == "" {
		log.Fatal("--db is required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	b, err := sqlite.Open(ctx, *dbPath)
	if err != nil {
		log.Fatalf("open bus: %v", err)
	}
	defer b.Close()

	switch *role {
	case "gatekeeper":
		runGatekeeper(ctx, b)
	case "service":
		if *name == "" || *version == "" {
			log.Fatal("--name and --version are required for the service role")
		}
		runService(ctx, b, *name, *version)
	default:
		log.Fatalf("unknown --role %q (want gatekeeper|service)", *role)
	}
}

func runGatekeeper(ctx context.Context, b *sqlite.Bus) {
	gk, err := agent.New(ctx, b, "gatekeeper", []string{gate.Topic(gateID)})
	if err != nil {
		log.Fatalf("gatekeeper: %v", err)
	}
	coord := gate.NewCoordinator(gk)
	coord.Register(gate.Spec{ID: gateID, Required: []string{"billing", "gateway"}, Runner: "runner"})
	coord.OnVerdict(func(v gate.Verdict) {
		if v.Passed {
			log.Printf("  ✓ gate %q PASSED  versions=%v", v.GateID, v.Versions)
		} else {
			log.Printf("  ✗ gate %q FAILED: %s", v.GateID, v.Detail)
		}
	})

	runner, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		log.Fatalf("runner: %v", err)
	}
	gate.ServeRunner(runner, func(ctx context.Context, id string, versions map[string]string) gate.Verdict {
		if versions["billing"] == "e5f6" {
			return gate.Verdict{GateID: id, Passed: false, Detail: "checkout_test: 402 from billing"}
		}
		return gate.Verdict{GateID: id, Passed: true}
	})

	go gk.Run(ctx)
	go runner.Run(ctx)

	log.Printf("gatekeeper + runner up; waiting for readiness on gate %q (Ctrl-C to stop)", gateID)
	<-ctx.Done()
}

func runService(ctx context.Context, b *sqlite.Bus, name, version string) {
	a, err := agent.New(ctx, b, name, []string{gate.Topic(gateID)})
	if err != nil {
		log.Fatalf("service %s: %v", name, err)
	}
	a.On(protocol.IntentBlock, func(ctx context.Context, ag *agent.Agent, m protocol.Message) *protocol.Message {
		log.Printf("  [%s] gate is failing on my change — will investigate", name)
		return nil
	})
	go a.Run(ctx)

	if err := gate.Ready(ctx, a, gateID, version); err != nil {
		log.Fatalf("declare ready: %v", err)
	}
	log.Printf("[%s] declared ready at %q; watching for the verdict (Ctrl-C to stop)", name, version)

	// Stay up long enough to observe the verdict/block, then exit.
	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second):
	}
}
