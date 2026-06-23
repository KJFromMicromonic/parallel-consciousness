// Command gatedemo shows a cross-service integration-test gate: two service
// agents declare readiness, a gatekeeper opens the gate, a runner executes the
// spanning test, and the verdict is broadcast — first a passing round, then a
// regression that fails and routes a block back to the owner.
//
//	go run ./cmd/gatedemo
package main

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

const gateID = "checkout"

func main() {
	log.SetFlags(0)
	log.SetOutput(os.Stdout)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := bus.NewInMemory(64)
	var wg sync.WaitGroup
	run := func(a *agent.Agent) { wg.Add(1); go func() { defer wg.Done(); a.Run(ctx) }() }

	// Gatekeeper hosts the coordinator; it must subscribe to the gate topic.
	gk := mustAgent(ctx, b, "gatekeeper", []string{gate.Topic(gateID)})
	coord := gate.NewCoordinator(gk)
	coord.Register(gate.Spec{ID: gateID, Required: []string{"billing", "gateway"}, Runner: "runner"})
	coord.OnVerdict(func(v gate.Verdict) {
		if v.Passed {
			log.Printf("  ✓ gate %q PASSED  versions=%v", v.GateID, v.Versions)
		} else {
			log.Printf("  ✗ gate %q FAILED: %s", v.GateID, v.Detail)
		}
	})

	// Runner: the spanning test. Fails if billing shipped the regression e5f6.
	runner := mustAgent(ctx, b, "runner", nil)
	gate.ServeRunner(runner, func(ctx context.Context, id string, versions map[string]string) gate.Verdict {
		if versions["billing"] == "e5f6" {
			return gate.Verdict{GateID: id, Passed: false, Detail: "checkout_test: 402 from billing"}
		}
		return gate.Verdict{GateID: id, Passed: true}
	})

	// Service agents subscribe to the gate topic to hear verdicts; billing also
	// reacts to a routed block.
	billing := mustAgent(ctx, b, "billing", []string{gate.Topic(gateID)})
	billing.On(protocol.IntentBlock, func(ctx context.Context, a *agent.Agent, m protocol.Message) *protocol.Message {
		log.Printf("  [billing] heard the gate is failing on my change — will investigate")
		return nil
	})
	gateway := mustAgent(ctx, b, "gateway", []string{gate.Topic(gateID)})

	for _, a := range []*agent.Agent{gk, runner, billing, gateway} {
		run(a)
	}

	log.Println("── parallel-consciousness: cross-service test gate ──")
	log.Println("Round 1 — both services at compatible versions:")
	_ = gate.Ready(ctx, gateway, gateID, "a1b2")
	_ = gate.Ready(ctx, billing, gateID, "c3d4")
	time.Sleep(500 * time.Millisecond)

	log.Println("Round 2 — billing ships a regression (e5f6):")
	_ = gate.Ready(ctx, billing, gateID, "e5f6")
	_ = gate.Ready(ctx, gateway, gateID, "a1b2")
	time.Sleep(500 * time.Millisecond)

	log.Println("── done ──")
	cancel()
	wg.Wait()
}

func mustAgent(ctx context.Context, b bus.Bus, name string, topics []string) *agent.Agent {
	a, err := agent.New(ctx, b, name, topics)
	if err != nil {
		log.Fatalf("create %s: %v", name, err)
	}
	return a
}
