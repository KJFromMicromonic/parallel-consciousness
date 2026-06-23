// Command demo runs three agents collaborating on a complementary task over
// the in-memory bus, so you can watch them take turns and negotiate.
//
//	go run ./cmd/demo
//
// Topology: manager/worker. The planner owns the goal and decomposes it; the
// researcher and writer are workers. The writer depends on the researcher's
// output, so it BLOCKs until the research lands — demonstrating the
// dependency-negotiation that a shared MD file can't express.
package main

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

const project = "project.alpha"

func main() {
	log.SetFlags(0)
	log.SetOutput(os.Stdout)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := bus.NewInMemory(64)
	var wg sync.WaitGroup

	planner := mustAgent(ctx, b, "planner", []string{project})
	researcher := mustAgent(ctx, b, "research", []string{project})
	writer := mustAgent(ctx, b, "writer", []string{project})

	// Shared state across this worker for the demo's dependency handoff.
	var research struct {
		sync.Mutex
		done    bool
		finding string
	}

	// --- researcher: completes a unit of work, then informs the project ---
	researcher.On(protocol.IntentRequest, func(ctx context.Context, a *agent.Agent, m protocol.Message) *protocol.Message {
		go func() {
			time.Sleep(400 * time.Millisecond) // simulate work
			research.Lock()
			research.done = true
			research.finding = "users churn most in week 2"
			research.Unlock()
			// Tell everyone on the project the dependency is satisfied.
			a.Send(ctx, protocol.New(
				protocol.Address{Agent: a.Name},
				protocol.Address{Topic: project},
				protocol.IntentInform,
				map[string]any{"text": "research complete: " + research.finding},
			))
			// And report done to the planner who requested it.
			a.Send(ctx, m.Reply(protocol.Address{Agent: a.Name}, protocol.IntentDone,
				map[string]any{"text": research.finding}))
		}()
		return nil // work is async; no immediate reply
	})

	// --- writer: needs research first; blocks, then proceeds on inform ---
	writer.On(protocol.IntentRequest, func(ctx context.Context, a *agent.Agent, m protocol.Message) *protocol.Message {
		research.Lock()
		ready := research.done
		research.Unlock()
		if !ready {
			// Negotiate instead of failing: tell the planner we're blocked.
			return ptr(m.Reply(protocol.Address{Agent: a.Name}, protocol.IntentBlock,
				map[string]any{"text": "need research before I can write"}))
		}
		return ptr(m.Reply(protocol.Address{Agent: a.Name}, protocol.IntentDone,
			map[string]any{"text": "draft written using: " + research.finding}))
	})

	// When the writer hears research is done (topic inform), it proceeds.
	writer.On(protocol.IntentInform, func(ctx context.Context, a *agent.Agent, m protocol.Message) *protocol.Message {
		research.Lock()
		ready := research.done
		research.Unlock()
		if ready {
			a.Send(ctx, protocol.New(
				protocol.Address{Agent: a.Name},
				protocol.Address{Topic: project},
				protocol.IntentInform,
				map[string]any{"text": "draft written using: " + research.finding},
			))
		}
		return nil
	})

	// --- planner: orchestrates, and breaks the block by re-sequencing work ---
	planner.On(protocol.IntentBlock, func(ctx context.Context, a *agent.Agent, m protocol.Message) *protocol.Message {
		// A worker is blocked on a dependency. The manager resolves the cycle
		// by dispatching the dependency first — this is why manager/worker
		// avoids the deadlocks a flat peer mesh invites.
		log.Printf("  [planner] %s is blocked; dispatching research first", m.From.Agent)
		return ptr(protocol.New(
			protocol.Address{Agent: a.Name},
			protocol.Address{Agent: "research"},
			protocol.IntentRequest,
			map[string]any{"text": "produce churn research"},
		))
	})

	for _, ag := range []*agent.Agent{planner, researcher, writer} {
		wg.Add(1)
		go func(x *agent.Agent) { defer wg.Done(); x.Run(ctx) }(ag)
	}

	// Kick off: planner asks the writer to write (which it can't yet) — the
	// block/escalate/re-sequence dance unfolds from here.
	log.Println("── parallel-consciousness: 3 agents, one complementary task ──")
	planner.Send(ctx, protocol.New(
		protocol.Address{Agent: "planner"},
		protocol.Address{Agent: "writer"},
		protocol.IntentRequest,
		map[string]any{"text": "write the churn summary"},
	))

	time.Sleep(2 * time.Second)
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

func ptr(m protocol.Message) *protocol.Message { return &m }
