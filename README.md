# Conclave

**A conversation layer for coordinated AI agents.**

Most multi-agent frameworks give agents a place to *append* — a shared file, a
scratchpad, a state object. Conclave gives them a way to *talk*: intent-typed
messages, threaded conversations, turn-taking, and dependency negotiation, so
three agents on complementary tasks coordinate like coworkers instead of
leaving each other notes.

```
go run ./cmd/demo
```

You'll watch a planner, a researcher, and a writer take turns: the writer
blocks because it needs research first, the planner re-sequences the work to
break the block, the researcher finishes and broadcasts, the writer proceeds.
None of that handoff is expressible in a shared markdown doc.

## Why this exists

The transport (a message bus) is the easy part — and it's well covered. The
hard, underbaked part is the *conversation*: turn-taking without deadlock,
threading so agents remember context cheaply, interruption that doesn't
corrupt in-flight work, and the discipline of separating durable shared state
from live signaling. Conclave is opinionated about exactly that layer.

## Architecture

```
pkg/protocol   the wire contract: envelope, intents, threading   (no deps)
pkg/bus        pluggable transport; in-memory default
   └─ redpanda Kafka/Redpanda adapter (roadmap)
pkg/agent      runtime: conversation loop, ack/timeout, interruption
cmd/demo       3-agent manager/worker collaboration
```

Transport is one interface (`Publish` / `Subscribe`). The in-memory bus makes
`go run` work with zero infrastructure; the Redpanda adapter implements the
same interface for production scale without touching agent code.

## Status

v0.1 — runnable demo + protocol spec. See [PROTOCOL.md](./PROTOCOL.md).

## Roadmap

- [ ] Redpanda/Kafka adapter (reply-to + correlation for request/response)
- [ ] Deadline enforcement & escalation in the runtime (envelope field exists)
- [ ] Blackboard topology example alongside manager/worker
- [ ] Context compaction: summarize old turns, reference shared log by pointer
- [ ] Conformance test suite any transport adapter must pass
- [ ] LLM-backed agents (the demo uses deterministic handlers for clarity)

## License

Apache-2.0 (suggested).
