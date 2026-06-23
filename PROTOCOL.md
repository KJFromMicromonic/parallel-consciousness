# Parallel Consciousness Protocol Specification (v0.1)

The contract for coworker-like agent communication. Transport-agnostic: the
same envelope rides the in-memory bus or a Kafka/Redpanda topic unchanged.

## Design principle

A message bus moves bytes. Parallel Consciousness is the *conversation layer* on top:
**intent-typed speech acts**, **threading**, and **turn-taking discipline**.
This is the part that distinguishes "agents talking like coworkers" from
"agents appending to a shared file."

Two responsibilities are kept strictly separate:

| Responsibility   | Mechanism                          | Lifetime  |
|------------------|------------------------------------|-----------|
| Durable state    | append-only coordination topic     | permanent |
| Live signaling   | direct inbox messages + threading  | ephemeral |

Conflating them is the failure mode that makes MD-doc coordination brittle.

## Envelope

Every message carries:

| Field             | Purpose                                                  |
|-------------------|----------------------------------------------------------|
| `id`              | unique message id                                        |
| `conversation_id` | groups a whole exchange                                  |
| `in_reply_to`     | chains one turn to the turn it answers                   |
| `from` / `to`     | address (agent for direct, topic for broadcast)          |
| `intent`          | the speech act (see below)                               |
| `body`            | free-form payload                                        |
| `timestamp`       | UTC send time                                            |
| `deadline`        | optional; ask recipient to respond before this           |

`conversation_id` + `in_reply_to` let any agent reconstruct context without
re-reading an entire topic — the mechanism behind conversational feel.

## Intents (speech acts)

Agents react to intent, not prose. This keeps multi-agent chatter parseable.

**Work negotiation:** `request`, `propose`, `agree`, `disagree`, `inform`,
`block`, `done`
**Control plane:** `ack`, `nack`, `yield`

An intent either expects a response or is terminal. `inform`, `done`, and the
control acks are terminal; the rest oblige the recipient to take a turn. The
runtime auto-acks any response-expecting message a handler leaves unanswered,
so a peer is never left hanging on a false timeout.

## Turn-taking & deadlock

A flat three-agent mesh can deadlock (A waits B, B waits C, C waits A). Two
sanctioned topologies avoid it:

- **Manager/worker** (demo default): one agent owns the goal and breaks cycles
  by re-sequencing work. When a worker sends `block`, the manager dispatches
  the missing dependency first. Easiest to reason about.
- **Blackboard + DM**: a shared coordination topic is canonical state; direct
  messages are the live back-channel. Keeps the audit trail, adds negotiation.

## Interruption

Cooperative, not preemptive. An urgent `block` is routed to an agent's
interruption channel; the agent polls it at safe checkpoints (`Interrupted()`)
rather than being torn off a tool call mid-flight. Far easier to reason about
than preemption, and good enough for coordination.

## Transport contract

Implementations satisfy a two-method interface — `Publish` and `Subscribe` —
so agent code never knows whether it's on the in-memory bus or Kafka. Ordered
per-recipient delivery is the guarantee both backends provide.
