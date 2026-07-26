# Parallel Consciousness

**A local-first control plane and loop studio for coding agents.**

Coding agents are becoming capable enough to work on substantial parts of a
codebase, but using more than one at a time is still mostly a manual exercise.
One agent works in Codex, another works in Claude Code, progress lives in
separate conversations, dependencies are discovered late, and the developer
becomes the message bus, project manager, and integration engineer.

Parallel Consciousness is an experiment in changing that. The product is
intended to give independent coding agents a shared, auditable way to plan work,
claim isolated workspaces, report progress, hand off dependencies, review each
other, and converge on tested code. This repository currently provides the
communication and test-gate substrate for that direction.

The larger product vision adds a **Loop Studio**: describe an engineering
outcome in plain language, receive an explainable multi-agent loop, edit and
approve it, then run it with whichever coding agents you already use.

> **Status:** early coordination-kernel prototype. The message protocol,
> in-memory and SQLite transports, agent runtime, and cross-agent test gates are
> implemented. The task control plane, Loop Studio, dashboard, and Codex/Claude
> integration surfaces described below are the next product milestone.

## The problem

Today's coding-agent experience breaks down as soon as work becomes parallel or
iterative:

- Agents do not share a reliable model of tasks, dependencies, or ownership.
- Progress is buried in chat transcripts and self-reported percentages.
- Two agents can edit the same files or invalidate each other's assumptions.
- Review, testing, and failure routing are improvised by the human operator.
- Agent loops are powerful, but difficult to design: most developers do not
  know how to choose roles, partition work, isolate context, define objective
  verification, or decide when a loop should stop.
- High concurrency amplifies mistakes and cost unless workspaces, retries,
  budgets, and destructive operations are governed deliberately.

A shared Markdown file or task board records conclusions after the fact. It
does not coordinate live work, negotiate dependencies, interrupt safely, or
turn test failures into the next actionable unit of work.

## The product thesis

The useful unit of agentic software development is not a single prompt or even
a single agent. It is an **explainable, measurable loop**:

```text
goal
  -> propose a loop and explain why it should work
  -> approve tasks, roles, dependencies, budgets, and stopping conditions
  -> implement in isolated workspaces
  -> review from independent contexts
  -> fix the findings
  -> verify against objective evidence
  -> route failures to the responsible owner
  -> repeat until the acceptance criteria converge
```

Parallel Consciousness aims to make those loops understandable and accessible,
without forcing a team to adopt one model vendor or coding-agent harness.

## What the MVP should do

The first complete product loop targets one repository on one developer
machine:

1. A developer describes a goal in natural language.
2. A designated lead agent proposes an explainable task and dependency graph.
3. The developer edits and approves the plan.
4. Separately launched Codex and Claude Code sessions register through a generic
   CLI or MCP interface.
5. Each work item has one active owner and an exclusive Git worktree lease.
6. Agents report structured status, blockers, handoffs, commits, and test
   evidence.
7. Submitted changes enter an implement -> adversarial review -> fix -> verify
   loop.
8. A designated integration agent combines approved branches and runs shared
   test gates.
9. A failing gate creates a blocker, attributes it to the relevant work item,
   and routes it back to its owner.
10. The dashboard prioritizes blockers, dependencies, and the next useful
    action until the goal passes.

The MVP coordinates externally launched agents. Launching and supervising
agent processes is a later runtime adapter, not a prerequisite for the control
plane.

## Loop Studio

The Loop Studio is the intended differentiator beyond orchestration. It helps a
developer answer both **how** a loop should run and **why** it is structured that
way.

Given a goal, it proposes a versioned workflow containing:

- stages and dependencies;
- implementer, reviewer, fixer, and integration roles;
- the context each role should and should not receive;
- partitioning and workspace strategy;
- verification commands and evidence requirements;
- retry, escalation, and stopping rules;
- concurrency, token, time, and cost budgets;
- assumptions, risks, and a plain-language rationale.

Successful loops become reusable team assets. Failed runs preserve enough
evidence to improve the generating workflow instead of repeatedly hand-fixing
the same class of output.

The design is inspired by the operational lesson in Bun's
[Zig-to-Rust rewrite](https://bun.com/blog/bun-in-rust): high parallelism became
useful when it was organized into repeatable implementation, adversarial review,
fix, and verification loops with objective convergence signals.

## Architecture direction

```text
Codex / Claude Code / other agents
           | CLI or MCP
           v
      local `pc` daemon
       /      |       \
Loop engine  event log  coordination protocol
       \      |       /
       SQLite projections
              |
     HTTP + live event stream
              |
      local web dashboard
```

- **One core, multiple surfaces.** CLI, MCP, and dashboard call the same domain
  service.
- **Durable events plus projections.** Every transition is auditable; current
  task and run state remains cheap to query.
- **Live signaling stays separate from durable state.** The existing protocol
  and bus handle conversations and interruption; the domain store owns goals,
  workflows, work items, runs, leases, and evidence.
- **Agent-agnostic contracts.** Harness-specific setup lives at the edge.
- **Local first.** The MVP keeps source code and execution on the developer's
  machine.
- **Managed execution later.** A future runtime interface can launch agents
  without changing the workflow or event model.

## What exists today

This repository contains the working coordination substrate:

```text
pkg/protocol       typed messages, addressing, threading, deadlines
pkg/bus            pluggable transport contract and in-memory implementation
pkg/bus/sqlite     durable cross-process transport with replayable cursors
pkg/agent          conversation loop, intent dispatch, acknowledgements,
                   cooperative interruption
pkg/gate           cross-agent readiness quorum, test execution, verdicts,
                   and failure routing
cmd/demo           manager/worker dependency-negotiation demo
cmd/gatedemo       cross-service integration-gate demo
cmd/sqlitedemo     cross-process SQLite transport demo
```

The transport contract is intentionally small: `Publish` and `Subscribe`.
Agent behavior does not depend on whether messages are in-memory or durable.

### Run the collaboration demo

```bash
go run ./cmd/demo
```

The planner, researcher, and writer negotiate a dependency and re-sequence work
without polling a shared document.

### Run the integration-gate demo

```bash
go run ./cmd/gatedemo
```

Two service owners declare readiness, a runner executes the spanning test, and
the coordinator broadcasts the verdict. A failing round routes a blocker to
the participating owners; precise attribution is part of the planned product
layer.

### Run the tests

```bash
go test ./...
```

See [PROTOCOL.md](./PROTOCOL.md) for the current wire contract and
[the product design](./docs/superpowers/specs/2026-07-21-agent-coordination-loop-studio-design.md)
for the complete direction.

## Principles

1. **Outcomes over activity.** Progress is grounded in commits, reviews, tests,
   and decreasing failure sets—not arbitrary percentages.
2. **Explain the loop.** Developers should understand why each role, boundary,
   and stopping condition exists.
3. **Separate contexts deliberately.** An independent reviewer should not
   inherit the implementer's reasoning or incentive to accept its own work.
4. **One active owner.** Work items and workspace leases make responsibility
   explicit; collaboration happens through dependencies and handoffs.
5. **Fix the process.** Repeated failures should improve the loop definition,
   not create an endless stream of local patches.
6. **Bound autonomy.** Concurrency, retries, cost, time, commands, and workspace
   access require explicit policies.
7. **Keep agents replaceable.** The control plane belongs to the developer, not
   to one model or harness.

## Roadmap

- [x] Intent-typed conversation protocol
- [x] In-memory transport
- [x] Durable SQLite transport
- [x] Cooperative interruption
- [x] Cross-agent integration-test gates
- [ ] Local `pc` daemon and event-backed domain core
- [ ] Goals, approved loop revisions, work items, attempts, and evidence
- [ ] Generic CLI and MCP contracts
- [ ] Codex and Claude Code setup recipes
- [ ] Worktree registration and exclusive leases
- [ ] Explainable natural-language loop authoring
- [ ] Implement/review/fix/verify workflow
- [ ] Integration agent and automated failure routing
- [ ] Blocker-first local dashboard
- [ ] Private design-partner beta and outcome telemetry
- [ ] Managed agent launching and remote/team runtimes

## Early design partners

We are preparing a small developer beta for people already pushing coding
agents beyond single-session tasks. The most useful collaborators are teams
running multiple agents, building their own loops, or repeatedly acting as the
manual coordinator between implementation, review, and integration.

If that describes your workflow, reach out to the repository maintainer with
the task you wish agents could complete together and where the coordination
currently breaks down.

## License

The coordination-kernel code currently in this repository is licensed under
the [Apache License 2.0](./LICENSE). The licensing and distribution boundary for
future commercial product layers has not yet been finalized.

Copyright 2026 Micromonic Digital Inc.
