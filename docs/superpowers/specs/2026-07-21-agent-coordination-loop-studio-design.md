# Agent Coordination Plane and Loop Studio — Product Design

**Status:** Design proposal; product layers described here are planned  
**Date:** 2026-07-21  
**Project:** Parallel Consciousness

## Executive summary

Parallel Consciousness will become a local-first control plane and assisted
workflow studio for coding agents. It coordinates independently launched tools
such as Codex and Claude Code while giving developers a guided way to design,
understand, run, and improve multi-agent loops.

The MVP is not a generic autonomous-agent platform. It targets a precise
workflow: one developer, one repository, multiple Git worktrees, multiple
externally launched coding agents, an approved task graph, structured progress,
independent review, integration testing, and automatic failure routing.

The product's central belief is that the durable unit of agentic development is
an explainable loop with objective convergence—not an isolated prompt, chat, or
agent process.

## Problem

Coding-agent capability is advancing faster than the operating model around it.
A developer can ask multiple agents to work in parallel, but must still perform
the coordination manually:

- translate a goal into compatible units of work;
- decide which work can safely proceed in parallel;
- carry context and dependency updates between separate conversations;
- prevent agents from colliding in a checkout;
- determine whether reported progress is real;
- arrange review from a context independent of the implementer;
- combine branches and run spanning tests;
- identify which owner should react to an integration failure;
- control retries, concurrency, time, and cost;
- redesign the workflow when the same failure repeats.

Existing task boards describe work for humans. Shared files provide passive
state. Chat interfaces contain isolated conversational context. Generic
workflow builders expose primitives but assume the user already knows how to
compose a reliable agent loop. The product gap targeted here is a cohesive,
agent-agnostic operating model for multi-agent software delivery.

## Product thesis

The product has two inseparable capabilities:

1. **Coordination plane:** a durable, agent-agnostic system of record for goals,
   approved plans, ownership, dependencies, workspaces, attempts, evidence,
   blockers, reviews, integration runs, and outcomes.
2. **Loop Studio:** an assisted authoring environment that translates a natural
   language goal into an explainable workflow and teaches the developer how and
   why the stages, roles, context boundaries, verification, and stopping rules
   fit the problem.

The wedge is coding-agent coordination. The longer-term opportunity is to
become the system of record for agentic development workflows: planning,
execution, review, CI, policy, cost, knowledge, and team operations.

## Positioning

**The agent-agnostic, local mission control for building and running coding
loops.**

The initial differentiation is deliberately narrow:

- coding workflows rather than general enterprise AI;
- local repositories and worktrees rather than a cloud execution requirement;
- explicit task graphs and convergence loops rather than chat-first delegation;
- objective code, review, and test evidence rather than narrative status;
- interoperability across agent vendors and harnesses;
- explainable loop construction rather than a blank workflow canvas.

The product should be visually calm and operational rather than theatrical. Its
primary screen answers: **What is blocked, why, who owns the next action, and
what evidence will unblock it?**

## Intended customer and beta strategy

The product will begin as a closed commercial private beta with a small set of
developers already using more than one coding agent or maintaining custom
agent loops. The current repository remains an Apache-2.0 coordination-kernel
prototype; the precise code, repository, licensing, and distribution boundary
for commercial layers must be established before those layers are implemented.

The beta is not intended to validate whether developers like a novel UI. It is
intended to measure whether the system improves real engineering outcomes:

- fewer human coordination interventions per completed goal;
- shorter time from approved goal to green integration;
- lower blocker duration;
- fewer duplicate edits and workspace collisions;
- higher first-pass and eventual integration success;
- predictable token and monetary cost per accepted outcome;
- successful reuse of a loop on a second comparable problem.

The first paying customer segment remains a go-to-market decision. The product
architecture does not require multi-tenant enterprise infrastructure in the
MVP.

## MVP boundary

The MVP targets one repository on one developer machine. Agents are launched
separately by the developer and connect through CLI or MCP. The complete
acceptance scenario is:

1. A human creates a goal with acceptance criteria.
2. A designated lead agent proposes a loop and task dependency graph.
3. The proposal explains its stages, roles, context boundaries, verification,
   budgets, risks, and stopping conditions.
4. The human edits and approves an immutable loop revision.
5. Codex and Claude Code sessions register with stable identities and declared
   capabilities.
6. Each ready work item is claimed by one active owner in an exclusive Git
   worktree and branch.
7. Agents send structured progress, blocker, handoff, submission, review, and
   completion events through CLI or MCP.
8. Submitted changes follow an implement -> two adversarial reviews -> fix ->
   verify loop.
9. A designated integration agent combines eligible commits in an integration
   worktree and runs shared gates.
10. A failing gate creates an attributable blocker, routes it to the responsible
    owner, and re-enters the corrective loop.
11. The run completes only when approved acceptance criteria and shared gates
    pass.
12. The dashboard preserves the full causal history and foregrounds the next
    useful action.

### In scope

- local daemon, SQLite persistence, HTTP API, and live updates;
- local web dashboard;
- natural-language goal to explainable loop proposal;
- human editing and approval of versioned loop definitions;
- task DAGs and automatic readiness calculation;
- one active owner per work item;
- agent registration, capabilities, heartbeat leases, CLI, and MCP;
- Codex and Claude Code setup recipes over the generic contracts;
- Git repository, branch, and worktree validation;
- exclusive workspace leases;
- structured progress and evidence;
- one built-in implement/review/fix/verify workflow;
- integration work items, shared test gates, and failure routing;
- daemon-controlled admission limits for retries and concurrency, plus
  cooperative time, token, and cost tracking for external agents;
- event export and privacy-conscious beta telemetry.

### Out of scope

- launching, killing, or supervising coding-agent processes;
- remote or multi-machine workers;
- multi-repository workflows;
- multi-tenant organizations, SSO, RBAC, and enterprise policy administration;
- a general-purpose drag-and-drop workflow platform;
- arbitrary SaaS and data connectors;
- autonomous changes to approved loop structure;
- replacing Git hosting, CI, issue trackers, or code-review platforms;
- billing infrastructure beyond beta license/invite activation.

## User experience

### 1. Describe the outcome

The developer enters a goal, constraints, and acceptance criteria. The system
may inspect the repository through an agent, but should distinguish discovered
facts from assumptions.

### 2. Receive an explained loop proposal

The lead agent returns a structured proposal rather than only prose. Every
stage includes:

- purpose and output;
- dependencies and parallelism;
- suitable agent capabilities;
- context inputs and intentional exclusions;
- workspace strategy;
- verification evidence;
- retry and escalation behavior;
- resource budget;
- rationale and known risks.

The interface should teach the user why the loop fits the problem. It should
also explain when a problem is poorly suited to looping—for example, when it
lacks objective verification or contains tightly coupled edits that cannot be
partitioned safely.

### 3. Edit and approve

The developer can edit task boundaries, dependencies, role counts,
verification, budgets, and stopping conditions. Approval freezes an immutable
`LoopRevision`. Material changes create another revision and require approval.

### 4. Connect agents and workspaces

Separately launched agents register through `pc` CLI commands or equivalent MCP
tools. Registration includes a stable identity, harness, model when available,
capabilities, repository, worktree, branch, and base commit. A renewable lease
indicates liveness without giving the daemon process control.

### 5. Run and observe

Agents claim ready work, acknowledge constraints, and emit structured events.
The dashboard presents blockers, dependency state, ownership, leases,
convergence metrics, budgets, and evidence. Conversation history remains
available but is secondary to actionable state.

### 6. Integrate and learn

Submitted commits are reviewed and verified. Integration failures reopen
responsible work and preserve the evidence chain. Repeated or systematic
failures create a proposed loop revision with an explanation of what should
change. Successful revisions can be saved as reusable team templates.

## Architecture

### Local daemon

`pc serve` is the single local authority for the MVP. It owns:

- command validation and domain transitions;
- the SQLite connection and transactional event writes;
- current-state projections;
- coordination-message publication;
- HTTP API and authenticated local event stream;
- embedded or co-distributed dashboard assets;
- lease expiry, retry scheduling, work-admission limits, and budget tracking.

Only one daemon may own a project database at a time. The daemon exposes a
health endpoint and project identity so clients cannot accidentally connect to
the wrong repository.

The daemon binds to loopback by default. Project initialization creates a
random administrative credential stored with owner-only filesystem
permissions. CLI and MCP clients authenticate with short-lived session tokens
bound to their registered identities and active claims. The dashboard uses a
one-time bootstrap token to obtain a SameSite, HttpOnly session cookie. Mutating
HTTP endpoints validate `Host` and `Origin` and require CSRF protection.

The MVP trust boundary is the current operating-system user: another process
running as that user and able to read the project credential can impersonate a
client. Stronger process isolation belongs to the managed runtime, not to the
local HTTP protocol.

### Agent surfaces

`pc` is a thin CLI client. `pc mcp` is a thin stdio MCP bridge. Both call the
daemon and expose the same domain commands; neither duplicates coordination
logic or writes the database directly.

The generic agent contract includes operations equivalent to:

- register or renew a session;
- inspect available work and relevant context;
- claim or release a work item;
- report progress or a blocker;
- attach an artifact or handoff;
- submit an attempt;
- record review findings;
- record verification evidence;
- inspect messages, dependencies, and current run state.

Codex and Claude Code integrations are documented configuration recipes over
this contract. Vendor-specific behavior is not allowed inside the domain core.

### Durable state and live signaling

Each accepted command executes one SQLite transaction:

1. load and validate current aggregate state;
2. append one or more immutable domain events;
3. update query projections;
4. append an outbox entry for agent notifications;
5. commit.

A dispatcher publishes outbox entries through the existing protocol/bus and
marks them delivered idempotently. This avoids a committed state transition
without its corresponding notification.

The existing protocol remains responsible for live, intent-typed
communication, topic fan-out, threading, acknowledgements, and cooperative
interruption. Domain events remain the authoritative record of product state.

### Dashboard

The local web dashboard calls the daemon's read API and subscribes to a live
event stream. Its primary hierarchy is:

1. active blockers and budget/policy alerts;
2. next ready actions and their owners;
3. task and loop convergence;
4. agent and workspace health;
5. evidence and causal activity timeline;
6. raw conversations and logs.

The dashboard must remain useful with two agents and avoid visual patterns that
only look impressive at artificial swarm scale.

### Future managed runtime

Managed launching is deferred behind an `AgentRuntime` boundary responsible for
start, stop, observe, and resource accounting. Adding a runtime later must not
change loop definitions, work-item semantics, evidence, leases, or events.

## Domain model

### Goal

A human-owned desired outcome with constraints and acceptance criteria. A goal
references the approved loop revision and its current run.

### LoopRevision

An immutable, versioned workflow definition containing:

- stages and dependency edges;
- role and capability requirements;
- context assembly rules;
- partition and shard rules;
- workspace policy;
- verification and evidence contracts;
- retry and escalation rules;
- stop conditions;
- concurrency, token, time, and cost budgets;
- assumptions, rationale, and risks.

Only one revision is approved for a run. Runtime observations may create a
proposed successor revision but may not activate it automatically in the MVP.

### Run

One execution of one approved loop revision. States:

`queued -> running -> converged | failed | cancelled`

`blocked` is a derived operational condition when no useful work can proceed,
not a terminal run state.

### WorkItem

A runtime instance of a stage or shard with one active owner. States:

`ready -> claimed -> in_progress -> submitted -> reviewing -> verified`

A work item may become `blocked`, return to `in_progress` after resolution, or
be `cancelled`. Dependency completion determines readiness.

Review stages create separate child work items—one per reviewer—so each review
has one owner, an independent context, its own attempts, and explicit evidence.
Those child work items may run concurrently. The submitted implementation work
item remains in `reviewing` until all required review children finish and their
findings are reconciled. A fixer is another child work item rather than a second
simultaneous owner of the implementation work item.

### Attempt

One bounded execution cycle for a work item. It records the acting session,
input context references, instructions, start/end times, artifacts, review
findings, verification results, token/cost data when available, and outcome.

### AgentSession

A stable logical identity plus a renewable connection lease. It records the
agent harness, declared capabilities, optional model metadata, current work,
and last heartbeat. Lease expiry releases claims only according to policy; it
does not assume the external process was terminated.

### WorkspaceLease

An exclusive coordination-plane claim for one work item to modify a validated
Git worktree and branch. It records repository identity, canonical path,
branch, base commit, holder, expiry, and allowed command policy. The claim is a
cooperative contract in the external-agent MVP, not filesystem containment. A
session and workspace are separate concepts so future high-concurrency runs can
use shard-level worktrees rather than one worktree per process.

### Artifact

Immutable or content-addressed evidence: commit SHA, diff reference, test
result, review finding, log, report, handoff note, or generated guide. Large
payloads live outside event rows and are referenced by digest and location.

### Blocker

A first-class impediment connected to a run and one or more affected work
items. It records its source, current owner, evidence, attribution method and
confidence, resolution, and lifecycle:

`open -> acknowledged -> resolved | dismissed`

An open blocker makes its affected work unavailable unless the approved loop
defines other useful work that can proceed. Resolution is an explicit event and
may return a work item to `in_progress`, create corrective work, or propose a
new loop revision.

### Event

The immutable causal record. Every event includes project, run, actor,
correlation, causation, timestamp, schema version, and structured payload.
Events are append-only; corrections are new events.

## Built-in loop

The MVP includes one opinionated workflow:

1. **Implement:** one agent receives the approved task context and creates a
   commit in the leased worktree.
2. **Adversarial review:** two independent child work items receive the task,
   acceptance criteria, and diff but not the implementer's private reasoning.
   Each reviewer owns one child item and tries to find reasons the change is
   wrong.
3. **Fix:** a fixer child work item receives deduplicated findings and produces
   a new commit in the implementation workspace under a renewed exclusive
   lease.
4. **Verify:** deterministic commands run and attach structured evidence.
5. **Decide:** verified work proceeds to integration; unresolved findings or
   failed evidence create another bounded attempt; exhausted limits escalate.

The role counts are defaults and can be edited before approval. The system
must not claim that two AI reviews guarantee correctness. Confidence comes from
independent contexts plus objective verification.

## Integration flow

A designated integration work item becomes ready when its required submitted
work is verified. The integration agent:

1. leases a dedicated integration worktree;
2. records the exact input commit SHAs;
3. combines them in deterministic order;
4. records merge or cherry-pick conflicts as evidence;
5. runs configured shared gates;
6. publishes the verdict.

On success, the participating work items and goal advance. On failure, the MVP
attributes a blocker only when evidence provides a deterministic mapping: a
structured verifier result identifies an owning work item, a merge conflict
identifies the contributing commits, or exactly one submitted work item changed
the failing component. Otherwise, the system creates an integration-level
blocker and a dedicated diagnostic work item listing the candidate inputs. It
does not guess. The existing `pkg/gate` readiness and verdict model is the
starting substrate, not the final persistence model.

## Progress and convergence

Agents report semantic milestones but do not assign arbitrary completion
percentages. The control plane derives progress from evidence such as:

- dependency edges satisfied;
- files or shards processed;
- compiler errors remaining;
- failing tests remaining;
- review findings open versus resolved;
- required platforms or gates passing;
- retries and budget remaining.

The dashboard may summarize these signals, but must keep the underlying counts
and evidence inspectable.

## Safety, budgets, and recovery

### Command and workspace policy

The product records a command and workspace policy per loop. In the MVP this is
a cooperative contract: the daemon can reject commands requested through its
own API, refuse invalid leases, inspect Git state, detect policy violations, and
halt further work admission. It cannot prevent an independently launched agent
from using its separate shell to modify files or run Git directly. The MVP
should detect and surface shared stash use, unrelated resets, branch changes
outside the lease, and cross-worktree modifications when observable.

Actual command denial, filesystem containment, and process termination require
the future managed runtime or an external sandbox. The UI and documentation
must not describe cooperative policy as containment.

### Budget admission and tracking

Each loop and run defines ceilings for concurrency, attempts, wall-clock time,
tokens where measurable, and estimated cost. Reservation and consumption are
atomic for work the daemon admits. Exhaustion prevents new claims or attempts
and creates an operator decision.

Because external agents own their processes and provider sessions, token and
cost figures may be delayed, incomplete, or self-reported; the daemon cannot
guarantee that an already-running process stops spending. Hard provider-level
ceilings and process termination are managed-runtime capabilities. The MVP must
label estimated versus enforced budgets accurately and never silently admit new
work after a daemon-controlled limit is exhausted.

### Idempotency

Every client command carries an idempotency key. Event append, projection
updates, claims, artifact attachment, and notification dispatch are safe to
retry after client or daemon failure.

### Leases and disappearance

Expired agent or workspace leases make work visibly unattended. The daemon
does not immediately reassign mutable work because the external process may
still be alive. Policy chooses between a grace period, human confirmation, and
safe reassignment from the last committed artifact.

### Repeated failure

The system distinguishes:

- a local implementation defect, which creates another attempt;
- a missing dependency or ambiguous requirement, which blocks and escalates;
- a systematic workflow defect, which proposes a revised loop;
- an infrastructure failure, which retries without blaming code;
- an unattributable integration failure, which creates diagnostic work.

## Privacy and beta telemetry

The local event store is the source of truth. Beta telemetry is explicit,
documented, and minimizes source-code or prompt content. Preferred aggregate
signals include timings, state transitions, counts, error classes, budgets,
and feature usage. Users can inspect and export what would be sent.

Rich session export for support is an explicit user action and should support
redaction. Product analytics must never become an undisclosed copy of a
customer's repository, conversations, or credentials.

## Testing strategy

### Domain tests

- command validation and every allowed/disallowed state transition;
- immutable loop revision approval;
- DAG cycle rejection and readiness calculation;
- exclusive claims and workspace leases;
- attempt, retry, escalation, and budget boundaries;
- idempotent command replay;
- failure attribution and ambiguous-failure diagnostics;
- projection rebuild from the event log.

### Persistence tests

- transactional event, projection, and outbox writes;
- daemon restart and outbox redelivery;
- schema migration and event-version compatibility;
- lease expiry and grace behavior;
- concurrent claim and budget-reservation races under `-race`.

### Adapter contract tests

One reusable suite verifies CLI and MCP adapters against the same domain
behavior. Codex and Claude Code recipes receive scripted smoke tests where their
harnesses permit it.

Before every beta release, the team also runs and records the complete
registration, claim, progress, artifact, review, blocker, and completion flow
through real Codex and Claude Code sessions. These release-level acceptance
runs complement deterministic fake-client tests and verify that the published
setup recipes still match the actual harnesses.

### End-to-end acceptance test

A deterministic fixture repository runs the full MVP story with fake agent
clients: plan approval, parallel claims, separate worktrees, submission,
independent reviews, a deliberately failing integration gate, failure routing,
correction, and a final passing verdict. The resulting event log is replayed to
prove the dashboard projection reaches the same state.

### Product validation

Private-beta runs are evaluated by outcomes, not generated lines or agent
activity. The team reviews intervention count, time-to-green, blocker duration,
cost, regressions, and loop reuse. A workflow that generates large quantities
of unreviewable code is considered a product failure even if throughput looks
impressive.

## Delivery sequence

The product is large enough to require staged implementation while preserving
the end-to-end thesis.

### Milestone 1 — Durable control plane

Local daemon, event store, projections, goals, approved loop revisions, work
items, agent sessions, leases, CLI, and read API.

### Milestone 2 — Agent coordination

MCP bridge, Codex/Claude recipes, structured progress, blockers, artifacts,
handoffs, and actionable dashboard.

### Milestone 3 — Built-in convergence loop

Implementer/reviewer/fixer roles, attempts, evidence contracts, deterministic
verification, retries, budgets, and loop-run visualization.

### Milestone 4 — Integration and beta

Integration worktrees, shared gates, failure attribution and routing, complete
acceptance fixture, signed distribution, invite activation, feedback capture,
and privacy-conscious telemetry.

### After validated MVP

Managed agent runtimes, remote workers, multi-repository workflows, team and
enterprise controls, broader loop templates, and data-driven loop suggestions.

## Strategic risks

- **Scope expansion:** the DX opportunity is broad, but the initial product
  must win multi-agent coding convergence before absorbing adjacent tools.
- **False confidence:** multiple AI reviewers can agree and still be wrong;
  deterministic evidence and human accountability remain essential.
- **Cost opacity:** concurrency can create attractive activity while destroying
  unit economics; accepted-outcome metrics, honest estimates, and hard limits
  where the daemon or a future runtime has control are core product features.
- **Integration bottleneck:** parallel generation can exceed the rate at which
  changes can be safely combined; shard boundaries and verifier throughput must
  constrain concurrency.
- **Agent-vendor capture:** vendor-specific features are useful at the edge but
  must not leak into the core contract.
- **Licensing ambiguity:** the present Apache-2.0 repository and intended closed
  commercial layers require an explicit boundary before product code is added.

## Decisions intentionally deferred

These decisions do not block the single-machine MVP and will be made using beta
evidence before their corresponding milestones:

- the first paid segment: individual power users, small software teams, or
  enterprise engineering organizations;
- pricing unit: seat, active agent, workflow run, accepted outcome, or hybrid;
- commercial code location and licensing model;
- hosted control plane versus local/team deployment after MVP;
- autonomous loop-revision limits after human-approved revisioning proves safe;
- the first workflow templates beyond implement/review/fix/verify.

## Success definition

The MVP succeeds when a developer can connect two different coding-agent
harnesses, approve an understandable loop, leave them to execute independently
in isolated worktrees, and return to a dashboard that accurately identifies the
next action—through at least one integration failure and corrective cycle—until
the repository reaches a reproducibly green result.
