# Runtime, Workspace, and the Token-Free Loop (Phase A) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the full Parallel Consciousness coordination loop — leases, agent runtime, courier, gate, CLI — and prove it end to end with a scripted fake runtime, spending no model tokens.

**Architecture:** A harness-neutral `runtime.Runtime` interface spawns agent sessions; `pkg/workspace` gives each an exclusive git worktree; a `pkg/agent` courier bridges bus messages into session control verbs; `internal/pcops` composes these with the existing `pkg/gate` and `pkg/bus/sqlite`. `cmd/pc` is a thin CLI over `pcops`. Phase A's final task runs the whole loop against a scripted fake that fails the gate on round one and passes on round two.

**Tech Stack:** Go 1.22, `modernc.org/sqlite` (pure Go), `gopkg.in/yaml.v3`, `git` CLI, stdlib `os/exec`.

**Spec:** `docs/superpowers/specs/2026-09-01-runtime-workspace-poc-design.md`, as amended by `docs/superpowers/specs/2026-09-01-day-0-spike-findings.md`. Read both before starting.

## Global Constraints

- Module path is `github.com/KJFromMicromonic/parallel-consciousness`.
- **Go floor stays `go 1.22`.** Do not bump it. The `go 1.23` bump recorded in `97664d7` was forced solely by the MCP `go-sdk`, and `pc mcp` is out of scope for this phase.
- **`modernc.org/sqlite` stays pinned at `v1.33.1`.** Never run `go get modernc.org/sqlite@latest` — newer versions raise the Go floor.
- **Exactly one new module dependency is permitted in this phase: `gopkg.in/yaml.v3`.** Add no others. Pure Go only; no cgo.
- The `pkg/runtime` package itself imports **only the standard library**. Its `pkg/runtime/runtimetest` and `pkg/runtime/fake` subpackages may additionally import `pkg/runtime`.
- **Nothing under `pkg/` may import `internal/runtime/pi`.** Task 4 enforces this with a test.
- Leases live in the **same SQLite file as the bus**. Do not create a second store.
- `go test ./...` must pass with no network access and no API keys at every commit.
- Follow the existing house style: package-level doc comments explaining *why*, table-free small tests, `t.Helper()` on helpers, errors wrapped with `%w`.

---

## File Structure

| File | Responsibility |
|---|---|
| `pkg/bus/sqlite/sqlite.go` (modify) | cursor monotonicity guard |
| `pkg/bus/sqlite/cursor_internal_test.go` (create) | white-box cursor test |
| `pkg/workspace/git.go` (create) | git worktree add/remove plumbing |
| `pkg/workspace/workspace.go` (create) | `Manager`, `Lease`, acquire/release/heartbeat |
| `pkg/workspace/workspace_test.go` (create) | lease semantics against a temp repo |
| `pkg/runtime/runtime.go` (create) | `Runtime`, `Session`, `Spec`, `Event`, `Outcome` |
| `internal/arch/arch_test.go` (create) | import-boundary enforcement |
| `pkg/runtime/fake/fake.go` (create) | scriptable fake runtime |
| `pkg/runtime/runtimetest/runtimetest.go` (create) | adapter conformance suite |
| `internal/pcops/config.go` (create) | `.pc.yaml` loading + env overrides |
| `internal/pcops/submit.go` (create) | readiness + block for verdict |
| `internal/pcops/up.go` (create) | coordinator host |
| `internal/pcops/rungate.go` (create) | runner: merge branches, run test, report |
| `internal/pcops/send.go` (create) | agent-to-agent message |
| `internal/pcops/courier.go` (create) | bus → session verb bridge |
| `internal/pcops/run.go` (create) | scenario orchestrator |
| `internal/pcops/run_test.go` (create) | the deterministic whole-loop test |
| `cmd/pc/main.go` (create) | subcommand dispatch |

---

### Task 1: Cursor monotonicity guard

The day-0 spike found `saveCursor` upserts unconditionally. Two subscribers sharing an agent name share one `cursors` row, so a subscriber that is behind rewinds the stored position and the next subscription replays consumed messages. This lands first because the courier (Task 11) makes it reachable.

**Files:**
- Modify: `pkg/bus/sqlite/sqlite.go:310-319`
- Test: `pkg/bus/sqlite/cursor_internal_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: no API change. `saveCursor(ctx context.Context, agent string, seq int64)` becomes monotonic.

- [ ] **Step 1: Write the failing test**

Create `pkg/bus/sqlite/cursor_internal_test.go`. Note the package is `sqlite`, not `sqlite_test` — this is a white-box test because `saveCursor` and `initCursor` are unexported.

```go
package sqlite

import (
	"context"
	"path/filepath"
	"testing"
)

// A subscriber that is behind must never rewind the stored cursor: two
// subscribers share one row per agent name, and a rewind replays consumed
// messages into the next subscription.
func TestSaveCursorIsMonotonic(t *testing.T) {
	ctx := context.Background()
	b, err := Open(ctx, filepath.Join(t.TempDir(), "bus.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })

	b.saveCursor(ctx, "a", 10)
	b.saveCursor(ctx, "a", 4)

	got, err := b.initCursor(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if got != 10 {
		t.Fatalf("cursor rewound to %d, want 10", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/bus/sqlite/ -run TestSaveCursorIsMonotonic -v`
Expected: FAIL with `cursor rewound to 4, want 10`

- [ ] **Step 3: Add the monotonicity guard**

In `pkg/bus/sqlite/sqlite.go`, change the `saveCursor` statement:

```go
	_, err := b.db.ExecContext(ctx,
		`INSERT INTO cursors (agent, last_seq) VALUES (?, ?)
		 ON CONFLICT(agent) DO UPDATE SET last_seq = MAX(cursors.last_seq, excluded.last_seq)`,
		agent, seq)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/bus/...`
Expected: PASS, including the existing conformance suites.

- [ ] **Step 5: Commit**

```bash
git add pkg/bus/sqlite/sqlite.go pkg/bus/sqlite/cursor_internal_test.go
git commit -m "fix(bus/sqlite): make the durable cursor monotonic

Two subscribers sharing an agent name share one cursors row. Without a
guard, one that is behind rewinds the stored position and the next
subscription replays consumed messages. Found by the day-0 spike."
```

---

### Task 2: Git worktree plumbing

**Files:**
- Create: `pkg/workspace/git.go`
- Test: `pkg/workspace/git_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `func worktreeAdd(repo, path, branch string) error` and `func worktreeRemove(repo, path string) error` (both unexported, used by Task 3). Also the test helper `newRepo(t *testing.T) string`, reused in Task 3's tests.

- [ ] **Step 1: Write the failing test**

Create `pkg/workspace/git_test.go`:

```go
package workspace

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// newRepo creates a temp git repo with one commit on branch main.
func newRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		c := exec.Command("git", append([]string{"-C", dir}, args...)...)
		c.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com",
			"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com")
		if out, err := c.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	run("init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("seed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	run("add", ".")
	run("commit", "-q", "-m", "init")
	return dir
}

func TestWorktreeAddCreatesIsolatedTree(t *testing.T) {
	repo := newRepo(t)
	wt := filepath.Join(t.TempDir(), "billing")

	if err := worktreeAdd(repo, wt, "agent/billing"); err != nil {
		t.Fatalf("worktreeAdd: %v", err)
	}
	if _, err := os.Stat(filepath.Join(wt, "README.md")); err != nil {
		t.Fatalf("worktree missing seed file: %v", err)
	}

	// A write in the worktree must not appear in the origin repo.
	if err := os.WriteFile(filepath.Join(wt, "only-here.txt"), []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(repo, "only-here.txt")); !os.IsNotExist(err) {
		t.Fatal("worktree write leaked into the origin repo")
	}

	if err := worktreeRemove(repo, wt); err != nil {
		t.Fatalf("worktreeRemove: %v", err)
	}
	if _, err := os.Stat(wt); !os.IsNotExist(err) {
		t.Fatal("worktree directory still present after remove")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/workspace/ -run TestWorktreeAdd -v`
Expected: FAIL to compile — `undefined: worktreeAdd`

- [ ] **Step 3: Write the implementation**

Create `pkg/workspace/git.go`:

```go
// Package workspace hands each agent an exclusive git worktree. Isolation is
// structural rather than advisory: an agent cannot see another agent's edits,
// so "one active owner" is enforced by the filesystem instead of by protocol
// etiquette.
package workspace

import (
	"fmt"
	"os/exec"
)

// worktreeAdd creates (or resets) branch at the repo's HEAD and checks it out
// into its own worktree at path. -B is deliberate: re-acquiring a lease after a
// crash must be idempotent.
func worktreeAdd(repo, path, branch string) error {
	out, err := exec.Command("git", "-C", repo, "worktree", "add", "-B", branch, path).CombinedOutput()
	if err != nil {
		return fmt.Errorf("git worktree add %s: %w: %s", path, err, out)
	}
	return nil
}

// worktreeRemove detaches the worktree and deletes its directory. --force is
// required because an agent almost always leaves uncommitted work behind.
func worktreeRemove(repo, path string) error {
	out, err := exec.Command("git", "-C", repo, "worktree", "remove", "--force", path).CombinedOutput()
	if err != nil {
		return fmt.Errorf("git worktree remove %s: %w: %s", path, err, out)
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/workspace/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/workspace/git.go pkg/workspace/git_test.go
git commit -m "feat(workspace): git worktree plumbing"
```

---

### Task 3: Lease bookkeeping

**Files:**
- Create: `pkg/workspace/workspace.go`
- Test: `pkg/workspace/workspace_test.go`

**Interfaces:**
- Consumes: `worktreeAdd`, `worktreeRemove`, `newRepo` from Task 2.
- Produces:
  - `func New(ctx context.Context, repo, root, dbPath string) (*Manager, error)`
  - `func (m *Manager) SetTTL(d time.Duration)`
  - `func (m *Manager) Acquire(ctx context.Context, agent, branch string) (*Lease, error)`
  - `func (m *Manager) Close() error`
  - `type Lease struct { Agent, Path, Branch string }`
  - `func (l *Lease) Heartbeat(ctx context.Context) error`
  - `func (l *Lease) Release(ctx context.Context) error`
  - `var ErrLeased = errors.New("workspace: already leased by a live holder")`

  Note: the design spec sketched `New(repo, root string)`. The real signature carries the SQLite path, because leases share the bus's database file.

- [ ] **Step 1: Write the failing test**

Create `pkg/workspace/workspace_test.go`:

```go
package workspace

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func newManager(t *testing.T, repo string) *Manager {
	t.Helper()
	m, err := New(context.Background(), repo, t.TempDir(), filepath.Join(t.TempDir(), "bus.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

func TestAcquireIsExclusive(t *testing.T) {
	ctx := context.Background()
	repo := newRepo(t)
	m := newManager(t, repo)

	l, err := m.Acquire(ctx, "billing", "agent/billing")
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	if l.Path == "" {
		t.Fatal("lease has no path")
	}

	if _, err := m.Acquire(ctx, "billing", "agent/billing"); !errors.Is(err, ErrLeased) {
		t.Fatalf("second Acquire = %v, want ErrLeased", err)
	}

	if err := l.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, err := m.Acquire(ctx, "billing", "agent/billing"); err != nil {
		t.Fatalf("Acquire after Release: %v", err)
	}
}

func TestStaleLeaseIsReclaimable(t *testing.T) {
	ctx := context.Background()
	repo := newRepo(t)
	m := newManager(t, repo)
	m.SetTTL(30 * time.Millisecond)

	if _, err := m.Acquire(ctx, "gateway", "agent/gateway"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond) // holder "crashed": no heartbeat

	if _, err := m.Acquire(ctx, "gateway", "agent/gateway"); err != nil {
		t.Fatalf("reclaim of stale lease: %v", err)
	}
}

func TestHeartbeatKeepsLeaseHeld(t *testing.T) {
	ctx := context.Background()
	repo := newRepo(t)
	m := newManager(t, repo)
	m.SetTTL(80 * time.Millisecond)

	l, err := m.Acquire(ctx, "billing", "agent/billing")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		time.Sleep(30 * time.Millisecond)
		if err := l.Heartbeat(ctx); err != nil {
			t.Fatalf("Heartbeat: %v", err)
		}
	}
	if _, err := m.Acquire(ctx, "billing", "agent/billing"); !errors.Is(err, ErrLeased) {
		t.Fatalf("live lease was stolen: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/workspace/ -run TestAcquire -v`
Expected: FAIL to compile — `undefined: New`

- [ ] **Step 3: Write the implementation**

Create `pkg/workspace/workspace.go`:

```go
package workspace

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// ErrLeased means a live holder already owns that workspace.
var ErrLeased = errors.New("workspace: already leased by a live holder")

const leaseSchema = `
CREATE TABLE IF NOT EXISTS leases (
  path         TEXT PRIMARY KEY,
  agent        TEXT NOT NULL,
  branch       TEXT NOT NULL,
  heartbeat_at INTEGER NOT NULL
);`

// Manager hands out exclusive worktrees off one repository. Lease rows live in
// the same SQLite file as the bus so there is exactly one store to reason about.
type Manager struct {
	repo string
	root string
	db   *sql.DB

	mu  sync.Mutex
	ttl time.Duration
}

// New opens the lease store and ensures the schema exists. repo is the source
// repository, root is the directory worktrees are created under, dbPath is the
// bus's SQLite file.
func New(ctx context.Context, repo, root, dbPath string) (*Manager, error) {
	// Same pragmas as pkg/bus/sqlite.Open. Without WAL and a busy timeout this
	// connection contends with the bus's writes on the very same file and fails
	// with "database is locked".
	dsn := "file:" + dbPath + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open lease store: %w", err)
	}
	if _, err := db.ExecContext(ctx, leaseSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("lease schema: %w", err)
	}
	return &Manager{repo: repo, root: root, db: db, ttl: 30 * time.Second}, nil
}

// SetTTL sets how long a lease survives without a heartbeat before another
// caller may reclaim it. Default 30s.
func (m *Manager) SetTTL(d time.Duration) {
	m.mu.Lock()
	m.ttl = d
	m.mu.Unlock()
}

func (m *Manager) leaseTTL() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ttl
}

func (m *Manager) Close() error { return m.db.Close() }

// Acquire takes the lease for agent and materialises its worktree. The insert
// claims the row only when it is free or stale, so exclusivity is decided by
// SQLite rather than by a read-then-write race in Go.
func (m *Manager) Acquire(ctx context.Context, agent, branch string) (*Lease, error) {
	path := filepath.Join(m.root, agent)
	now := time.Now().UnixMilli()
	staleBefore := now - m.leaseTTL().Milliseconds()

	res, err := m.db.ExecContext(ctx,
		`INSERT INTO leases (path, agent, branch, heartbeat_at) VALUES (?, ?, ?, ?)
		 ON CONFLICT(path) DO UPDATE SET
		   agent = excluded.agent,
		   branch = excluded.branch,
		   heartbeat_at = excluded.heartbeat_at
		 WHERE leases.heartbeat_at < ?`,
		path, agent, branch, now, staleBefore)
	if err != nil {
		return nil, fmt.Errorf("claim lease %s: %w", path, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("claim lease %s: %w", path, err)
	}
	if n == 0 {
		return nil, ErrLeased
	}

	if err := worktreeAdd(m.repo, path, branch); err != nil {
		_, _ = m.db.ExecContext(ctx, `DELETE FROM leases WHERE path = ?`, path)
		return nil, err
	}
	return &Lease{Agent: agent, Path: path, Branch: branch, m: m}, nil
}

// Lease is one agent's exclusive hold on a worktree. Path becomes the spawned
// session's working directory.
type Lease struct {
	Agent  string
	Path   string
	Branch string

	m *Manager
}

// Heartbeat renews the lease. A holder that stops heartbeating is presumed dead
// and its workspace becomes reclaimable.
func (l *Lease) Heartbeat(ctx context.Context) error {
	_, err := l.m.db.ExecContext(ctx,
		`UPDATE leases SET heartbeat_at = ? WHERE path = ? AND agent = ?`,
		time.Now().UnixMilli(), l.Path, l.Agent)
	if err != nil {
		return fmt.Errorf("heartbeat %s: %w", l.Path, err)
	}
	return nil
}

// Release removes the worktree and drops the lease row.
func (l *Lease) Release(ctx context.Context) error {
	if err := worktreeRemove(l.m.repo, l.Path); err != nil {
		return err
	}
	_, err := l.m.db.ExecContext(ctx, `DELETE FROM leases WHERE path = ? AND agent = ?`, l.Path, l.Agent)
	if err != nil {
		return fmt.Errorf("release %s: %w", l.Path, err)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/workspace/ -v`
Expected: PASS — all four tests.

- [ ] **Step 5: Commit**

```bash
git add pkg/workspace/workspace.go pkg/workspace/workspace_test.go
git commit -m "feat(workspace): exclusive worktree leases with heartbeats"
```

---

### Task 4: Runtime types and the import boundary

`pkg/runtime` is public API so a Codex or Claude Code adapter stays writable by anyone. The boundary test is what keeps the harness-agnostic claim true rather than aspirational.

**Files:**
- Create: `pkg/runtime/runtime.go`
- Create: `internal/arch/arch_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: the full `pkg/runtime` API used by every later task — `Runtime`, `Session`, `Spec`, `Budget`, `Event`, `EventKind` constants, `ToolUse`, `Queue`, `Outcome`, `ExitReason`.

- [ ] **Step 1: Write the failing test**

Create `internal/arch/arch_test.go`:

```go
// Package arch holds architecture tests. They protect boundaries that no
// compiler check enforces: the harness-agnostic claim is only true while
// pkg/ stays free of harness-specific imports.
package arch

import (
	"os/exec"
	"strings"
	"testing"
)

const modulePath = "github.com/KJFromMicromonic/parallel-consciousness"

// deps returns package import path -> its full transitive dependency list.
// It shells out to `go list` so the check adds no module dependency.
func deps(t *testing.T) map[string][]string {
	t.Helper()
	out, err := exec.Command("go", "list", "-f", `{{.ImportPath}} {{join .Deps " "}}`, "../../...").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	m := map[string][]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		m[fields[0]] = fields[1:]
	}
	return m
}

func TestPkgNeverImportsAHarnessAdapter(t *testing.T) {
	for pkg, ds := range deps(t) {
		if !strings.HasPrefix(pkg, modulePath+"/pkg/") {
			continue
		}
		for _, d := range ds {
			if strings.HasPrefix(d, modulePath+"/internal/runtime/") {
				t.Errorf("%s imports harness adapter %s: pkg/ must stay harness-agnostic", pkg, d)
			}
		}
	}
}

func TestRuntimeInterfaceIsStdlibOnly(t *testing.T) {
	for _, d := range deps(t)[modulePath+"/pkg/runtime"] {
		// Standard library import paths have no dot in their first segment.
		if strings.Contains(strings.Split(d, "/")[0], ".") {
			t.Errorf("pkg/runtime imports non-stdlib package %s", d)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/arch/ -v`
Expected: FAIL — `TestRuntimeInterfaceIsStdlibOnly` finds no `pkg/runtime` entry, or `go list` errors because the package does not exist yet.

- [ ] **Step 3: Write the implementation**

Create `pkg/runtime/runtime.go`:

```go
// Package runtime is the harness-neutral contract for launching and supervising
// a coding agent. It imports only the standard library, and deliberately names
// nothing after any particular harness: adapters live behind this interface so
// the control plane never grows a dependency on one vendor's event model.
package runtime

import (
	"context"
	"time"
)

// Runtime launches agent sessions.
type Runtime interface {
	// Name identifies the adapter, e.g. "pi". Recorded in evidence.
	Name() string
	Start(ctx context.Context, spec Spec) (Session, error)
}

// Spec describes one agent session to launch.
type Spec struct {
	Agent   string            // control-plane-assigned identity
	Workdir string            // the lease path
	Role    string            // role framing, prefixed onto the task
	Task    string            // initial instruction
	Env     map[string]string // e.g. PC_AGENT, PC_DB, provider credentials
	Model   string            // opaque; the adapter maps it
	Budget  Budget
}

// Budget bounds a session. Wall is enforced by the caller; Tokens is reported
// only, because enforcement needs a policy decision this layer should not make.
type Budget struct {
	Wall   time.Duration
	Tokens int64 // 0 means unbounded
}

// Session is one running agent.
//
// Steer is NOT preemption. Adapters deliver it at the next turn boundary, so a
// message sent while a tool call is in flight waits for that call to finish.
// Interrupt is the only verb that preempts in-flight work.
type Session interface {
	Events() <-chan Event
	Steer(ctx context.Context, s string) error
	Follow(ctx context.Context, s string) error
	Interrupt(ctx context.Context) error
	Close(ctx context.Context) error
	Wait(ctx context.Context) (Outcome, error)
}

// EventKind is the control plane's vocabulary for what an agent is doing. It is
// deliberately smaller than any adapter's native event set.
type EventKind string

const (
	KindStarted      EventKind = "started"
	KindTurnBegan    EventKind = "turn_began"
	KindTurnEnded    EventKind = "turn_ended"
	KindToolUsed     EventKind = "tool_used"
	KindQueueChanged EventKind = "queue_changed"
	KindIdle         EventKind = "idle"
	KindErrored      EventKind = "errored"
	KindExited       EventKind = "exited"
)

// Event carries no adapter-native payload on purpose: a raw field would be the
// seam through which harness specifics reach callers, and no import test would
// catch it. Adapters write raw frames to a transcript file instead.
type Event struct {
	Kind    EventKind
	At      time.Time
	Agent   string
	Tool    *ToolUse // set when Kind == KindToolUsed
	Pending *Queue   // set when Kind == KindQueueChanged
	Err     string
}

// ToolUse is observed evidence: what the agent actually did, rather than what
// it reported doing.
type ToolUse struct {
	Name   string // read, write, edit, bash, grep, find, ls
	Target string // path or command
	Detail string
	Ok     bool
}

// Queue reports what an adapter has accepted but not yet applied — the receipt
// that separates "delivered" from "acted on".
type Queue struct {
	Steering int
	FollowUp int
}

// ExitReason explains why a session ended.
type ExitReason string

const (
	ExitCompleted   ExitReason = "completed"
	ExitInterrupted ExitReason = "interrupted"
	ExitBudget      ExitReason = "budget"
	ExitError       ExitReason = "error"
)

// Outcome summarises a finished session.
type Outcome struct {
	Reason   ExitReason
	Tokens   int64
	Duration time.Duration
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/arch/ ./pkg/runtime/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/runtime/runtime.go internal/arch/arch_test.go
git commit -m "feat(runtime): harness-neutral session contract + import boundary test"
```

---

### Task 5: Fake runtime and the conformance suite

The first `Runtime` implementation is a fake, not a harness. That is what makes the loop testable in CI at zero token cost — the same pattern `pkg/bus/bustest` already established for transports.

**Files:**
- Create: `pkg/runtime/runtimetest/runtimetest.go`
- Create: `pkg/runtime/fake/fake.go`
- Test: `pkg/runtime/fake/fake_test.go`

**Interfaces:**
- Consumes: all of `pkg/runtime` from Task 4.
- Produces:
  - `func runtimetest.Run(t *testing.T, newRuntime func(t *testing.T) runtime.Runtime, spec runtime.Spec)`
  - `type fake.Action interface{}` with implementations `fake.Write{Path, Content string}`, `fake.Exec{Args []string}`, `fake.Emit{Tool runtime.ToolUse}`
  - `type fake.Script struct { OnStart []Action; OnSteer func(text string) []Action }`
  - `func fake.New(scripts map[string]Script) *fake.Runtime` — keyed by `Spec.Agent`

- [ ] **Step 1: Write the failing test**

Create `pkg/runtime/runtimetest/runtimetest.go` — this is the suite, not a `_test.go` file, so adapters can call it:

```go
// Package runtimetest holds the conformance suite every runtime.Runtime adapter
// must pass. Properties are about lifecycle only, never about what a model
// says, so both the fake and a real harness can satisfy them.
package runtimetest

import (
	"context"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime"
)

// Run executes the suite. newRuntime must return a fresh Runtime; spec must be
// a task the adapter can actually complete.
func Run(t *testing.T, newRuntime func(t *testing.T) runtime.Runtime, spec runtime.Spec) {
	t.Run("ReachesIdle", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s, err := newRuntime(t).Start(ctx, spec)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close(ctx)

		var sawStarted bool
		for ev := range s.Events() {
			switch ev.Kind {
			case runtime.KindStarted:
				sawStarted = true
			case runtime.KindIdle:
				if !sawStarted {
					t.Fatal("idle before started")
				}
				return
			}
		}
		t.Fatal("event stream closed before idle")
	})

	t.Run("CloseEndsTheStreamAndWaitReturns", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s, err := newRuntime(t).Start(ctx, spec)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Close(ctx); err != nil {
			t.Fatalf("Close: %v", err)
		}
		for range s.Events() { // must drain and close, not block
		}
		if _, err := s.Wait(ctx); err != nil {
			t.Fatalf("Wait after Close: %v", err)
		}
	})

	t.Run("SteerIsAccepted", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s, err := newRuntime(t).Start(ctx, spec)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close(ctx)
		if err := s.Steer(ctx, "noted"); err != nil {
			t.Fatalf("Steer: %v", err)
		}
	})
}
```

Then create `pkg/runtime/fake/fake_test.go`:

```go
package fake_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime/fake"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime/runtimetest"
)

func TestFakeConformance(t *testing.T) {
	runtimetest.Run(t, func(t *testing.T) runtime.Runtime {
		return fake.New(map[string]fake.Script{
			"a": {OnStart: []fake.Action{fake.Emit{Tool: runtime.ToolUse{Name: "read", Target: "x", Ok: true}}}},
		})
	}, runtime.Spec{Agent: "a", Workdir: t.TempDir()})
}

func TestSteerRunsTheSteerScript(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	r := fake.New(map[string]fake.Script{
		"a": {
			OnStart: []fake.Action{fake.Write{Path: "original.txt", Content: "ORIGINAL"}},
			OnSteer: func(text string) []fake.Action {
				return []fake.Action{fake.Write{Path: "steered.txt", Content: text}}
			},
		},
	})
	s, err := r.Start(ctx, runtime.Spec{Agent: "a", Workdir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)

	// Wait for the first idle, then steer and wait for the second.
	waitIdle(t, s)
	if err := s.Steer(ctx, "fix it"); err != nil {
		t.Fatal(err)
	}
	waitIdle(t, s)

	got, err := os.ReadFile(filepath.Join(dir, "steered.txt"))
	if err != nil {
		t.Fatalf("steer script did not run: %v", err)
	}
	if string(got) != "fix it" {
		t.Fatalf("steered.txt = %q, want %q", got, "fix it")
	}
}

func waitIdle(t *testing.T, s runtime.Session) {
	t.Helper()
	for ev := range s.Events() {
		if ev.Kind == runtime.KindIdle {
			return
		}
	}
	t.Fatal("stream closed before idle")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/runtime/... -v`
Expected: FAIL to compile — `undefined: fake.New`

- [ ] **Step 3: Write the implementation**

Create `pkg/runtime/fake/fake.go`:

```go
// Package fake is a scripted runtime.Runtime for tests. It exists so the whole
// coordination loop can be exercised deterministically in CI without an API
// key, a network, or a single token of spend.
package fake

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime"
)

// Action is one scripted thing a fake agent does.
type Action interface{ isAction() }

// Write writes Content to Path, relative to the session's workdir.
type Write struct{ Path, Content string }

// Exec runs a command in the session's workdir with the session's env. This is
// how a fake agent calls `pc submit`.
type Exec struct{ Args []string }

// Emit reports a tool use without doing anything, for evidence assertions.
type Emit struct{ Tool runtime.ToolUse }

func (Write) isAction() {}
func (Exec) isAction()  {}
func (Emit) isAction()  {}

// Script is one agent's behaviour: what it does on start, and what it does when
// steered. OnSteer may be nil.
type Script struct {
	OnStart []Action
	OnSteer func(text string) []Action
}

// Runtime is the fake. Scripts are keyed by Spec.Agent.
type Runtime struct{ scripts map[string]Script }

func New(scripts map[string]Script) *Runtime { return &Runtime{scripts: scripts} }

func (r *Runtime) Name() string { return "fake" }

func (r *Runtime) Start(ctx context.Context, spec runtime.Spec) (runtime.Session, error) {
	s := &session{
		spec:   spec,
		script: r.scripts[spec.Agent],
		events: make(chan runtime.Event, 256),
		work:   make(chan []Action, 8),
		done:   make(chan struct{}),
		start:  time.Now(),
	}
	go s.loop()
	s.work <- s.script.OnStart
	return s, nil
}

type session struct {
	spec   runtime.Spec
	script Script

	events chan runtime.Event
	work   chan []Action

	once sync.Once
	done chan struct{}

	start time.Time
}

func (s *session) loop() {
	defer close(s.events)
	s.emit(runtime.Event{Kind: runtime.KindStarted})
	for {
		select {
		case <-s.done:
			s.emit(runtime.Event{Kind: runtime.KindExited})
			return
		case actions := <-s.work:
			s.emit(runtime.Event{Kind: runtime.KindTurnBegan})
			for _, a := range actions {
				s.run(a)
			}
			s.emit(runtime.Event{Kind: runtime.KindTurnEnded})
			s.emit(runtime.Event{Kind: runtime.KindIdle})
		}
	}
}

func (s *session) run(a Action) {
	switch v := a.(type) {
	case Write:
		path := filepath.Join(s.spec.Workdir, v.Path)
		err := os.WriteFile(path, []byte(v.Content), 0o644)
		s.emit(runtime.Event{Kind: runtime.KindToolUsed, Tool: &runtime.ToolUse{
			Name: "write", Target: v.Path, Ok: err == nil,
		}})
	case Exec:
		cmd := exec.Command(v.Args[0], v.Args[1:]...)
		cmd.Dir = s.spec.Workdir
		cmd.Env = os.Environ()
		for k, val := range s.spec.Env {
			cmd.Env = append(cmd.Env, k+"="+val)
		}
		out, err := cmd.CombinedOutput()
		s.emit(runtime.Event{Kind: runtime.KindToolUsed, Tool: &runtime.ToolUse{
			Name: "bash", Target: v.Args[0], Detail: string(out), Ok: err == nil,
		}})
	case Emit:
		t := v.Tool
		s.emit(runtime.Event{Kind: runtime.KindToolUsed, Tool: &t})
	}
}

func (s *session) emit(ev runtime.Event) {
	ev.At = time.Now()
	ev.Agent = s.spec.Agent
	select {
	case s.events <- ev:
	case <-s.done:
	}
}

func (s *session) Events() <-chan runtime.Event { return s.events }

// Steer queues the steer script. Like a real adapter it does not preempt: the
// actions run after whatever turn is currently in flight.
func (s *session) Steer(ctx context.Context, text string) error {
	if s.script.OnSteer == nil {
		return nil
	}
	select {
	case s.work <- s.script.OnSteer(text):
	case <-s.done:
	}
	return nil
}

func (s *session) Follow(ctx context.Context, text string) error { return s.Steer(ctx, text) }

func (s *session) Interrupt(ctx context.Context) error { return nil }

func (s *session) Close(ctx context.Context) error {
	s.once.Do(func() { close(s.done) })
	return nil
}

func (s *session) Wait(ctx context.Context) (runtime.Outcome, error) {
	<-s.done
	return runtime.Outcome{Reason: runtime.ExitCompleted, Duration: time.Since(s.start)}, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/runtime/... -v`
Expected: PASS — conformance subtests and the steer test.

- [ ] **Step 5: Commit**

```bash
git add pkg/runtime/fake pkg/runtime/runtimetest
git commit -m "feat(runtime): scripted fake adapter + conformance suite"
```

---

### Task 6: Scenario and gate configuration

**Files:**
- Create: `internal/pcops/config.go`
- Test: `internal/pcops/config_test.go`
- Modify: `go.mod`, `go.sum` (add `gopkg.in/yaml.v3`)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `type GateDef struct { Required []string; Runner, Run string }`
  - `type AgentDef struct { Name, Branch, Role, Task string }`
  - `type Config struct { Repo, DB string; Gate GateDef; GateID string; Agents []AgentDef; Runner AgentDef; SubmitTimeout, Wall time.Duration }`
  - `func LoadConfig(path string) (Config, error)`

- [ ] **Step 1: Write the failing test**

Create `internal/pcops/config_test.go`:

```go
package pcops_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
)

const sample = `
repo: ./fixtures/two-service
db: .pc/poc.db
gate:
  id: checkout
  required: [billing, gateway]
  runner: integrator
  run: go test ./integration/...
agents:
  - name: billing
    branch: agent/billing
    role: implementer
    task: "Accept a currency field."
  - name: gateway
    branch: agent/gateway
    role: implementer
    task: "Send a currency field."
runner:
  name: integrator
  branch: agent/integration
budget:
  wall: 15m
`

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "poc.yaml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadConfig(t *testing.T) {
	cfg, err := pcops.LoadConfig(writeConfig(t, sample))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GateID != "checkout" {
		t.Errorf("GateID = %q", cfg.GateID)
	}
	if cfg.Gate.Runner != "integrator" || cfg.Gate.Run != "go test ./integration/..." {
		t.Errorf("Gate = %+v", cfg.Gate)
	}
	if len(cfg.Agents) != 2 || cfg.Agents[0].Name != "billing" {
		t.Errorf("Agents = %+v", cfg.Agents)
	}
	if cfg.Wall != 15*time.Minute {
		t.Errorf("Wall = %v", cfg.Wall)
	}
	if cfg.SubmitTimeout != 5*time.Minute {
		t.Errorf("SubmitTimeout default = %v, want 5m", cfg.SubmitTimeout)
	}
}

func TestPCDBOverridesConfig(t *testing.T) {
	t.Setenv("PC_DB", "/tmp/override.db")
	cfg, err := pcops.LoadConfig(writeConfig(t, sample))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DB != "/tmp/override.db" {
		t.Errorf("DB = %q, want the PC_DB override", cfg.DB)
	}
}

func TestMissingDBIsAnError(t *testing.T) {
	if _, err := pcops.LoadConfig(writeConfig(t, "gate:\n  id: g\n")); err == nil {
		t.Fatal("want an error when no db is configured")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/pcops/ -v`
Expected: FAIL — the package does not exist.

- [ ] **Step 3: Add the dependency and write the implementation**

```bash
go get gopkg.in/yaml.v3@v3.0.1
```

Create `internal/pcops/config.go`:

```go
// Package pcops is the composition root: the one place that knows about the
// gate, the bus, workspace leases, and the agent runtime at the same time. The
// CLI in cmd/pc is a thin skin over these functions so no coordination logic
// ever lives in a command.
package pcops

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// DefaultSubmitTimeout is how long `pc submit` waits for a verdict before
// reporting that none was obtained.
const DefaultSubmitTimeout = 5 * time.Minute

// GateDef is one gate: who must be ready, who runs the spanning test, and what
// command that runner executes.
type GateDef struct {
	Required []string `yaml:"required"`
	Runner   string   `yaml:"runner"`
	Run      string   `yaml:"run"`
}

// AgentDef is one participant the orchestrator launches.
type AgentDef struct {
	Name   string `yaml:"name"`
	Branch string `yaml:"branch"`
	Role   string `yaml:"role"`
	Task   string `yaml:"task"`
}

// Config is a scenario: a hand-written loop definition. It is the seed of the
// loop engine, and eventually the artifact a loop author generates.
type Config struct {
	Repo          string
	DB            string
	GateID        string
	Gate          GateDef
	Agents        []AgentDef
	Runner        AgentDef
	SubmitTimeout time.Duration
	Wall          time.Duration
}

type rawConfig struct {
	Repo string `yaml:"repo"`
	DB   string `yaml:"db"`
	Gate struct {
		ID       string   `yaml:"id"`
		Required []string `yaml:"required"`
		Runner   string   `yaml:"runner"`
		Run      string   `yaml:"run"`
	} `yaml:"gate"`
	Agents []AgentDef `yaml:"agents"`
	Runner AgentDef   `yaml:"runner"`
	Budget struct {
		Wall          string `yaml:"wall"`
		SubmitTimeout string `yaml:"submit_timeout"`
	} `yaml:"budget"`
}

// LoadConfig reads a scenario file. $PC_DB overrides the configured database so
// a single scenario can be pointed at a scratch bus without editing the file.
func LoadConfig(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	var raw rawConfig
	if err := yaml.Unmarshal(b, &raw); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}

	cfg := Config{
		Repo:          raw.Repo,
		DB:            raw.DB,
		GateID:        raw.Gate.ID,
		Gate:          GateDef{Required: raw.Gate.Required, Runner: raw.Gate.Runner, Run: raw.Gate.Run},
		Agents:        raw.Agents,
		Runner:        raw.Runner,
		SubmitTimeout: DefaultSubmitTimeout,
	}
	if v := os.Getenv("PC_DB"); v != "" {
		cfg.DB = v
	}
	if cfg.DB == "" {
		return Config{}, fmt.Errorf("no database configured: set `db:` or $PC_DB")
	}
	if raw.Budget.Wall != "" {
		if cfg.Wall, err = time.ParseDuration(raw.Budget.Wall); err != nil {
			return Config{}, fmt.Errorf("budget.wall: %w", err)
		}
	}
	if raw.Budget.SubmitTimeout != "" {
		if cfg.SubmitTimeout, err = time.ParseDuration(raw.Budget.SubmitTimeout); err != nil {
			return Config{}, fmt.Errorf("budget.submit_timeout: %w", err)
		}
	}
	return cfg, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/pcops/ -v && go build ./...`
Expected: PASS. Confirm `go.mod` still declares `go 1.22` and `modernc.org/sqlite v1.33.1`.

- [ ] **Step 5: Commit**

```bash
git add go.mod go.sum internal/pcops/config.go internal/pcops/config_test.go
git commit -m "feat(pcops): scenario configuration"
```

---

### Task 7: Submit — declare readiness and block for the verdict

The coordinator broadcasts each verdict to `gate.Topic(gateID)` as an `IntentInform` carrying `{text, gate, passed}`, and additionally sends an `IntentBlock` directly to each owner on failure. Submit listens for the broadcast.

**Files:**
- Create: `internal/pcops/submit.go`
- Test: `internal/pcops/submit_test.go`

**Interfaces:**
- Consumes: `Config` from Task 6.
- Produces: `func Submit(ctx context.Context, cfg Config, gateID, agentName, version string) (gate.Verdict, error)` and `var ErrNoVerdict = errors.New("pcops: no verdict before timeout")`.

- [ ] **Step 1: Write the failing test**

Create `internal/pcops/submit_test.go`:

```go
package pcops_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
)

// A single-participant gate whose runner always passes: submit must return a
// passing verdict rather than time out.
func TestSubmitReturnsThePassingVerdict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{DB: db, SubmitTimeout: 10 * time.Second}

	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })

	coord, err := agent.New(ctx, b, "coordinator", []string{gate.Topic("g")})
	if err != nil {
		t.Fatal(err)
	}
	c := gate.NewCoordinator(coord)
	c.Register(gate.Spec{ID: "g", Required: []string{"billing"}, Runner: "runner"})
	go coord.Run(ctx)

	run, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.ServeRunner(run, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: true}
	})
	go run.Run(ctx)

	v, err := pcops.Submit(ctx, cfg, "g", "billing", "v1")
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if !v.Passed {
		t.Fatalf("verdict = %+v, want passed", v)
	}
}

func TestSubmitTimesOutWithoutACoordinator(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cfg := pcops.Config{
		DB:            filepath.Join(t.TempDir(), "bus.db"),
		SubmitTimeout: 300 * time.Millisecond,
	}
	if _, err := pcops.Submit(ctx, cfg, "g", "billing", "v1"); err == nil {
		t.Fatal("want a timeout error when nothing coordinates the gate")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/pcops/ -run TestSubmit -v`
Expected: FAIL to compile — `undefined: pcops.Submit`

- [ ] **Step 3: Write the implementation**

Create `internal/pcops/submit.go`:

```go
package pcops

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

// ErrNoVerdict means the spanning test never reported. Callers must keep this
// distinct from a failing verdict: "did not run" and "ran and failed" demand
// different responses from an agent.
var ErrNoVerdict = errors.New("pcops: no verdict before timeout")

// Submit declares readiness for a gate and blocks until the coordinator
// broadcasts a verdict for it.
//
// This is the whole harness-agnostic contract: a gate id, an opaque version, an
// agent name. Any tool that can run a shell command can participate.
func Submit(ctx context.Context, cfg Config, gateID, agentName, version string) (gate.Verdict, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.SubmitTimeout)
	defer cancel()

	b, err := sqlite.Open(ctx, cfg.DB, sqlite.WithPollInterval(25*time.Millisecond))
	if err != nil {
		return gate.Verdict{}, fmt.Errorf("open bus: %w", err)
	}
	defer b.Close()

	a, err := agent.New(ctx, b, agentName, []string{gate.Topic(gateID)})
	if err != nil {
		return gate.Verdict{}, fmt.Errorf("join as %q: %w", agentName, err)
	}

	verdicts := make(chan gate.Verdict, 1)
	a.On(protocol.IntentInform, func(_ context.Context, _ *agent.Agent, m protocol.Message) *protocol.Message {
		if id, _ := m.Body["gate"].(string); id != gateID {
			return nil
		}
		passed, ok := m.Body["passed"].(bool)
		if !ok {
			return nil // not a verdict broadcast
		}
		detail, _ := m.Body["text"].(string)
		select {
		case verdicts <- gate.Verdict{GateID: gateID, Passed: passed, Detail: detail}:
		default:
		}
		return nil
	})
	go a.Run(ctx)

	if err := gate.Ready(ctx, a, gateID, version); err != nil {
		return gate.Verdict{}, fmt.Errorf("declare ready: %w", err)
	}

	select {
	case v := <-verdicts:
		return v, nil
	case <-ctx.Done():
		return gate.Verdict{}, ErrNoVerdict
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/pcops/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/pcops/submit.go internal/pcops/submit_test.go
git commit -m "feat(pcops): submit declares readiness and blocks for the verdict"
```

---

### Task 8: Coordinator host

**Files:**
- Create: `internal/pcops/up.go`
- Test: `internal/pcops/up_test.go`

**Interfaces:**
- Consumes: `Config` from Task 6, `Submit` from Task 7.
- Produces: `func Up(ctx context.Context, cfg Config, onVerdict func(gate.Verdict)) error` — blocks until ctx is done; `onVerdict` may be nil.

- [ ] **Step 1: Write the failing test**

Create `internal/pcops/up_test.go`:

```go
package pcops_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
)

func TestUpCoordinatesUntilQuorum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{
		DB:            db,
		GateID:        "g",
		Gate:          pcops.GateDef{Required: []string{"billing"}, Runner: "runner"},
		SubmitTimeout: 10 * time.Second,
	}

	verdicts := make(chan gate.Verdict, 1)
	go pcops.Up(ctx, cfg, func(v gate.Verdict) { verdicts <- v })

	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	run, err := agent.New(ctx, b, "runner", nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.ServeRunner(run, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		return gate.Verdict{GateID: gateID, Passed: true}
	})
	go run.Run(ctx)

	if _, err := pcops.Submit(ctx, cfg, "g", "billing", "v1"); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	select {
	case v := <-verdicts:
		if !v.Passed {
			t.Fatalf("verdict = %+v", v)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("coordinator never resolved the gate")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/pcops/ -run TestUp -v`
Expected: FAIL to compile — `undefined: pcops.Up`

- [ ] **Step 3: Write the implementation**

Create `internal/pcops/up.go`:

```go
package pcops

import (
	"context"
	"fmt"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
)

// Up hosts the coordinator for the configured gate and blocks until ctx ends.
// It adds no coordination semantics of its own; pkg/gate owns all of them.
func Up(ctx context.Context, cfg Config, onVerdict func(gate.Verdict)) error {
	b, err := sqlite.Open(ctx, cfg.DB, sqlite.WithPollInterval(25*time.Millisecond))
	if err != nil {
		return fmt.Errorf("open bus: %w", err)
	}
	defer b.Close()

	a, err := agent.New(ctx, b, "coordinator", []string{gate.Topic(cfg.GateID)})
	if err != nil {
		return fmt.Errorf("join as coordinator: %w", err)
	}
	c := gate.NewCoordinator(a)
	// The runner shells out to a real test command, which is far slower than
	// the in-process default of 5s.
	c.SetRunnerTimeout(10 * time.Minute)
	c.Register(gate.Spec{ID: cfg.GateID, Required: cfg.Gate.Required, Runner: cfg.Gate.Runner})
	if onVerdict != nil {
		c.OnVerdict(onVerdict)
	}
	go a.Run(ctx)

	<-ctx.Done()
	return ctx.Err()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/pcops/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/pcops/up.go internal/pcops/up_test.go
git commit -m "feat(pcops): coordinator host"
```

---

### Task 9: Run-gate — merge branches, run the spanning test, report

The runner needs a workspace containing both agents' work. It merges every participating branch into its own worktree before running the test. A merge conflict is an ordinary failing verdict, not an exception — two agents editing a shared contract file is the normal case.

**Files:**
- Create: `internal/pcops/rungate.go`
- Test: `internal/pcops/rungate_test.go`

**Interfaces:**
- Consumes: `Config` from Task 6.
- Produces: `func RunGate(ctx context.Context, cfg Config, workdir string, branches []string) error` — blocks until ctx ends, serving verdicts.

- [ ] **Step 1: Write the failing test**

Create `internal/pcops/rungate_test.go`:

```go
package pcops_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
)

func git(t *testing.T, dir string, args ...string) {
	t.Helper()
	c := exec.Command("git", append([]string{"-C", dir}, args...)...)
	c.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com",
		"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com")
	if out, err := c.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
}

func TestRunGateMergesBranchesBeforeTesting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	repo := t.TempDir()
	git(t, repo, "init", "-q", "-b", "main")
	os.WriteFile(filepath.Join(repo, "seed.txt"), []byte("seed\n"), 0o644)
	git(t, repo, "add", ".")
	git(t, repo, "commit", "-q", "-m", "init")

	// Two branches, each adding a distinct file.
	for _, b := range []string{"a", "b"} {
		git(t, repo, "checkout", "-q", "-b", b, "main")
		os.WriteFile(filepath.Join(repo, b+".txt"), []byte(b), 0o644)
		git(t, repo, "add", ".")
		git(t, repo, "commit", "-q", "-m", b)
	}
	git(t, repo, "checkout", "-q", "main")

	work := filepath.Join(t.TempDir(), "integrator")
	git(t, repo, "worktree", "add", "-B", "agent/integration", work)

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{
		DB:     db,
		GateID: "g",
		Gate:   pcops.GateDef{Required: []string{"x"}, Runner: "integrator", Run: "test -f a.txt && test -f b.txt"},
	}

	go pcops.Up(ctx, cfg, nil)
	go pcops.RunGate(ctx, cfg, work, []string{"a", "b"})

	cfg.SubmitTimeout = 20 * time.Second
	v, err := pcops.Submit(ctx, cfg, "g", "x", "v1")
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if !v.Passed {
		t.Fatalf("verdict = %+v; the runner should have merged both branches", v)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/pcops/ -run TestRunGate -v`
Expected: FAIL to compile — `undefined: pcops.RunGate`

- [ ] **Step 3: Write the implementation**

Create `internal/pcops/rungate.go`:

```go
package pcops

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
)

// RunGate serves the spanning test for a gate. On each gate opening it resets
// its worktree, merges every participating branch, and runs the configured
// command. Merge conflicts are reported as an ordinary failing verdict, because
// two agents editing one contract file is the expected case, not an error.
func RunGate(ctx context.Context, cfg Config, workdir string, branches []string) error {
	b, err := sqlite.Open(ctx, cfg.DB, sqlite.WithPollInterval(25*time.Millisecond))
	if err != nil {
		return fmt.Errorf("open bus: %w", err)
	}
	defer b.Close()

	a, err := agent.New(ctx, b, cfg.Gate.Runner, nil)
	if err != nil {
		return fmt.Errorf("join as %q: %w", cfg.Gate.Runner, err)
	}
	gate.ServeRunner(a, func(ctx context.Context, gateID string, versions map[string]string) gate.Verdict {
		if detail, err := mergeAll(workdir, branches); err != nil {
			return gate.Verdict{GateID: gateID, Passed: false, Detail: detail, Versions: versions}
		}
		out, err := runShell(ctx, workdir, cfg.Gate.Run)
		if err != nil {
			return gate.Verdict{GateID: gateID, Passed: false, Detail: trim(out), Versions: versions}
		}
		return gate.Verdict{GateID: gateID, Passed: true, Versions: versions}
	})
	go a.Run(ctx)

	<-ctx.Done()
	return ctx.Err()
}

// mergeAll resets the runner's worktree to main and merges each branch in turn.
// The reset makes every round independent of the last.
func mergeAll(workdir string, branches []string) (string, error) {
	// Deliberately no `checkout main`: main is checked out in the primary
	// worktree, and git refuses to check out a branch twice. Resetting the
	// runner's own branch to main achieves the same clean baseline.
	for _, args := range [][]string{
		{"reset", "--hard", "-q", "main"},
		{"clean", "-qfd"},
	} {
		if out, err := gitIn(workdir, args...); err != nil {
			return trim(out), err
		}
	}
	for _, br := range branches {
		out, err := gitIn(workdir, "merge", "--no-edit", "-q", br)
		if err != nil {
			_, _ = gitIn(workdir, "merge", "--abort")
			return fmt.Sprintf("merge conflict on %s: %s", br, trim(out)), err
		}
	}
	return "", nil
}

func gitIn(dir string, args ...string) (string, error) {
	out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput()
	return string(out), err
}

func runShell(ctx context.Context, dir, command string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// trim keeps verdict details small enough to travel in a message body.
func trim(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 2000 {
		return s[len(s)-2000:]
	}
	return s
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/pcops/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/pcops/rungate.go internal/pcops/rungate_test.go
git commit -m "feat(pcops): run-gate merges participating branches before testing"
```

---

### Task 10: Send — agent-to-agent messaging

**Files:**
- Create: `internal/pcops/send.go`
- Test: `internal/pcops/send_test.go`

**Interfaces:**
- Consumes: `Config` from Task 6.
- Produces: `func Send(ctx context.Context, cfg Config, from, to, intent, text string) error`

- [ ] **Step 1: Write the failing test**

Create `internal/pcops/send_test.go`:

```go
package pcops_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

func TestSendDeliversToTheNamedAgent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	ch, err := b.Subscribe(ctx, "gateway", nil)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // let the subscriber settle at head

	if err := pcops.Send(ctx, pcops.Config{DB: db}, "billing", "gateway", "inform", "field is amount_minor"); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case m := <-ch:
		if m.From.Agent != "billing" || m.Intent != protocol.IntentInform {
			t.Fatalf("got %+v", m)
		}
		if m.Body["text"] != "field is amount_minor" {
			t.Fatalf("body = %+v", m.Body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("message never arrived")
	}
}

func TestSendRejectsAnUnknownIntent(t *testing.T) {
	err := pcops.Send(context.Background(), pcops.Config{DB: filepath.Join(t.TempDir(), "b.db")},
		"a", "b", "shout", "hi")
	if err == nil {
		t.Fatal("want an error for an unknown intent")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/pcops/ -run TestSend -v`
Expected: FAIL to compile — `undefined: pcops.Send`

- [ ] **Step 3: Write the implementation**

Create `internal/pcops/send.go`:

```go
package pcops

import (
	"context"
	"fmt"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

// sendableIntents is the allowlist an agent may use from the shell. Control
// intents such as ready and ack belong to the gate and the runtime, not to a
// model deciding what to type.
var sendableIntents = map[string]protocol.Intent{
	"inform":   protocol.IntentInform,
	"request":  protocol.IntentRequest,
	"propose":  protocol.IntentPropose,
	"agree":    protocol.IntentAgree,
	"disagree": protocol.IntentDisagree,
	"block":    protocol.IntentBlock,
	"done":     protocol.IntentDone,
}

// Send publishes one message from one agent to another.
func Send(ctx context.Context, cfg Config, from, to, intent, text string) error {
	in, ok := sendableIntents[intent]
	if !ok {
		return fmt.Errorf("unknown intent %q", intent)
	}
	b, err := sqlite.Open(ctx, cfg.DB, sqlite.WithPollInterval(25*time.Millisecond))
	if err != nil {
		return fmt.Errorf("open bus: %w", err)
	}
	defer b.Close()

	m := protocol.New(
		protocol.Address{Agent: from},
		protocol.Address{Agent: to},
		in,
		map[string]any{"text": text},
	)
	if err := b.Publish(ctx, m); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	time.Sleep(50 * time.Millisecond) // let the write land before the process exits
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/pcops/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/pcops/send.go internal/pcops/send_test.go
git commit -m "feat(pcops): agent-to-agent send"
```

---

### Task 11: The courier

Each spawned session gets one `pkg/agent.Agent` proxy on the bus, translating intents into session verbs. This is why no new coordination semantics are needed: a spawned agent appears on the bus as an ordinary protocol participant.

**Files:**
- Create: `internal/pcops/courier.go`
- Test: `internal/pcops/courier_test.go`

**Interfaces:**
- Consumes: `pkg/runtime` (Task 4), `pkg/agent`, `pkg/bus/sqlite`.
- Produces: `func StartCourier(ctx context.Context, b bus.Bus, name string, sess runtime.Session, topics []string) error`

- [ ] **Step 1: Write the failing test**

Create `internal/pcops/courier_test.go`:

```go
package pcops_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime/fake"
)

func TestCourierDeliversAMessageIntoTheSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	dir := t.TempDir()
	r := fake.New(map[string]fake.Script{
		"gateway": {
			OnSteer: func(text string) []fake.Action {
				return []fake.Action{fake.Write{Path: "received.txt", Content: text}}
			},
		},
	})
	sess, err := r.Start(ctx, runtime.Spec{Agent: "gateway", Workdir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close(ctx)

	db := filepath.Join(t.TempDir(), "bus.db")
	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })

	if err := pcops.StartCourier(ctx, b, "gateway", sess, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	if err := pcops.Send(ctx, pcops.Config{DB: db}, "billing", "gateway", "inform", "amount_minor"); err != nil {
		t.Fatal(err)
	}

	deadline := time.After(10 * time.Second)
	for {
		if body, err := os.ReadFile(filepath.Join(dir, "received.txt")); err == nil {
			if string(body) != "amount_minor" {
				t.Fatalf("received %q", body)
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("courier never delivered the message into the session")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// The sqlite bus filters senders only on the topic path, so a self-addressed
// direct message is delivered. Without a guard the courier would steer an agent
// with its own outbound message.
func TestCourierIgnoresItsOwnMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	dir := t.TempDir()
	r := fake.New(map[string]fake.Script{
		"billing": {
			OnSteer: func(text string) []fake.Action {
				return []fake.Action{fake.Write{Path: "looped.txt", Content: text}}
			},
		},
	})
	sess, err := r.Start(ctx, runtime.Spec{Agent: "billing", Workdir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close(ctx)

	db := filepath.Join(t.TempDir(), "bus.db")
	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	if err := pcops.StartCourier(ctx, b, "billing", sess, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	// billing addresses itself.
	m := protocol.New(protocol.Address{Agent: "billing"}, protocol.Address{Agent: "billing"},
		protocol.IntentInform, map[string]any{"text": "echo"})
	if err := b.Publish(ctx, m); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)

	if _, err := os.Stat(filepath.Join(dir, "looped.txt")); err == nil {
		t.Fatal("courier steered the agent with its own message")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/pcops/ -run TestCourier -v`
Expected: FAIL to compile — `undefined: pcops.StartCourier`

- [ ] **Step 3: Write the implementation**

Create `internal/pcops/courier.go`:

```go
package pcops

import (
	"context"
	"fmt"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime"
)

// StartCourier bridges the bus into one agent session. It runs a pkg/agent.Agent
// as the session's proxy, so a spawned coding agent appears on the bus as an
// ordinary protocol participant and pkg/gate never learns a model is involved.
//
// Neither delivery path preempts. A block arriving while the agent runs its test
// suite waits for that suite to finish, which is the right trade: aborting the
// tool would destroy work the agent is seconds from reporting.
func StartCourier(ctx context.Context, b bus.Bus, name string, sess runtime.Session, topics []string) error {
	a, err := agent.New(ctx, b, name, topics)
	if err != nil {
		return fmt.Errorf("courier for %q: %w", name, err)
	}

	deliver := func(steer bool) agent.Handler {
		return func(ctx context.Context, _ *agent.Agent, m protocol.Message) *protocol.Message {
			// The sqlite bus filters senders on the topic path only, so a
			// self-addressed direct message arrives here. Drop it, or the agent
			// gets steered by its own outbound send.
			if m.From.Agent == name {
				return nil
			}
			text, _ := m.Body["text"].(string)
			if text == "" {
				return nil
			}
			if steer {
				_ = sess.Steer(ctx, text)
			} else {
				_ = sess.Follow(ctx, text)
			}
			return nil
		}
	}

	// Blocks are urgent, so they take the steer path; everything else queues
	// behind pending work.
	a.On(protocol.IntentBlock, deliver(true))
	for _, in := range []protocol.Intent{
		protocol.IntentInform, protocol.IntentRequest, protocol.IntentPropose,
		protocol.IntentAgree, protocol.IntentDisagree, protocol.IntentDone,
	} {
		a.On(in, deliver(false))
	}

	go a.Run(ctx)
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/pcops/ -v`
Expected: PASS — both courier tests.

- [ ] **Step 5: Commit**

```bash
git add internal/pcops/courier.go internal/pcops/courier_test.go
git commit -m "feat(pcops): courier bridges bus messages into session control verbs"
```

---

### Task 12: The orchestrator and the whole-loop test

The payoff task: `Run` acquires leases, spawns sessions, starts couriers, hosts the coordinator and the runner, and waits for convergence. Its test drives the entire loop over the fake — failing round one, passing round two — with no tokens spent.

**Files:**
- Create: `internal/pcops/run.go`
- Create: `cmd/pc/main.go`
- Test: `internal/pcops/run_test.go`

**Interfaces:**
- Consumes: everything from Tasks 3–11.
- Produces: `func Run(ctx context.Context, cfg Config, r runtime.Runtime) (gate.Verdict, error)`

- [ ] **Step 1: Write the failing test**

Create `internal/pcops/run_test.go`:

```go
package pcops_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime/fake"
)

// buildPC compiles cmd/pc so the fake agents can invoke the real CLI, exactly
// as a coding agent's bash tool would.
func buildPC(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "pc")
	out, err := exec.Command("go", "build", "-o", bin,
		"github.com/KJFromMicromonic/parallel-consciousness/cmd/pc").CombinedOutput()
	if err != nil {
		t.Fatalf("build pc: %v: %s", err, out)
	}
	return bin
}

// twoServiceRepo builds the fixture: a spanning check that passes only when
// BOTH files carry the currency field. Neither agent can satisfy it alone.
func twoServiceRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	git(t, dir, "init", "-q", "-b", "main")
	os.WriteFile(filepath.Join(dir, "billing.txt"), []byte("amount\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "gateway.txt"), []byte("amount\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "check.sh"),
		[]byte("#!/bin/sh\ngrep -q currency billing.txt && grep -q currency gateway.txt\n"), 0o755)
	git(t, dir, "add", ".")
	git(t, dir, "commit", "-q", "-m", "init")
	return dir
}

func TestRunConvergesAfterAFailingRound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	repo := twoServiceRepo(t)
	pc := buildPC(t)
	db := filepath.Join(t.TempDir(), "bus.db")

	cfg := pcops.Config{
		Repo:   repo,
		DB:     db,
		GateID: "checkout",
		Gate: pcops.GateDef{
			Required: []string{"billing", "gateway"},
			Runner:   "integrator",
			Run:      "sh check.sh",
		},
		Agents: []pcops.AgentDef{
			{Name: "billing", Branch: "agent/billing", Role: "implementer", Task: "add currency"},
			{Name: "gateway", Branch: "agent/gateway", Role: "implementer", Task: "send currency"},
		},
		Runner:        pcops.AgentDef{Name: "integrator", Branch: "agent/integration"},
		SubmitTimeout: 60 * time.Second,
		Wall:          90 * time.Second,
	}

	// Round one: billing does its half, gateway does NOT. The spanning check
	// must fail, routing a block to both owners. On being steered, each agent
	// writes the currency field and re-submits.
	submit := fake.Exec{Args: []string{pc, "submit", "--gate", "checkout"}}
	fixAndResubmit := func(path string) func(string) []fake.Action {
		return func(text string) []fake.Action {
			return []fake.Action{fake.Write{Path: path, Content: "amount currency\n"}, commitAll, submit}
		}
	}

	r := fake.New(map[string]fake.Script{
		"billing": {
			OnStart: []fake.Action{fake.Write{Path: "billing.txt", Content: "amount currency\n"}, commitAll, submit},
			OnSteer: fixAndResubmit("billing.txt"),
		},
		"gateway": {
			OnStart: []fake.Action{fake.Write{Path: "gateway.txt", Content: "amount\n"}, commitAll, submit},
			OnSteer: fixAndResubmit("gateway.txt"),
		},
	})

	v, err := pcops.Run(ctx, cfg, r)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !v.Passed {
		t.Fatalf("loop never converged: %+v", v)
	}
}

// commitAll is how a fake agent publishes its work to its branch.
var commitAll = fake.Exec{Args: []string{"sh", "-c",
	"git add -A && git -c user.email=t@example.com -c user.name=t commit -q -m work"}}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/pcops/ -run TestRunConverges -v`
Expected: FAIL to compile — `undefined: pcops.Run`, and `cmd/pc` does not build.

- [ ] **Step 3: Write the orchestrator**

Create `internal/pcops/run.go`:

```go
package pcops

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/workspace"
)

// Run executes one scenario: lease a workspace per participant, launch each
// agent, bridge them onto the bus, host the gate, and wait for a verdict.
//
// Identity is assigned here rather than negotiated with a model: PC_AGENT and
// PC_DB are injected into each session's environment, so the durable cursor key
// is always correct.
func Run(ctx context.Context, cfg Config, r runtime.Runtime) (gate.Verdict, error) {
	if cfg.Wall > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Wall)
		defer cancel()
	}

	root := filepath.Join(filepath.Dir(cfg.DB), "worktrees")
	wm, err := workspace.New(ctx, cfg.Repo, root, cfg.DB)
	if err != nil {
		return gate.Verdict{}, err
	}
	defer wm.Close()

	verdicts := make(chan gate.Verdict, 4)
	go Up(ctx, cfg, func(v gate.Verdict) { verdicts <- v })

	// The runner gets its own worktree so it can merge every branch.
	runnerLease, err := wm.Acquire(ctx, cfg.Runner.Name, cfg.Runner.Branch)
	if err != nil {
		return gate.Verdict{}, fmt.Errorf("lease runner workspace: %w", err)
	}
	defer runnerLease.Release(context.Background())

	branches := make([]string, 0, len(cfg.Agents))
	for _, a := range cfg.Agents {
		branches = append(branches, a.Branch)
	}
	go RunGate(ctx, cfg, runnerLease.Path, branches)

	b, err := sqlite.Open(ctx, cfg.DB, sqlite.WithPollInterval(25*time.Millisecond))
	if err != nil {
		return gate.Verdict{}, fmt.Errorf("open bus: %w", err)
	}
	defer b.Close()

	for _, def := range cfg.Agents {
		lease, err := wm.Acquire(ctx, def.Name, def.Branch)
		if err != nil {
			return gate.Verdict{}, fmt.Errorf("lease workspace for %s: %w", def.Name, err)
		}
		defer lease.Release(context.Background())

		sess, err := r.Start(ctx, runtime.Spec{
			Agent:   def.Name,
			Workdir: lease.Path,
			Role:    def.Role,
			Task:    def.Task,
			Env:     map[string]string{"PC_AGENT": def.Name, "PC_DB": cfg.DB},
			Budget:  runtime.Budget{Wall: cfg.Wall},
		})
		if err != nil {
			return gate.Verdict{}, fmt.Errorf("start %s: %w", def.Name, err)
		}
		defer sess.Close(context.Background())

		if err := StartCourier(ctx, b, def.Name, sess, nil); err != nil {
			return gate.Verdict{}, err
		}
		go drainEvents(sess)
	}

	for {
		select {
		case v := <-verdicts:
			if v.Passed {
				return v, nil
			}
			// A failing verdict already routed blocks to the owners; their
			// couriers steer them into a fix. Keep waiting for the next round.
		case <-ctx.Done():
			return gate.Verdict{GateID: cfg.GateID}, fmt.Errorf("scenario budget exhausted: %w", ctx.Err())
		}
	}
}

// drainEvents keeps a session's event channel moving. Evidence collection lands
// with the domain store; for now the events must simply not back up.
func drainEvents(s runtime.Session) {
	for range s.Events() {
	}
}
```

- [ ] **Step 4: Write the CLI**

Create `cmd/pc/main.go`:

```go
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
	"errors"
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
		if errors.Is(err, pcops.ErrNoVerdict) {
			return 2
		}
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
```

- [ ] **Step 5: Run the whole-loop test**

Run: `go test ./internal/pcops/ -run TestRunConverges -v`
Expected: PASS. The log should show a failing first round (the spanning check fails because `gateway.txt` lacks `currency`), blocks routed to both owners, and a passing second round.

- [ ] **Step 6: Run the full suite and commit**

Run: `go test ./...`
Expected: PASS, everything, with no network and no API key.

```bash
git add internal/pcops/run.go internal/pcops/run_test.go internal/pcops/config.go cmd/pc/main.go
git commit -m "feat(pc): scenario orchestrator + CLI, with the loop proven over the fake runtime"
```

---

## Phase B (separate plan, written after Phase A lands)

Phase A ends with the coordination loop proven and no harness involved. Phase B adds the real one, and gets its own plan because its tasks should harden around the interfaces Phase A actually produced rather than the ones this document predicts:

1. `internal/runtime/pi` — the adapter over `pi --mode rpc`, satisfying `runtimetest` under `PC_E2E_PI=1`.
2. `fixtures/two-service` — the real Go fixture repository with a spanning integration test.
3. `pc run --scenario`, `pc up`, `pc run-gate`, `pc watch` as CLI subcommands.
4. The manual end-to-end PoC with two real pi sessions, recorded, measuring wall-clock and token cost against the spec's six success criteria.

### Spec coverage map

Every in-scope item from the design spec, and where it lands:

| Spec item | Phase |
|---|---|
| `pkg/runtime` value types and interfaces | A — Task 4 |
| `pkg/runtime/runtimetest` conformance suite | A — Task 5 |
| Scripted fake runtime | A — Task 5 |
| `pkg/workspace` worktree leases | A — Tasks 2, 3 |
| Cursor monotonicity prerequisite | A — Task 1 |
| Import-boundary test | A — Task 4 |
| `pc submit`, `pc send` | A — Tasks 7, 10, 12 |
| `Up`, `RunGate`, `Run` as library functions | A — Tasks 8, 9, 12 |
| Deterministic whole-loop integration test | A — Task 12 |
| `internal/runtime/pi` adapter | B |
| JSONL framing tests (>64KB, U+2028, CRLF) | B — they test the adapter, which does not exist until then |
| `up`, `run-gate`, `run`, `watch` as CLI subcommands | B |
| `fixtures/two-service` Go fixture repository | B — Phase A's loop test uses a minimal inline fixture |
| Manual end-to-end PoC and its six success criteria | B |

Nothing in the spec is dropped; the split is only about what can be proven without a harness.
