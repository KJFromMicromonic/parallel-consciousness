package workspace

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
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

	// Reclaiming a stale lease is crash recovery: the previous holder never
	// called Release, so its worktree directory is still on disk. git worktree
	// add fails outright on an existing directory (-B only resets the branch
	// pointer, it doesn't help here), so only add a fresh worktree when the
	// path doesn't already exist; otherwise reattach to what's there.
	if _, statErr := os.Stat(path); statErr != nil {
		if !os.IsNotExist(statErr) {
			_, _ = m.db.ExecContext(ctx, `DELETE FROM leases WHERE path = ?`, path)
			return nil, fmt.Errorf("stat worktree %s: %w", path, statErr)
		}
		if err := worktreeAdd(m.repo, path, branch); err != nil {
			_, _ = m.db.ExecContext(ctx, `DELETE FROM leases WHERE path = ?`, path)
			return nil, err
		}
	} else {
		// Reattach path: the worktree on disk belongs to whatever branch the
		// crashed holder had checked out, which may not be the branch this
		// caller is asking for now (the lease is keyed by agent, not by
		// branch). We never silently check out a different branch here —
		// doing so during crash recovery could orphan or discard the crashed
		// holder's committed work, and a reclaim naming a different branch is
		// a genuine ownership conflict the caller needs to see, not something
		// to paper over. So verify and fail loudly on mismatch instead.
		onDisk, err := worktreeBranch(path)
		if err != nil {
			_, _ = m.db.ExecContext(ctx, `DELETE FROM leases WHERE path = ?`, path)
			return nil, err
		}
		if onDisk != branch {
			_, _ = m.db.ExecContext(ctx, `DELETE FROM leases WHERE path = ?`, path)
			return nil, fmt.Errorf("workspace: reclaim of agent %q at %s found branch %q checked out, requested %q", agent, path, onDisk, branch)
		}
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
