package workspace

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
)

// ErrLeased means a live holder already owns that workspace.
var ErrLeased = errors.New("workspace: already leased by a live holder")

// ErrLeaseLost means the lease row no longer belongs to this *Lease: another
// caller reclaimed the workspace after this holder's lease went stale. It is the
// fence. A holder that sees it must stop using its worktree immediately and must
// NOT release it, because the directory now belongs to somebody else.
var ErrLeaseLost = errors.New("workspace: lease lost to another holder")

// holder carries a per-Lease token so a renewal or a release can be attributed
// to one specific holder rather than to an agent name. Without it the reattach
// path — which reclaims a stale row for the SAME agent and branch, deliberately,
// because that is how crash recovery works — left both holders matching on
// (path, agent): the original kept renewing a lease it had lost, and its Release
// deleted the row and force-removed a directory the new holder was working in.
//
// The schema uses CREATE TABLE IF NOT EXISTS, so a database created before this
// column existed will not gain it and every statement below will error on the
// unknown column. That is accepted rather than migrated: nothing here is
// deployed, and the file is a scratch bus recreated per run. A first real
// deployment needs a migration step, not another IF NOT EXISTS.
const leaseSchema = `
CREATE TABLE IF NOT EXISTS leases (
  path         TEXT PRIMARY KEY,
  agent        TEXT NOT NULL,
  branch       TEXT NOT NULL,
  holder       TEXT NOT NULL,
  heartbeat_at INTEGER NOT NULL
);`

// newHolderToken mints a lease's fencing token. crypto/rand keeps two holders
// from ever colliding without adding a UUID dependency for 16 bytes of entropy.
func newHolderToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("mint holder token: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

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
	// dbPath is the same SQLite file the bus opens, so this goes through
	// sqlite.OpenDB rather than building its own DSN — that used to be
	// duplicated here, and the duplication is what let this file inherit a
	// pragma-order bug from pkg/bus/sqlite before it was fixed there. One
	// helper, one place to get "how this project opens its database file"
	// right; see sqlite.OpenDB's doc comment for why WAL is set post-connect.
	db, err := sqlite.OpenDB(ctx, dbPath)
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

	// A fresh token on both paths — insert and stale reclaim — is what fences the
	// previous holder out of the row it no longer owns.
	holder, err := newHolderToken()
	if err != nil {
		return nil, err
	}

	res, err := m.db.ExecContext(ctx,
		`INSERT INTO leases (path, agent, branch, holder, heartbeat_at) VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(path) DO UPDATE SET
		   agent = excluded.agent,
		   branch = excluded.branch,
		   holder = excluded.holder,
		   heartbeat_at = excluded.heartbeat_at
		 WHERE leases.heartbeat_at < ?`,
		path, agent, branch, holder, now, staleBefore)
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

	// rollbackCtx deliberately outlives ctx's cancellation: git invocations
	// below honour ctx (see git.go), so a cancellation timed inside e.g.
	// worktreeAdd would fail both the add AND a rollback DELETE that used the
	// same ctx, leaving a claimed lease row with no worktree behind it. That
	// self-heals after the lease TTL, but there is no reason to leave a
	// reclaimable-only-by-timeout row when the compensating delete could just
	// run to completion instead. Cleanup running on a cancelled ctx's request
	// is the whole point of cleanup, so it gets its own, uncancellable ctx.
	rollbackCtx := context.WithoutCancel(ctx)

	// Reclaiming a stale lease is crash recovery: the previous holder never
	// called Release, so its worktree directory is still on disk. git worktree
	// add fails outright on an existing directory (-B only resets the branch
	// pointer, it doesn't help here), so only add a fresh worktree when the
	// path doesn't already exist; otherwise reattach to what's there.
	if _, statErr := os.Stat(path); statErr != nil {
		if !os.IsNotExist(statErr) {
			_, _ = m.db.ExecContext(rollbackCtx, `DELETE FROM leases WHERE path = ?`, path)
			return nil, fmt.Errorf("stat worktree %s: %w", path, statErr)
		}
		if err := worktreeAdd(ctx, m.repo, path, branch); err != nil {
			_, _ = m.db.ExecContext(rollbackCtx, `DELETE FROM leases WHERE path = ?`, path)
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
		onDisk, err := worktreeBranch(ctx, path)
		if err != nil {
			_, _ = m.db.ExecContext(rollbackCtx, `DELETE FROM leases WHERE path = ?`, path)
			return nil, err
		}
		if onDisk != branch {
			_, _ = m.db.ExecContext(rollbackCtx, `DELETE FROM leases WHERE path = ?`, path)
			return nil, fmt.Errorf("workspace: reclaim of agent %q at %s found branch %q checked out, requested %q", agent, path, onDisk, branch)
		}
	}
	return &Lease{Agent: agent, Path: path, Branch: branch, holder: holder, m: m}, nil
}

// Lease is one agent's exclusive hold on a worktree. Path becomes the spawned
// session's working directory.
type Lease struct {
	Agent  string
	Path   string
	Branch string

	// holder is this lease's fencing token; see the leaseSchema comment.
	holder string

	m *Manager
}

// Heartbeat renews the lease. A holder that stops heartbeating is presumed dead
// and its workspace becomes reclaimable.
//
// It returns ErrLeaseLost when the row is no longer this holder's, which is the
// only way a holder can learn it was reclaimed. Renewing on agent name alone
// silently kept a lost lease alive, so both holders believed they owned it.
func (l *Lease) Heartbeat(ctx context.Context) error {
	res, err := l.m.db.ExecContext(ctx,
		`UPDATE leases SET heartbeat_at = ? WHERE path = ? AND agent = ? AND holder = ?`,
		time.Now().UnixMilli(), l.Path, l.Agent, l.holder)
	if err != nil {
		return fmt.Errorf("heartbeat %s: %w", l.Path, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("heartbeat %s: %w", l.Path, err)
	}
	if n == 0 {
		return fmt.Errorf("heartbeat %s: %w", l.Path, ErrLeaseLost)
	}
	return nil
}

// Release drops the lease row and removes the worktree, in that order.
//
// The row goes first because deleting it IS the ownership check: if it affects
// no rows this holder was already fenced out, and returning before touching the
// filesystem is what stops a stale holder from force-removing a directory the
// new holder is working in.
func (l *Lease) Release(ctx context.Context) error {
	res, err := l.m.db.ExecContext(ctx,
		`DELETE FROM leases WHERE path = ? AND agent = ? AND holder = ?`, l.Path, l.Agent, l.holder)
	if err != nil {
		return fmt.Errorf("release %s: %w", l.Path, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("release %s: %w", l.Path, err)
	}
	if n == 0 {
		return fmt.Errorf("release %s: %w", l.Path, ErrLeaseLost)
	}
	if err := worktreeRemove(ctx, l.m.repo, l.Path); err != nil {
		return err
	}
	return nil
}
