package workspace

import (
	"context"
	"errors"
	"os"
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

func TestStaleLeaseBranchMismatchIsRejected(t *testing.T) {
	ctx := context.Background()
	repo := newRepo(t)
	m := newManager(t, repo)
	m.SetTTL(30 * time.Millisecond)

	if _, err := m.Acquire(ctx, "gateway", "agent/gateway"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond) // holder "crashed": no heartbeat

	_, err := m.Acquire(ctx, "gateway", "agent/gateway-v2")
	if err == nil {
		t.Fatal("reclaim with mismatched branch should have failed")
	}
	if !contains(err.Error(), "agent/gateway") || !contains(err.Error(), "agent/gateway-v2") {
		t.Fatalf("error should name both branches, got: %v", err)
	}

	// Lease row must have been rolled back: a reclaim with the correct
	// branch should now succeed.
	if _, err := m.Acquire(ctx, "gateway", "agent/gateway"); err != nil {
		t.Fatalf("reclaim with correct branch after rollback: %v", err)
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

// A stale reclaim hands the SAME agent and branch back at the SAME path — that
// is how crash recovery works — so the original holder is still a match on
// (path, agent) and, before the fencing token, kept renewing a lease it no
// longer owned while its Release force-removed the new holder's worktree.
//
// The reclaim is forced with a negative TTL rather than a sleep: staleBefore is
// now-TTL, so a negative TTL puts the threshold in the future and makes any
// existing row unconditionally stale. That is deterministic, where sleeping past
// a millisecond-resolution heartbeat is not.
func TestReclaimFencesTheOriginalHolder(t *testing.T) {
	ctx := context.Background()
	repo := newRepo(t)
	m := newManager(t, repo)

	first, err := m.Acquire(ctx, "billing", "agent/billing")
	if err != nil {
		t.Fatal(err)
	}

	m.SetTTL(-time.Second)
	second, err := m.Acquire(ctx, "billing", "agent/billing")
	if err != nil {
		t.Fatalf("stale reclaim: %v", err)
	}
	if second.Path != first.Path {
		t.Fatalf("reclaim moved the workspace: %q -> %q", first.Path, second.Path)
	}

	if err := first.Heartbeat(ctx); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("fenced holder's Heartbeat = %v, want ErrLeaseLost", err)
	}
	if err := first.Release(ctx); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("fenced holder's Release = %v, want ErrLeaseLost", err)
	}

	// The point of the fence: the new holder's worktree is still there, and the
	// new holder still owns the row.
	if _, err := os.Stat(second.Path); err != nil {
		t.Fatalf("fenced holder destroyed the new holder's worktree: %v", err)
	}
	if err := second.Heartbeat(ctx); err != nil {
		t.Fatalf("new holder's Heartbeat = %v, want nil", err)
	}
}
