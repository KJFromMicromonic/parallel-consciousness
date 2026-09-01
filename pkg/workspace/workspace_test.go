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
