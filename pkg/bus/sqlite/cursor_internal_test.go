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
