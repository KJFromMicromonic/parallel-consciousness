package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
)

// openersPerRepeat and coldRepeats mirror the throwaway probe that diagnosed
// F3: it saw 5 failures in 15 trials with just two processes racing a cold,
// not-yet-existing database file while journal_mode(WAL) sat in the DSN.
// Several openers per repeat, over several repeats, gives this test the same
// (or better) odds of catching a regression that reintroduces the race.
const (
	openersPerRepeat = 8
	coldRepeats      = 5
)

// TestOpenDBConcurrentColdStart opens the SAME cold (not-yet-existing)
// database path from several goroutines at once, several times over with a
// fresh path each time, and requires every opener to succeed with the
// database actually left in WAL mode. This is the regression test for F3:
// unit tests that each open their own database can never catch a race that
// only exists when multiple processes contend for the SAME fresh file.
func TestOpenDBConcurrentColdStart(t *testing.T) {
	for r := 0; r < coldRepeats; r++ {
		path := filepath.Join(t.TempDir(), "bus.db")
		dbs, errs := openConcurrently(t, path, openersPerRepeat)
		for i, err := range errs {
			if err != nil {
				t.Fatalf("repeat %d opener %d: OpenDB(%q): %v", r, i, path, err)
			}
		}
		requireWAL(t, dbs, r)
		closeAll(dbs)
	}
}

// TestOpenDBConcurrentWarm asserts the counterpart the probe measured: once a
// database has been opened and closed once (so journal_mode=wal is already
// persisted in its header), a fresh round of concurrent opens against that
// now-warm file must all succeed too — the retry path in ensureWAL should
// never even engage.
func TestOpenDBConcurrentWarm(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bus.db")

	warm, err := sqlite.OpenDB(context.Background(), path)
	if err != nil {
		t.Fatalf("warm-up OpenDB: %v", err)
	}
	if err := warm.Close(); err != nil {
		t.Fatalf("warm-up Close: %v", err)
	}

	dbs, errs := openConcurrently(t, path, openersPerRepeat)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("warm opener %d: OpenDB(%q): %v", i, path, err)
		}
	}
	requireWAL(t, dbs, -1)
	closeAll(dbs)
}

// openConcurrently starts n goroutines all calling sqlite.OpenDB against the
// same path at once and waits for all of them to return.
func openConcurrently(t *testing.T, path string, n int) ([]*sql.DB, []error) {
	t.Helper()
	dbs := make([]*sql.DB, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			db, err := sqlite.OpenDB(context.Background(), path)
			dbs[i] = db
			errs[i] = err
		}(i)
	}
	wg.Wait()
	return dbs, errs
}

// requireWAL asserts every non-nil *sql.DB reports journal_mode "wal".
func requireWAL(t *testing.T, dbs []*sql.DB, repeat int) {
	t.Helper()
	for i, db := range dbs {
		if db == nil {
			continue // the corresponding error already failed the test
		}
		var mode string
		if err := db.QueryRowContext(context.Background(), `PRAGMA journal_mode`).Scan(&mode); err != nil {
			t.Fatalf("repeat %d opener %d: read journal_mode: %v", repeat, i, err)
		}
		if !strings.EqualFold(mode, "wal") {
			t.Errorf("repeat %d opener %d: journal_mode = %q, want wal", repeat, i, mode)
		}
	}
}

func closeAll(dbs []*sql.DB) {
	for _, db := range dbs {
		if db != nil {
			db.Close()
		}
	}
}
