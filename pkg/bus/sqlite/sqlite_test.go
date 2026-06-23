package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

func TestOpenCreatesSchema(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "bus.db")

	b, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer b.Close()

	// Inspect the file with an independent connection.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tables := map[string]bool{}
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		tables[n] = true
	}
	if !tables["messages"] || !tables["cursors"] {
		t.Fatalf("missing tables, got %v", tables)
	}
}

func TestPublishInsertsRow(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "bus.db")
	b, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	msg := protocol.New(
		protocol.Address{Agent: "billing"},
		protocol.Address{Topic: "gate.checkout"},
		protocol.IntentReady,
		map[string]any{"gate": "checkout", "version": "c3d4"},
	)
	if err := b.Publish(ctx, msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var fromAgent, toTopic, intent, body string
	row := db.QueryRow(`SELECT from_agent, to_topic, intent, body FROM messages WHERE id = ?`, msg.ID)
	if err := row.Scan(&fromAgent, &toTopic, &intent, &body); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if fromAgent != "billing" || toTopic != "gate.checkout" || intent != "ready" {
		t.Fatalf("row = from=%q topic=%q intent=%q", fromAgent, toTopic, intent)
	}
	if !strings.Contains(body, `"version":"c3d4"`) {
		t.Fatalf("body = %q", body)
	}
}
