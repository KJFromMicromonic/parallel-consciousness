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
