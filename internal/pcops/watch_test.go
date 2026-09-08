package pcops_test

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/agent"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/gate"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

// notifyingWriter is a thread-safe io.Writer that closes notify on its first
// write. A test running Watch's follow loop in a goroutine uses this to wait
// for actual output — proof the loop is live and has made it past its first
// Tail poll — rather than guessing at how long that takes with a fixed delay.
type notifyingWriter struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	notify chan struct{}
	once   sync.Once
}

func newNotifyingWriter() *notifyingWriter {
	return &notifyingWriter{notify: make(chan struct{})}
}

func (w *notifyingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.buf.Write(p)
	w.once.Do(func() { close(w.notify) })
	return n, err
}

func (w *notifyingWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func rec(from, toAgent, toTopic string, intent protocol.Intent, body map[string]any) sqlite.Record {
	m := protocol.New(protocol.Address{Agent: from},
		protocol.Address{Agent: toAgent, Topic: toTopic}, intent, body)
	m.Timestamp = time.Date(2026, 9, 8, 15, 52, 58, 0, time.UTC)
	return sqlite.Record{Seq: 1, Msg: m}
}

func TestFormatRecord(t *testing.T) {
	cases := []struct {
		name string
		in   sqlite.Record
		want []string // substrings that must all appear
	}{
		{
			name: "ready abbreviates the version",
			in:   rec("billing", "", "gate.currency", protocol.IntentReady, map[string]any{"gate": "currency", "version": "acef4043778b966dc1cae9819e282d65f6b95e22"}),
			want: []string{"15:52:58", "billing", "#gate.currency", "ready", "v=acef4043"},
		},
		{
			name: "request lists each participant's version",
			in:   rec("coordinator", "integrator", "", protocol.IntentRequest, map[string]any{"gate": "currency", "versions": map[string]any{"billing": "acef4043778b966dc1cae9819e282d65f6b95e22"}}),
			want: []string{"coordinator", "integrator", "request", "billing=acef4043"},
		},
		{
			name: "inform shows the verdict text",
			in:   rec("coordinator", "", "gate.currency", protocol.IntentInform, map[string]any{"gate": "currency", "passed": false, "text": "currency FAILED: boom"}),
			want: []string{"inform", "currency FAILED: boom"},
		},
		{
			name: "block shows the routed detail",
			in:   rec("coordinator", "billing", "", protocol.IntentBlock, map[string]any{"gate": "currency", "text": "currency gate failing: boom"}),
			want: []string{"billing", "block", "currency gate failing"},
		},
		{
			name: "ack shows who it is still waiting on",
			in:   rec("coordinator", "billing", "", protocol.IntentAck, map[string]any{"gate": "currency", "outstanding": []any{"gateway"}}),
			want: []string{"billing", "ack", "waiting on gateway"},
		},
		{
			name: "nack shows what the in-flight round is testing, not outstanding",
			in: rec("coordinator", "gateway", "", protocol.IntentNack, map[string]any{
				"gate": "currency",
				"testing": map[string]any{
					"billing": "acef4043778b966dc1cae9819e282d65f6b95e22",
					"gateway": "v1",
				},
			}),
			want: []string{"gateway", "nack", "mid-round, testing", "billing@acef4043", "gateway@v1"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := pcops.FormatRecord(c.in, false)
			for _, w := range c.want {
				if !strings.Contains(got, w) {
					t.Errorf("line %q missing %q", got, w)
				}
			}
		})
	}
}

func TestFormatRecordTruncatesUnlessFull(t *testing.T) {
	long := strings.Repeat("x", 400)
	r := rec("integrator", "coordinator", "", protocol.IntentDisagree,
		map[string]any{"gate": "currency", "detail": long})

	short := pcops.FormatRecord(r, false)
	if len(short) > 200 {
		t.Errorf("not truncated: %d chars", len(short))
	}
	if !strings.Contains(short, "…") {
		t.Error("truncation not marked")
	}
	if full := pcops.FormatRecord(r, true); !strings.Contains(full, long) {
		t.Error("--full did not include the whole detail")
	}
}

// Watch must render a real round: readiness, the runner request, the verdict
// and the routed blocks — including the direct messages Subscribe would hide.
func TestWatchRendersACompletedRound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	cfg := pcops.Config{
		DB:            db,
		GateID:        "g",
		Gate:          pcops.GateDef{Required: []string{"billing"}, Runner: "runner"},
		SubmitTimeout: 20 * time.Second,
	}

	cstop, err := pcops.StartCoordinator(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cstop()

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
		return gate.Verdict{GateID: gateID, Passed: false, Detail: "spanning test failed"}
	})
	go run.Run(ctx)

	if _, err := pcops.Submit(ctx, cfg, "g", "billing", "v1"); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	var buf bytes.Buffer
	if err := pcops.Watch(ctx, cfg, "g", false, false, &buf); err != nil {
		t.Fatalf("Watch: %v", err)
	}
	got := buf.String()
	for _, want := range []string{
		"billing", "ready", "v=v1",
		"runner", "request",
		"disagree",
		"inform", "g FAILED",
		"block",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("watch output missing %q\n--- got ---\n%s", want, got)
		}
	}
}

func TestWatchFiltersByGate(t *testing.T) {
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "bus.db")
	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })

	mine := protocol.New(protocol.Address{Agent: "a"}, protocol.Address{Topic: gate.Topic("mine")},
		protocol.IntentInform, map[string]any{"gate": "mine", "text": "KEEP"})
	other := protocol.New(protocol.Address{Agent: "a"}, protocol.Address{Topic: gate.Topic("other")},
		protocol.IntentInform, map[string]any{"gate": "other", "text": "DROP"})
	for _, m := range []protocol.Message{mine, other} {
		if err := b.Publish(ctx, m); err != nil {
			t.Fatal(err)
		}
	}

	var buf bytes.Buffer
	if err := pcops.Watch(ctx, pcops.Config{DB: db}, "mine", false, false, &buf); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); !strings.Contains(got, "KEEP") || strings.Contains(got, "DROP") {
		t.Fatalf("gate filter wrong:\n%s", got)
	}
}

// A clean Ctrl-C while --follow is blocked inside Tail must surface as
// context.Canceled specifically — not merely as some non-nil error, which a
// mid-tail database failure would also produce. This is what actually drives
// Watch's follow loop and cancels while it is live and polling, unlike the
// cmd/pc-level cancellation test, which cancels before Watch ever opens its
// bus and so never reaches Tail at all.
func TestWatchStopsCleanlyWhenCancelledMidFollow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := filepath.Join(t.TempDir(), "bus.db")
	b, err := sqlite.Open(ctx, db, sqlite.WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })

	m := protocol.New(protocol.Address{Agent: "a"}, protocol.Address{Topic: gate.Topic("g")},
		protocol.IntentInform, map[string]any{"gate": "g", "text": "hello"})
	if err := b.Publish(ctx, m); err != nil {
		t.Fatal(err)
	}

	out := newNotifyingWriter()
	errCh := make(chan error, 1)
	go func() {
		errCh <- pcops.Watch(ctx, pcops.Config{DB: db}, "g", false, true, out)
	}()

	select {
	case <-out.notify:
		// The follow loop has replayed the published message and is now
		// parked in Tail's poll, exactly the state this test needs to cancel
		// mid-stream rather than before Watch even starts.
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for Watch to produce output before cancelling")
	}

	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Watch after mid-follow cancellation = %v, want an error satisfying errors.Is(err, context.Canceled)", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for Watch to return after cancellation")
	}
}
