package pcops_test

import (
	"strings"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/bus/sqlite"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/protocol"
)

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
