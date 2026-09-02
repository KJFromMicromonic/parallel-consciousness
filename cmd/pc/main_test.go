package main

import (
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
)

// pcops.Run injects PC_SUBMIT_TIMEOUT into every session it spawns. Ignoring it
// here is what made budget.submit_timeout dead configuration: a spawned agent's
// `pc submit` parked for the 5-minute default whatever the scenario said.
func TestSubmitTimeoutComesFromTheEnvironment(t *testing.T) {
	t.Setenv("PC_DB", "/tmp/pc-test.db")
	t.Setenv("PC_SUBMIT_TIMEOUT", "90s")

	cfg, err := loadConfig("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SubmitTimeout != 90*time.Second {
		t.Errorf("SubmitTimeout = %v, want 90s from $PC_SUBMIT_TIMEOUT", cfg.SubmitTimeout)
	}
}

// An absent or unusable value must fall back to the default, never to zero: a
// zero timeout builds an already-expired context and reports "no verdict"
// instantly, which an agent cannot distinguish from a real stall.
func TestSubmitTimeoutFallsBackToTheDefault(t *testing.T) {
	wantDefault := func(value string) {
		t.Helper()
		t.Setenv("PC_SUBMIT_TIMEOUT", value)
		if got := submitTimeoutFromEnv(); got != pcops.DefaultSubmitTimeout {
			t.Errorf("submitTimeoutFromEnv() with %q = %v, want the %v default",
				value, got, pcops.DefaultSubmitTimeout)
		}
	}
	wantDefault("")     // unset
	wantDefault("soon") // unparseable
	wantDefault("0s")   // zero expires instantly
	wantDefault("-5s")
}
