package fake_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime/fake"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime/runtimetest"
)

func TestFakeConformance(t *testing.T) {
	scripts := map[string]fake.Script{
		"a": {
			OnStart: []fake.Action{fake.Emit{Tool: runtime.ToolUse{Name: "read", Target: "x", Ok: true}}},
			OnSteer: func(text string) []fake.Action {
				return []fake.Action{fake.Emit{Tool: runtime.ToolUse{Name: "read", Target: "steer", Ok: true}}}
			},
		},
		// "b" ends on its own with no Close/Interrupt, so the suite's
		// natural-end property has something to observe.
		"b": {
			OnStart: []fake.Action{fake.Exit{}},
		},
		// "c" spends 30s in a single Exec so InterruptPreemptsInFlightWork
		// can catch it genuinely mid-flight and prove Interrupt cuts it off
		// in seconds rather than waiting the 30s out.
		"c": {
			OnStart: []fake.Action{fake.Exec{Args: []string{"sh", "-c", "sleep 30"}}},
		},
		// "d" spends 3s in a single Exec, then reports a distinctive
		// completion marker — short enough that AnInFlightSteerDoesNotPreempt
		// and AnInFlightFollowDoesNotPreempt (run under -count=5) don't stall
		// on tens of seconds of real sleep the way "c" deliberately does.
		// Its steer/follow scripts each report their own distinctive marker
		// once run, so the properties can assert the order those three
		// markers arrive in.
		"d": {
			OnStart: []fake.Action{
				fake.Exec{Args: []string{"sh", "-c", "sleep 3"}},
				fake.Emit{Tool: runtime.ToolUse{Name: "exec", Target: "work-done", Ok: true}},
			},
			OnSteer: func(text string) []fake.Action {
				return []fake.Action{fake.Emit{Tool: runtime.ToolUse{Name: "steer", Target: "steer-applied", Ok: true}}}
			},
			OnFollow: func(text string) []fake.Action {
				return []fake.Action{fake.Emit{Tool: runtime.ToolUse{Name: "follow", Target: "follow-applied", Ok: true}}}
			},
		},
	}
	runtimetest.Run(t, func(t *testing.T) runtime.Runtime {
		return fake.New(scripts)
	}, runtime.Spec{Agent: "a", Workdir: t.TempDir()}, runtimetest.Options{
		Completes:           runtime.Spec{Agent: "b", Workdir: t.TempDir()},
		LongRunning:         runtime.Spec{Agent: "c", Workdir: t.TempDir()},
		InFlightWork:        runtime.Spec{Agent: "d", Workdir: t.TempDir()},
		WorkDoneMarker:      "work-done",
		SteerAppliedMarker:  "steer-applied",
		FollowAppliedMarker: "follow-applied",
	})
}

func TestSteerRunsTheSteerScript(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	r := fake.New(map[string]fake.Script{
		"a": {
			OnStart: []fake.Action{fake.Write{Path: "original.txt", Content: "ORIGINAL"}},
			OnSteer: func(text string) []fake.Action {
				return []fake.Action{fake.Write{Path: "steered.txt", Content: text}}
			},
		},
	})
	s, err := r.Start(ctx, runtime.Spec{Agent: "a", Workdir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)

	// Wait for the first idle, then steer and wait for the second.
	waitIdle(t, s)
	if err := s.Steer(ctx, "fix it"); err != nil {
		t.Fatal(err)
	}
	waitIdle(t, s)

	got, err := os.ReadFile(filepath.Join(dir, "steered.txt"))
	if err != nil {
		t.Fatalf("steer script did not run: %v", err)
	}
	if string(got) != "fix it" {
		t.Fatalf("steered.txt = %q, want %q", got, "fix it")
	}
}

func waitIdle(t *testing.T, s runtime.Session) {
	t.Helper()
	for ev := range s.Events() {
		if ev.Kind == runtime.KindIdle {
			return
		}
	}
	t.Fatal("stream closed before idle")
}
