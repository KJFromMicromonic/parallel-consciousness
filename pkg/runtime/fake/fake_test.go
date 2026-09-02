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
	}
	runtimetest.Run(t, func(t *testing.T) runtime.Runtime {
		return fake.New(scripts)
	}, runtime.Spec{Agent: "a", Workdir: t.TempDir()}, runtimetest.Options{
		Completes: runtime.Spec{Agent: "b", Workdir: t.TempDir()},
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
