package pcops_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
	"github.com/KJFromMicromonic/parallel-consciousness/pkg/runtime/fake"
)

// buildPC compiles cmd/pc so the fake agents can invoke the real CLI, exactly
// as a coding agent's bash tool would.
func buildPC(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "pc")
	out, err := exec.Command("go", "build", "-o", bin,
		"github.com/KJFromMicromonic/parallel-consciousness/cmd/pc").CombinedOutput()
	if err != nil {
		t.Fatalf("build pc: %v: %s", err, out)
	}
	return bin
}

// twoServiceRepo builds the fixture: a spanning check that passes only when
// BOTH files carry the currency field. Neither agent can satisfy it alone.
func twoServiceRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	git(t, dir, "init", "-q", "-b", "main")
	os.WriteFile(filepath.Join(dir, "billing.txt"), []byte("amount\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "gateway.txt"), []byte("amount\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "check.sh"),
		[]byte("#!/bin/sh\ngrep -q currency billing.txt && grep -q currency gateway.txt\n"), 0o755)
	git(t, dir, "add", ".")
	git(t, dir, "commit", "-q", "-m", "init")
	return dir
}

func TestRunConvergesAfterAFailingRound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	repo := twoServiceRepo(t)
	pc := buildPC(t)
	db := filepath.Join(t.TempDir(), "bus.db")

	cfg := pcops.Config{
		Repo:   repo,
		DB:     db,
		GateID: "checkout",
		Gate: pcops.GateDef{
			Required: []string{"billing", "gateway"},
			Runner:   "integrator",
			Run:      "sh check.sh",
		},
		Agents: []pcops.AgentDef{
			{Name: "billing", Branch: "agent/billing", Role: "implementer", Task: "add currency"},
			{Name: "gateway", Branch: "agent/gateway", Role: "implementer", Task: "send currency"},
		},
		Runner:        pcops.AgentDef{Name: "integrator", Branch: "agent/integration"},
		SubmitTimeout: 60 * time.Second,
		Wall:          90 * time.Second,
	}

	// Round one: billing does its half, gateway does NOT. The spanning check
	// must fail, routing a block to both owners. On being steered, each agent
	// writes the currency field and re-submits.
	submit := fake.Exec{Args: []string{pc, "submit", "--gate", "checkout"}}
	fixAndResubmit := func(path string) func(string) []fake.Action {
		return func(text string) []fake.Action {
			return []fake.Action{fake.Write{Path: path, Content: "amount currency\n"}, commitAll, submit}
		}
	}

	r := fake.New(map[string]fake.Script{
		"billing": {
			OnStart: []fake.Action{fake.Write{Path: "billing.txt", Content: "amount currency\n"}, commitAll, submit},
			OnSteer: fixAndResubmit("billing.txt"),
		},
		"gateway": {
			OnStart: []fake.Action{fake.Write{Path: "gateway.txt", Content: "amount\n"}, commitAll, submit},
			OnSteer: fixAndResubmit("gateway.txt"),
		},
	})

	v, err := pcops.Run(ctx, cfg, r)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !v.Passed {
		t.Fatalf("loop never converged: %+v", v)
	}
}

// commitAll is how a fake agent publishes its work to its branch.
var commitAll = fake.Exec{Args: []string{"sh", "-c",
	"git add -A && git -c user.email=t@example.com -c user.name=t commit -q -m work"}}
