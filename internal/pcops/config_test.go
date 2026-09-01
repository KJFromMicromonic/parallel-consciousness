package pcops_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
)

const sample = `
repo: ./fixtures/two-service
db: .pc/poc.db
gate:
  id: checkout
  required: [billing, gateway]
  runner: integrator
  run: go test ./integration/...
agents:
  - name: billing
    branch: agent/billing
    role: implementer
    task: "Accept a currency field."
  - name: gateway
    branch: agent/gateway
    role: implementer
    task: "Send a currency field."
runner:
  name: integrator
  branch: agent/integration
budget:
  wall: 15m
`

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "poc.yaml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadConfig(t *testing.T) {
	cfg, err := pcops.LoadConfig(writeConfig(t, sample))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GateID != "checkout" {
		t.Errorf("GateID = %q", cfg.GateID)
	}
	if cfg.Gate.Runner != "integrator" || cfg.Gate.Run != "go test ./integration/..." {
		t.Errorf("Gate = %+v", cfg.Gate)
	}
	if len(cfg.Agents) != 2 || cfg.Agents[0].Name != "billing" {
		t.Errorf("Agents = %+v", cfg.Agents)
	}
	if cfg.Wall != 15*time.Minute {
		t.Errorf("Wall = %v", cfg.Wall)
	}
	if cfg.SubmitTimeout != 5*time.Minute {
		t.Errorf("SubmitTimeout default = %v, want 5m", cfg.SubmitTimeout)
	}
}

func TestPCDBOverridesConfig(t *testing.T) {
	t.Setenv("PC_DB", "/tmp/override.db")
	cfg, err := pcops.LoadConfig(writeConfig(t, sample))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DB != "/tmp/override.db" {
		t.Errorf("DB = %q, want the PC_DB override", cfg.DB)
	}
}

func TestMissingDBIsAnError(t *testing.T) {
	if _, err := pcops.LoadConfig(writeConfig(t, "gate:\n  id: g\n")); err == nil {
		t.Fatal("want an error when no db is configured")
	}
}
