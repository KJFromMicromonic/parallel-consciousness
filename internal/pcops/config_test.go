package pcops_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KJFromMicromonic/parallel-consciousness/internal/pcops"
	"gopkg.in/yaml.v3"
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
	if cfg.Repo != "./fixtures/two-service" {
		t.Errorf("Repo = %q", cfg.Repo)
	}
	if cfg.GateID != "checkout" {
		t.Errorf("GateID = %q", cfg.GateID)
	}
	if cfg.Gate.Runner != "integrator" || cfg.Gate.Run != "go test ./integration/..." {
		t.Errorf("Gate = %+v", cfg.Gate)
	}
	if len(cfg.Gate.Required) != 2 || cfg.Gate.Required[0] != "billing" || cfg.Gate.Required[1] != "gateway" {
		t.Errorf("Gate.Required = %v", cfg.Gate.Required)
	}
	if len(cfg.Agents) != 2 {
		t.Fatalf("Agents length = %d, want 2", len(cfg.Agents))
	}
	if cfg.Agents[0].Name != "billing" {
		t.Errorf("Agents[0].Name = %q", cfg.Agents[0].Name)
	}
	if cfg.Agents[0].Branch != "agent/billing" {
		t.Errorf("Agents[0].Branch = %q", cfg.Agents[0].Branch)
	}
	if cfg.Agents[0].Role != "implementer" {
		t.Errorf("Agents[0].Role = %q", cfg.Agents[0].Role)
	}
	if cfg.Agents[0].Task != "Accept a currency field." {
		t.Errorf("Agents[0].Task = %q", cfg.Agents[0].Task)
	}
	if cfg.Agents[1].Name != "gateway" {
		t.Errorf("Agents[1].Name = %q", cfg.Agents[1].Name)
	}
	if cfg.Agents[1].Branch != "agent/gateway" {
		t.Errorf("Agents[1].Branch = %q", cfg.Agents[1].Branch)
	}
	if cfg.Agents[1].Role != "implementer" {
		t.Errorf("Agents[1].Role = %q", cfg.Agents[1].Role)
	}
	if cfg.Agents[1].Task != "Send a currency field." {
		t.Errorf("Agents[1].Task = %q", cfg.Agents[1].Task)
	}
	if cfg.Runner.Name != "integrator" {
		t.Errorf("Runner.Name = %q", cfg.Runner.Name)
	}
	if cfg.Runner.Branch != "agent/integration" {
		t.Errorf("Runner.Branch = %q", cfg.Runner.Branch)
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

// TestGateDefYAMLTagsAreLoadBearing unmarshals straight into a GateDef,
// bypassing LoadConfig entirely. rawConfig.Gate embeds GateDef inline so that
// GateDef's own yaml tags are what populate it — a field added to GateDef
// without also being wired into a hand-copied intermediate struct would
// otherwise be silently dropped. TestLoadConfig alone cannot distinguish that
// failure mode from GateDef's tags genuinely working, because it goes through
// LoadConfig's own field-by-field assembly either way.
func TestGateDefYAMLTagsAreLoadBearing(t *testing.T) {
	var g pcops.GateDef
	body := `
required: [billing, gateway]
runner: integrator
run: go test ./integration/...
`
	if err := yaml.Unmarshal([]byte(body), &g); err != nil {
		t.Fatal(err)
	}
	if len(g.Required) != 2 || g.Required[0] != "billing" || g.Required[1] != "gateway" {
		t.Errorf("Required = %v", g.Required)
	}
	if g.Runner != "integrator" {
		t.Errorf("Runner = %q", g.Runner)
	}
	if g.Run != "go test ./integration/..." {
		t.Errorf("Run = %q", g.Run)
	}
}

func TestMissingDBIsAnError(t *testing.T) {
	if _, err := pcops.LoadConfig(writeConfig(t, "gate:\n  id: g\n")); err == nil {
		t.Fatal("want an error when no db is configured")
	}
}
