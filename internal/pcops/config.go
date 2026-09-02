// Package pcops is the composition root: the one place that knows about the
// gate, the bus, workspace leases, and the agent runtime at the same time. The
// CLI in cmd/pc is a thin skin over these functions so no coordination logic
// ever lives in a command.
package pcops

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// DefaultSubmitTimeout is how long `pc submit` waits for a verdict before
// reporting that none was obtained.
const DefaultSubmitTimeout = 5 * time.Minute

// GateDef is one gate: who must be ready, who runs the spanning test, and what
// command that runner executes.
type GateDef struct {
	Required []string `yaml:"required"`
	Runner   string   `yaml:"runner"`
	Run      string   `yaml:"run"`
}

// AgentDef is one participant the orchestrator launches.
type AgentDef struct {
	Name   string `yaml:"name"`
	Branch string `yaml:"branch"`
	Role   string `yaml:"role"`
	Task   string `yaml:"task"`
}

// Config is a scenario: a hand-written loop definition. It is the seed of the
// loop engine, and eventually the artifact a loop author generates.
type Config struct {
	Repo          string
	DB            string
	GateID        string
	Gate          GateDef
	Agents        []AgentDef
	Runner        AgentDef
	SubmitTimeout time.Duration
	Wall          time.Duration // zero means unbounded
}

type rawConfig struct {
	Repo string `yaml:"repo"`
	DB   string `yaml:"db"`
	Gate struct {
		ID       string   `yaml:"id"`
		Required []string `yaml:"required"`
		Runner   string   `yaml:"runner"`
		Run      string   `yaml:"run"`
	} `yaml:"gate"`
	Agents []AgentDef `yaml:"agents"`
	Runner AgentDef   `yaml:"runner"`
	Budget struct {
		Wall          string `yaml:"wall"`
		SubmitTimeout string `yaml:"submit_timeout"`
	} `yaml:"budget"`
}

// LoadConfig reads a scenario file. $PC_DB overrides the configured database so
// a single scenario can be pointed at a scratch bus without editing the file.
func LoadConfig(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	var raw rawConfig
	if err := yaml.Unmarshal(b, &raw); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}

	cfg := Config{
		Repo:          raw.Repo,
		DB:            raw.DB,
		GateID:        raw.Gate.ID,
		Gate:          GateDef{Required: raw.Gate.Required, Runner: raw.Gate.Runner, Run: raw.Gate.Run},
		Agents:        raw.Agents,
		Runner:        raw.Runner,
		SubmitTimeout: DefaultSubmitTimeout,
	}
	if v := os.Getenv("PC_DB"); v != "" {
		cfg.DB = v
	}
	if cfg.DB == "" {
		return Config{}, fmt.Errorf("no database configured: set `db:` or $PC_DB")
	}
	if raw.Budget.Wall != "" {
		if cfg.Wall, err = time.ParseDuration(raw.Budget.Wall); err != nil {
			return Config{}, fmt.Errorf("budget.wall: %w", err)
		}
	}
	if raw.Budget.SubmitTimeout != "" {
		if cfg.SubmitTimeout, err = time.ParseDuration(raw.Budget.SubmitTimeout); err != nil {
			return Config{}, fmt.Errorf("budget.submit_timeout: %w", err)
		}
	}
	return cfg, nil
}
