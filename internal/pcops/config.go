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
//
// It must exceed DefaultRunnerTimeout. A readiness that lands while a round is
// already in flight is nacked, and the submit then waits for that round to
// resolve before re-declaring — so a submit whose own budget is shorter than a
// round's exhausts its context before the round it is waiting for can finish,
// and reports ErrNoVerdict for a gate that was working correctly. LoadConfig
// enforces the same relationship for configured values.
const DefaultSubmitTimeout = 12 * time.Minute

// DefaultRunnerTimeout bounds how long the coordinator waits for the runner's
// spanning test before declaring the round stalled. Ten minutes is sized for a
// real integration suite, not a unit test.
const DefaultRunnerTimeout = 10 * time.Minute

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
	RunnerTimeout time.Duration
	Wall          time.Duration // zero means unbounded
}

type rawConfig struct {
	Repo string `yaml:"repo"`
	DB   string `yaml:"db"`
	// Gate embeds GateDef inline instead of redeclaring its fields, so
	// GateDef's own yaml tags are what actually populate it. Two struct
	// definitions carrying the same tags used to exist side by side, with
	// LoadConfig manually copying field by field between them — which meant
	// GateDef's tags were decorative: a field added there without also
	// touching this struct and the copy was silently dropped, with no error.
	Gate struct {
		ID      string `yaml:"id"`
		GateDef `yaml:",inline"`
	} `yaml:"gate"`
	Agents []AgentDef `yaml:"agents"`
	Runner AgentDef   `yaml:"runner"`
	Budget struct {
		Wall          string `yaml:"wall"`
		SubmitTimeout string `yaml:"submit_timeout"`
		RunnerTimeout string `yaml:"runner_timeout"`
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
		Gate:          raw.Gate.GateDef,
		Agents:        raw.Agents,
		Runner:        raw.Runner,
		SubmitTimeout: DefaultSubmitTimeout,
		RunnerTimeout: DefaultRunnerTimeout,
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
	if raw.Budget.RunnerTimeout != "" {
		if cfg.RunnerTimeout, err = time.ParseDuration(raw.Budget.RunnerTimeout); err != nil {
			return Config{}, fmt.Errorf("budget.runner_timeout: %w", err)
		}
	}

	if err := cfg.validate(); err != nil {
		return Config{}, fmt.Errorf("invalid scenario %s: %w", path, err)
	}
	return cfg, nil
}

// validate rejects a scenario that parses but cannot work.
//
// Every rule here was a silent hang before it was an error. A gate naming a
// participant absent from agents waits for a readiness nobody will ever
// declare; a gate.runner that matches no runner sends its spanning-test
// request to an agent that does not exist; two agents sharing a branch fight
// over one worktree. In each case `pc up` started cleanly and the operator
// waited out the wall budget to learn nothing. Failing at load costs one line
// of output instead.
func (c Config) validate() error {
	if c.GateID == "" {
		return fmt.Errorf("gate.id must not be empty")
	}
	if c.Gate.Runner == "" {
		return fmt.Errorf("gate.runner must name the runner agent")
	}
	if c.Gate.Runner != c.Runner.Name {
		return fmt.Errorf("gate.runner is %q but runner.name is %q: the spanning-test request would go to an agent that does not exist",
			c.Gate.Runner, c.Runner.Name)
	}
	if len(c.Gate.Required) == 0 {
		return fmt.Errorf("gate.required must name at least one participant")
	}

	names := make(map[string]bool, len(c.Agents)+1)
	branches := make(map[string]bool, len(c.Agents)+1)
	for _, a := range append(append([]AgentDef(nil), c.Agents...), c.Runner) {
		if a.Name == "" {
			return fmt.Errorf("every agent needs a name")
		}
		if a.Branch == "" {
			return fmt.Errorf("agent %q needs a branch", a.Name)
		}
		if names[a.Name] {
			return fmt.Errorf("duplicate agent name %q: one active owner per participant", a.Name)
		}
		if branches[a.Branch] {
			return fmt.Errorf("duplicate branch %q: two participants would contend for one worktree", a.Branch)
		}
		names[a.Name] = true
		branches[a.Branch] = true
	}
	for _, r := range c.Gate.Required {
		if !names[r] {
			return fmt.Errorf("gate.required names %q, which is not in agents: the gate would wait forever for a readiness nobody declares", r)
		}
	}

	if c.SubmitTimeout <= c.RunnerTimeout {
		return fmt.Errorf("budget.submit_timeout (%v) must exceed budget.runner_timeout (%v): a readiness that lands mid-round is nacked and waits for that round to resolve, so a shorter submit budget expires before the round it is waiting for can finish",
			c.SubmitTimeout, c.RunnerTimeout)
	}
	return nil
}
