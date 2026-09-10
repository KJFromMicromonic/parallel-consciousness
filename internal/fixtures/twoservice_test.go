package fixtures_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// fixtureRoot is the committed fixture. It is a separate Go module, so nothing
// in the parent module's ./... ever compiles or runs it — which is exactly why
// it needs a test here: an artifact excluded from the build surface rots
// silently, and a fixture that has rotted into a PASSING state is worse than a
// missing one, because a live run then proves nothing while appearing to work.
func fixtureRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", "fixtures", "two-service"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("fixture module not found at %s: %v", root, err)
	}
	return root
}

// copyTree copies src to dst so a test can run the fixture's own suite without
// mutating the committed copy.
//
// It skips .git: that directory is machine state a live run's `git init` /
// `git worktree add` leaves behind, not fixture content, and copying an
// entire repository (including worktree admin files) into a temp dir for
// every test run is both wasted work and a source of permission-bit and
// worktree-link surprises neither test here cares about.
func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == ".git" {
			return filepath.SkipDir
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			return os.Chmod(target, info.Mode())
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, b, info.Mode())
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Constraint 3, and the fixture's whole purpose: the baseline must FAIL under
// the gate command. If this ever passes, round one of a live run succeeds
// immediately, the failure-detail path is never exercised, and the
// demonstration silently stops demonstrating anything.
func TestFixtureBaselineFailsUnderTheGateCommand(t *testing.T) {
	dst := t.TempDir()
	copyTree(t, fixtureRoot(t), dst)

	cmd := exec.Command("go", "test", "./integration/...")
	cmd.Dir = dst
	cmd.Env = append(os.Environ(), "EXPECTED_CURRENCY=USD")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("fixture baseline PASSED under EXPECTED_CURRENCY=USD; round one of a live run would succeed immediately and the failure-detail path would never be exercised:\n%s", out)
	}
	if !strings.Contains(string(out), "EUR") {
		t.Errorf("baseline failed, but the failure detail does not mention the wrong value it found; an agent learns the right value from this text:\n%s", out)
	}
	if !strings.Contains(string(out), "USD") {
		t.Errorf("baseline failed, but the failure detail does not name the wanted value; that text is the ONLY channel through which an agent can learn a value absent from its worktree:\n%s", out)
	}
}

// Constraint 4: without the variable the spanning test skips, so an agent
// running the suite in its own worktree is not misled into thinking it broke
// something.
//
// The exit-zero check alone is insufficient: it cannot distinguish "the test
// skipped" from "the test ran and happened to pass", and those are different
// fixture states — only the former satisfies constraint 4. So this also
// asserts on the verbose output for the literal "--- SKIP" marker, which is
// the only way to confirm the test actually took the skip branch rather than
// running to completion.
func TestFixtureSpanningTestSkipsWithoutTheVariable(t *testing.T) {
	dst := t.TempDir()
	copyTree(t, fixtureRoot(t), dst)

	cmd := exec.Command("go", "test", "-v", "./integration/...")
	cmd.Dir = dst
	// Explicitly cleared rather than merely unset in this process's env.
	cmd.Env = append(os.Environ(), "EXPECTED_CURRENCY=")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("spanning test did not pass-by-skipping with no EXPECTED_CURRENCY; an agent running the suite locally would think it had broken something:\n%s", out)
	}
	if !strings.Contains(string(out), "--- SKIP") {
		t.Errorf("suite exited zero, but the output does not show the test skipping; an exit-zero pass could also mean the test ran and happened to pass, which does not satisfy constraint 4:\n%s", out)
	}
}

// Constraint 1: each half builds alone. If one service cannot compile without
// the other's change, the two agents deadlock instead of coordinating — which
// is what an earlier arrangement of this fixture actually caused.
func TestFixtureHalvesCompileIndependently(t *testing.T) {
	root := fixtureRoot(t)
	for _, pkg := range []string{"./billing/...", "./gateway/..."} {
		cmd := exec.Command("go", "build", pkg)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Errorf("go build %s failed, so this half cannot compile alone:\n%s", pkg, out)
		}
	}
}

// Constraint 2: the agreed value must be undiscoverable from either worktree.
// A model that can read USD out of the fixture fixes the code first try, and
// the failure branch — the interesting one — is never reached.
//
// This walk has exactly one exclusion: .git, and for the opposite reason a
// content exclusion would have one. .git under the fixture root is machine
// state a live run's `git init` / `git worktree add` leaves behind — reflogs
// and COMMIT_EDITMSG containing agents' own commit subjects — not something
// an agent is meant to read. Skipping it makes this walk cover MORE of what
// actually matters, not less. That is the opposite of the README.md
// exclusion this test used to carry: README.md is checked out into every
// agent's own worktree by live-run.sh, so it is exactly the kind of
// agent-visible content this test exists to catch, and excluding it by name
// was wrong. The fixture's maintainer documentation now lives outside the
// fixture module, at docs/fixtures/two-service.md, so there is nothing left
// to exempt.
func TestFixtureDoesNotContainTheAgreedValue(t *testing.T) {
	root := fixtureRoot(t)
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if info.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if strings.Contains(string(b), "USD") {
			rel, _ := filepath.Rel(root, path)
			t.Errorf("%s contains the agreed value: an agent can read it instead of learning it from a failing gate", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
