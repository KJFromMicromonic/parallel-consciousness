// Package arch holds architecture tests. They protect boundaries that no
// compiler check enforces: the harness-agnostic claim is only true while pkg/
// stays free of harness-specific imports, and pkg/ is only usable as public API
// while it stays free of internal/ altogether.
package arch

import (
	"errors"
	"os/exec"
	"strings"
	"testing"
)

const modulePath = "github.com/KJFromMicromonic/parallel-consciousness"

// deps returns package import path -> its full transitive dependency list.
// It shells out to `go list` so the check adds no module dependency.
func deps(t *testing.T) map[string][]string {
	t.Helper()
	out, err := exec.Command("go", "list", "-f", `{{.ImportPath}} {{join .Deps " "}}`, "../../...").Output()
	if err != nil {
		// Without Stderr a failure here prints only "exit status 1", which says
		// nothing about the broken package or module state that caused it.
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			t.Fatalf("go list: %v: %s", err, ee.Stderr)
		}
		t.Fatalf("go list: %v", err)
	}
	m := map[string][]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		m[fields[0]] = fields[1:]
	}
	return m
}

// The rule that actually matters is the layering one: pkg/ is public API and
// internal/ is private, so nothing under pkg/ may import internal/ AT ALL.
// Banning only internal/runtime/ left the boundary open to every other private
// package — including internal/pcops, the composition root, which would invert
// the dependency arrows the spec draws.
func TestPkgNeverImportsInternal(t *testing.T) {
	all := deps(t)
	seen := 0
	for pkg, ds := range all {
		if !strings.HasPrefix(pkg, modulePath+"/pkg/") {
			continue
		}
		seen++
		for _, d := range ds {
			if strings.HasPrefix(d, modulePath+"/internal/") {
				t.Errorf("%s imports %s: pkg/ is public API and may not reach into internal/", pkg, d)
			}
		}
	}
	// A rename, or `go list` degrading, would otherwise leave this test
	// iterating over nothing and passing without checking anything.
	if seen == 0 {
		t.Fatalf("no packages under %s/pkg/ found in the import graph (%d packages listed): the check would pass vacuously",
			modulePath, len(all))
	}
}

func TestRuntimeInterfaceIsStdlibOnly(t *testing.T) {
	all := deps(t)
	ds, ok := all[modulePath+"/pkg/runtime"]
	// A missing key yields nil, and a range over nil passes silently — so a
	// package rename or a degraded `go list` would retire this check without
	// anyone noticing. Demand the key.
	if !ok {
		t.Fatalf("%s/pkg/runtime is absent from the import graph (%d packages listed): the check would pass vacuously",
			modulePath, len(all))
	}
	for _, d := range ds {
		// Standard library import paths have no dot in their first segment.
		if strings.Contains(strings.Split(d, "/")[0], ".") {
			t.Errorf("pkg/runtime imports non-stdlib package %s", d)
		}
	}
}
