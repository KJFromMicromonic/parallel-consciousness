// Package arch holds architecture tests. They protect boundaries that no
// compiler check enforces: the harness-agnostic claim is only true while
// pkg/ stays free of harness-specific imports.
package arch

import (
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

func TestPkgNeverImportsAHarnessAdapter(t *testing.T) {
	for pkg, ds := range deps(t) {
		if !strings.HasPrefix(pkg, modulePath+"/pkg/") {
			continue
		}
		for _, d := range ds {
			if strings.HasPrefix(d, modulePath+"/internal/runtime/") {
				t.Errorf("%s imports harness adapter %s: pkg/ must stay harness-agnostic", pkg, d)
			}
		}
	}
}

func TestRuntimeInterfaceIsStdlibOnly(t *testing.T) {
	for _, d := range deps(t)[modulePath+"/pkg/runtime"] {
		// Standard library import paths have no dot in their first segment.
		if strings.Contains(strings.Split(d, "/")[0], ".") {
			t.Errorf("pkg/runtime imports non-stdlib package %s", d)
		}
	}
}
