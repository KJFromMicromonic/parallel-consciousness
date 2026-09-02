package pcops

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// gitT runs git in dir with a fixed identity so commits work on a bare CI box.
func gitT(t *testing.T, dir string, args ...string) {
	t.Helper()
	c := exec.Command("git", append([]string{"-C", dir}, args...)...)
	c.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com",
		"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com")
	if out, err := c.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
}

// A real conflict and a merge that failed for any other reason demand different
// responses from an agent, and the detail is the only thing it is told: routing
// "merge conflict" for a missing branch sends both owners hunting a conflict
// that does not exist.
func TestMergeAllDistinguishesAConflictFromAnyOtherFailure(t *testing.T) {
	ctx := context.Background()
	repo := t.TempDir()
	gitT(t, repo, "init", "-q", "-b", "main")
	write := func(dir, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, "contract.txt"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(repo, "amount\n")
	gitT(t, repo, "add", ".")
	gitT(t, repo, "commit", "-q", "-m", "init")

	// Two branches editing the same line: the second merge must conflict.
	for _, b := range []string{"a", "b"} {
		gitT(t, repo, "checkout", "-q", "-b", b, "main")
		write(repo, "amount "+b+"\n")
		gitT(t, repo, "add", ".")
		gitT(t, repo, "commit", "-q", "-m", b)
	}
	gitT(t, repo, "checkout", "-q", "main")

	work := filepath.Join(t.TempDir(), "integrator")
	gitT(t, repo, "worktree", "add", "-q", "-B", "agent/integration", work)

	detail, err := mergeAll(ctx, work, []string{"a", "b"})
	if err == nil {
		t.Fatal("merging two branches that edit the same line should have failed")
	}
	if !strings.Contains(detail, "merge conflict on b") {
		t.Fatalf("detail = %q, want it to report a conflict on b", detail)
	}

	// A branch that does not exist is not a conflict, and must not be reported
	// as one.
	detail, err = mergeAll(ctx, work, []string{"no-such-branch"})
	if err == nil {
		t.Fatal("merging a nonexistent branch should have failed")
	}
	if strings.Contains(detail, "conflict") {
		t.Fatalf("detail = %q, want it NOT to claim a conflict", detail)
	}
	if !strings.Contains(detail, "merge failed on no-such-branch") {
		t.Fatalf("detail = %q, want it to report a plain merge failure", detail)
	}
}
