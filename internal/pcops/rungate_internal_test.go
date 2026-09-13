package pcops

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

	_, detail, err := mergeAll(ctx, work, []string{"a", "b"})
	if err == nil {
		t.Fatal("merging two branches that edit the same line should have failed")
	}
	if !strings.Contains(detail, "merge conflict on b") {
		t.Fatalf("detail = %q, want it to report a conflict on b", detail)
	}

	// A branch that does not exist is not a conflict, and must not be reported
	// as one.
	_, detail, err = mergeAll(ctx, work, []string{"no-such-branch"})
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

// The verdict has to be able to say what was actually tested, which means the
// runner must report the commit it merged rather than echoing back the version
// a participant declared. Those two diverge the moment an agent commits again
// after submitting, and a verdict naming the declared value is then a lie about
// what ran.
func TestMergeAllReportsTheShasItMerged(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	repo := t.TempDir()
	gitT(t, repo, "init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(repo, "seed.txt"), []byte("seed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitT(t, repo, "add", ".")
	gitT(t, repo, "commit", "-q", "-m", "init")

	want := map[string]string{}
	for _, b := range []string{"a", "b"} {
		gitT(t, repo, "checkout", "-q", "-b", b, "main")
		if err := os.WriteFile(filepath.Join(repo, b+".txt"), []byte(b), 0o644); err != nil {
			t.Fatal(err)
		}
		gitT(t, repo, "add", ".")
		gitT(t, repo, "commit", "-q", "-m", b)
		out, err := exec.Command("git", "-C", repo, "rev-parse", b).Output()
		if err != nil {
			t.Fatal(err)
		}
		want[b] = strings.TrimSpace(string(out))
	}
	gitT(t, repo, "checkout", "-q", "main")

	work := filepath.Join(t.TempDir(), "integrator")
	gitT(t, repo, "worktree", "add", "-B", "agent/integration", work)

	merged, detail, err := mergeAll(ctx, work, []string{"a", "b"})
	if err != nil {
		t.Fatalf("mergeAll: %v (%s)", err, detail)
	}
	if len(merged) != 2 {
		t.Fatalf("merged = %v, want an entry per branch", merged)
	}
	for br, sha := range want {
		if merged[br] != sha {
			t.Errorf("merged[%q] = %q, want the branch tip %q", br, merged[br], sha)
		}
	}
}

// A failed merge is aborted, so nothing coherent was tested and there is no
// honest SHA to name. Reporting a partial set would let a verdict claim it
// tested branches whose merge was rolled back.
func TestMergeAllReportsNothingWhenAMergeFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	repo := t.TempDir()
	gitT(t, repo, "init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(repo, "f.txt"), []byte("base\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitT(t, repo, "add", ".")
	gitT(t, repo, "commit", "-q", "-m", "init")

	// Two branches that edit the same line: the second merge conflicts.
	for _, b := range []string{"x", "y"} {
		gitT(t, repo, "checkout", "-q", "-b", b, "main")
		if err := os.WriteFile(filepath.Join(repo, "f.txt"), []byte(b+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		gitT(t, repo, "add", ".")
		gitT(t, repo, "commit", "-q", "-m", b)
	}
	gitT(t, repo, "checkout", "-q", "main")

	work := filepath.Join(t.TempDir(), "integrator")
	gitT(t, repo, "worktree", "add", "-B", "agent/integration", work)

	merged, detail, err := mergeAll(ctx, work, []string{"x", "y"})
	if err == nil {
		t.Fatalf("mergeAll succeeded on conflicting branches; detail=%q merged=%v", detail, merged)
	}
	if merged != nil {
		t.Errorf("merged = %v, want nil: the merge was aborted, so nothing was tested", merged)
	}
	if !strings.Contains(detail, "conflict") {
		t.Errorf("detail = %q, want it to name the conflict", detail)
	}
}
