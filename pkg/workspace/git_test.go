package workspace

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// newRepo creates a temp git repo with one commit on branch main.
func newRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		c := exec.Command("git", append([]string{"-C", dir}, args...)...)
		c.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com",
			"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com")
		if out, err := c.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	run("init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("seed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	run("add", ".")
	run("commit", "-q", "-m", "init")
	return dir
}

func TestWorktreeAddCreatesIsolatedTree(t *testing.T) {
	repo := newRepo(t)
	wt := filepath.Join(t.TempDir(), "billing")

	if err := worktreeAdd(context.Background(), repo, wt, "agent/billing"); err != nil {
		t.Fatalf("worktreeAdd: %v", err)
	}
	if _, err := os.Stat(filepath.Join(wt, "README.md")); err != nil {
		t.Fatalf("worktree missing seed file: %v", err)
	}

	// Worktree is registered with git: verify it appears in worktree list.
	out, err := exec.Command("git", "-C", repo, "worktree", "list").CombinedOutput()
	if err != nil {
		t.Fatalf("git worktree list: %v: %s", err, out)
	}
	if !contains(string(out), wt) {
		t.Fatalf("worktree path not found in git worktree list:\n%s", out)
	}

	// Independent working trees: modify tracked file in the worktree.
	// The worktree's index and working tree are independent from the origin repo.
	wtContent := "modified in worktree\n"
	if err := os.WriteFile(filepath.Join(wt, "README.md"), []byte(wtContent), 0o644); err != nil {
		t.Fatal(err)
	}

	// Verify origin repo's README.md still has original content.
	repoContent, err := os.ReadFile(filepath.Join(repo, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if string(repoContent) != "seed\n" {
		t.Fatalf("origin repo README.md was modified: got %q", string(repoContent))
	}

	// Verify git status differs: worktree shows README.md modified, origin is clean.
	wtStatus, err := exec.Command("git", "-C", wt, "status", "--porcelain").CombinedOutput()
	if err != nil {
		t.Fatalf("git -C worktree status: %v: %s", err, wtStatus)
	}
	if !contains(string(wtStatus), "README.md") {
		t.Fatalf("worktree git status should show README.md modified, got:\n%s", wtStatus)
	}

	repoStatus, err := exec.Command("git", "-C", repo, "status", "--porcelain").CombinedOutput()
	if err != nil {
		t.Fatalf("git -C repo status: %v: %s", err, repoStatus)
	}
	if string(repoStatus) != "" {
		t.Fatalf("origin repo should be clean, got status:\n%s", repoStatus)
	}

	if err := worktreeRemove(context.Background(), repo, wt); err != nil {
		t.Fatalf("worktreeRemove: %v", err)
	}
	if _, err := os.Stat(wt); !os.IsNotExist(err) {
		t.Fatal("worktree directory still present after remove")
	}
}

// contains is a helper to check if a substring is present in a string.
func contains(haystack, needle string) bool {
	return len(haystack) > 0 && len(needle) > 0 && (haystack == needle || bytes.Contains([]byte(haystack), []byte(needle)))
}
