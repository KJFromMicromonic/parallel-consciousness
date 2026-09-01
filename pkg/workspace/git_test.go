package workspace

import (
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

	if err := worktreeAdd(repo, wt, "agent/billing"); err != nil {
		t.Fatalf("worktreeAdd: %v", err)
	}
	if _, err := os.Stat(filepath.Join(wt, "README.md")); err != nil {
		t.Fatalf("worktree missing seed file: %v", err)
	}

	// A write in the worktree must not appear in the origin repo.
	if err := os.WriteFile(filepath.Join(wt, "only-here.txt"), []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(repo, "only-here.txt")); !os.IsNotExist(err) {
		t.Fatal("worktree write leaked into the origin repo")
	}

	if err := worktreeRemove(repo, wt); err != nil {
		t.Fatalf("worktreeRemove: %v", err)
	}
	if _, err := os.Stat(wt); !os.IsNotExist(err) {
		t.Fatal("worktree directory still present after remove")
	}
}
