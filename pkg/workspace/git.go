// Package workspace hands each agent an exclusive git worktree. Isolation is
// structural rather than advisory: an agent cannot see another agent's edits,
// so "one active owner" is enforced by the filesystem instead of by protocol
// etiquette.
package workspace

import (
	"fmt"
	"os/exec"
)

// worktreeAdd creates (or resets) branch at the repo's HEAD and checks it out
// into its own worktree at path. -B resets the branch pointer to the repo's
// HEAD so a re-created worktree starts from a known state. Reattaching to an
// already-existing worktree directory is handled by Acquire in a later task.
func worktreeAdd(repo, path, branch string) error {
	out, err := exec.Command("git", "-C", repo, "worktree", "add", "-B", branch, path).CombinedOutput()
	if err != nil {
		return fmt.Errorf("git worktree add %s: %w: %s", path, err, out)
	}
	return nil
}

// worktreeRemove detaches the worktree and deletes its directory. --force is
// required because an agent almost always leaves uncommitted work behind.
func worktreeRemove(repo, path string) error {
	out, err := exec.Command("git", "-C", repo, "worktree", "remove", "--force", path).CombinedOutput()
	if err != nil {
		return fmt.Errorf("git worktree remove %s: %w: %s", path, err, out)
	}
	return nil
}
