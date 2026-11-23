package main

import (
	"os/exec"
	"path/filepath"
	"testing"
)

// TestRaceLogInCommonGitDir: the detached race run's log is .git/race.out of
// the repository (the package comment), also when the release runs in a
// linked worktree, whose own git dir is .git/worktrees/<name>. Mutation it
// catches: the per-worktree git dir (git rev-parse --git-dir).
func TestRaceLogInCommonGitDir(t *testing.T) {
	repo := t.TempDir()
	run := func(dir string, args ...string) {
		t.Helper()
		c := exec.Command("git", args...)
		c.Dir = dir
		if out, err := c.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	run(repo, "init", "-q")
	run(repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "x")
	wt := filepath.Join(repo, ".worktrees", "w")
	run(repo, "worktree", "add", "-q", "-b", "w", wt)
	for _, dir := range []string{repo, wt} {
		t.Chdir(dir)
		abs, err := filepath.Abs(raceLog())
		if err != nil {
			t.Fatal(err)
		}
		got, err := filepath.EvalSymlinks(filepath.Dir(abs))
		if err != nil {
			t.Fatal(err)
		}
		want, err := filepath.EvalSymlinks(filepath.Join(repo, ".git"))
		if err != nil {
			t.Fatal(err)
		}
		if got != want || filepath.Base(raceLog()) != "race.out" {
			t.Fatalf("in %s the race log is %s; want %s/race.out", dir, raceLog(), want)
		}
	}
}
