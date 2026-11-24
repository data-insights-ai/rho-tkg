package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// afterPushRepo is a repository whose main checkout holds main (unless
// mainOut) and a linked worktree .worktrees/dev on dev, one commit ahead of
// main and tagged v1.0.1, as the release leaves it just after its push. The
// test runs in the dev worktree.
func afterPushRepo(t *testing.T, mainOut bool) (repo, tagCommit string) {
	t.Helper()
	repo = t.TempDir()
	run := func(dir string, args ...string) string {
		t.Helper()
		c := exec.Command("git", append([]string{"-c", "user.name=t", "-c", "user.email=t@t"}, args...)...)
		c.Dir = dir
		out, err := c.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	run(repo, "init", "-q", "-b", "main")
	run(repo, "commit", "-q", "--allow-empty", "-m", "base")
	dev := filepath.Join(repo, ".worktrees", "dev")
	run(repo, "worktree", "add", "-q", "-b", "dev", dev)
	run(dev, "commit", "-q", "--allow-empty", "-m", "next")
	run(dev, "tag", "-a", "v1.0.1", "-m", "v1.0.1")
	if mainOut {
		run(repo, "checkout", "-q", "--detach")
	}
	t.Chdir(dev)
	return repo, run(dev, "rev-parse", "HEAD")
}

// TestMoveMainWhereverMainIsCheckedOut: after the push local main reaches the
// tag whether the main checkout holds main (git branch -f refuses that branch;
// v4.107.15 and .16 ended red there) or no worktree does.
func TestMoveMainWhereverMainIsCheckedOut(t *testing.T) {
	for _, mainOut := range []bool{false, true} {
		repo, want := afterPushRepo(t, mainOut)
		if err := moveMain("v1.0.1"); err != nil {
			t.Fatalf("main checked out elsewhere %v: %v", !mainOut, err)
		}
		if got, _ := git("rev-parse", "main"); got != want {
			t.Fatalf("main checked out elsewhere %v: main at %s, want %s", !mainOut, got, want)
		}
		if !mainOut {
			if s, _ := git("-C", repo, "status", "--porcelain", "--untracked-files=no"); s != "" {
				t.Fatalf("the main checkout is not at main's tree after the move:\n%s", s)
			}
		}
	}
}

// TestRaceRunsAtTagAndRemovesItsWorktree: the detached race run executes in a
// worktree at the tag, which is gone after the run, green or red; the run's
// exit status is kept.
func TestRaceRunsAtTagAndRemovesItsWorktree(t *testing.T) {
	for _, rc := range []int{0, 3} {
		repo, want := afterPushRepo(t, false)
		raceCmd = "git rev-parse HEAD; exit " + strconv.Itoa(rc)
		t.Cleanup(func() { raceCmd = "make race" })
		c, err := startRace("v1.0.1")
		if err != nil {
			t.Fatal(err)
		}
		err = c.Wait()
		var ee *exec.ExitError
		if (rc == 0 && err != nil) || (rc != 0 && (!errors.As(err, &ee) || ee.ExitCode() != rc)) {
			t.Fatalf("rc %d: wait: %v", rc, err)
		}
		log, err := os.ReadFile(filepath.Join(repo, ".git", "race.out"))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(log), "\n"+want+"\n") {
			t.Fatalf("rc %d: the run did not execute at the tag's commit %s:\n%s", rc, want, log)
		}
		wt := filepath.Join(repo, ".worktrees", "race-v1.0.1")
		if _, err := os.Stat(wt); !os.IsNotExist(err) {
			t.Fatalf("rc %d: race worktree %s left behind (%v)", rc, wt, err)
		}
		if l, _ := git("worktree", "list"); strings.Contains(l, "race-v1.0.1") {
			t.Fatalf("rc %d: race worktree still registered:\n%s", rc, l)
		}
	}
}
