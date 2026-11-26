package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestMain drops every inherited GIT_* variable, so the tests' git commands
// act on their temp repositories only, also inside a git hook.
func TestMain(m *testing.M) {
	for _, kv := range os.Environ() {
		if name, _, _ := strings.Cut(kv, "="); strings.HasPrefix(name, "GIT_") {
			_ = os.Unsetenv(name)
		}
	}
	os.Exit(m.Run())
}

// TestGitEnvironmentLeavesTheCallersRepoAlone: run with GIT_DIR naming another
// repository, as a hook sets it, a test that builds a repository and a
// worktree leaves that repository's config, branches and commits untouched.
func TestGitEnvironmentLeavesTheCallersRepoAlone(t *testing.T) {
	other := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		c := exec.Command("git", append([]string{"-c", "user.name=t", "-c", "user.email=t@t", "-C", other}, args...)...)
		out, err := c.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return string(out)
	}
	git("init", "-q", "-b", "main")
	git("commit", "-q", "--allow-empty", "-m", "base")
	state := func() string {
		return git("config", "--local", "--list") + git("for-each-ref") + git("rev-list", "--all")
	}
	before := state()
	c := exec.Command(os.Args[0], "-test.run=^TestRaceLogInCommonGitDir$", "-test.count=1")
	c.Env = append(os.Environ(), "GIT_DIR="+other+"/.git", "GIT_WORK_TREE="+other)
	if out, err := c.CombinedOutput(); err != nil {
		t.Fatalf("the test under GIT_DIR: %v\n%s", err, out)
	}
	if after := state(); after != before {
		t.Errorf("the caller's repository changed:\nbefore %s\nafter %s", before, after)
	}
}
