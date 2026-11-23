// Command release holds the release mechanics make and go cannot do (RH,
// 4 Nov: "just a makefile and just go test and release mechanisms"). The
// checks themselves are Makefile targets; the gates run them at once
// (`make gate`, a major `make gate-major`).
//
//	go run ./scripts/release <patch|major> [-dry-run]
//	go run ./scripts/release kind
//
// A release: preconditions (on dev, clean, the remote's main an ancestor of
// HEAD, a push dry-run passes, the next tag absent), the diff's kind is at
// most the requested one (kind.go), docs/CHANGELOG.md's newest section is the
// next version and [Unreleased] is empty, `make gate` (a major: make gate-major), git tag -a
// (message: that section), git push --atomic of dev as main and dev, and the
// tag; local main then moves to the tag (dev is merged into main). The tag is
// the version: it edits no file. -dry-run: everything but the tag and the
// push. After a patch push it starts `make race` detached (log .git/race.out
// of the repository, also from a linked worktree): -race runs after the tag,
// nobody waits on it, and a red race run is fixed in the next patch.
//
// kind prints the diff's kind since the last tag and why.
//
// Race is make race. Kind and tag push live here.
package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		usage()
	}
	if top, err := exec.Command("git", "rev-parse", "--show-toplevel").Output(); err == nil {
		_ = os.Chdir(strings.TrimSpace(string(top)))
	}
	switch args[0] {
	case "kind":
		k, why, err := diffKind()
		if err != nil {
			fail(err)
		}
		fmt.Println(kindNames[k])
		for _, w := range why {
			fmt.Println("  " + w)
		}
	default:
		k := parseKind(args[0])
		dry := len(args) == 2 && args[1] == "-dry-run"
		if len(args) > 2 || (len(args) == 2 && !dry) {
			usage()
		}
		release(k, dry)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: release <patch|major> [-dry-run] | release kind")
	os.Exit(2)
}

func parseKind(s string) int {
	for k, n := range kindNames {
		if n == s {
			return k
		}
	}
	usage()
	return 0
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "release:", err)
	os.Exit(1)
}

func since(t time.Time) string { return fmt.Sprintf("%d s", int(time.Since(t).Seconds())) }

// raceLog is the detached race run's log: race.out in the repository's .git
// (the common git dir, also from a linked worktree), as an absolute path.
func raceLog() string {
	b, err := exec.Command("git", "rev-parse", "--path-format=absolute", "--git-common-dir").Output()
	if err != nil {
		fail(err)
	}
	return strings.TrimSpace(string(b)) + "/race.out"
}

// remote is where a release fetches from and pushes to: origin (GitHub), or
// RELEASE_REMOTE when set.
func remote() string {
	if r := os.Getenv("RELEASE_REMOTE"); r != "" {
		return r
	}
	return "origin"
}

// gitNet runs a git command that talks to the remote, up to 5 times with a
// growing pause: GitHub refuses this host's SSH logins now and then (4 Nov:
// one in three, the same key), and a release must not die on one refusal.
func gitNet(args ...string) (string, error) {
	var out string
	var err error
	for try := 1; try <= 5; try++ {
		if out, err = git(args...); err == nil {
			return out, nil
		}
		time.Sleep(time.Duration(try) * 2 * time.Second)
	}
	return out, err
}

func git(args ...string) (string, error) {
	out, err := exec.Command("git", args...).CombinedOutput() // #nosec G204 -- fixed git subcommands
	return strings.TrimSpace(string(out)), err
}

// lastTag is the newest release tag reachable from HEAD.
func lastTag() string {
	t, err := git("describe", "--tags", "--abbrev=0", "--match", "v[0-9]*.[0-9]*.[0-9]*")
	if err != nil {
		fail(fmt.Errorf("git describe: %s", t))
	}
	return t
}

func diffKind() (int, []string, error) {
	base, err := treeAt(lastTag())
	if err != nil {
		return 0, nil, err
	}
	head, err := workTree()
	if err != nil {
		return 0, nil, err
	}
	k, why := kindOf(base, head)
	return k, why, nil
}

func nextVersion(tag string, k int) string {
	var v [3]int
	for i, p := range strings.SplitN(strings.TrimPrefix(tag, "v"), ".", 3) {
		v[i], _ = strconv.Atoi(p)
	}
	switch k {
	case major:
		return fmt.Sprintf("v%d.0.0", v[0]+1)
	}
	return fmt.Sprintf("v%d.%d.%d", v[0], v[1], v[2]+1)
}

var headingRE = regexp.MustCompile(`(?m)^## \[(Unreleased|[0-9]+\.[0-9]+\.[0-9]+)\][^\n]*\n`)

// changelog returns docs/CHANGELOG.md's [Unreleased] body and the newest
// version's heading and body.
func changelog() (unreleased, version, body string, err error) {
	b, err := os.ReadFile("docs/CHANGELOG.md")
	if err != nil {
		return "", "", "", err
	}
	s := string(b)
	idx := headingRE.FindAllStringSubmatchIndex(s, -1)
	for i, m := range idx {
		end := len(s)
		if i+1 < len(idx) {
			end = idx[i+1][0]
		}
		name, text := s[m[2]:m[3]], strings.TrimSpace(s[m[1]:end])
		if name == "Unreleased" {
			unreleased = text
			continue
		}
		return unreleased, name, text, nil
	}
	return unreleased, "", "", fmt.Errorf("docs/CHANGELOG.md has no version section")
}

func release(k int, dry bool) {
	start := time.Now()
	check := func(name string, f func() error) {
		t := time.Now()
		if err := f(); err != nil {
			fmt.Printf("FAIL %-13s %s\n%v\nrelease: red at %s, %s\n", name, since(t), err, name, since(start))
			if !dry || name != "preconditions" {
				os.Exit(1)
			}
			return
		}
		fmt.Printf("PASS %-13s %s\n", name, since(t))
	}
	tag := lastTag()
	next := nextVersion(tag, k)
	fmt.Printf("release: %s -> %s (%s)\n", tag, next, kindNames[k])
	check("preconditions", func() error {
		var errs []string
		if b, _ := git("branch", "--show-current"); b != "dev" {
			errs = append(errs, "not on dev (the working branch; main is the released state)")
		}
		if s, _ := git("status", "--porcelain"); s != "" {
			errs = append(errs, "tree not clean")
		}
		if out, err := gitNet("fetch", "-q", remote(), "main", "--tags"); err != nil {
			errs = append(errs, "git fetch: "+out)
		} else if _, err := git("merge-base", "--is-ancestor", remote()+"/main", "HEAD"); err != nil {
			errs = append(errs, remote()+"/main is not an ancestor of HEAD")
		} else if out, err := gitNet("push", "--dry-run", "-q", remote(), "HEAD:main"); err != nil {
			errs = append(errs, "cannot push to "+remote()+" (checked before the gate): "+out)
		}
		if _, err := git("rev-parse", "-q", "--verify", "refs/tags/"+next); err == nil {
			errs = append(errs, next+" exists (a failed push? `git tag -d "+next+"`)")
		}
		if len(errs) > 0 {
			return fmt.Errorf("%s", strings.Join(errs, "; "))
		}
		return nil
	})
	check("kind", func() error {
		d, why, err := diffKind()
		if err != nil {
			return err
		}
		if d > k {
			return fmt.Errorf("the diff is a %s, not a %s:\n  %s", kindNames[d], kindNames[k], strings.Join(why, "\n  "))
		}
		if k == major && !strings.HasSuffix(modulePath(mustRead("go.mod")), "/v"+strings.Split(next[1:], ".")[0]) {
			return fmt.Errorf("a major needs go.mod's module path to end in /v%s", strings.Split(next[1:], ".")[0])
		}
		return nil
	})
	var section string
	check("changelog", func() error {
		unreleased, version, body, err := changelog()
		if err != nil {
			return err
		}
		if "v"+version != next {
			return fmt.Errorf("docs/CHANGELOG.md's newest section is [%s]; write [%s] (with README) first", version, next[1:])
		}
		if body == "" {
			return fmt.Errorf("docs/CHANGELOG.md [%s] is empty", version)
		}
		if unreleased != "" {
			return fmt.Errorf("docs/CHANGELOG.md [Unreleased] is not empty: it would ship unlisted")
		}
		section = body
		return nil
	})
	gateStart := time.Now()
	gate := exec.Command("make", "gate") // #nosec G204 -- a fixed target
	if k == major {
		gate = exec.Command("make", "gate-major") // #nosec G204 -- make gate-major
	}
	gate.Stdout, gate.Stderr = os.Stdout, os.Stderr
	if err := gate.Run(); err != nil {
		fmt.Printf("release: red at the gate, %s\n", since(start))
		os.Exit(1)
	}
	gateTime := since(gateStart)
	if dry {
		fmt.Printf("release: dry-run, %s not tagged, gate %s, %s\n", next, gateTime, since(start))
		return
	}
	check("tag", func() error {
		// verbatim: the CHANGELOG section keeps its lines as written (no # stripped).
		if out, err := git("tag", "-a", "--cleanup=verbatim", next, "-m", next+"\n\n"+section); err != nil {
			return fmt.Errorf("%s", out)
		}
		return nil
	})
	check("push", func() error {
		if out, err := gitNet("push", "--atomic", remote(), "HEAD:main", "HEAD:dev", "refs/tags/"+next); err != nil {
			if _, derr := git("tag", "-d", next); derr == nil {
				return fmt.Errorf("%s\nthe local tag %s is deleted again: fix, run again", out, next)
			}
			return fmt.Errorf("%s\nrecover: git tag -d %s, fix, run again", out, next)
		}
		if out, err := git("branch", "-f", "main", "HEAD"); err != nil {
			return fmt.Errorf("pushed, but local main did not move to %s: %s", next, out)
		}
		return nil
	})
	fmt.Printf("%s, %s gate %s, release %s\n", next, kindNames[k], gateTime, since(start))
	if k != major {
		startRace(next)
	}
}

// startRace runs release race detached, in its own session so it outlives the
// release and the terminal, its output in .git/race.out after a header naming
// the tag.
func startRace(tag string) {
	log := raceLog()
	f, err := os.Create(log) // #nosec G304 -- a path under .git
	if err != nil {
		fmt.Printf("race: not started: %v (run make race)\n", err)
		return
	}
	defer func() { _ = f.Close() }() // the child holds its own descriptor
	_, _ = fmt.Fprintf(f, "race at %s, started %s\n", tag, time.Now().Format(time.RFC3339))
	c := exec.Command("make", "race") // #nosec G204 -- make race under RACE_LOCK
	c.Env = append(os.Environ(), "GOFLAGS=-trimpath")
	c.Stdout, c.Stderr = f, f
	c.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := c.Start(); err != nil {
		fmt.Printf("race: not started: %v (run make race)\n", err)
		return
	}
	fmt.Printf("race: started in the background (pid %d), log %s\n", c.Process.Pid, log)
}

func mustRead(p string) []byte {
	b, err := os.ReadFile(p) // #nosec G304 -- a fixed repository path
	if err != nil {
		fail(err)
	}
	return bytes.TrimSpace(b)
}
