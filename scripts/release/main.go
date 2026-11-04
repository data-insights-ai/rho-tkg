// Command release is rho's release cycle (René, 4 Nov: "a professional release
// cycle: very fast patch, fast minor, comprehensive major"; "use all of go
// facilities"). Go's build and test caches make every gate incremental: a
// package reruns only when its sources, dependencies, flags, or the files and
// env its tests read change; everything else answers "(cached)".
//
//	go run ./scripts/release gate <patch|minor|major>
//	go run ./scripts/release <patch|minor|major> [-dry-run]
//	go run ./scripts/release kind
//
// Gates (steps at once, one PASS/FAIL line each with its seconds):
//   - patch: gofmt, build (+GOARCH=386), vet, lint, go mod tidy -diff and
//     verify, the docs tests, go test -race -short ./... (cached);
//   - minor: the patch gate with go test -race ./... (no -short: the long
//     tier too, cached), and gosec;
//   - major: the minor gate cold (-count=1 -shuffle=on), coverage, fuzz-smoke,
//     govulncheck.
//
// A release: preconditions (on main, clean, origin/main an ancestor of HEAD,
// the next tag absent here and on origin), the diff's kind is at most the
// requested one (kind.go), docs/CHANGELOG.md's newest section is the next
// version and [Unreleased] is empty, the gate, git tag -a (message: that
// section), git push --atomic of main and the tag. The tag is the version: it
// edits no file. -dry-run: everything but the tag and the push.
package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
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
	case "gate":
		if len(args) != 2 {
			usage()
		}
		k := parseKind(args[1])
		start := time.Now()
		if !gate(k) {
			fmt.Printf("gate %s: red, %s\n", kindNames[k], since(start))
			os.Exit(1)
		}
		fmt.Printf("gate %s: ok, %s (load %s)\n", kindNames[k], since(start), load())
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
	fmt.Fprintln(os.Stderr, "usage: release gate <patch|minor|major> | release <patch|minor|major> [-dry-run] | release kind")
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

func load() string {
	b, _ := os.ReadFile("/proc/loadavg")
	return strings.Fields(string(b) + " ?")[0]
}

// step is one gate command.
type step struct {
	name string
	cmd  []string
	env  []string
}

func gateSteps(k int) []step {
	race := []string{"go", "test", "-race", "-short", "-timeout", "20m", "./..."}
	steps := []step{
		{name: "gofmt", cmd: []string{"sh", "-c", `out=$(gofmt -l $(git ls-files '*.go')) && [ -z "$out" ] || { echo "$out"; exit 1; }`}},
		{name: "build", cmd: []string{"go", "build", "./..."}},
		{name: "build-386", cmd: []string{"go", "build", "./..."}, env: []string{"GOOS=linux", "GOARCH=386", "CGO_ENABLED=0"}},
		{name: "vet", cmd: []string{"go", "vet", "./..."}},
		{name: "lint", cmd: []string{"make", "--no-print-directory", "lint-docker"}},
		{name: "mod", cmd: []string{"sh", "-c", "go mod tidy -diff && go mod verify"}},
		// The docs layout test lists files with git, which the test cache cannot see.
		{name: "docs", cmd: []string{"go", "test", "-count=1", "-run", "Docs|Consistency|MD|Errors", "./..."}},
		{name: "ledger", cmd: []string{"scripts/check-ledger.sh"}},
	}
	switch k {
	case patch:
		steps = append(steps, step{name: "test", cmd: race})
	case minor:
		steps = append(steps,
			step{name: "test", cmd: []string{"go", "test", "-race", "-timeout", "60m", "./..."}},
			step{name: "gosec", cmd: []string{"make", "--no-print-directory", "security-docker"}})
	case major:
		steps = append(steps,
			step{name: "test", cmd: []string{"go", "test", "-count=1", "-shuffle=on", "-race", "-timeout", "90m", "./..."}},
			step{name: "gosec", cmd: []string{"make", "--no-print-directory", "security-docker"}},
			step{name: "cover", cmd: []string{"make", "--no-print-directory", "cover-gate"}},
			step{name: "fuzz", cmd: []string{"make", "--no-print-directory", "fuzz-smoke"}},
			step{name: "vuln", cmd: []string{"make", "--no-print-directory", "vulncheck-docker"}})
	}
	return steps
}

// gate runs the steps of kind k at once and reports whether all passed.
func gate(k int) bool {
	logDir := gitDir() + "/gate" // the last run's step logs, overwritten
	_ = os.MkdirAll(logDir, 0o750)
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := true
	for _, s := range gateSteps(k) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			c := exec.Command(s.cmd[0], s.cmd[1:]...) // #nosec G204 -- fixed commands
			// A session's GOFLAGS (-count=1, -tags) would change results and cache keys.
			c.Env = append(append(os.Environ(), "GOFLAGS=", "CGO_ENABLED=1"), s.env...)
			out, err := c.CombinedOutput()
			_ = os.WriteFile(logDir+"/"+s.name+".out", out, 0o600)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				fmt.Printf("PASS %-9s %s\n", s.name, since(start))
				return
			}
			ok = false
			fmt.Printf("FAIL %-9s %s\n%s", s.name, since(start), tail(out, 40))
		}()
	}
	wg.Wait()
	return ok
}

// tail returns the last n lines of out that are not a package's "ok" line.
func tail(out []byte, n int) string {
	var keep []string
	for _, l := range strings.Split(string(out), "\n") {
		if !strings.HasPrefix(l, "ok ") && !strings.HasPrefix(l, "?") && l != "" {
			keep = append(keep, l)
		}
	}
	if len(keep) > n {
		keep = keep[len(keep)-n:]
	}
	return strings.Join(keep, "\n") + "\n"
}

func gitDir() string {
	b, err := exec.Command("git", "rev-parse", "--git-dir").Output()
	if err != nil {
		fail(err)
	}
	return strings.TrimSpace(string(b))
}

// gitNet runs a git command that talks to origin, up to 5 times with a growing
// pause: GitHub refuses this host's SSH logins now and then (4 Nov: one in
// three, the same key), and a release must not die on one refusal.
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
	case minor:
		return fmt.Sprintf("v%d.%d.0", v[0], v[1]+1)
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
		if b, _ := git("branch", "--show-current"); b != "main" {
			errs = append(errs, "not on main")
		}
		if s, _ := git("status", "--porcelain"); s != "" {
			errs = append(errs, "tree not clean")
		}
		if out, err := gitNet("fetch", "-q", "origin", "main", "--tags"); err != nil {
			errs = append(errs, "git fetch: "+out)
		} else if _, err := git("merge-base", "--is-ancestor", "origin/main", "HEAD"); err != nil {
			errs = append(errs, "origin/main is not an ancestor of HEAD")
		} else if out, err := gitNet("push", "--dry-run", "-q", "origin", "main"); err != nil {
			errs = append(errs, "cannot push to origin (checked before the gate): "+out)
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
	if !gate(k) {
		fmt.Printf("release: red at the gate, %s\n", since(start))
		os.Exit(1)
	}
	gateTime := since(gateStart)
	if dry {
		fmt.Printf("release: dry-run, %s not tagged, gate %s, %s\n", next, gateTime, since(start))
		return
	}
	check("tag", func() error {
		if out, err := git("tag", "-a", next, "-m", next+"\n\n"+section); err != nil {
			return fmt.Errorf("%s", out)
		}
		return nil
	})
	check("push", func() error {
		if out, err := gitNet("push", "--atomic", "origin", "main", "refs/tags/"+next); err != nil {
			if _, derr := git("tag", "-d", next); derr == nil {
				return fmt.Errorf("%s\nthe local tag %s is deleted again: fix, run again", out, next)
			}
			return fmt.Errorf("%s\nrecover: git tag -d %s, fix, run again", out, next)
		}
		return nil
	})
	fmt.Printf("%s, %s gate %s, release %s\n", next, kindNames[k], gateTime, since(start))
}

func mustRead(p string) []byte {
	b, err := os.ReadFile(p) // #nosec G304 -- a fixed repository path
	if err != nil {
		fail(err)
	}
	return bytes.TrimSpace(b)
}
