// Command release is rho's release cycle (René, 4 Nov: "a professional release
// cycle: very fast patch, fast minor, comprehensive major"; "use all of go
// facilities"). Go's build and test caches make every gate incremental: a
// package reruns only when its sources, dependencies, flags, or the files and
// env its tests read change; everything else answers "(cached)".
//
//	go run ./scripts/release gate <patch|minor|major>
//	go run ./scripts/release <patch|minor|major> [-dry-run]
//	go run ./scripts/release kind
//	go run ./scripts/release race
//	go run ./scripts/release test
//
// Gates (steps at once, one PASS/FAIL line each with its seconds):
//   - patch: gofmt, build (+GOARCH=386), vet, lint, go mod tidy -diff and
//     verify, the docs tests, go test -short ./... (cached, no -race, no
//     -failfast: a red run lists every failing test);
//   - minor: the patch gate and gosec;
//   - major: the minor gate cold with -race (-count=1 -shuffle=on), coverage,
//     fuzz-smoke, govulncheck.
//
// test (a builder's presubmit, as sigma's): go vet and go test -short -p 4 of
// the packages a change reaches: the packages of the files changed against
// origin/main (committed, staged, unstaged or untracked; a testdata file
// counts for its package, go.mod or go.sum means all) and every package whose
// tests depend on one of them.
//
// -race (René, 4 Nov, as sigma's): not in the patch and minor gates. A patch or
// minor release starts `release race` in the background after its push, and
// `go run ./scripts/release race` runs it by hand or from cron: one -race
// suite on the host at a time (the race lock, shared with sigma), nobody
// waiting on it; its log is .git/gate/race.out. The long tier (make test-long)
// runs by hand, never before a tag.
//
// A release: preconditions (on main, clean, the remote's main an ancestor of
// HEAD, a push dry-run passes, the next tag absent), the diff's kind is at most the
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
	case "race":
		os.Exit(runRace())
	case "test":
		os.Exit(runAffected())
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
	fmt.Fprintln(os.Stderr, "usage: release gate <patch|minor|major> | release <patch|minor|major> [-dry-run] | release kind | release race | release test")
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
	// The race run takes the host's race lock, shared with sigma's (4 Nov):
	// two -race suites at once starve each other and flake timing tests.
	short := []string{"go", "test", "-short", "-timeout", "20m", "./..."}
	steps := []step{
		{name: "gofmt", cmd: []string{"sh", "-c", `out=$(gofmt -l $(git ls-files '*.go')) && [ -z "$out" ] || { echo "$out"; exit 1; }`}},
		{name: "build", cmd: []string{"go", "build", "./..."}},
		{name: "build-386", cmd: []string{"go", "build", "./..."}, env: []string{"GOOS=linux", "GOARCH=386", "CGO_ENABLED=0"}},
		{name: "vet", cmd: []string{"go", "vet", "./..."}},
		{name: "lint", cmd: []string{"make", "--no-print-directory", "lint-docker"}},
		{name: "mod", cmd: []string{"sh", "-c", "go mod tidy -diff && go mod verify"}},
		// Cached: the docs tests read the files they check, so an edit reruns them.
		{name: "docs", cmd: []string{"go", "test", "./internal/docscheck/"}},
		{name: "ledger", cmd: []string{"scripts/check-ledger.sh"}},
	}
	switch k {
	case patch:
		steps = append(steps, step{name: "test", cmd: short})
	case minor:
		steps = append(steps,
			step{name: "test", cmd: short},
			step{name: "gosec", cmd: []string{"make", "--no-print-directory", "security-docker"}})
	case major:
		steps = append(steps,
			step{name: "test", cmd: []string{"flock", raceLock(), "go", "test", "-count=1", "-shuffle=on", "-race", "-timeout", "90m", "./..."}},
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
			// A session's GOFLAGS (-count=1, -tags) would change results and cache
			// keys; -trimpath keeps the checkout's path out of them (Makefile too).
			c.Env = append(append(os.Environ(), "GOFLAGS=-trimpath", "CGO_ENABLED=1"), s.env...)
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

// remote is where a release fetches from and pushes to: origin (GitHub), or
// RELEASE_REMOTE when set.
func remote() string {
	if r := os.Getenv("RELEASE_REMOTE"); r != "" {
		return r
	}
	return "origin"
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
		if out, err := gitNet("fetch", "-q", remote(), "main", "--tags"); err != nil {
			errs = append(errs, "git fetch: "+out)
		} else if _, err := git("merge-base", "--is-ancestor", remote()+"/main", "HEAD"); err != nil {
			errs = append(errs, remote()+"/main is not an ancestor of HEAD")
		} else if out, err := gitNet("push", "--dry-run", "-q", remote(), "main"); err != nil {
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
		// verbatim: the CHANGELOG section keeps its lines as written (no # stripped).
		if out, err := git("tag", "-a", "--cleanup=verbatim", next, "-m", next+"\n\n"+section); err != nil {
			return fmt.Errorf("%s", out)
		}
		return nil
	})
	check("push", func() error {
		if out, err := gitNet("push", "--atomic", remote(), "main", "refs/tags/"+next); err != nil {
			if _, derr := git("tag", "-d", next); derr == nil {
				return fmt.Errorf("%s\nthe local tag %s is deleted again: fix, run again", out, next)
			}
			return fmt.Errorf("%s\nrecover: git tag -d %s, fix, run again", out, next)
		}
		return nil
	})
	fmt.Printf("%s, %s gate %s, release %s\n", next, kindNames[k], gateTime, since(start))
	if k != major {
		// -race after the tag, in the background (René, 4 Nov).
		c := exec.Command("go", "run", "./scripts/release", "race") // #nosec G204 -- fixed command
		c.SysProcAttr = detached()
		if err := c.Start(); err != nil {
			fmt.Printf("race: not started: %v (run `go run ./scripts/release race`)\n", err)
			return
		}
		fmt.Printf("race: started in the background (pid %d), log %s/gate/race.out\n", c.Process.Pid, gitDir())
	}
}

// raceCmd is the -race suite under the host's race lock.
func raceCmd() []string {
	return []string{"flock", raceLock(), "go", "test", "-race", "-short", "-timeout", "30m", "./..."}
}

// runRace runs raceCmd, writes its output to .git/gate/race.out (with the
// commit and the result) and returns its exit code.
func runRace() int {
	_ = os.MkdirAll(gitDir()+"/gate", 0o750)
	head, _ := git("rev-parse", "--short", "HEAD")
	start := time.Now()
	cmd := raceCmd()
	c := exec.Command(cmd[0], cmd[1:]...) // #nosec G204 -- fixed command
	c.Env = append(os.Environ(), "GOFLAGS=-trimpath", "CGO_ENABLED=1")
	out, err := c.CombinedOutput()
	code, result := 0, "ok"
	if err != nil {
		code, result = 1, "RED"
	}
	summary := fmt.Sprintf("race %s at %s: %s, %s\n", result, head, since(start), time.Now().Format(time.RFC3339))
	_ = os.WriteFile(gitDir()+"/gate/race.out", append(out, summary...), 0o600)
	fmt.Print(summary)
	return code
}

func mustRead(p string) []byte {
	b, err := os.ReadFile(p) // #nosec G304 -- a fixed repository path
	if err != nil {
		fail(err)
	}
	return bytes.TrimSpace(b)
}

// raceLock is the host-wide lock every -race suite on this host takes
// (RACE_LOCK overrides it).
func raceLock() string {
	if p := os.Getenv("RACE_LOCK"); p != "" {
		return p
	}
	home, _ := os.UserHomeDir()
	return home + "/.cache/race-gate.lock"
}

// detached starts the background race run in its own session, so it outlives
// the release and the terminal.
func detached() *syscall.SysProcAttr { return &syscall.SysProcAttr{Setsid: true} }

// runAffected runs go vet and go test -short -p 4 on the packages a change
// reaches (see the package comment) and returns the exit code.
func runAffected() int {
	pkgs, err := affectedPackages()
	if err != nil {
		fmt.Println("test:", err)
		return 1
	}
	if len(pkgs) == 0 {
		fmt.Println("test: no Go package reached by the change")
		return 0
	}
	fmt.Printf("test: %d packages reached\n", len(pkgs))
	env := append(os.Environ(), "GOFLAGS=-trimpath", "CGO_ENABLED=1")
	for _, args := range [][]string{
		append([]string{"vet"}, pkgs...),
		append([]string{"test", "-short", "-p", "4", "-timeout", "20m"}, pkgs...),
	} {
		c := exec.Command("go", args...) // #nosec G204 -- go with package paths from go list
		c.Env, c.Stdout, c.Stderr = env, os.Stdout, os.Stderr
		if err := c.Run(); err != nil {
			return 1
		}
	}
	return 0
}

// affectedPackages lists the module's packages whose tests reach a package
// changed against origin/main.
func affectedPackages() ([]string, error) {
	base, err := git("merge-base", "HEAD", remote()+"/main")
	if err != nil {
		return nil, fmt.Errorf("merge-base: %s", base)
	}
	var files []string
	for _, args := range [][]string{
		{"diff", "--name-only", base},
		{"ls-files", "--others", "--exclude-standard"},
	} {
		out, err := git(args...)
		if err != nil {
			return nil, fmt.Errorf("git %s: %s", args[0], out)
		}
		files = append(files, strings.Fields(out)...)
	}
	mod, err := exec.Command("go", "list", "-m").Output()
	if err != nil {
		return nil, err
	}
	module := strings.TrimSpace(string(mod))
	all := false
	changed := map[string]bool{}
	for _, f := range files {
		if f == "go.mod" || f == "go.sum" {
			all = true
		}
		if i := strings.Index(f, "/testdata/"); i >= 0 {
			f = f[:i] + "/x.go"
		} else if strings.HasPrefix(f, "testdata/") {
			f = "x.go"
		}
		if !strings.HasSuffix(f, ".go") {
			continue
		}
		dir := module
		if i := strings.LastIndex(f, "/"); i >= 0 {
			dir += "/" + f[:i]
		}
		changed[dir] = true
	}
	out, err := exec.Command("go", "list", "-test", "-f", "{{.ImportPath}}|{{join .Deps \" \"}}", "./...").Output()
	if err != nil {
		return nil, fmt.Errorf("go list: %v", err)
	}
	seen := map[string]bool{}
	var pkgs []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		path, deps, _ := strings.Cut(line, "|")
		if strings.Contains(path, " ") || strings.HasSuffix(path, "_test") {
			continue // a test variant; its .test main carries the same deps
		}
		pkg := strings.TrimSuffix(path, ".test")
		if seen[pkg] {
			continue
		}
		hit := all || changed[pkg]
		for _, d := range strings.Fields(deps) {
			if hit {
				break
			}
			hit = changed[d]
		}
		if hit {
			seen[pkg] = true
			pkgs = append(pkgs, pkg)
		}
	}
	return pkgs, nil
}
