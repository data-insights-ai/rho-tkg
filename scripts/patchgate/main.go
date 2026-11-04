// Command patchgate plans the patch gate (`make ci-fast`) for the diff
// between a base ref (the last release tag) and the work tree, and applies
// rho's own minor rules on top of gorelease's verdict (scripts/release.sh).
//
// Plan: the patch gate tests only new or changed code, with go/ast alone (no
// type checking, well under a second). It prints one line per package,
// "race <pkg> ^(<names>)$", the tests to run by name under -race:
//   - every new or changed Test and Fuzz function;
//   - every test of a moved test file (a rename runs its tests by name,
//     never its package);
//   - in each package with a changed declaration (function, method, type,
//     const or var; test helpers included), the tests whose body names it,
//     directly or through test helpers that name it (transitively). A
//     declaration no test names fails closed one level at a time to the
//     package's code that names it, until some test names that code; a
//     declaration nothing reaches is named on stderr.
//
// Changes that run nothing: comments (a //go: directive is code), files no
// gate build compiles (build constraints and _GOOS/_GOARCH suffixes evaluated
// with go/build/constraint on both sides: a measure-tagged file is not live),
// a declaration moved between files unchanged, non-Go files outside testdata.
// A changed testdata file runs the owning package's tests that name it (a fuzz
// corpus entry: its Fuzz function), else every test of that package.
//
// Minor rules (gorelease classifies exported API of the public packages):
//   - a new or changed store or wire format marker: a const or var of shipped
//     code whose name contains FormatVersion (older releases refuse the store);
//   - a minor or major bump of github.com/dgraph-io/badger/v4 in go.mod;
//   - a new exported name in an internal package (gorelease ignores internal
//     packages), while PATCHGATE_INTERNAL_EXPORTS is not "ignore" (René's open
//     question; one switch).
//
// Usage:
//
//	go run ./scripts/patchgate <base-ref>        refuse a minor (exit 1), else print the plan
//	go run ./scripts/patchgate -kind <base-ref>  print "patch" or "minor" and the reasons
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type change struct {
	status byte   // git name-status letter: A, M, D, R, C
	path   string // the path in the work tree (new side)
	old    string // the path at the base (old side); "" for an added file
}

// refRE accepts a tag, branch or commit name; anything else (an option, a
// path with spaces) never reaches git.
var refRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]*$`)

type reader func(string) ([]byte, error)

// pkgKey identifies a package: its directory and package clause (so an
// external _test package is its own).
type pkgKey struct{ dir, name string }

// changed is one new or changed non-test declaration.
type changed struct {
	file  string
	key   string
	names []string
}

// result is what a diff asks of a release.
type result struct {
	minor                 []string                   // why this is a minor by rho's rules (empty: a patch)
	tests                 map[string]map[string]bool // package dir -> test names to run by name
	decls                 map[string][]changed       // package dir -> changed non-test declarations
	notes                 []string                   // what was decided and why (stderr)
	ignoreInternalExports bool
}

func (r *result) addTest(dir, name string) {
	if r.tests[dir] == nil {
		r.tests[dir] = map[string]bool{}
	}
	r.tests[dir][name] = true
}

// analyze classifies the diff: readOld reads a base path, readNew a work-tree path.
func analyze(changes []change, readOld, readNew reader, ignoreInternalExports bool) *result {
	r := &result{tests: map[string]map[string]bool{}, decls: map[string][]changed{}, ignoreInternalExports: ignoreInternalExports}
	oldFiles := map[pkgKey]map[string][]byte{}
	newFiles := map[pkgKey]map[string][]byte{}
	put := func(m map[pkgKey]map[string][]byte, k pkgKey, p string, src []byte) {
		if m[k] == nil {
			m[k] = map[string][]byte{}
		}
		m[k][p] = src
	}
	for _, c := range changes {
		if c.path == "go.mod" {
			r.goMod(readOld, readNew)
			continue
		}
		if owner, rest, found := strings.Cut("/"+c.path, "/testdata/"); found {
			if c.status != 'D' {
				r.testdata("."+owner+"/", rest, readNew)
			}
			continue
		}
		if !strings.HasSuffix(c.path, ".go") && !strings.HasSuffix(c.old, ".go") {
			continue
		}
		if c.old != "" && strings.HasSuffix(c.old, ".go") {
			if src, err := readOld(c.old); err == nil && live(c.old, src) {
				if name, _, ok := parseDecls(src); ok {
					put(oldFiles, pkgKey{"./" + path.Dir(c.old) + "/", name}, c.old, src)
				}
			}
		}
		if c.status == 'D' || !strings.HasSuffix(c.path, ".go") {
			continue
		}
		src, err := readNew(c.path)
		if err != nil || !live(c.path, src) {
			continue
		}
		name, _, ok := parseDecls(src)
		if !ok {
			r.notes = append(r.notes, c.path+": does not parse; the build step fails")
			continue
		}
		k := pkgKey{"./" + path.Dir(c.path) + "/", name}
		put(newFiles, k, c.path, src)
		if c.status == 'R' && strings.HasSuffix(c.path, "_test.go") {
			for _, n := range testNames(src) {
				r.addTest(k.dir, n)
			}
		}
	}
	keys := map[pkgKey]bool{}
	for k := range oldFiles {
		keys[k] = true
	}
	for k := range newFiles {
		keys[k] = true
	}
	var sorted []pkgKey
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].dir+sorted[i].name < sorted[j].dir+sorted[j].name })
	for _, k := range sorted {
		r.pkg(k, oldFiles[k], newFiles[k])
	}
	return r
}

// pkg compares one package's changed files, old against new.
func (r *result) pkg(k pkgKey, oldF, newF map[string][]byte) {
	old := declsOf(oldF)
	// The exported names of the package's changed files on each side: a name
	// moved between two changed files is no new API.
	oldAPI, newAPI := map[string]bool{}, map[string]string{}
	for p, src := range oldF {
		if shipped(p) {
			for _, n := range exportedNames(src) {
				oldAPI[n] = true
			}
		}
	}
	var files []string
	for p := range newF {
		files = append(files, p)
	}
	sort.Strings(files)
	for _, p := range files {
		src := newF[p]
		if shipped(p) {
			for _, n := range exportedNames(src) {
				newAPI[n] = p
			}
		}
		_, ds, _ := parseDecls(src)
		for _, d := range ds {
			if o, ok := old[d.key]; ok && o.text == d.text {
				continue
			}
			if shipped(p) && (strings.HasPrefix(d.key, "const ") || strings.HasPrefix(d.key, "var ")) {
				for _, n := range d.names {
					if formatMarkerRE.MatchString(n) {
						r.minor = append(r.minor, fmt.Sprintf("%s: format marker %s is new or changed", p, n))
					}
				}
			}
			if d.isTest && strings.HasSuffix(p, "_test.go") {
				r.addTest(k.dir, d.names[0])
				continue
			}
			names := d.names
			if o, ok := old[d.key]; ok {
				if members, ok := changedMembers(o, d); ok {
					names = members // a struct or interface: its changed members, not every user of the type
				}
			}
			if len(names) > 0 {
				r.decls[k.dir] = append(r.decls[k.dir], changed{file: p, key: d.key, names: names})
			}
		}
	}
	if r.ignoreInternalExports || !strings.Contains(k.dir, "/internal/") {
		return
	}
	var added []string
	for n, p := range newAPI {
		if !oldAPI[n] {
			added = append(added, p+": "+n)
		}
	}
	sort.Strings(added)
	for _, a := range added {
		r.minor = append(r.minor, a+" is a new exported name in an internal package")
	}
}

// badgerRE reads the badger requirement of a go.mod.
var badgerRE = regexp.MustCompile(`(?m)^\s*(?:require\s+)?github\.com/dgraph-io/badger/v4\s+v(\d+)\.(\d+)\.`)

// goMod flags a minor or major badger bump.
func (r *result) goMod(readOld, readNew reader) {
	a, errA := readOld("go.mod")
	b, errB := readNew("go.mod")
	if errA != nil || errB != nil {
		return
	}
	ma, mb := badgerRE.FindSubmatch(a), badgerRE.FindSubmatch(b)
	if ma == nil || mb == nil {
		return
	}
	if string(ma[1]) != string(mb[1]) || string(ma[2]) != string(mb[2]) {
		r.minor = append(r.minor, fmt.Sprintf("go.mod: badger v%s.%s -> v%s.%s is a minor or major bump", ma[1], ma[2], mb[1], mb[2]))
	}
}

// testdata plans a changed fixture of package dir owner (rest: its path
// below testdata/). A fuzz corpus entry runs its Fuzz function; another file
// runs the owner's tests whose code names the file or its top directory under
// testdata; none found, every test of the owner.
func (r *result) testdata(owner, rest string, readNew reader) {
	parts := strings.Split(rest, "/")
	if len(parts) >= 3 && parts[0] == "fuzz" && isTestName(parts[1]) {
		r.addTest(owner, parts[1])
		return
	}
	words := []string{parts[0], path.Base(rest)}
	var all []string
	found := false
	for _, f := range testFiles(owner, readNew) {
		for _, d := range f.decls {
			if !d.isTest {
				continue
			}
			all = append(all, d.names[0])
			body := string(f.src[d.start:d.end])
			for _, w := range words {
				if w != "" && strings.Contains(body, w) {
					r.addTest(owner, d.names[0])
					found = true
					break
				}
			}
		}
	}
	if !found {
		r.notes = append(r.notes, fmt.Sprintf("%stestdata/%s: no test names it; every test of %s", owner, rest, owner))
		for _, n := range all {
			r.addTest(owner, n)
		}
	}
}

// globGo lists a directory's .go files (a variable for the tests).
var globGo = func(dir string) []string {
	m, _ := filepath.Glob(filepath.Join(dir, "*.go"))
	return m
}

type parsedFile struct {
	path  string
	src   []byte
	decls []decl
}

// pkgFiles returns the live Go files of a package directory, test files or not.
func pkgFiles(dir string, tests bool, readNew reader) []parsedFile {
	matches := globGo(strings.TrimPrefix(dir, "./"))
	var out []parsedFile
	for _, f := range matches {
		f = filepath.ToSlash(f)
		if strings.HasSuffix(f, "_test.go") != tests {
			continue
		}
		src, err := readNew(f)
		if err != nil || !live(f, src) {
			continue
		}
		_, ds, ok := parseDecls(src)
		if ok {
			out = append(out, parsedFile{f, src, ds})
		}
	}
	return out
}

func testFiles(dir string, readNew reader) []parsedFile { return pkgFiles(dir, true, readNew) }

// selectTests adds, per package with changed declarations, the tests that
// name them directly or through test helpers (transitively); a declaration
// no test reaches widens one level at a time to the package code naming it.
func (r *result) selectTests(readNew reader) {
	dirs := make([]string, 0, len(r.decls))
	for d := range r.decls {
		dirs = append(dirs, d)
	}
	sort.Strings(dirs)
	for _, dir := range dirs {
		tf := testFiles(dir, readNew)
		var code []parsedFile
		codeLoaded := false
		for _, c := range r.decls[dir] {
			names := map[string]bool{}
			for _, n := range c.names {
				names[n] = true
			}
			if strings.HasSuffix(c.file, "_test.go") {
				names = reachHelpers(tf, names)
			}
			for level := 0; ; level++ {
				found := testsNaming(tf, names)
				if len(found) > 0 {
					for _, n := range found {
						r.addTest(dir, n)
					}
					if level > 0 {
						r.notes = append(r.notes, fmt.Sprintf("%s: %s is named by no test; %d tests reach it through package code (%d levels)", c.file, c.key, len(found), level))
					}
					break
				}
				if !codeLoaded {
					code, codeLoaded = pkgFiles(dir, false, readNew), true
				}
				grown := callersNaming(code, names)
				if len(grown) == len(names) {
					r.notes = append(r.notes, fmt.Sprintf("%s: %s: no test of %s reaches it", c.file, c.key, dir))
					break
				}
				names = grown
			}
		}
	}
}

// reachHelpers returns names plus every non-test declaration of the test
// files that names one of them, to a fixpoint.
func reachHelpers(tf []parsedFile, names map[string]bool) map[string]bool {
	out := map[string]bool{}
	for n := range names {
		out[n] = true
	}
	for {
		before := len(out)
		for _, f := range tf {
			for _, d := range f.decls {
				if d.isTest || containsAll(out, d.names) {
					continue
				}
				if namesAny(d.idents, out) {
					for _, n := range d.names {
						out[n] = true
					}
				}
			}
		}
		if len(out) == before {
			return out
		}
	}
}

// testsNaming returns the tests naming any of names directly or through test helpers.
func testsNaming(tf []parsedFile, names map[string]bool) []string {
	all := reachHelpers(tf, names)
	var out []string
	for _, f := range tf {
		for _, d := range f.decls {
			if d.isTest && namesAny(d.idents, all) {
				out = append(out, d.names[0])
			}
		}
	}
	return out
}

// callersNaming returns names plus the names of every declaration of the
// package's non-test code naming one of them (one level).
func callersNaming(code []parsedFile, names map[string]bool) map[string]bool {
	out := map[string]bool{}
	for n := range names {
		out[n] = true
	}
	for _, f := range code {
		for _, d := range f.decls {
			if namesAny(d.idents, names) {
				for _, n := range d.names {
					out[n] = true
				}
			}
		}
	}
	return out
}

func namesAny(idents map[string]bool, names map[string]bool) bool {
	for n := range names {
		if idents[n] {
			return true
		}
	}
	return false
}

func containsAll(set map[string]bool, names []string) bool {
	for _, n := range names {
		if !set[n] {
			return false
		}
	}
	return true
}

// lines renders the plan, one sorted "race <dir> ^(<names>)$" per package.
func (r *result) lines() []string {
	var out []string
	for d, names := range r.tests {
		var ns []string
		for n := range names {
			ns = append(ns, n)
		}
		sort.Strings(ns)
		out = append(out, "race "+d+" ^("+strings.Join(ns, "|")+")$")
	}
	sort.Strings(out)
	return out
}

// parseNameStatus reads `git diff -M --name-status` output; a rename or copy
// keeps both paths.
func parseNameStatus(out string) []change {
	var cs []change
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		f := strings.Split(line, "\t")
		if len(f) < 2 || f[0] == "" {
			continue
		}
		c := change{status: f[0][0], path: f[len(f)-1]}
		switch {
		case len(f) >= 3:
			c.old = f[1]
		case c.status != 'A':
			c.old = c.path
		}
		cs = append(cs, c)
	}
	return cs
}

func git(args ...string) ([]byte, error) {
	return exec.Command("git", args...).Output() // #nosec G204 G702 -- fixed binary; args are a refRE-checked ref and git-reported paths
}

func main() {
	args := os.Args[1:]
	kindOnly := false
	if len(args) > 0 && args[0] == "-kind" {
		kindOnly, args = true, args[1:]
	}
	if len(args) != 1 || !refRE.MatchString(args[0]) {
		fmt.Fprintln(os.Stderr, "usage: patchgate [-kind] <base-ref>")
		os.Exit(2)
	}
	base := args[0]
	if top, err := git("rev-parse", "--show-toplevel"); err == nil {
		_ = os.Chdir(strings.TrimSpace(string(top)))
	}
	out, err := git("diff", "-M", "--name-status", base, "--")
	if err != nil {
		fmt.Fprintln(os.Stderr, "git diff:", err)
		os.Exit(2)
	}
	untracked, err := git("ls-files", "--others", "--exclude-standard")
	if err != nil {
		fmt.Fprintln(os.Stderr, "git ls-files:", err)
		os.Exit(2)
	}
	changes := parseNameStatus(string(out))
	for _, p := range strings.Fields(string(untracked)) {
		changes = append(changes, change{status: 'A', path: p})
	}
	readOld := func(p string) ([]byte, error) { return git("show", base+":"+p) }
	r := analyze(changes, readOld, os.ReadFile, os.Getenv("PATCHGATE_INTERNAL_EXPORTS") == "ignore")
	if kindOnly {
		if len(r.minor) > 0 {
			fmt.Println("minor")
			for _, m := range r.minor {
				fmt.Println("  " + m)
			}
		} else {
			fmt.Println("patch")
		}
		return
	}
	if len(r.minor) > 0 {
		fmt.Fprintln(os.Stderr, "patch gate refused: this diff is a minor:")
		for _, m := range r.minor {
			fmt.Fprintln(os.Stderr, "  "+m)
		}
		os.Exit(1)
	}
	r.selectTests(os.ReadFile)
	for _, n := range r.notes {
		fmt.Fprintln(os.Stderr, "patchgate: "+n)
	}
	for _, line := range r.lines() {
		fmt.Println(line)
	}
}
