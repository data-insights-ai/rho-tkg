// Command commentonly decides whether a release batch may use the fast-track
// gate (`make ci-fast`, todo 115): no shipped Go code changed since the base
// ref. It prints the packages whose Go files changed (for a targeted race run)
// and exits 1 with the reasons when the batch needs the full `make ci-docker`.
//
// Usage: go run ./scripts/commentonly <base-ref>   (compares base with the work tree)
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/scanner"
	"go/token"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"
	"unicode"
)

type change struct {
	status byte // git name-status letter: A, M, D, R, ...
	path   string
}

// verdict returns why the changes need the full gate; empty = fast track allowed.
// Only shipped Go code counts (see shipped); go.mod/go.sum change the build;
// a shipped .go file may change only in its comments.
func verdict(changes []change, readOld, readNew func(string) ([]byte, error)) []string {
	var reasons []string
	for _, c := range changes {
		switch {
		case c.path == "go.mod" || c.path == "go.sum":
			reasons = append(reasons, c.path+": module files changed")
		case !shipped(c.path):
		case c.status != 'M':
			reasons = append(reasons, fmt.Sprintf("%s: non-test Go file %c", c.path, c.status))
		default:
			same, err := sameCode(c.path, readOld, readNew)
			if err != nil {
				reasons = append(reasons, c.path+": "+err.Error())
			} else if !same {
				reasons = append(reasons, c.path+": code changed (not only comments)")
			}
		}
	}
	return reasons
}

// shipped reports whether a path is Go code a consumer of the module compiles:
// not a test, not a dev tool (scripts/, bench/, tutorials/), not testdata.
func shipped(p string) bool {
	if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") || strings.Contains("/"+p, "/testdata/") {
		return false
	}
	for _, dev := range []string{"scripts/", "bench/", "tutorials/"} {
		if strings.HasPrefix(p, dev) {
			return false
		}
	}
	return true
}

func sameCode(p string, readOld, readNew func(string) ([]byte, error)) (bool, error) {
	a, err := readOld(p)
	if err != nil {
		return false, err
	}
	b, err := readNew(p)
	if err != nil {
		return false, err
	}
	return tokens(a) == tokens(b), nil
}

// tokens renders the comment-free token stream; automatic semicolons are
// normalised because a removed trailing comment can change their literal.
func tokens(src []byte) string {
	fset := token.NewFileSet()
	var s scanner.Scanner
	s.Init(fset.AddFile("", fset.Base(), len(src)), src, nil, 0)
	var out strings.Builder
	for {
		_, tok, lit := s.Scan()
		if tok == token.EOF {
			return out.String()
		}
		if tok == token.SEMICOLON {
			lit = ";"
		}
		out.WriteString(tok.String())
		out.WriteByte(' ')
		out.WriteString(lit)
		out.WriteByte('\n')
	}
}

// parseNameStatus reads `git diff --name-status` output; a rename keeps its new path.
func parseNameStatus(out string) []change {
	var cs []change
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		f := strings.Split(line, "\t")
		if len(f) < 2 || f[0] == "" {
			continue
		}
		cs = append(cs, change{status: f[0][0], path: f[len(f)-1]})
	}
	return cs
}

func git(args ...string) ([]byte, error) {
	return exec.Command("git", args...).Output()
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: commentonly <base-ref>")
		os.Exit(2)
	}
	base := os.Args[1]
	out, err := git("diff", "--name-status", base, "--")
	if err != nil {
		fmt.Fprintln(os.Stderr, "git diff:", err)
		os.Exit(2)
	}
	untracked, err := git("ls-files", "--others", "--exclude-standard", "--", "*.go")
	if err != nil {
		fmt.Fprintln(os.Stderr, "git ls-files:", err)
		os.Exit(2)
	}
	changes := parseNameStatus(string(out))
	for _, p := range strings.Fields(string(untracked)) {
		changes = append(changes, change{status: 'A', path: p})
	}
	readOld := func(p string) ([]byte, error) { return git("show", base+":"+p) }
	if reasons := verdict(changes, readOld, os.ReadFile); len(reasons) > 0 {
		fmt.Fprintln(os.Stderr, "fast track refused, run make ci-docker:")
		for _, r := range reasons {
			fmt.Fprintln(os.Stderr, "  "+r)
		}
		os.Exit(1)
	}
	fmt.Println(strings.Join(testPackages(changes, readOld, os.ReadFile), " "))
}

// testPackages lists what the fast gate race-tests, one target per package:
// "<pkg>" runs the whole package, "<pkg>@<regex>" only the named tests. A test
// file whose tokens changed contributes its Test/Fuzz functions; a changed file
// without any (a helper), an unreadable one, or dev-tool code runs the whole
// package. A comment-only change compiles to identical code and runs nothing;
// shipped files passed verdict. GitHub CI still runs everything.
func testPackages(changes []change, readOld, readNew func(string) ([]byte, error)) []string {
	whole := map[string]bool{}
	names := map[string][]string{}
	for _, c := range changes {
		if !strings.HasSuffix(c.path, ".go") || c.status == 'D' || shipped(c.path) {
			continue
		}
		dir := "./" + path.Dir(c.path) + "/"
		if !strings.HasSuffix(c.path, "_test.go") {
			whole[dir] = true // dev-tool code
			continue
		}
		if c.status == 'M' {
			if same, err := sameCode(c.path, readOld, readNew); err == nil && same {
				continue
			}
		}
		src, err := readNew(c.path)
		if err != nil {
			whole[dir] = true
			continue
		}
		n := testNames(src)
		if len(n) == 0 {
			whole[dir] = true
			continue
		}
		names[dir] = append(names[dir], n...)
	}
	var list []string
	for d := range whole {
		list = append(list, d)
	}
	for d, n := range names {
		if !whole[d] {
			list = append(list, d+"@^("+strings.Join(n, "|")+")$")
		}
	}
	sort.Strings(list)
	return list
}

// testNames returns the top-level Test and Fuzz functions of a test file in
// source order (Go's rule: the name continues with a non-lowercase rune).
func testNames(src []byte) []string {
	f, err := parser.ParseFile(token.NewFileSet(), "", src, parser.SkipObjectResolution)
	if err != nil {
		return nil
	}
	var out []string
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Recv != nil {
			continue
		}
		for _, prefix := range []string{"Test", "Fuzz"} {
			rest, found := strings.CutPrefix(fn.Name.Name, prefix)
			if found && (rest == "" || !unicode.IsLower([]rune(rest)[0])) {
				out = append(out, fn.Name.Name)
			}
		}
	}
	return out
}
