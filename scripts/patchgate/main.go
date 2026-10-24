// Command patchgate plans the patch release gate (`make ci-fast`, todo 115/116)
// for the batch between a base ref (the last release tag) and the work tree.
//
// It refuses (exit 1) when shipped code gains exported API: a new door belongs
// in a minor (lesson 79), whose gate is `make ci-docker`. Otherwise it prints
// one test target per line:
//
//	short <pkg>          the package's shipped code changed: go test -short
//	race <pkg> [<regex>] its test or dev-tool code changed: go test -short -race,
//	                     only the named Test/Fuzz functions when the regex is set
//
// A change only in comments compiles to identical code and runs nothing.
// gosec, govulncheck and the full race suite run in GitHub CI on the push.
//
// Usage: go run ./scripts/patchgate <base-ref>
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
	"regexp"
	"sort"
	"strings"
	"unicode"
)

type change struct {
	status byte // git name-status letter: A, M, D, R, ...
	path   string
}

// refRE accepts a tag, branch or commit name; anything else (an option, a
// path with spaces) never reaches git.
var refRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]*$`)

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

// codeChanged reports whether a modified file's comment-free tokens differ;
// unreadable sides count as changed.
func codeChanged(c change, readOld, readNew func(string) ([]byte, error)) bool {
	if c.status != 'M' {
		return true
	}
	a, err := readOld(c.path)
	if err != nil {
		return true
	}
	b, err := readNew(c.path)
	if err != nil {
		return true
	}
	return tokens(a) != tokens(b)
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

// plan returns the sorted test targets (see the package comment).
func plan(changes []change, readOld, readNew func(string) ([]byte, error)) []string {
	short := map[string]bool{}
	whole := map[string]bool{}
	names := map[string][]string{}
	for _, c := range changes {
		if !strings.HasSuffix(c.path, ".go") || c.status == 'D' || !codeChanged(c, readOld, readNew) {
			continue
		}
		dir := "./" + path.Dir(c.path) + "/"
		switch {
		case shipped(c.path):
			short[dir] = true
		case !strings.HasSuffix(c.path, "_test.go"):
			whole[dir] = true // dev-tool or testdata code
		default:
			src, err := readNew(c.path)
			n := testNames(src)
			if err != nil || len(n) == 0 {
				whole[dir] = true // a changed helper: its users are unknown
			} else {
				names[dir] = append(names[dir], n...)
			}
		}
	}
	var out []string
	for d := range short {
		out = append(out, "short "+d)
	}
	for d := range whole {
		out = append(out, "race "+d)
	}
	for d, n := range names {
		if !whole[d] {
			out = append(out, "race "+d+" ^("+strings.Join(n, "|")+")$")
		}
	}
	sort.Strings(out)
	return out
}

// newExportedAPI lists exported top-level names and methods that shipped files
// gained, as "<path>: A, B".
func newExportedAPI(changes []change, readOld, readNew func(string) ([]byte, error)) []string {
	var out []string
	for _, c := range changes {
		if !shipped(c.path) || c.status == 'D' || !codeChanged(c, readOld, readNew) {
			continue
		}
		src, err := readNew(c.path)
		if err != nil {
			continue
		}
		have := map[string]bool{}
		if old, err := readOld(c.path); err == nil {
			for _, n := range exportedNames(old) {
				have[n] = true
			}
		}
		var added []string
		for _, n := range exportedNames(src) {
			if !have[n] {
				added = append(added, n)
			}
		}
		if len(added) > 0 {
			sort.Strings(added)
			out = append(out, c.path+": "+strings.Join(added, ", "))
		}
	}
	return out
}

// exportedNames returns a file's exported top-level funcs, types, consts and
// vars, and the exported methods of its exported types as "Recv.Name".
func exportedNames(src []byte) []string {
	f, err := parser.ParseFile(token.NewFileSet(), "", src, parser.SkipObjectResolution)
	if err != nil {
		return nil
	}
	var out []string
	for _, d := range f.Decls {
		switch d := d.(type) {
		case *ast.FuncDecl:
			if !d.Name.IsExported() {
				continue
			}
			if d.Recv == nil || len(d.Recv.List) == 0 {
				out = append(out, d.Name.Name)
				continue
			}
			// A method of an unexported type is reachable only through an
			// interface it satisfies (e.g. a badger.Logger), not as rho's API.
			// Embedding such a type in an exported struct would promote the
			// method; the gate does not detect that, the review must.
			if recv := recvName(d.Recv.List[0].Type); ast.IsExported(recv) {
				out = append(out, recv+"."+d.Name.Name)
			}
		case *ast.GenDecl:
			for _, s := range d.Specs {
				switch s := s.(type) {
				case *ast.TypeSpec:
					if s.Name.IsExported() {
						out = append(out, s.Name.Name)
					}
				case *ast.ValueSpec:
					for _, n := range s.Names {
						if n.IsExported() {
							out = append(out, n.Name)
						}
					}
				}
			}
		}
	}
	return out
}

func recvName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return recvName(t.X)
	case *ast.IndexExpr:
		return recvName(t.X)
	case *ast.IndexListExpr:
		return recvName(t.X)
	case *ast.Ident:
		return t.Name
	}
	return "?"
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
	return exec.Command("git", args...).Output() // #nosec G204 G702 -- fixed binary; args are a refRE-checked ref and git-reported paths
}

func main() {
	if len(os.Args) != 2 || !refRE.MatchString(os.Args[1]) {
		fmt.Fprintln(os.Stderr, "usage: patchgate <base-ref>")
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
	if api := newExportedAPI(changes, readOld, os.ReadFile); len(api) > 0 {
		fmt.Fprintln(os.Stderr, "patch gate refused: new exported API is a door, ship it as a minor (make ci-docker):")
		for _, a := range api {
			fmt.Fprintln(os.Stderr, "  "+a)
		}
		os.Exit(1)
	}
	for _, line := range plan(changes, readOld, os.ReadFile) {
		fmt.Println(line)
	}
}
