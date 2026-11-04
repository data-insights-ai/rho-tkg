package main

import (
	"go/ast"
	"go/build/constraint"
	"go/parser"
	"go/scanner"
	"go/token"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"unicode"
)

// shipped reports whether a path is Go code a consumer of the module compiles:
// not a test, not a dev tool (scripts/, bench/, tutorials/, internal/synthhop), not testdata.
func shipped(p string) bool {
	if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") || strings.Contains("/"+p, "/testdata/") {
		return false
	}
	for _, dev := range []string{"scripts/", "bench/", "tutorials/", "internal/synthhop/"} {
		if strings.HasPrefix(p, dev) {
			return false
		}
	}
	return true
}

// buildTags are the tags a gate build satisfies: this platform, the gc
// toolchain, cgo, race (the gate runs -race) and every go1.N release tag. A
// file behind any other tag (measure, examples, ...) is not live code.
func buildTags() map[string]bool {
	t := map[string]bool{runtime.GOOS: true, runtime.GOARCH: true, "gc": true, "cgo": true, "race": true}
	switch runtime.GOOS {
	case "linux", "darwin", "freebsd", "netbsd", "openbsd", "dragonfly", "solaris", "illumos", "aix", "android", "ios", "hurd":
		t["unix"] = true
	}
	for i := 1; i <= 99; i++ {
		t["go1."+itoa(i)] = true
	}
	return t
}

func itoa(i int) string {
	if i < 10 {
		return string(rune('0' + i))
	}
	return itoa(i/10) + string(rune('0'+i%10))
}

var gateTags = buildTags()

// fileSuffixRE matches GOOS/GOARCH file-name constraints (x_linux.go, x_amd64_test.go).
var knownOS = map[string]bool{"aix": true, "android": true, "darwin": true, "dragonfly": true, "freebsd": true, "hurd": true,
	"illumos": true, "ios": true, "js": true, "linux": true, "netbsd": true, "openbsd": true, "plan9": true, "solaris": true,
	"wasip1": true, "windows": true, "zos": true}
var knownArch = map[string]bool{"386": true, "amd64": true, "arm": true, "arm64": true, "loong64": true, "mips": true,
	"mips64": true, "mips64le": true, "mipsle": true, "ppc64": true, "ppc64le": true, "riscv64": true, "s390x": true, "wasm": true}

// live reports whether a Go file is compiled by the gate's build: its
// //go:build (or // +build) lines and its _GOOS/_GOARCH name suffix are
// evaluated with go/build/constraint against gateTags. Unparsable
// constraints count as live (fail closed).
func live(p string, src []byte) bool {
	name := strings.TrimSuffix(strings.TrimSuffix(p[strings.LastIndex(p, "/")+1:], ".go"), "_test")
	parts := strings.Split(name, "_")
	if n := len(parts); n >= 2 {
		last := parts[n-1]
		if knownArch[last] && !gateTags[last] {
			return false
		}
		if knownOS[last] && !gateTags[last] {
			return false
		}
		if n >= 3 && knownArch[last] && knownOS[parts[n-2]] && !gateTags[parts[n-2]] {
			return false
		}
	}
	for _, line := range headerLines(src) {
		if !constraint.IsGoBuild(line) && !constraint.IsPlusBuild(line) {
			continue
		}
		e, err := constraint.Parse(line)
		if err != nil {
			return true
		}
		if !e.Eval(func(tag string) bool { return gateTags[tag] }) {
			return false
		}
	}
	return true
}

// headerLines returns the comment lines before the package clause.
func headerLines(src []byte) []string {
	var out []string
	for _, line := range strings.Split(string(src), "\n") {
		l := strings.TrimSpace(line)
		if strings.HasPrefix(l, "package ") || l == "package" {
			break
		}
		if strings.HasPrefix(l, "//") {
			out = append(out, l)
		}
	}
	return out
}

// tokens renders the comment-free token stream; automatic semicolons are
// normalised because a removed trailing comment can change their literal. A
// //go: directive is code.
func tokens(src []byte) string {
	fset := token.NewFileSet()
	var s scanner.Scanner
	s.Init(fset.AddFile("", fset.Base(), len(src)), src, nil, scanner.ScanComments)
	var out strings.Builder
	for {
		_, tok, lit := s.Scan()
		if tok == token.EOF {
			return out.String()
		}
		if tok == token.COMMENT && !strings.HasPrefix(lit, "//go:") {
			continue
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

// decl is one top-level declaration of a file: a func (key "Recv.Name" or
// "Name"; each init its own "init#N") or one spec of a const/var/type group
// (key: its names). text is its comment-free token stream, //go: directives
// of its doc included; start/end its byte range in the file.
type decl struct {
	key        string
	text       string
	start, end int
	isTest     bool // a top-level Test or Fuzz func
	names      []string
	idents     map[string]bool // every identifier it names (selectors included)
	// fields: a struct's fields or an interface's methods by name (an
	// embedded one by its type's text), each its token text; nil otherwise.
	fields map[string]string
}

// membersOf returns a struct or interface type's members (see decl.fields).
func membersOf(sp *ast.TypeSpec, src []byte, off func(token.Pos) int) map[string]string {
	var list *ast.FieldList
	switch t := sp.Type.(type) {
	case *ast.StructType:
		list = t.Fields
	case *ast.InterfaceType:
		list = t.Methods
	default:
		return nil
	}
	if sp.TypeParams != nil || list == nil {
		return nil
	}
	out := map[string]string{}
	for _, f := range list.List {
		text := tokens(src[off(f.Pos()):off(f.End())])
		if len(f.Names) == 0 {
			out[tokens(src[off(f.Type.Pos()):off(f.Type.End())])] = text
			continue
		}
		for _, n := range f.Names {
			out[n.Name] = text
		}
	}
	return out
}

// changedMembers returns the members of new that old lacks or that differ
// (a removed member has no users left to test), and whether the two types
// are comparable member by member.
func changedMembers(old, new decl) ([]string, bool) {
	if old.fields == nil || new.fields == nil {
		return nil, false
	}
	var out []string
	for n, t := range new.fields {
		if old.fields[n] != t {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out, true
}

func identsOf(n ast.Node) map[string]bool {
	out := map[string]bool{}
	ast.Inspect(n, func(x ast.Node) bool {
		if id, ok := x.(*ast.Ident); ok {
			out[id.Name] = true
		}
		return true
	})
	return out
}

// parseDecls returns the file's package name and declarations; ok is false
// when the file does not parse.
func parseDecls(src []byte) (pkg string, ds []decl, ok bool) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "", src, parser.ParseComments|parser.SkipObjectResolution)
	if err != nil {
		return "", nil, false
	}
	off := func(p token.Pos) int { return fset.Position(p).Offset }
	directives := func(doc *ast.CommentGroup) string {
		if doc == nil {
			return ""
		}
		var b strings.Builder
		for _, c := range doc.List {
			if strings.HasPrefix(c.Text, "//go:") {
				b.WriteString(c.Text + "\n")
			}
		}
		return b.String()
	}
	inits := 0
	for _, d := range f.Decls {
		switch d := d.(type) {
		case *ast.FuncDecl:
			key := d.Name.Name
			if d.Recv != nil && len(d.Recv.List) > 0 {
				key = recvName(d.Recv.List[0].Type) + "." + key
			} else if key == "init" {
				inits++
				key = "init#" + itoa(inits)
			}
			s, e := off(d.Pos()), off(d.End())
			ds = append(ds, decl{key: key, text: directives(d.Doc) + tokens(src[s:e]), start: s, end: e,
				isTest: d.Recv == nil && isTestName(d.Name.Name), names: []string{d.Name.Name}, idents: identsOf(d)})
		case *ast.GenDecl:
			if d.Tok == token.IMPORT {
				continue
			}
			for i, sp := range d.Specs {
				s, e := off(sp.Pos()), off(sp.End())
				if len(d.Specs) == 1 {
					s, e = off(d.Pos()), off(d.End())
				}
				var names []string
				var fields map[string]string
				switch sp := sp.(type) {
				case *ast.TypeSpec:
					names = append(names, sp.Name.Name)
					fields = membersOf(sp, src, off)
				case *ast.ValueSpec:
					for _, n := range sp.Names {
						names = append(names, n.Name)
					}
				}
				key := d.Tok.String() + " " + strings.Join(names, ",")
				text := tokens(src[s:e])
				// An iota const's value depends on its position in the group.
				if d.Tok == token.CONST && len(d.Specs) > 1 {
					text = itoa(i) + " " + tokens(src[off(d.Pos()):off(d.End())])
				}
				ds = append(ds, decl{key: key, text: directives(d.Doc) + text, start: s, end: e, names: names, idents: identsOf(sp), fields: fields})
			}
		}
	}
	return f.Name.Name, ds, true
}

// declsOf indexes a set of files' declarations by key.
func declsOf(files map[string][]byte) map[string]decl {
	out := map[string]decl{}
	for _, src := range files {
		_, ds, ok := parseDecls(src)
		if !ok {
			continue
		}
		for _, d := range ds {
			out[d.key] = d
		}
	}
	return out
}

// formatMarkerRE names the store and wire format markers: a new or changed
// const or var whose name contains FormatVersion is a new format (a door).
var formatMarkerRE = regexp.MustCompile(`FormatVersion`)

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
	sort.Strings(out)
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
	case *ast.ParenExpr:
		return recvName(t.X)
	case *ast.Ident:
		return t.Name
	}
	return "?"
}

// testNames returns the top-level Test and Fuzz functions of a test file in
// source order (Go's rule: the name continues with a non-lowercase rune).
func testNames(src []byte) []string {
	_, ds, _ := parseDecls(src)
	var out []string
	for _, d := range ds {
		if d.isTest {
			out = append(out, d.names[0])
		}
	}
	return out
}

func isTestName(name string) bool {
	for _, prefix := range []string{"Test", "Fuzz"} {
		rest, found := strings.CutPrefix(name, prefix)
		if found && (rest == "" || !unicode.IsLower([]rune(rest)[0])) {
			return true
		}
	}
	return false
}
