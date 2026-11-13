package main

import (
	"bufio"
	"bytes"
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/printer"
	"go/token"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The kind of a diff, by Go's rules plus René's (4 Nov; 13 Nov: a new API is
// no minor):
//   - major: an exported name of a public (non-internal) package is removed
//     or its signature changed (parameter names aside), or go.mod's module
//     path changes its major;
//   - patch: anything else, new exported names, a changed store format marker
//     (a const or var named *FormatVersion*), a badger minor bump and a raised
//     go directive included (listed as the reasons of a patch; René, 13 Nov:
//     every release that is not a major is a patch).
// Each package's live files are chosen by go/build (build constraints and
// GOOS/GOARCH suffixes) on both sides, so a measure-tagged file is no API.

const (
	patch = iota
	minor
	major
)

var kindNames = []string{"patch", "minor", "major"}

// tree is one side of the diff: path -> content of every file it holds.
type tree map[string][]byte

// devDirs hold Go code no consumer compiles.
var devDirs = []string{"scripts/", "bench/", "tutorials/", "internal/synthhop/"}

func shippedDir(dir string) bool {
	for _, d := range devDirs {
		if strings.HasPrefix(dir+"/", d) {
			return false
		}
	}
	return !strings.Contains("/"+dir+"/", "/testdata/")
}

// api returns the exported API and format markers of every shipped package of
// t: package dir -> name -> signature.
func (t tree) api() (map[string]map[string]string, map[string]string) {
	dirs := map[string]bool{}
	for p := range t {
		if strings.HasSuffix(p, ".go") && shippedDir(path.Dir(p)) {
			dirs[path.Dir(p)] = true
		}
	}
	ctx := build.Default
	ctx.CgoEnabled = true
	ctx.JoinPath = path.Join
	ctx.IsDir = func(p string) bool { return dirs[p] }
	ctx.HasSubdir = func(root, dir string) (string, bool) { return "", false }
	ctx.OpenFile = func(p string) (io.ReadCloser, error) {
		b, ok := t[p]
		if !ok {
			return nil, fs.ErrNotExist
		}
		return io.NopCloser(bytes.NewReader(b)), nil
	}
	ctx.ReadDir = func(dir string) ([]fs.FileInfo, error) {
		var out []fs.FileInfo
		for p, b := range t {
			if path.Dir(p) == dir {
				out = append(out, fileInfo{path.Base(p), int64(len(b))})
			}
		}
		return out, nil
	}
	apis, markers := map[string]map[string]string{}, map[string]string{}
	for dir := range dirs {
		pkg, err := ctx.ImportDir(dir, build.ImportComment)
		if err != nil {
			continue // no live Go files (measure-only, tests only, or broken: the build step says so)
		}
		a := map[string]string{}
		for _, f := range pkg.GoFiles {
			exportedAPI(t[path.Join(dir, f)], a, func(name, spec string) { markers[dir+"."+name] = spec })
		}
		apis[dir] = a
	}
	return apis, markers
}

// kindOf compares base and head: the kind and the reasons for it.
func kindOf(base, head tree) (int, []string) {
	kind, why := patch, []string(nil)
	raise := func(k int, format string, a ...any) {
		kind = max(kind, k)
		why = append(why, kindNames[k]+": "+fmt.Sprintf(format, a...))
	}
	oldAPI, oldMarkers := base.api()
	newAPI, newMarkers := head.api()
	for dir, a := range newAPI {
		for n := range a {
			if _, ok := oldAPI[dir][n]; !ok {
				raise(patch, "%s: %s is a new exported name", dir, n)
			}
		}
	}
	for dir, a := range oldAPI {
		if strings.Contains("/"+dir+"/", "/internal/") {
			continue // no consumer imports it
		}
		for n, sig := range a {
			if now, ok := newAPI[dir][n]; !ok {
				raise(major, "%s: %s is removed", dir, n)
			} else if now != sig {
				raise(major, "%s: %s changed from %q to %q", dir, n, sig, now)
			}
		}
	}
	for n, spec := range newMarkers {
		if oldMarkers[n] != spec {
			raise(patch, "format marker %s is new or changed", n)
		}
	}
	if a, b := modulePath(base["go.mod"]), modulePath(head["go.mod"]); a != b {
		raise(major, "go.mod: module %s -> %s", a, b)
	}
	if a, b := badgerMinor(base["go.mod"]), badgerMinor(head["go.mod"]); a != "" && b != "" && a != b {
		raise(patch, "go.mod: badger v%s -> v%s", a, b)
	}
	if a, b := goDirective(base["go.mod"]), goDirective(head["go.mod"]); a != b {
		raise(patch, "go.mod: go %s -> %s", a, b)
	}
	sort.Strings(why)
	return kind, why
}

var (
	moduleRE = regexp.MustCompile(`(?m)^module\s+(\S+)`)
	badgerRE = regexp.MustCompile(`(?m)^\s*(?:require\s+)?github\.com/dgraph-io/badger/v4\s+v(\d+\.\d+)\.`)
	markerRE = regexp.MustCompile(`FormatVersion`)
	goRE     = regexp.MustCompile(`(?m)^go\s+(\S+)`)
)

func goDirective(gomod []byte) string {
	if m := goRE.FindSubmatch(gomod); m != nil {
		return string(m[1])
	}
	return ""
}

func modulePath(gomod []byte) string {
	if m := moduleRE.FindSubmatch(gomod); m != nil {
		return string(m[1])
	}
	return ""
}

func badgerMinor(gomod []byte) string {
	if m := badgerRE.FindSubmatch(gomod); m != nil {
		return string(m[1])
	}
	return ""
}

// exportedAPI adds a file's exported API to out: top-level funcs, types,
// consts and vars, methods of exported types as "Recv.Name", exported and
// embedded fields and interface methods as "Type.Name", each with its
// signature, so a changed signature is told from a new name. A signature
// holds types, not parameter names (renaming one breaks no caller); a type's
// holds its type parameters and "=" for an alias; a const's or var's its
// declared type, or the kind of an untyped literal. marker gets every const or
// var spec whose name matches markerRE, with its text.
func exportedAPI(src []byte, out map[string]string, marker func(name, spec string)) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "", src, parser.SkipObjectResolution)
	if err != nil {
		return
	}
	text := func(n ast.Node) string {
		var b strings.Builder
		_ = printer.Fprint(&b, fset, n)
		return b.String()
	}
	types := func(l *ast.FieldList) string {
		if l == nil {
			return ""
		}
		var ts []string
		for _, fl := range l.List {
			for range max(1, len(fl.Names)) {
				ts = append(ts, text(fl.Type))
			}
		}
		return strings.Join(ts, ", ")
	}
	tparams := func(l *ast.FieldList) string { // printer prints no bare FieldList
		if l == nil {
			return ""
		}
		var ps []string
		for _, fl := range l.List {
			for _, n := range fl.Names {
				ps = append(ps, n.Name+" "+text(fl.Type))
			}
		}
		return "[" + strings.Join(ps, ", ") + "]"
	}
	sig := func(e ast.Expr) string {
		ft, ok := e.(*ast.FuncType)
		if !ok {
			return text(e)
		}
		return "func" + tparams(ft.TypeParams) + "(" + types(ft.Params) + ") (" + types(ft.Results) + ")"
	}
	for _, d := range f.Decls {
		switch d := d.(type) {
		case *ast.FuncDecl:
			if !d.Name.IsExported() {
				continue
			}
			if d.Recv == nil || len(d.Recv.List) == 0 {
				out[d.Name.Name] = sig(d.Type)
			} else if recv := recvName(d.Recv.List[0].Type); ast.IsExported(recv) {
				// A method of an unexported type is no API of rho's (only an
				// interface it satisfies, e.g. badger.Logger, reaches it).
				out[recv+"."+d.Name.Name] = sig(d.Type)
			}
		case *ast.GenDecl:
			for _, s := range d.Specs {
				switch s := s.(type) {
				case *ast.TypeSpec:
					if !s.Name.IsExported() {
						continue
					}
					head := tparams(s.TypeParams)
					if s.Assign.IsValid() {
						head += "= "
					}
					var fields *ast.FieldList
					switch t := s.Type.(type) {
					case *ast.StructType:
						fields = t.Fields
					case *ast.InterfaceType:
						fields = t.Methods
					default:
						out[s.Name.Name] = head + text(s.Type)
					}
					if fields == nil {
						continue
					}
					out[s.Name.Name] = head
					for _, fl := range fields.List {
						if len(fl.Names) == 0 { // embedded: its names are promoted
							out[s.Name.Name+".("+text(fl.Type)+")"] = "embedded"
						}
						for _, n := range fl.Names {
							if n.IsExported() {
								out[s.Name.Name+"."+n.Name] = sig(fl.Type)
							}
						}
					}
				case *ast.ValueSpec:
					for i, n := range s.Names {
						if n.IsExported() {
							switch {
							case s.Type != nil:
								out[n.Name] = text(s.Type)
							case i < len(s.Values):
								if lit, ok := s.Values[i].(*ast.BasicLit); ok {
									out[n.Name] = "untyped " + lit.Kind.String()
								} else {
									out[n.Name] = ""
								}
							default:
								out[n.Name] = ""
							}
						}
						if markerRE.MatchString(n.Name) {
							marker(n.Name, text(s))
						}
					}
				}
			}
		}
	}
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

// fileInfo is the fs.FileInfo go/build needs from ReadDir: a name and a size.
type fileInfo struct {
	name string
	size int64
}

func (f fileInfo) Name() string       { return f.name }
func (f fileInfo) Size() int64        { return f.size }
func (f fileInfo) Mode() fs.FileMode  { return 0o444 }
func (f fileInfo) ModTime() time.Time { return time.Time{} }
func (f fileInfo) IsDir() bool        { return false }
func (f fileInfo) Sys() any           { return nil }

// wanted: the files the kind reads (Go sources and go.mod).
func wanted(p string) bool { return p == "go.mod" || strings.HasSuffix(p, ".go") }

// treeAt reads rev's files from git in one `git cat-file --batch` stream.
func treeAt(rev string) (tree, error) {
	ls, err := exec.Command("git", "ls-tree", "-r", "-z", rev).Output() // #nosec G204 -- rev is a tag name from git
	if err != nil {
		return nil, fmt.Errorf("git ls-tree %s: %w", rev, err)
	}
	var paths, objs []string
	for _, rec := range strings.Split(strings.TrimSuffix(string(ls), "\x00"), "\x00") {
		meta, p, ok := strings.Cut(rec, "\t")
		fields := strings.Fields(meta)
		if ok && len(fields) == 3 && fields[1] == "blob" && wanted(p) {
			paths, objs = append(paths, p), append(objs, fields[2])
		}
	}
	cmd := exec.Command("git", "cat-file", "--batch")
	cmd.Stdin = strings.NewReader(strings.Join(objs, "\n") + "\n")
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git cat-file: %w", err)
	}
	t, r := tree{}, bufio.NewReader(bytes.NewReader(out))
	for _, p := range paths {
		header, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		f := strings.Fields(header)
		if len(f) != 3 {
			return nil, fmt.Errorf("git cat-file: %q", header)
		}
		n, err := strconv.Atoi(f[2])
		if err != nil {
			return nil, err
		}
		b := make([]byte, n+1) // content and its newline
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, err
		}
		t[p] = b[:n]
	}
	return t, nil
}

// workTree reads the work tree's tracked and untracked (not ignored) files.
func workTree() (tree, error) {
	ls, err := exec.Command("git", "ls-files", "-z", "--cached", "--others", "--exclude-standard").Output()
	if err != nil {
		return nil, fmt.Errorf("git ls-files: %w", err)
	}
	t := tree{}
	for _, p := range strings.Split(strings.TrimSuffix(string(ls), "\x00"), "\x00") {
		if !wanted(p) {
			continue
		}
		b, err := os.ReadFile(p) // #nosec G304 -- a path git lists in this repository
		if err != nil {
			continue // deleted in the work tree
		}
		t[p] = b
	}
	return t, nil
}
