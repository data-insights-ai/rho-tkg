package main

import (
	"strings"
	"testing"
)

const gomod = "module github.com/x/y/v4\n\nrequire github.com/dgraph-io/badger/v4 v4.9.2\n"

// TestKindOf pins the kind rules. Mutations it catches: ignoring build
// constraints (the measure row turns minor), skipping fields or interface
// methods (those rows turn patch), comparing names but not signatures (the
// signature row turns patch), treating internal removals as breaking (the
// internal row turns major), keeping parameter names in signatures (the rename
// row turns major), and dropping embedded fields, type parameters, the alias
// "=", const types or the go directive (their rows turn patch).
func TestKindOf(t *testing.T) {
	base := tree{
		"go.mod":                   []byte(gomod),
		"pkg/a/a.go":               []byte("package a\n\nfunc F(x int) int { return x }\n\ntype S struct{ A int }\n\ntype I interface{ M() }\n\nconst StoreFormatVersion = 3\n"),
		"pkg/internal/i/i.go":      []byte("package i\n\nfunc Old() {}\n"),
		"pkg/a/m_test.go":          []byte("package a\n\nfunc TestX() {}\n"),
		"scripts/tool/main.go":     []byte("package main\n\nfunc main() {}\n"),
		"pkg/a/measure_helpers.go": []byte("//go:build measure\n\npackage a\n\nfunc Measured() {}\n"),
	}
	edit := func(p, src string) tree {
		t := tree{}
		for k, v := range base {
			t[k] = v
		}
		if src == "" {
			delete(t, p)
		} else {
			t[p] = []byte(src)
		}
		return t
	}
	aWith := func(extra string) string { return string(base["pkg/a/a.go"]) + extra }
	// editFrom compares pkg/a/a.go as before against as after, both on base.
	type pair struct{ before, after tree }
	var pairs = map[string]pair{}
	editFrom := func(before, after string) tree {
		h := edit("pkg/a/a.go", after)
		pairs[after] = pair{edit("pkg/a/a.go", before), h}
		return h
	}
	cases := []struct {
		name string
		head tree
		want int
		why  string
	}{
		{"unchanged", base, patch, ""},
		{"a body change", edit("pkg/a/a.go", strings.Replace(string(base["pkg/a/a.go"]), "return x", "return x + 1", 1)), patch, ""},
		{"a test or a tool", edit("scripts/tool/main.go", "package main\n\nfunc main() {}\n\nfunc New() {}\n"), patch, ""},
		{"a measure-only export", edit("pkg/a/measure_helpers.go", "//go:build measure\n\npackage a\n\nfunc Measured() {}\n\nfunc More() {}\n"), patch, ""},
		{"a new func", edit("pkg/a/a.go", aWith("\nfunc G() {}\n")), minor, "G is a new exported name"},
		{"a new field", edit("pkg/a/a.go", strings.Replace(string(base["pkg/a/a.go"]), "A int }", "A int; B bool }", 1)), minor, "S.B is a new"},
		{"a new interface method", edit("pkg/a/a.go", strings.Replace(string(base["pkg/a/a.go"]), "M() }", "M(); N() }", 1)), minor, "I.N is a new method of an interface"},
		{"a new method of a sealed interface", edit("pkg/a/a.go", aWith("\ntype J interface{ M(); seal() }\n")+"\n"), minor, "J is a new"},
		{"a parameter renamed", edit("pkg/a/a.go", strings.Replace(string(base["pkg/a/a.go"]), "F(x int) int { return x }", "F(y int) int { return y }", 1)), patch, ""},
		{"an embedded field", edit("pkg/a/a.go", strings.Replace(string(base["pkg/a/a.go"]), "A int }", "A int; io.Reader }", 1)), minor, "S.(io.Reader) is a new"},
		{"a removed embedded field", editFrom(aWith("\ntype E struct{ S }\n"), aWith("\ntype E struct{}\n")), major, "E.(S) is removed"},
		{"a type parameter", editFrom(aWith("\ntype G[T any] struct{}\n"), aWith("\ntype G[T comparable] struct{}\n")), major, "G changed"},
		{"a defined type to an alias", editFrom(aWith("\ntype N int\n"), aWith("\ntype N = int\n")), major, "N changed"},
		{"a const's type", editFrom(aWith("\nconst C = 1\n"), aWith("\nconst C = \"1\"\n")), major, "C changed"},
		{"a go directive bump", edit("go.mod", strings.Replace(gomod, "\n\n", "\n\ngo 1.26\n", 1)), minor, "go.mod: go"},
		{"an internal export", edit("pkg/internal/i/i.go", "package i\n\nfunc Old() {}\n\nfunc New() {}\n"), minor, "New is a new"},
		{"an internal removal", edit("pkg/internal/i/i.go", "package i\n"), patch, ""},
		{"a format marker", edit("pkg/a/a.go", strings.Replace(string(base["pkg/a/a.go"]), "= 3", "= 4", 1)), minor, "format marker"},
		{"a badger minor bump", edit("go.mod", strings.Replace(gomod, "v4.9.2", "v4.10.0", 1)), minor, "badger v4.9 -> v4.10"},
		{"a badger patch bump", edit("go.mod", strings.Replace(gomod, "v4.9.2", "v4.9.3", 1)), patch, ""},
		{"a removed func", edit("pkg/a/a.go", strings.Replace(string(base["pkg/a/a.go"]), "func F(x int) int { return x }\n", "", 1)), major, "F is removed"},
		{"a changed signature", edit("pkg/a/a.go", strings.Replace(string(base["pkg/a/a.go"]), "F(x int) int", "F(x int64) int", 1)), major, "F changed"},
		{"a new module major", edit("go.mod", strings.Replace(gomod, "/v4\n", "/v5\n", 1)), major, "module"},
	}
	for _, c := range cases {
		b := base
		if p, ok := pairs[string(c.head["pkg/a/a.go"])]; ok {
			b = p.before
		}
		got, why := kindOf(b, c.head)
		all := strings.Join(why, "; ")
		if got != c.want || (c.why != "" && !strings.Contains(all, c.why)) || (c.want == patch && all != "") {
			t.Errorf("%s: got %s (%s), want %s with %q", c.name, kindNames[got], all, kindNames[c.want], c.why)
		}
	}
}

func TestNextVersion(t *testing.T) {
	for _, c := range []struct {
		k    int
		want string
	}{{patch, "v4.104.2"}, {minor, "v4.105.0"}, {major, "v5.0.0"}} {
		if got := nextVersion("v4.104.1", c.k); got != c.want {
			t.Errorf("%s: got %s, want %s", kindNames[c.k], got, c.want)
		}
	}
}
