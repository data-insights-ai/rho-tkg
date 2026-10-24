package main

import (
	"errors"
	"strings"
	"testing"
)

type files map[string][2]string // path -> {old, new}; "" = absent on that side

func (f files) side(i int) func(string) ([]byte, error) {
	return func(p string) ([]byte, error) {
		v, ok := f[p]
		if !ok || v[i] == "" {
			return nil, errors.New("no file " + p)
		}
		return []byte(v[i]), nil
	}
}

// TestPlan pins the patch gate (todo 115/116): what a patch batch must run.
// Mutations it catches: comparing bytes instead of comment-free tokens (the
// comment-only rows gain targets); dropping the literal value (the string row
// loses its short target); listing every Go package (comment-only packages run).
func TestPlan(t *testing.T) {
	f := files{
		"pkg/a/doc.go":       {"package a\n// old\nfunc F() int { return 1 }\n", "package a\n// new\n// more\nfunc F() int { return 1 } // tail\n"},
		"pkg/a/code.go":      {"package a\nfunc g() int { return 1 }\n", "package a\nfunc g() int { return 2 }\n"},
		"pkg/b/str.go":       {"package b\nconst s = \"x\"\n", "package b\nconst s = \"y\"\n"},
		"pkg/c/c_test.go":    {"package c\n// a\nfunc TestC(t *testing.T) {}\n", "package c\n// b\nfunc TestC(t *testing.T) {}\n"},
		"pkg/d/d_test.go":    {"package d\nfunc TestD(t *testing.T) {}\n", "package d\nfunc TestD(t *testing.T) { t.Log() }\nfunc FuzzD(f *testing.F) {}\n"},
		"pkg/e/help_test.go": {"package e\nvar x = 1\n", "package e\nvar x = 2\n"},
		"pkg/f/new_test.go":  {"", "package f\nfunc TestNew(t *testing.T) {}\n"},
		"scripts/t/main.go":  {"", "package main\nfunc main() {}\n"},
	}
	cases := []struct {
		name    string
		changes []change
		want    string // plan lines joined by "; "
	}{
		{"empty", nil, ""},
		{"docs only", []change{{'M', "README.md"}, {'D', "docs/x.md"}}, ""},
		{"comment-only shipped code", []change{{'M', "pkg/a/doc.go"}}, ""},
		{"comment-only test", []change{{'M', "pkg/c/c_test.go"}}, ""},
		{"code change", []change{{'M', "pkg/a/code.go"}}, "short ./pkg/a/"},
		{"string literal change", []change{{'M', "pkg/b/str.go"}}, "short ./pkg/b/"},
		{"changed tests by name", []change{{'M', "pkg/d/d_test.go"}}, "race ./pkg/d/ ^(TestD|FuzzD)$"},
		{"changed helper runs the package", []change{{'M', "pkg/e/help_test.go"}}, "race ./pkg/e/"},
		{"new test file", []change{{'A', "pkg/f/new_test.go"}}, "race ./pkg/f/ ^(TestNew)$"},
		{"dev tool", []change{{'A', "scripts/t/main.go"}}, "race ./scripts/t/"},
		{"deleted test file", []change{{'D', "pkg/g/gone_test.go"}}, ""},
		{"new shipped file", []change{{'A', "pkg/h/n.go"}}, "short ./pkg/h/"},
		{"go.mod is CI's job (security)", []change{{'M', "go.mod"}, {'M', "go.sum"}}, ""},
		{"mixed, sorted, deduplicated", []change{{'M', "pkg/a/code.go"}, {'M', "pkg/a/doc.go"}, {'M', "pkg/d/d_test.go"}, {'M', "go.sum"}},
			"race ./pkg/d/ ^(TestD|FuzzD)$; short ./pkg/a/"},
	}
	for _, c := range cases {
		got := strings.Join(plan(c.changes, f.side(0), f.side(1)), "; ")
		if got != c.want {
			t.Errorf("%s: plan %q, want %q", c.name, got, c.want)
		}
	}
}

// TestNewExportedAPI: a door belongs in a minor, never a patch (lesson 79).
// Mutation: collecting all names instead of exported ones refuses the private
// helper row; skipping methods misses the method row.
func TestNewExportedAPI(t *testing.T) {
	f := files{
		"pkg/a/a.go": {"package a\nfunc F() {}\nfunc (s *S) M() {}\ntype S struct{}\n",
			"package a\nfunc F() {}\nfunc (s *S) M() {}\nfunc (s *S) N() {}\ntype S struct{}\nfunc G() {}\nfunc helper() {}\nconst K = 1\n"},
		"pkg/a/priv.go":     {"package a\nfunc f() {}\n", "package a\nfunc f() {}\nfunc g() {}\n"},
		"pkg/a/new.go":      {"", "package a\ntype T int\n"},
		"pkg/a/impl.go":     {"", "package a\ntype impl struct{}\nfunc (impl) Errorf() {}\nfunc (p *impl) Warn() {}\n"},
		"pkg/a/a_test.go":   {"package a\n", "package a\nfunc TestX(t *testing.T) {}\n"},
		"scripts/t/main.go": {"", "package main\nfunc Exported() {}\n"},
	}
	cases := []struct {
		name    string
		changes []change
		want    string
	}{
		{"none", nil, ""},
		{"private helper only", []change{{'M', "pkg/a/priv.go"}}, ""},
		{"new func, method, const", []change{{'M', "pkg/a/a.go"}}, "pkg/a/a.go: G, K, S.N"},
		{"new file with a type", []change{{'A', "pkg/a/new.go"}}, "pkg/a/new.go: T"},
		{"exported methods of an unexported type are not API", []change{{'A', "pkg/a/impl.go"}}, ""},
		{"tests and tools are not API", []change{{'M', "pkg/a/a_test.go"}, {'A', "scripts/t/main.go"}}, ""},
	}
	for _, c := range cases {
		got := strings.Join(newExportedAPI(c.changes, f.side(0), f.side(1)), "; ")
		if got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

func TestParseNameStatus(t *testing.T) {
	got := parseNameStatus("M\tpkg/a.go\nA\tdocs/x.md\nR100\told.go\tnew.go\nD\tgone.go\n")
	want := []change{{'M', "pkg/a.go"}, {'A', "docs/x.md"}, {'R', "new.go"}, {'D', "gone.go"}}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d: got %v, want %v", i, got[i], want[i])
		}
	}
	if len(parseNameStatus("")) != 0 {
		t.Error("empty output must give no changes")
	}
}

// Mutation: dropping Go's "next rune is not lowercase" rule lists Testlower.
func TestTestNames(t *testing.T) {
	src := []byte("package core\n\nfunc TestA(t *testing.T) {}\nfunc helper() {}\nfunc (s *S) TestM(t *testing.T) {}\nfunc BenchmarkB(b *testing.B) {}\nfunc FuzzF(f *testing.F) {}\nfunc Testlower(t *testing.T) {}\nfunc TestB(t *testing.T) {}\n")
	if got := strings.Join(testNames(src), ","); got != "TestA,FuzzF,TestB" {
		t.Errorf("got %q, want TestA,FuzzF,TestB", got)
	}
	if got := testNames([]byte("package core\n")); len(got) != 0 {
		t.Errorf("no tests: got %v", got)
	}
}

// Mutation: a refRE that accepts a leading '-' lets an option reach git.
func TestRefRE(t *testing.T) {
	for ref, ok := range map[string]bool{"v4.70.1": true, "main": true, "origin/main": true, "6e20157c": true,
		"": false, "-o": false, "--output=x": false, "a b": false, "v1;rm": false, "../x": false} {
		if refRE.MatchString(ref) != ok {
			t.Errorf("refRE(%q) = %v, want %v", ref, !ok, ok)
		}
	}
}
