package main

import (
	"errors"
	"strings"
	"testing"
)

// TestCommentOnly pins the fast-track eligibility rule (todo 115): a batch may skip
// the full release gate only when no shipped Go code changes. Mutation it
// catches: comparing source bytes instead of comment-free token streams (the
// comment-only case turns red), or skipping the literal value (the string case
// turns green when it must refuse).
func TestCommentOnly(t *testing.T) {
	src := map[string]string{
		"old:a.go":      "package a\n\n// old note\nfunc F() int { return 1 }\n",
		"new:a.go":      "package a\n\n// new note, longer\n// and a second line\nfunc F() int { return 1 } // tail\n",
		"old:code.go":   "package a\nfunc G() int { return 1 }\n",
		"new:code.go":   "package a\nfunc G() int { return 2 }\n",
		"old:str.go":    "package a\nconst S = \"x\"\n",
		"new:str.go":    "package a\nconst S = \"y\"\n",
		"old:a_test.go": "package a\nfunc TestX() {}\n",
		"new:a_test.go": "package a\nfunc TestX() { panic(1) }\n",
	}
	read := func(side string) func(string) ([]byte, error) {
		return func(p string) ([]byte, error) {
			s, ok := src[side+":"+p]
			if !ok {
				return nil, errors.New("no file " + p)
			}
			return []byte(s), nil
		}
	}
	cases := []struct {
		name    string
		changes []change
		refuse  string // substring of the joined reasons; "" = eligible
	}{
		{"empty diff", nil, ""},
		{"comment-only change", []change{{'M', "a.go"}}, ""},
		{"test file change", []change{{'M', "a_test.go"}, {'A', "b_test.go"}}, ""},
		{"docs and scripts", []change{{'M', "README.md"}, {'D', "docs/x.md"}, {'M', "Makefile"}}, ""},
		{"code change", []change{{'M', "code.go"}}, "code.go"},
		{"string literal change", []change{{'M', "str.go"}}, "str.go"},
		{"new non-test file", []change{{'A', "n.go"}}, "n.go"},
		{"removed non-test file", []change{{'D', "d.go"}}, "d.go"},
		{"renamed non-test file", []change{{'R', "r.go"}}, "r.go"},
		{"go.mod change", []change{{'M', "go.mod"}}, "go.mod"},
		{"dev tools are not shipped", []change{{'A', "scripts/tool/main.go"}, {'M', "bench/b.go"}, {'D', "tutorials/001/main.go"}, {'M', "x/testdata/t.go"}}, ""},
		{"new file under pkg", []change{{'A', "pkg/graph/n.go"}}, "pkg/graph/n.go"},
		{"go.sum change", []change{{'M', "go.sum"}}, "go.sum"},
		{"unreadable file refuses", []change{{'M', "missing.go"}}, "missing.go"},
		{"one bad among good", []change{{'M', "a.go"}, {'M', "code.go"}}, "code.go"},
	}
	for _, c := range cases {
		got := strings.Join(verdict(c.changes, read("old"), read("new")), "; ")
		if c.refuse == "" && got != "" {
			t.Errorf("%s: refused %q, want eligible", c.name, got)
		}
		if c.refuse != "" && !strings.Contains(got, c.refuse) {
			t.Errorf("%s: reasons %q, want one naming %q", c.name, got, c.refuse)
		}
	}
}

func TestParseNameStatus(t *testing.T) {
	out := "M\tpkg/a.go\nA\tdocs/x.md\nR100\told.go\tnew.go\nD\tgone.go\n"
	got := parseNameStatus(out)
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

// Mutation: listing every changed Go file's package (the first version did) puts
// comment-only packages back into the race run; the want list turns red.
func TestTestPackages(t *testing.T) {
	cs := []change{
		{'M', "pkg/graph/internal/core/a.go"},       // comment-only shipped code: compiled code unchanged, no test run
		{'M', "pkg/graph/internal/core/b_test.go"},  // changed test
		{'M', "pkg/graph/internal/index/n_test.go"}, // comment-only test change: no run
		{'A', "pkg/graph/store/tiered/x_test.go"},   // new test
		{'A', "scripts/commentonly/main.go"},        // dev tool
		{'D', "pkg/graph/gone_test.go"},             // deleted test: its package may be gone
		{'M', "README.md"},                          // not Go
		{'M', "pkg/graph/replication/r_test.go"},    // test code changed: only its tests run
		{'M', "pkg/graph/internal/core/c_test.go"},  // duplicate package
	}
	files := map[string][2]string{
		"pkg/graph/internal/core/b_test.go":  {"package core\nfunc TestB() {}\n", "package core\nfunc TestB() { t() }\n"},
		"pkg/graph/internal/index/n_test.go": {"package index\n// a\nfunc TestN() {}\n", "package index\n// b\nfunc TestN() {}\n"},
		"pkg/graph/internal/core/c_test.go":  {"package core\nvar x = 1\n", "package core\nvar x = 2\n"},
		"pkg/graph/replication/r_test.go":    {"package replication\nfunc TestR(t *testing.T) {}\n", "package replication\nfunc TestR(t *testing.T) { t.Log() }\nfunc FuzzR(f *testing.F) {}\n"},
	}
	side := func(i int) func(string) ([]byte, error) {
		return func(p string) ([]byte, error) {
			f, ok := files[p]
			if !ok {
				return nil, errors.New("no file " + p)
			}
			return []byte(f[i]), nil
		}
	}
	got := strings.Join(testPackages(cs, side(0), side(1)), " ")
	// core: c_test.go changed a helper (no Test func) -> whole package; tiered: new
	// file not readable here -> whole package; tool -> whole package.
	want := "./pkg/graph/internal/core/ ./pkg/graph/replication/@^(TestR|FuzzR)$ ./pkg/graph/store/tiered/ ./scripts/commentonly/"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if len(testPackages(nil, side(0), side(1))) != 0 {
		t.Error("no changes must give no packages")
	}
}

// Mutation: dropping Go's "next rune is not lowercase" rule lists Testlower,
// which go test would not run as a test (red: got TestA,FuzzF,Testlower,TestB).
func TestTestNames(t *testing.T) {
	src := []byte("package core\n\nfunc TestA(t *testing.T) {}\nfunc helper() {}\nfunc (s *S) TestM(t *testing.T) {}\nfunc BenchmarkB(b *testing.B) {}\nfunc FuzzF(f *testing.F) {}\nfunc Testlower(t *testing.T) {}\nfunc TestB(t *testing.T) {}\n")
	if got := strings.Join(testNames(src), ","); got != "TestA,FuzzF,TestB" {
		t.Errorf("got %q, want TestA,FuzzF,TestB", got)
	}
	if got := testNames([]byte("package core\n")); len(got) != 0 {
		t.Errorf("no tests: got %v", got)
	}
}
