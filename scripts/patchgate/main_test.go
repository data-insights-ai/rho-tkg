package main

import (
	"errors"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
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

// plan runs analyze and the test selection over the fake tree f, as main does.
func (f files) plan(t *testing.T, changes []change) (string, *result) {
	t.Helper()
	prev := globGo
	globGo = func(dir string) []string {
		var out []string
		for p, v := range f {
			if path.Dir(p) == strings.TrimSuffix(dir, "/") && strings.HasSuffix(p, ".go") && v[1] != "" {
				out = append(out, p)
			}
		}
		sort.Strings(out)
		return out
	}
	defer func() { globGo = prev }()
	r := analyze(changes, f.side(0), f.side(1))
	r.selectTests(f.side(1))
	return strings.Join(r.lines(), "; "), r
}

// same is a file present unchanged on both sides.
func same(src string) [2]string { return [2]string{src, src} }

// TestPlan pins what a patch runs: only new or changed code, by test name.
// Mutations it catches: comparing bytes instead of comment-free tokens (the
// comment rows gain targets); dropping helper transitivity (the "through a
// helper" row loses TestViaHelper); running a struct's every user instead of
// its changed fields (the field row gains TestOther); keeping a //go:build
// measure file live (the measure row gains a target); following no renames
// (the moved rows run nothing or the package).
func TestPlan(t *testing.T) {
	f := files{
		// package a: shipped code and its tests.
		"pkg/a/a.go": {"package a\n// old\nfunc F() int { return 1 }\nfunc g() int { return 1 }\nfunc Outer() int { return deep() }\nfunc deep() int { return 1 }\ntype S struct {\n\tx int\n\ty int\n}\n",
			"package a\n// new\nfunc F() int { return 1 } // tail\nfunc g() int { return 2 }\nfunc Outer() int { return deep() }\nfunc deep() int { return 2 }\ntype S struct {\n\tx int\n\ty int\n\tz int\n}\n"},
		"pkg/a/a_test.go": same("package a\nimport \"testing\"\n" +
			"func TestG(t *testing.T) { _ = g() }\n" +
			"func helper() int { return g() }\n" +
			"func outerHelper() int { return helper() }\n" +
			"func TestViaHelper(t *testing.T) { _ = outerHelper() }\n" +
			"func TestF(t *testing.T) { _ = F() }\n" +
			"func TestOuter(t *testing.T) { _ = Outer() }\n" +
			"func TestZ(t *testing.T) { var s S; _ = s.z }\n" +
			"func TestOther(t *testing.T) { var s S; _ = s.x }\n"),
		"pkg/a/comment.go": {"package a\n// a\nfunc c() {}\n", "package a\n// b\nfunc c() {}\n"},
		// package b: changed tests and test helpers.
		"pkg/b/b_test.go": {"package b\nfunc TestB(t *testing.T) {}\nfunc TestKeep(t *testing.T) {}\nfunc h() int { return 1 }\nfunc TestH(t *testing.T) { h() }\n",
			"package b\nfunc TestB(t *testing.T) { t.Log() }\nfunc TestKeep(t *testing.T) {}\nfunc h() int { return 2 }\nfunc TestH(t *testing.T) { h() }\nfunc FuzzNew(f *testing.F) {}\n"},
		// build constraints.
		"pkg/c/m_measure_test.go": {"//go:build measure\n\npackage c\nfunc TestM(t *testing.T) {}\n", "//go:build measure\n\npackage c\nfunc TestM(t *testing.T) { t.Log() }\n"},
		"pkg/c/w_windows.go":      {"package c\nfunc w() int { return 1 }\n", "package c\nfunc w() int { return 2 }\n"},
		"pkg/c/up_test.go":        {"//go:build measure\n\npackage c\nfunc TestUp(t *testing.T) {}\n", "package c\nfunc TestUp(t *testing.T) {}\n"},
		// moves.
		"pkg/d/new_test.go":   {"", "package d\nfunc TestMoved(t *testing.T) {}\n"},
		"pkg/d/old_test.go":   {"package d\nfunc TestMoved(t *testing.T) {}\n", ""},
		"pkg/e/there_test.go": {"", "package e\nfunc TestAcross(t *testing.T) {}\n"},
		"pkg/d/gone_test.go":  {"package d\nfunc TestAcross(t *testing.T) {}\n", ""},
		"pkg/m/x.go":          {"package m\nfunc moved() int { return 1 }\n", "package m\n"},
		"pkg/m/y.go":          {"package m\n", "package m\nfunc moved() int { return 1 }\n"},
		"pkg/m/m_test.go":     same("package m\nfunc TestMovedFunc(t *testing.T) { moved() }\n"),
		// testdata.
		"pkg/t/t_test.go": same("package t\nfunc TestReadsGolden(t *testing.T) { load(\"golden.json\") }\nfunc TestNot(t *testing.T) {}\nfunc FuzzX(f *testing.F) {}\n"),
	}
	cases := []struct {
		name    string
		changes []change
		want    string
	}{
		{"empty", nil, ""},
		{"docs only", []change{{'M', "README.md", "README.md"}, {'D', "docs/x.md", "docs/x.md"}}, ""},
		{"comment-only code", []change{{'M', "pkg/a/comment.go", "pkg/a/comment.go"}}, ""},
		{"changed func: its direct and helper users",
			[]change{{'M', "pkg/a/a.go", "pkg/a/a.go"}},
			"race ./pkg/a/ ^(TestG|TestOuter|TestViaHelper|TestZ)$"},
		{"changed tests and a changed test helper", []change{{'M', "pkg/b/b_test.go", "pkg/b/b_test.go"}},
			"race ./pkg/b/ ^(FuzzNew|TestB|TestH)$"},
		{"a measure-tagged test is not live", []change{{'M', "pkg/c/m_measure_test.go", "pkg/c/m_measure_test.go"}}, ""},
		{"a windows file is not live here", []change{{'M', "pkg/c/w_windows.go", "pkg/c/w_windows.go"}}, ""},
		{"a test that becomes live runs", []change{{'M', "pkg/c/up_test.go", "pkg/c/up_test.go"}}, "race ./pkg/c/ ^(TestUp)$"},
		{"a renamed test file runs its tests by name", []change{{'R', "pkg/d/new_test.go", "pkg/d/old_test.go"}}, "race ./pkg/d/ ^(TestMoved)$"},
		{"a test moved across packages", []change{{'R', "pkg/e/there_test.go", "pkg/d/gone_test.go"}}, "race ./pkg/e/ ^(TestAcross)$"},
		{"an unchanged func moved between files runs nothing",
			[]change{{'M', "pkg/m/x.go", "pkg/m/x.go"}, {'M', "pkg/m/y.go", "pkg/m/y.go"}}, ""},
		{"a fuzz corpus entry runs its fuzz test", []change{{'A', "pkg/t/testdata/fuzz/FuzzX/abc", ""}}, "race ./pkg/t/ ^(FuzzX)$"},
		{"a fixture runs the tests naming it", []change{{'M', "pkg/t/testdata/golden.json", "pkg/t/testdata/golden.json"}}, "race ./pkg/t/ ^(TestReadsGolden)$"},
		{"an unnamed fixture runs the package's tests", []change{{'M', "pkg/t/testdata/other.bin", "pkg/t/testdata/other.bin"}},
			"race ./pkg/t/ ^(FuzzX|TestNot|TestReadsGolden)$"},
		{"a deleted test runs nothing", []change{{'D', "pkg/d/gone_test.go", "pkg/d/gone_test.go"}}, ""},
	}
	for _, c := range cases {
		got, r := f.plan(t, c.changes)
		if got != c.want {
			t.Errorf("%s: plan %q, want %q", c.name, got, c.want)
		}
		if len(r.minor) > 0 {
			t.Errorf("%s: minor %v", c.name, r.minor)
		}
	}
}

// TestPlanWidensToPackageCode: a changed function no test names runs the
// tests naming the package code that calls it, one level at a time, and never
// the whole package. Mutation: stopping at level 0 runs nothing.
func TestPlanWidensToPackageCode(t *testing.T) {
	f := files{
		"pkg/a/a.go": {"package a\nfunc Outer() int { return mid() }\nfunc mid() int { return deep() }\nfunc deep() int { return 1 }\nfunc Unrelated() {}\n",
			"package a\nfunc Outer() int { return mid() }\nfunc mid() int { return deep() }\nfunc deep() int { return 2 }\nfunc Unrelated() {}\n"},
		"pkg/a/a_test.go": same("package a\nfunc TestOuter(t *testing.T) { Outer() }\nfunc TestUnrelated(t *testing.T) { Unrelated() }\n"),
	}
	got, r := f.plan(t, []change{{'M', "pkg/a/a.go", "pkg/a/a.go"}})
	if got != "race ./pkg/a/ ^(TestOuter)$" {
		t.Errorf("plan %q", got)
	}
	if len(r.notes) != 1 || !strings.Contains(r.notes[0], "2 levels") {
		t.Errorf("notes %q", r.notes)
	}
}

// TestKind: rho's minor rules on top of gorelease. Mutations: dropping the
// FormatVersion check passes the marker rows; comparing badger's patch
// number refuses the patch-bump row; checking public packages refuses the
// public row (gorelease's job); not honouring the switch refuses the
// "ignored" row.
func TestKind(t *testing.T) {
	f := files{
		"pkg/s/wire.go":          {"package s\nconst AFormatVersion = 3\n", "package s\nconst AFormatVersion = 3\nconst BFormatVersion = 4\n"},
		"pkg/s/bump.go":          {"package s\nconst MaxFormatVersion = 3\n", "package s\nconst MaxFormatVersion = 4\n"},
		"pkg/s/s_test.go":        {"package s\n", "package s\nconst TestFormatVersion = 9\n"},
		"pkg/internal/i/i.go":    {"package i\nfunc F() {}\n", "package i\nfunc F() {}\nfunc G() {}\n"},
		"pkg/internal/i/meth.go": {"package i\ntype impl struct{}\n", "package i\ntype impl struct{}\nfunc (impl) Errorf() {}\n"},
		"pkg/internal/i/x.go":    {"package i\nfunc Moved() {}\n", "package i\n"},
		"pkg/internal/i/y.go":    {"package i\n", "package i\nfunc Moved() {}\n"},
		"pkg/pub/p.go":           {"package pub\n", "package pub\nfunc New() {}\n"},
		"scripts/t/main.go":      {"", "package main\nfunc Exported() {}\n"},
		"go.mod":                 {"module m\nrequire (\n\tgithub.com/dgraph-io/badger/v4 v4.9.2\n)\n", "module m\nrequire (\n\tgithub.com/dgraph-io/badger/v4 v4.10.0\n)\n"},
	}
	patchBump := files{"go.mod": {"module m\nrequire github.com/dgraph-io/badger/v4 v4.9.2\n", "module m\nrequire github.com/dgraph-io/badger/v4 v4.9.3\n"}}
	cases := []struct {
		name    string
		f       files
		changes []change
		want    string
	}{
		{"a new format marker", f, []change{{'M', "pkg/s/wire.go", "pkg/s/wire.go"}}, "pkg/s/wire.go: format marker BFormatVersion is new or changed; pkg/s/wire.go: BFormatVersion is a new exported name"},
		{"a raised format marker", f, []change{{'M', "pkg/s/bump.go", "pkg/s/bump.go"}}, "pkg/s/bump.go: format marker MaxFormatVersion is new or changed"},
		{"a test constant is no marker", f, []change{{'M', "pkg/s/s_test.go", "pkg/s/s_test.go"}}, ""},
		{"a new internal export", f, []change{{'M', "pkg/internal/i/i.go", "pkg/internal/i/i.go"}}, "pkg/internal/i/i.go: G is a new exported name"},
		{"a method of an unexported type is no API", f, []change{{'M', "pkg/internal/i/meth.go", "pkg/internal/i/meth.go"}}, ""},
		{"an export moved between files is no new API", f, []change{{'M', "pkg/internal/i/x.go", "pkg/internal/i/x.go"}, {'M', "pkg/internal/i/y.go", "pkg/internal/i/y.go"}}, ""},
		{"a new public export", f, []change{{'M', "pkg/pub/p.go", "pkg/pub/p.go"}}, "pkg/pub/p.go: New is a new exported name"},
		{"tools are not API", f, []change{{'A', "scripts/t/main.go", ""}}, ""},
		{"a badger minor bump", f, []change{{'M', "go.mod", "go.mod"}}, "go.mod: badger v4.9 -> v4.10 is a minor or major bump"},
		{"a badger patch bump", patchBump, []change{{'M', "go.mod", "go.mod"}}, ""},
	}
	for _, c := range cases {
		r := analyze(c.changes, c.f.side(0), c.f.side(1))
		if got := strings.Join(r.minor, "; "); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

// TestLive: build constraints decide what is code. Mutation: treating
// //go:build lines as comments (or as code) fails the measure row.
func TestLive(t *testing.T) {
	for _, c := range []struct {
		path, src string
		want      bool
	}{
		{"a.go", "package a\n", true},
		{"a_test.go", "//go:build measure\n\npackage a\n", false},
		{"a_test.go", "//go:build !measure\n\npackage a\n", true},
		{"a.go", "//go:build race\n\npackage a\n", true},
		{"a.go", "//go:build !race\n\npackage a\n", false},
		{"a.go", "// +build ignore\n\npackage a\n", false},
		{"a_windows.go", "package a\n", false},
		{"a_linux_amd64_test.go", "package a\n", true},
		{"a_plan9_test.go", "package a\n", false},
	} {
		if got := live(c.path, []byte(c.src)); got != c.want {
			t.Errorf("live(%s, %q) = %v, want %v", c.path, c.src, got, c.want)
		}
	}
}

// TestTestNamedScript: a named test that did not run fails the gate, though go
// test exits 0 on a regex matching nothing. Mutations: dropping the name check
// passes the "gone" row; dropping the rerun leaves the "short skip" row unrun.
func TestTestNamedScript(t *testing.T) {
	script, err := filepath.Abs("../test-named.sh")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	src := "package m\nimport (\"os\"; \"testing\")\n" +
		"func TestA(t *testing.T) {}\n" +
		"func TestLong(t *testing.T) { if testing.Short() { t.Skip() }; os.WriteFile(\"long.ran\", nil, 0o600) }\n" +
		"func TestSub(t *testing.T) { t.Run(\"TestInner\", func(t *testing.T) {}) }\n" +
		"func TestBad(t *testing.T) { t.Fail() }\n" +
		"func FuzzF(f *testing.F) { f.Fuzz(func(t *testing.T, b []byte) {}) }\n"
	for name, body := range map[string]string{"go.mod": "module m\n\ngo 1.21\n", "m_test.go": src} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	run := func(args ...string) error {
		cmd := exec.Command(script, args...) // #nosec G204 -- the repo's own script, fixed test arguments
		cmd.Dir = dir
		return cmd.Run()
	}
	for _, c := range []struct {
		name string
		args []string
		ok   bool
	}{
		{"ran", []string{"./", "TestA", "--"}, true},
		{"gone", []string{"./", "TestA|TestGone", "--"}, false},
		{"only a subtest of that name", []string{"./", "TestInner", "--", "-run", "TestSub"}, false},
		{"failing", []string{"./", "TestBad", "--"}, false},
		{"fuzz seed run", []string{"./", "FuzzF", "--"}, true},
		{"short skip", []string{"./", "TestLong", "--", "-short"}, true},
	} {
		if err := run(c.args...); (err == nil) != c.ok {
			t.Errorf("%s: err %v, want ok %v", c.name, err, c.ok)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "long.ran")); err != nil {
		t.Errorf("a test skipped under -short must rerun without it: %v", err)
	}
}

// TestGateTimeScript: a gate's cost is logged and its exit status survives.
// Mutation: exiting 0 fails the status check; not appending fails the line count.
func TestGateTimeScript(t *testing.T) {
	script, err := filepath.Abs("../gate-time.sh")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for _, args := range [][]string{{"init", "-q"}, {"-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "x"}} {
		cmd := exec.Command("git", args...) // #nosec G204 -- fixed arguments
		cmd.Dir, cmd.Env = dir, envWithoutGit()
		if err := cmd.Run(); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []struct {
		command string
		status  int
	}{{"true", 0}, {"false", 1}} {
		cmd := exec.Command(script, "g", c.command) // #nosec G204 -- the repo's own script, fixed arguments
		cmd.Dir, cmd.Env = dir, envWithoutGit()
		_ = cmd.Run()
		if got := cmd.ProcessState.ExitCode(); got != c.status {
			t.Errorf("%s: exit %d, want %d", c.command, got, c.status)
		}
	}
	log, err := os.ReadFile(filepath.Join(dir, ".git", "gate-times.log"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(log)), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], " g ") || !strings.Contains(lines[0], "exit=0") || !strings.Contains(lines[1], "exit=1") {
		t.Errorf("log %q", log)
	}
}

// envWithoutGit is the test's environment without GIT_* variables. Under the
// pre-commit hook git exports GIT_DIR (and GIT_INDEX_FILE): a git init in a temp
// dir would then write the committing repo's config (core.bare=true), and from a
// worktree that config is the main checkout's.
func envWithoutGit() []string {
	var env []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "GIT_") {
			env = append(env, kv)
		}
	}
	return env
}

func TestParseNameStatus(t *testing.T) {
	got := parseNameStatus("M\tpkg/a.go\nA\tdocs/x.md\nR100\told.go\tnew.go\nD\tgone.go\n")
	want := []change{{'M', "pkg/a.go", "pkg/a.go"}, {'A', "docs/x.md", ""}, {'R', "new.go", "old.go"}, {'D', "gone.go", "gone.go"}}
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
