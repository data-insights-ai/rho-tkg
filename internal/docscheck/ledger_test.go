package docscheck

import (
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// TestLedger: tasks/todo.md does not lag behind the repo. The tag is
// the version, no doc names it: docs/CHANGELOG.md's newest version heading is
// the latest tag or a newer release written before `make release` tags it;
// tasks/todo.md has its sections and holds current work only (shipped, later
// and parked work is docs/CHANGELOG.md's).
func TestLedger(t *testing.T) {
	t.Parallel()
	changelog := readRepo(t, "docs/CHANGELOG.md")
	todo := readRepo(t, "tasks/todo.md")

	top := regexp.MustCompile(`(?m)^## \[([0-9]+\.[0-9]+\.[0-9]+)\]`).FindStringSubmatch(changelog)
	if top == nil {
		t.Fatal("docs/CHANGELOG.md has no version heading")
	}
	if latest := latestTag(t); latest != "" && versionLess(top[1], latest) {
		t.Errorf("docs/CHANGELOG.md's newest heading [%s] is older than the latest tag v%s", top[1], latest)
	}

	for _, sec := range []string{"Now", "Next", "Waiting"} {
		if !strings.Contains(todo, "\n## "+sec+"\n") {
			t.Errorf("tasks/todo.md lacks a '## %s' section", sec)
		}
	}

	// A ticked step "N. [x]" of a plan in progress stays until the plan is finished.
	done := regexp.MustCompile(`(?i)✅|^- +\*\*[A-Z0-9-]+\*\* +— +(done|shipped|answered)\b|\b(done|shipped|answered)[.)]*$`)
	for _, line := range strings.Split(todo, "\n") {
		if done.MatchString(line) {
			t.Errorf("done item left in tasks/todo.md: %.90s", line)
		}
	}
}

func readRepo(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile("../../" + path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// latestTag is the highest vX.Y.Z tag, without the v, or "" when git lists
// none (a shallow checkout).
func latestTag(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("git", "tag", "--list", "v[0-9]*.[0-9]*.[0-9]*").Output()
	if err != nil {
		t.Skipf("git tag: %v", err)
	}
	latest := ""
	for _, tag := range strings.Fields(string(out)) {
		v := strings.TrimPrefix(tag, "v")
		if latest == "" || versionLess(latest, v) {
			latest = v
		}
	}
	return latest
}

// versionLess reports whether a sorts before b, both "X.Y.Z".
func versionLess(a, b string) bool {
	pa, pb := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < 3 && i < len(pa) && i < len(pb); i++ {
		x, _ := strconv.Atoi(pa[i])
		y, _ := strconv.Atoi(pb[i])
		if x != y {
			return x < y
		}
	}
	return false
}

func TestVersionLessOrdersNumerically(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		a, b string
		want bool
	}{{"4.9.9", "4.10.0", true}, {"4.105.3", "4.105.3", false}, {"5.0.0", "4.200.1", false}, {"4.105.2", "4.105.10", true}} {
		if got := versionLess(c.a, c.b); got != c.want {
			t.Errorf("versionLess(%s, %s) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

// TestDocSizes (RH 2026-11-07): tasks/todo.md stays short. The cap is the 7 Nov
// line count plus 10 %; a row longer than 400 characters is split or moved to
// the file that owns the detail. HANDOVER.md is gitignored local state.
func TestDocSizes(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		path     string
		maxLines int
	}{{"tasks/todo.md", 175}} {
		lines := strings.Split(strings.TrimRight(readRepo(t, c.path), "\n"), "\n")
		if len(lines) > c.maxLines {
			t.Errorf("%s has %d lines, cap %d: consolidate", c.path, len(lines), c.maxLines)
		}
		for i, l := range lines {
			if n := len([]rune(l)); n > 400 {
				t.Errorf("%s:%d is %d characters, cap 400", c.path, i+1, n)
			}
		}
	}
}
