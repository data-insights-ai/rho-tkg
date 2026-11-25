package docscheck

import (
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// TestLedger: the tracking does not lag behind the repo. The tag is the
// version, no doc names it: docs/CHANGELOG.md's newest version heading is the
// latest tag or a newer release written before `make release` tags it;
// tasks/index.md lists open items only, each row's key is one item header in
// the topic file the row names, and each header there has its row.
func TestLedger(t *testing.T) {
	t.Parallel()
	changelog := readRepo(t, "docs/CHANGELOG.md")
	index := readRepo(t, "tasks/index.md")

	top := regexp.MustCompile(`(?m)^## \[([0-9]+\.[0-9]+\.[0-9]+)\]`).FindStringSubmatch(changelog)
	if top == nil {
		t.Fatal("docs/CHANGELOG.md has no version heading")
	}
	if latest := latestTag(t); latest != "" && versionLess(top[1], latest) {
		t.Errorf("docs/CHANGELOG.md's newest heading [%s] is older than the latest tag v%s", top[1], latest)
	}

	if !strings.Contains(index, "\n## Items\n") {
		t.Error("tasks/index.md lacks its '## Items' section")
	}
	for _, f := range indexFaults(index, func(file string) string { return readRepo(t, "tasks/"+file) }) {
		t.Error(f)
	}
}

var (
	indexRow    = regexp.MustCompile(`(?m)^\| ([A-Z]+-[0-9]+) \|.*\]\(([a-z0-9-]+\.md)\) \|$`)
	itemHeader  = regexp.MustCompile(`(?m)^### ([A-Z]+-[0-9]+) `)
	doneInIndex = regexp.MustCompile(`(?i)✅|\| +(done|shipped|answered) +\|`)
)

// indexFaults names every index row that is done or has no header in its topic
// file, and every topic header without an index row. read returns a topic file.
func indexFaults(index string, read func(file string) string) []string {
	var out []string
	topics := map[string]map[string]bool{}
	var files []string
	for _, m := range indexRow.FindAllStringSubmatch(index, -1) {
		if doneInIndex.MatchString(m[0]) {
			out = append(out, "done item left in tasks/index.md: "+m[1])
		}
		if topics[m[2]] == nil {
			topics[m[2]] = map[string]bool{}
			files = append(files, m[2])
			for _, h := range itemHeader.FindAllStringSubmatch(read(m[2]), -1) {
				topics[m[2]][h[1]] = false
			}
		}
		if _, ok := topics[m[2]][m[1]]; !ok {
			out = append(out, m[1]+": no item header in tasks/"+m[2])
		}
		topics[m[2]][m[1]] = true
	}
	for _, file := range files {
		for key, listed := range topics[file] {
			if !listed {
				out = append(out, "tasks/"+file+": item "+key+" has no row in tasks/index.md")
			}
		}
	}
	return out
}

func TestIndexFaultsNamesEachGap(t *testing.T) {
	t.Parallel()
	topic := "### RHO-1 a\n\n### RHO-3 c\n"
	read := func(string) string { return topic }
	index := "| RHO-1 | a | idea | later | - | [t](t.md) |\n| RHO-2 | b | idea | later | - | [t](t.md) |\n"
	got := strings.Join(indexFaults(index, read), "; ")
	for _, want := range []string{"RHO-2: no item header", "item RHO-3 has no row"} {
		if !strings.Contains(got, want) {
			t.Errorf("faults %q lack %q", got, want)
		}
	}
	if strings.Contains(got, "RHO-1") {
		t.Errorf("a row with its header is no fault: %q", got)
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
// none (a shallow checkout) or cannot run. It never skips: the caller's other
// checks (the index and its topic files) do not depend on git.
func latestTag(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("git", "tag", "--list", "v[0-9]*.[0-9]*.[0-9]*").Output()
	if err != nil {
		t.Logf("git tag: %v; checking without the latest tag", err)
		return ""
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
