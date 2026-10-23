package core

// The documentation pin: every MD file in the repository is one README.md "Documentation" names, so a new MD file
// fails here instead of joining the hundreds that piled up (René 2026-10-22: "do not create new md files on your own"; todo row 110).

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var docPathRE = regexp.MustCompile("`([^`\\s]+\\.md)`")

// documentedMD returns the MD paths named in backticks in README.md's "## Documentation" section.
func documentedMD(readme string) (map[string]bool, error) {
	start := strings.Index(readme, "\n## Documentation\n")
	if start < 0 {
		return nil, errors.New(`README.md has no "## Documentation" section`)
	}
	section := readme[start+len("\n## Documentation\n"):]
	if end := strings.Index(section, "\n## "); end >= 0 {
		section = section[:end]
	}
	allowed := map[string]bool{}
	for _, m := range docPathRE.FindAllStringSubmatch(section, -1) {
		p := m[1]
		if filepath.IsAbs(p) || strings.HasPrefix(filepath.Clean(p), "..") {
			return nil, errors.New("documentation path outside the repository: " + p)
		}
		if allowed[p] {
			return nil, errors.New("documentation path listed twice: " + p)
		}
		allowed[p] = true
	}
	if len(allowed) == 0 {
		return nil, errors.New(`README.md "Documentation" names no MD file`)
	}
	return allowed, nil
}

// strayMD returns, sorted, the MD files that the documentation does not name.
func strayMD(files []string, allowed map[string]bool) []string {
	var stray []string
	for _, f := range files {
		if strings.HasSuffix(f, ".md") && !allowed[f] {
			stray = append(stray, f)
		}
	}
	sort.Strings(stray)
	return stray
}

func TestDocumentedMDParsesTheSection(t *testing.T) {
	t.Parallel()
	ok := "# x\n\n## Documentation\n\n| `README.md` | a |\n| `doc/A.md`, `doc/B.md` | b |\n\n## Next\n`doc/C.md`\n"
	cases := []struct {
		name, readme string
		want         []string
		err          bool
	}{
		{"table rows, section ends at the next heading", ok, []string{"README.md", "doc/A.md", "doc/B.md"}, false},
		{"no section", "# x\n\n## Read further\n`doc/A.md`\n", nil, true},
		{"empty section", "# x\n\n## Documentation\n\nnothing\n", nil, true},
		{"path outside the repo", "## x\n\n## Documentation\n`../other/A.md`\n", nil, true},
		{"absolute path", "## x\n\n## Documentation\n`/etc/A.md`\n", nil, true},
		{"duplicate row", "## x\n\n## Documentation\n`doc/A.md` `doc/A.md`\n", nil, true},
		{"a non-MD name is not a row", "## x\n\n## Documentation\n`paper/main.tex` `doc/A.md`\n", []string{"doc/A.md"}, false},
	}
	for _, c := range cases {
		got, err := documentedMD(c.readme)
		if (err != nil) != c.err {
			t.Errorf("%s: err %v, want error %v", c.name, err, c.err)
			continue
		}
		var keys []string
		for k := range got {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		if strings.Join(keys, ",") != strings.Join(c.want, ",") {
			t.Errorf("%s: got %v, want %v", c.name, keys, c.want)
		}
	}
}

func TestStrayMDNamesOnlyUnlistedMDFiles(t *testing.T) {
	t.Parallel()
	allowed := map[string]bool{"README.md": true, "doc/A.md": true}
	files := []string{"doc/Z.md", "README.md", "main.go", "doc/A.md", "tasks/x.md", "doc/A.md.txt"}
	if got := strings.Join(strayMD(files, allowed), ","); got != "doc/Z.md,tasks/x.md" {
		t.Errorf("stray %q, want doc/Z.md,tasks/x.md", got)
	}
	if got := strayMD(nil, allowed); len(got) != 0 {
		t.Errorf("no files: stray %v", got)
	}
}

func TestNoMDFileOutsideTheDocumentation(t *testing.T) {
	t.Parallel()
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	readme, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	allowed, err := documentedMD(string(readme))
	if err != nil {
		t.Fatal(err)
	}
	// tracked and untracked-but-not-ignored files: a new MD file fails before it is committed
	out, err := exec.Command("git", "-C", root, "ls-files", "-z", "-co", "--exclude-standard", "--", "*.md").Output()
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}
	files := strings.Split(strings.TrimRight(string(out), "\x00"), "\x00")
	for _, f := range strayMD(files, allowed) {
		t.Errorf("%s: not in README.md \"Documentation\"; put its facts into the owner listed there", f)
	}
	for p := range allowed {
		if _, err := os.Stat(filepath.Join(root, p)); err != nil {
			t.Errorf("README.md \"Documentation\" names %s, which does not exist", p)
		}
	}
}
