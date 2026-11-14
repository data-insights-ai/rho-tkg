// Optional builder check: go vet and go test -short -p 4 on packages reached
// since origin/main (make presubmit). Not in gate or release.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func presubmitRun() int {
	pkgs, err := presubmitAffected()
	if err != nil {
		fmt.Println("presubmit:", err)
		return 1
	}
	if len(pkgs) == 0 {
		fmt.Println("presubmit: no Go package reached by the change")
		return 0
	}
	fmt.Printf("presubmit: %d packages reached\n", len(pkgs))
	env := append(os.Environ(), "GOFLAGS=-trimpath")
	for _, args := range [][]string{
		append([]string{"vet"}, pkgs...),
		append([]string{"test", "-short", "-p", "4", "-timeout", "20m"}, pkgs...),
	} {
		c := exec.Command("go", args...) // #nosec G204 -- go with package paths from go list
		c.Env, c.Stdout, c.Stderr = env, os.Stdout, os.Stderr
		if err := c.Run(); err != nil {
			return 1
		}
	}
	return 0
}

func presubmitAffected() ([]string, error) {
	base, err := git("merge-base", "HEAD", remote()+"/main")
	if err != nil {
		return nil, fmt.Errorf("merge-base: %s", base)
	}
	var files []string
	for _, args := range [][]string{
		{"diff", "--name-only", base},
		{"ls-files", "--others", "--exclude-standard"},
	} {
		out, err := git(args...)
		if err != nil {
			return nil, fmt.Errorf("git %s: %s", args[0], out)
		}
		files = append(files, strings.Fields(out)...)
	}
	mod, err := exec.Command("go", "list", "-m").Output()
	if err != nil {
		return nil, err
	}
	module := strings.TrimSpace(string(mod))
	all := false
	changed := map[string]bool{}
	for _, f := range files {
		if f == "go.mod" || f == "go.sum" {
			all = true
		}
		if i := strings.Index(f, "/testdata/"); i >= 0 {
			f = f[:i] + "/x.go"
		} else if strings.HasPrefix(f, "testdata/") {
			f = "x.go"
		}
		if !strings.HasSuffix(f, ".go") {
			continue
		}
		dir := module
		if i := strings.LastIndex(f, "/"); i >= 0 {
			dir += "/" + f[:i]
		}
		changed[dir] = true
	}
	out, err := exec.Command("go", "list", "-test", "-f", "{{.ImportPath}}|{{join .Deps \" \"}}", "./...").Output()
	if err != nil {
		return nil, fmt.Errorf("go list: %v", err)
	}
	seen := map[string]bool{}
	var pkgs []string
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		path, deps, _ := strings.Cut(line, "|")
		if strings.Contains(path, " ") || strings.HasSuffix(path, "_test") {
			continue
		}
		pkg := strings.TrimSuffix(path, ".test")
		if seen[pkg] {
			continue
		}
		hit := all || changed[pkg]
		for d := range strings.FieldsSeq(deps) {
			if hit {
				break
			}
			hit = changed[d]
		}
		if hit {
			seen[pkg] = true
			pkgs = append(pkgs, pkg)
		}
	}
	return pkgs, nil
}