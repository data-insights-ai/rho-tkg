package core

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestDocsMetadataMatchesSourceOfTruth(t *testing.T) {
	goMod, err := os.ReadFile("../../../../go.mod")
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	goMatch := regexp.MustCompile(`(?m)^go\s+([0-9]+\.[0-9]+\.[0-9]+)$`).FindStringSubmatch(string(goMod))
	if len(goMatch) != 2 {
		t.Fatalf("could not parse Go version from go.mod")
	}
	goVersion := goMatch[1]

	changelog, err := os.ReadFile("../../../../CHANGELOG.md")
	if err != nil {
		t.Fatalf("read CHANGELOG.md: %v", err)
	}
	versionMatch := regexp.MustCompile(`(?m)^## \[([0-9]+\.[0-9]+\.[0-9]+)\]`).FindStringSubmatch(string(changelog))
	if len(versionMatch) != 2 {
		t.Fatalf("could not parse current version from CHANGELOG.md")
	}
	currentVersion := versionMatch[1]

	checks := []struct {
		path string
		want []string
	}{
		{path: "../../../../README.md", want: []string{"**Go:** " + goVersion}},
		{path: "../../../../AGENTS.md", want: []string{"Go: " + goVersion, "Status: v" + currentVersion}},
		{path: "../../../../docs/architecture.md", want: []string{"(v" + currentVersion + ")", "Go:      " + goVersion}},
	}

	for _, check := range checks {
		body, err := os.ReadFile(check.path)
		if err != nil {
			t.Fatalf("read %s: %v", check.path, err)
		}
		text := string(body)
		for _, want := range check.want {
			if !strings.Contains(text, want) {
				t.Fatalf("%s missing %q", check.path, want)
			}
		}
	}
}
