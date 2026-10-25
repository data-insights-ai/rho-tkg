package badger

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestNoIteratorPrefetchesValues guards docs/badger.md rule 3 (PREFETCH): rho's
// values are inline, so a prefetching iterator only starts a goroutine per row
// (iterator.go:707-713). No iterator of the package turns prefetch on, and none
// takes badger's DefaultIteratorOptions (PrefetchValues true) unchanged.
func TestNoIteratorPrefetchesValues(t *testing.T) {
	t.Parallel()
	on := regexp.MustCompile(`PrefetchValues\s*(=|:)\s*(true|[a-zA-Z_][\w.]*\s*(==|!=))`)
	bare := regexp.MustCompile(`NewIterator\(badgerv4\.DefaultIteratorOptions\)`)
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f) // #nosec G304 -- the package's own sources
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(src), "\n") {
			if on.MatchString(line) || bare.MatchString(line) {
				t.Errorf("%s:%d prefetches values: %s", f, i+1, strings.TrimSpace(line))
			}
		}
	}
}
