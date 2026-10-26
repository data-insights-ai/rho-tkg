package storeutil

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestDistinctIndexKeys(t *testing.T) {
	t.Parallel()
	clean := []string{"i64:3", "f64:3"}
	if got := DistinctIndexKeys(clean); !slices.Equal(got, clean) || &got[0] != &clean[0] {
		t.Errorf("clean keys: %v (copied: %v), want the input itself", got, &got[0] != &clean[0])
	}
	if got := DistinctIndexKeys([]string{"a", "", "b", "a", ""}); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("repeats and empty: %v", got)
	}
	if got := DistinctIndexKeys(nil); len(got) != 0 {
		t.Errorf("nil: %v", got)
	}
	if got := DistinctIndexKeys([]string{""}); len(got) != 0 {
		t.Errorf("only the empty key: %v", got)
	}
	// Past the scan bound the set path: same answer.
	var many, want []string
	for i := range 3 * distinctKeysByScan {
		many = append(many, fmt.Sprintf("s:%d", i%distinctKeysByScan), "")
	}
	for i := range distinctKeysByScan {
		want = append(want, fmt.Sprintf("s:%d", i))
	}
	if got := DistinctIndexKeys(many); !slices.Equal(got, want) {
		t.Errorf("many keys: %v, want %v", got, want)
	}
}

func TestValueIndexKeys(t *testing.T) {
	t.Parallel()
	got, err := ValueIndexKeys(int64(3), "door")
	if err != nil || !slices.Equal(got, []string{"i64:3"}) {
		t.Errorf("int64(3): %v %v", got, err)
	}
	got, err = ValueIndexKeys([]any{int64(1)}, "door")
	if err != nil || !slices.Equal(got, []string{""}) {
		t.Errorf("a list: %v %v, want the empty key", got, err)
	}
	if _, err := ValueIndexKeys(struct{}{}, "door"); !errors.Is(err, types.ErrUnsupportedValueType) {
		t.Errorf("unsupported value: %v", err)
	}
}

// TestIndexKeyMatch checks a prepared key list against node and relationship
// rows both ways it answers: a scan of up to distinctKeysByScan keys and a set
// above (padded with keys no row carries). Catches a set lookup of the wrong
// key form, an Elements list matched as a scalar, and a missing property or an
// empty list that matches.
func TestIndexKeyMatch(t *testing.T) {
	t.Parallel()
	n := types.NewNode(1, 1, nil)
	if err := n.SetProperty("k", []any{int64(3), "x"}); err != nil {
		t.Fatal(err)
	}
	r := types.NewRelationship(2, 1, 1, 1)
	if err := r.SetProperty("k", 3.0); err != nil {
		t.Fatal(err)
	}
	for _, padded := range []bool{false, true} {
		keys := func(k ...string) IndexKeyMatch {
			if padded {
				for i := range distinctKeysByScan {
					k = append(k, fmt.Sprintf("s:pad%d", i))
				}
			}
			return NewIndexKeyMatch(k)
		}
		if got := keys("f64:3", "s:x"); (got.set != nil) != padded {
			t.Fatalf("padded=%v: set mode %v", padded, got.set != nil)
		}
		if !keys("f64:3", "s:x").Node(n, "k", true) || keys("i64:3").Node(n, "k", false) {
			t.Errorf("padded=%v: node: Elements list match or plain mismatch wrong", padded)
		}
		if keys().Node(n, "k", true) || keys("s:x").Node(n, "other", true) {
			t.Errorf("padded=%v: node: no keys or a missing property must not match", padded)
		}
		if !keys("i64:3", "f64:3").Rel(r, "k", false) || keys("i64:3").Rel(r, "k", false) {
			t.Errorf("padded=%v: rel: match wrong", padded)
		}
	}
}
