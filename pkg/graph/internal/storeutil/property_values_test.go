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

func TestHasAnyIndexKey(t *testing.T) {
	t.Parallel()
	n := types.NewNode(1, 1, nil)
	if err := n.SetProperty("k", []any{int64(3), "x"}); err != nil {
		t.Fatal(err)
	}
	r := types.NewRelationship(2, 1, 1, 1)
	if err := r.SetProperty("k", 3.0); err != nil {
		t.Fatal(err)
	}
	if !NodeHasAnyIndexKey(n, "k", true, []string{"f64:3", "s:x"}) || NodeHasAnyIndexKey(n, "k", false, []string{"i64:3"}) {
		t.Error("node: Elements list match or plain mismatch wrong")
	}
	if NodeHasAnyIndexKey(n, "k", true, nil) || NodeHasAnyIndexKey(n, "other", true, []string{"s:x"}) {
		t.Error("node: no keys or a missing property must not match")
	}
	if !RelHasAnyIndexKey(r, "k", false, []string{"i64:3", "f64:3"}) || RelHasAnyIndexKey(r, "k", false, []string{"i64:3"}) {
		t.Error("rel: match wrong")
	}
}
