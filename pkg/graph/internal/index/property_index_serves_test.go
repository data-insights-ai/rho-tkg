package index

import (
	"math"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// TestPropertyIndexDeclinesWhileBuilding (round 10 review, pattern 4): an
// index being built (Mutated set) answers none of the doors that read it as a
// whole — the ordered walks and their value pages, the range and equality
// counts, the bounds, the value statistics — so a composite store never
// merges a part's partial entries; once built they answer.
func TestPropertyIndexDeclinesWhileBuilding(t *testing.T) {
	t.Parallel()
	for _, rangeCounts := range []bool{false, true} {
		pi := NewPropertyIndexWith(rangeCounts, false)
		pi.Add(snowflake.ID(1), int64(3))
		pi.Add(snowflake.ID(2), "ab")
		pi.Mutated = map[snowflake.ID]struct{}{}
		if _, _, _, ok := pi.RangeOrderedPage(math.Inf(-1), math.Inf(1), false, RangeOrderedCursor{}, 10); ok {
			t.Fatal("RangeOrderedPage answered while building")
		}
		if _, _, _, _, ok := pi.RangeOrderedValuePage(math.Inf(-1), math.Inf(1), true, true, false, RangeOrderedCursor{}, 10); ok {
			t.Fatal("RangeOrderedValuePage answered while building")
		}
		if _, _, _, ok := pi.PrefixOrderedPage("a", false, StrOrderedCursor{}, 10); ok {
			t.Fatal("PrefixOrderedPage answered while building")
		}
		if _, _, _, _, ok := pi.PrefixOrderedValuePage("a", false, StrOrderedCursor{}, 10); ok {
			t.Fatal("PrefixOrderedValuePage answered while building")
		}
		if _, ok := pi.RangeCardinality(0, 10, true, true); ok {
			t.Fatal("RangeCardinality answered while building")
		}
		if _, ok := pi.EqualCount(int64(3)); ok {
			t.Fatal("EqualCount answered while building")
		}
		if _, _, _, _, _, ok := pi.NumericBounds(); ok {
			t.Fatal("NumericBounds answered while building")
		}
		if _, ok := pi.ValueStats(); ok {
			t.Fatal("ValueStats answered while building")
		}
		if _, ok := pi.RangeNodeIDs(0, 10, true, true); ok {
			t.Fatal("RangeNodeIDs answered while building")
		}
		if _, ok := pi.RangeRelIDs(0, 10, true, true); ok {
			t.Fatal("RangeRelIDs answered while building")
		}
		if _, ok := pi.PrefixNodeIDs("a"); ok {
			t.Fatal("PrefixNodeIDs answered while building")
		}
		pi.Mutated = nil
		if ids, _, _, ok := pi.RangeOrderedPage(math.Inf(-1), math.Inf(1), false, RangeOrderedCursor{}, 10); !ok || len(ids) != 1 {
			t.Fatalf("built: RangeOrderedPage %v %v", ids, ok)
		}
		if ids, _, _, ok := pi.PrefixOrderedPage("a", false, StrOrderedCursor{}, 10); !ok || len(ids) != 1 {
			t.Fatalf("built: PrefixOrderedPage %v %v", ids, ok)
		}
		if ids, ok := pi.RangeNodeIDs(0, 10, true, true); !ok || len(ids) != 1 {
			t.Fatalf("built: RangeNodeIDs %v %v", ids, ok)
		}
		if ids, ok := pi.RangeRelIDs(0, 10, true, true); !ok || len(ids) != 1 {
			t.Fatalf("built: RangeRelIDs %v %v", ids, ok)
		}
		if ids, ok := pi.PrefixNodeIDs("a"); !ok || len(ids) != 1 {
			t.Fatalf("built: PrefixNodeIDs %v %v", ids, ok)
		}
		if n, ok := pi.RangeCardinality(0, 10, true, true); !ok || n != 1 {
			t.Fatalf("built: RangeCardinality %d %v", n, ok)
		}
	}
}
