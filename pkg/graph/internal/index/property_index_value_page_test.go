package index

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// TestOrderedValuePagesMatchBruteForce (sigma-tkgd round 7 request 5): paged
// in sizes 1, 3 and 1,000, both directions, the value pages return exactly the
// stored (ID, value) pairs inside the exact bounds (or with the prefix), in
// value order, ties by ID ascending, each value in its stored type; an index
// holding an integer past 2^53, an Elements index and an index being built
// decline.
func TestOrderedValuePagesMatchBruteForce(t *testing.T) {
	t.Parallel()
	stored := map[snowflake.ID]any{}
	pi := NewPropertyIndex()
	put := func(id snowflake.ID, v any) { stored[id] = v; pi.Add(id, v) }
	vals := []any{int64(3), 3.0, int8(3), uint16(3), float32(2.5), 2.5, int64(-7), -0.5, int(10), uint64(10), 1e9, float32(0.1), 0.1, int64(5), int32(5), math.Inf(1), math.Inf(-1), math.NaN(), "apple", "apricot", "banana", "app", "", true}
	for i := range 60 {
		put(snowflake.ID(1000-i*7), vals[i%len(vals)])
	}
	type pair struct {
		id snowflake.ID
		v  any
	}
	render := func(ps []pair) string {
		var b strings.Builder
		for _, p := range ps {
			fmt.Fprintf(&b, "%d=%T(%v) ", p.id, p.v, p.v)
		}
		return b.String()
	}
	sortKey := func(v any) (float64, bool) {
		k, ok := numericSortKey(PropertyValueKey(v))
		return k, ok
	}
	for _, bounds := range []struct {
		lo, hi         float64
		inclLo, inclHi bool
	}{{3, 10, true, true}, {3, 10, false, false}, {math.Inf(-1), math.Inf(1), true, true}, {-0.5, 2.5, false, true}, {0.1, 0.1, true, true}, {4, 4, true, true}} {
		for _, desc := range []bool{false, true} {
			var want []pair
			for id, v := range stored {
				k, ok := sortKey(v)
				if !ok || k < bounds.lo || (k == bounds.lo && !bounds.inclLo) || k > bounds.hi || (k == bounds.hi && !bounds.inclHi) {
					continue
				}
				want = append(want, pair{id, v})
			}
			slices.SortFunc(want, func(a, b pair) int {
				ka, _ := sortKey(a.v)
				kb, _ := sortKey(b.v)
				if c := cmp.Compare(ka, kb); c != 0 {
					if desc {
						return -c
					}
					return c
				}
				return cmp.Compare(a.id, b.id)
			})
			for _, limit := range []int{1, 3, 1000} {
				var got []pair
				var cur RangeOrderedCursor
				for {
					ids, vs, next, done, ok := pi.RangeOrderedValuePage(bounds.lo, bounds.hi, bounds.inclLo, bounds.inclHi, desc, cur, limit)
					if !ok {
						t.Fatal("declined")
					}
					for j := range ids {
						got = append(got, pair{ids[j], vs[j]})
					}
					if done {
						break
					}
					cur = next
				}
				if render(got) != render(want) {
					t.Fatalf("%+v desc=%v limit=%d:\n got  %s\n want %s", bounds, desc, limit, render(got), render(want))
				}
			}
		}
	}
	for _, prefix := range []string{"ap", "", "b", "z"} {
		for _, desc := range []bool{false, true} {
			var want []pair
			for id, v := range stored {
				if s, ok := v.(string); ok && strings.HasPrefix(s, prefix) {
					want = append(want, pair{id, v})
				}
			}
			slices.SortFunc(want, func(a, b pair) int {
				c := strings.Compare(a.v.(string), b.v.(string))
				if desc {
					c = -c
				}
				if c != 0 {
					return c
				}
				return cmp.Compare(a.id, b.id)
			})
			for _, limit := range []int{1, 1000} {
				var got []pair
				var cur StrOrderedCursor
				for {
					ids, vs, next, done, ok := pi.PrefixOrderedValuePage(prefix, desc, cur, limit)
					if !ok {
						t.Fatal("prefix declined")
					}
					for j := range ids {
						got = append(got, pair{ids[j], vs[j]})
					}
					if done {
						break
					}
					cur = next
				}
				if render(got) != render(want) {
					t.Fatalf("prefix %q desc=%v limit=%d:\n got  %s\n want %s", prefix, desc, limit, render(got), render(want))
				}
			}
		}
	}

	pi.Mutated = map[snowflake.ID]struct{}{}
	if _, _, _, _, ok := pi.RangeOrderedValuePage(0, 1, true, true, false, RangeOrderedCursor{}, 10); ok {
		t.Error("index being built: range page supported")
	}
	if _, _, _, _, ok := pi.PrefixOrderedValuePage("a", false, StrOrderedCursor{}, 10); ok {
		t.Error("index being built: prefix page supported")
	}
	pi.Mutated = nil
	put(1, int64(1<<53+1))
	if _, _, _, _, ok := pi.RangeOrderedValuePage(0, 1, true, true, false, RangeOrderedCursor{}, 10); ok {
		t.Error("integer past 2^53: supported")
	}
	elements := NewPropertyIndexWith(false, true)
	if _, _, _, _, ok := elements.RangeOrderedValuePage(0, 1, true, true, false, RangeOrderedCursor{}, 10); ok {
		t.Error("Elements index: range page supported")
	}
	if _, _, _, _, ok := elements.PrefixOrderedValuePage("", false, StrOrderedCursor{}, 10); ok {
		t.Error("Elements index: prefix page supported")
	}
}
