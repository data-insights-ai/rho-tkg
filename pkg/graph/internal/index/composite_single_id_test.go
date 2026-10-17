package index

import (
	"slices"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// Row 66: a composite value key held by exactly one entity keeps that ID
// inline; the second ID promotes the key to a posting list, a removal back
// to one demotes it. Every reader sees the same IDs through Postings.
func TestCompositeSingleIDPromoteDemote(t *testing.T) {
	t.Parallel()
	ids := func(ci *CompositePropertyIndex, vk string) []snowflake.ID {
		p := ci.Postings(vk)
		got := p.AppendAfter(nil, 0, 0)
		if p.Len() != len(got) {
			t.Fatalf("%q: Len %d, walk %v", vk, p.Len(), got)
		}
		return got
	}
	want := func(ci *CompositePropertyIndex, vk string, w ...snowflake.ID) {
		t.Helper()
		if got := ids(ci, vk); !slices.Equal(got, w) {
			t.Fatalf("%q = %v, want %v", vk, got, w)
		}
	}
	ci := NewCompositePropertyIndex([]string{"a", "b"})
	want(ci, "k")    // 0 IDs
	want(ci, "")     // empty key: nothing
	ci.AddKey(9, "") // empty key ignored
	want(ci, "")
	ci.removeKey(9, "k") // remove from an absent key
	want(ci, "k")
	ci.AddKey(5, "k")
	want(ci, "k", 5)  // 1 ID
	ci.AddKey(5, "k") // duplicate
	want(ci, "k", 5)
	ci.removeKey(7, "k") // remove an ID the key does not hold
	want(ci, "k", 5)
	ci.AddKey(3, "k") // second ID, lower: order reversed
	want(ci, "k", 3, 5)
	if got := ci.Postings("k").AppendAfter(nil, 3, 0); !slices.Equal(got, []snowflake.ID{5}) {
		t.Fatalf("After(3) = %v", got)
	}
	if got := ci.Postings("k").AppendAfter(nil, 0, 1); !slices.Equal(got, []snowflake.ID{3}) {
		t.Fatalf("limit 1 = %v", got)
	}
	ci.AddKey(3, "k") // duplicate in the list
	want(ci, "k", 3, 5)
	ci.removeKey(5, "k") // back to one
	want(ci, "k", 3)
	ci.AddKey(8, "k") // promote again after a demotion
	want(ci, "k", 3, 8)
	ci.removeKey(3, "k")
	ci.removeKey(8, "k") // drained
	want(ci, "k")
	ci.removeKey(8, "k")
	want(ci, "k")

	// Two keys, one ID each, and a third key with two: purge drops the ID
	// from every key and demotes the pair.
	ci.AddKey(1, "x")
	ci.AddKey(2, "y")
	ci.AddKey(1, "z")
	ci.AddKey(2, "z")
	PurgeNodeFromAllCompositeIndexes(map[CompositeIndexKey]*CompositePropertyIndex{{LabelToken: 1, Keys: "ab"}: ci}, 1)
	want(ci, "x")
	want(ci, "y", 2)
	want(ci, "z", 2)
	ci.AddKey(1, "z")
	want(ci, "z", 1, 2)

	// Mutated tracking marks every touched ID on both shapes.
	ci.Mutated = map[snowflake.ID]struct{}{}
	ci.AddKey(40, "m")
	ci.AddKey(41, "m")
	ci.removeKey(40, "m")
	for _, id := range []snowflake.ID{40, 41} {
		if _, ok := ci.Mutated[id]; !ok {
			t.Fatalf("Mutated lacks %d", id)
		}
	}
}
