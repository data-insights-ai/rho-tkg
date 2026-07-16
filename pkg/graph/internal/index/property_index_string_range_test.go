package index

import (
	"sort"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func idset(ids []types.NodeID) []uint64 {
	out := make([]uint64, len(ids))
	for i, id := range ids {
		out[i] = uint64(id.SnowflakeID())
	}
	sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
	return out
}

func eqU64(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// collectPrefixOrdered drains PrefixOrderedPage across pages (pageLimit at a time)
// and returns the full ordered id sequence — exercising the cursor threading.
func collectPrefixOrdered(pi *PropertyIndex, prefix string, desc bool, pageLimit int) []uint64 {
	var out []uint64
	var cur StrOrderedCursor
	for {
		ids, next, done, ok := pi.PrefixOrderedPage(prefix, desc, cur, pageLimit)
		if !ok {
			return nil
		}
		for _, id := range ids {
			out = append(out, uint64(id))
		}
		if done {
			return out
		}
		cur = next
	}
}

// TestPropertyIndexStringPrefix covers the ordered string view: prefix membership
// (PrefixNodeIDs), contractual lex ordering asc/desc with ID tie-break
// (PrefixOrderedPage), empty/no-match prefixes, boundary correctness (a value
// equal to the prefix successor must NOT match), maintenance (remove/purge), and
// that non-string values never enter the string view.
func TestPropertyIndexStringPrefix(t *testing.T) {
	t.Parallel()
	pi := NewPropertyIndex()
	// ids chosen so tie-break (ascending id) is observable: two "apple"s.
	pi.Add(snowflake.ID(10), "app")
	pi.Add(snowflake.ID(20), "apple")
	pi.Add(snowflake.ID(5), "apple") // tie on value "apple", smaller id
	pi.Add(snowflake.ID(30), "apricot")
	pi.Add(snowflake.ID(40), "banana")
	pi.Add(snowflake.ID(50), "aq")      // boundary: "aq" is exactly prefixSuccessor("ap")
	pi.Add(snowflake.ID(60), int64(99)) // non-string: must be invisible to string view

	t.Run("PrefixNodeIDs_ap", func(t *testing.T) {
		got, ok := pi.PrefixNodeIDs("ap")
		if !ok {
			t.Fatal("supported=false")
		}
		// "app","apple"×2,"apricot" match; "aq"(==successor),"banana",99 do not.
		if want := []uint64{5, 10, 20, 30}; !eqU64(idset(got), want) {
			t.Errorf("PrefixNodeIDs(ap) = %v, want %v", idset(got), want)
		}
	})

	t.Run("PrefixNodeIDs_app", func(t *testing.T) {
		got, _ := pi.PrefixNodeIDs("app")
		if want := []uint64{5, 10, 20}; !eqU64(idset(got), want) { // app, apple×2
			t.Errorf("PrefixNodeIDs(app) = %v, want %v", idset(got), want)
		}
	})

	t.Run("ordered_asc", func(t *testing.T) {
		// lex order: app(10), apple(5), apple(20), apricot(30)
		got := collectPrefixOrdered(pi, "ap", false, 2) // page size 2 to force cursor threading
		if want := []uint64{10, 5, 20, 30}; !eqU64(got, want) {
			t.Errorf("asc = %v, want %v (lex value, id tie-break)", got, want)
		}
	})

	t.Run("ordered_desc", func(t *testing.T) {
		// desc lex: apricot(30), apple(5,20 — TIES STILL ID ASC), app(10)
		got := collectPrefixOrdered(pi, "ap", true, 2)
		if want := []uint64{30, 5, 20, 10}; !eqU64(got, want) {
			t.Errorf("desc = %v, want %v (desc value, id tie-break still asc)", got, want)
		}
	})

	t.Run("empty_prefix_matches_all_strings", func(t *testing.T) {
		got, _ := pi.PrefixNodeIDs("")
		// every string value: app,apple×2,apricot,aq,banana — NOT the int64.
		if want := []uint64{5, 10, 20, 30, 40, 50}; !eqU64(idset(got), want) {
			t.Errorf("PrefixNodeIDs('') = %v, want %v", idset(got), want)
		}
	})

	t.Run("no_match", func(t *testing.T) {
		got, ok := pi.PrefixNodeIDs("zzz")
		if !ok || len(got) != 0 {
			t.Errorf("PrefixNodeIDs(zzz) = %v ok=%v, want empty", idset(got), ok)
		}
	})

	t.Run("boundary_successor_excluded", func(t *testing.T) {
		// "aq" == prefixSuccessor("ap") must be excluded from the "ap" prefix but
		// included in its own "aq" prefix.
		got, _ := pi.PrefixNodeIDs("aq")
		if want := []uint64{50}; !eqU64(idset(got), want) {
			t.Errorf("PrefixNodeIDs(aq) = %v, want %v", idset(got), want)
		}
	})
}

// TestPropertyIndexStringPrefixMaintenance verifies remove/purge keep the ordered
// string view consistent with Entries, including dropping a value key when its
// last holder leaves.
func TestPropertyIndexStringPrefixMaintenance(t *testing.T) {
	t.Parallel()
	pi := NewPropertyIndex()
	pi.Add(snowflake.ID(1), "cat")
	pi.Add(snowflake.ID(2), "cat")
	pi.Add(snowflake.ID(3), "car")

	// Remove one of the two "cat" holders: value key survives, id gone.
	pi.Remove(snowflake.ID(1), "cat")
	got, _ := pi.PrefixNodeIDs("cat")
	if want := []uint64{2}; !eqU64(idset(got), want) {
		t.Errorf("after remove id1: PrefixNodeIDs(cat) = %v, want %v", idset(got), want)
	}
	// Remove the last "cat" holder: the value key must vanish from strKeys.
	pi.Remove(snowflake.ID(2), "cat")
	if got, _ := pi.PrefixNodeIDs("cat"); len(got) != 0 {
		t.Errorf("after removing all cats: PrefixNodeIDs(cat) = %v, want empty", idset(got))
	}
	if _, present := pi.strBuckets["cat"]; present {
		t.Error("strBuckets still holds an empty 'cat' bucket")
	}
	// "car" still there.
	if got, _ := pi.PrefixNodeIDs("ca"); !eqU64(idset(got), []uint64{3}) {
		t.Errorf("PrefixNodeIDs(ca) = %v, want [3]", idset(got))
	}

	// Purge id3 from all buckets (corruption path).
	PurgeNodeFromAllPropertyIndexes(map[PropertyIndexKey]*PropertyIndex{{}: pi}, snowflake.ID(3))
	if got, _ := pi.PrefixNodeIDs(""); len(got) != 0 {
		t.Errorf("after purge: PrefixNodeIDs('') = %v, want empty", idset(got))
	}
	if pi.strKeys.n != 0 {
		t.Errorf("strKeys.n = %d after purge, want 0", pi.strKeys.n)
	}
}
