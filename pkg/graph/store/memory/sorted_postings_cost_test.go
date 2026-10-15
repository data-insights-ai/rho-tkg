package memory

import (
	"runtime"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 72 (ai-soc): an equality page with After/Limit reads its page from
// the index's sorted postings — no copy or sort of the whole value key — so the
// bytes one page allocates do not grow with the key. Before, every call copied
// and sorted every ID of the key (8 B per member plus the row slice).

// fillOneKey stores n nodes of label 1 sharing (a=1, b="x") and n relationships
// of type 7 sharing k=1, in a shuffled ID order (postings are not filled
// ascending), with the property, composite and rel property indexes declared.
func fillOneKey(tb testing.TB, n int) (*Store, []types.NodeID, []types.RelID) {
	tb.Helper()
	ms := New()
	if err := ms.CreatePropertyIndex(1, "a"); err != nil {
		tb.Fatal(err)
	}
	if err := ms.CreateCompositePropertyIndex(1, []string{"a", "b"}); err != nil {
		tb.Fatal(err)
	}
	if err := ms.CreateRelPropertyIndex(7, "k"); err != nil {
		tb.Fatal(err)
	}
	perm := make([]int, n)
	for i := range perm {
		perm[i] = i
	}
	// Deterministic shuffle (xorshift) without math/rand state shared across tests.
	x := uint64(88172645463325252)
	for i := n - 1; i > 0; i-- {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		j := int(x % uint64(i+1)) // #nosec G115 -- test shuffle
		perm[i], perm[j] = perm[j], perm[i]
	}
	nids := make([]types.NodeID, n)
	rids := make([]types.RelID, n)
	for _, p := range perm {
		nid := types.NodeID(snowflake.ID(int64(p+1) << 15))
		node := types.NewNode(nid, 1, nil)
		_ = node.SetProperty("a", int64(1))
		_ = node.SetProperty("b", "x")
		if err := ms.PutNode(node); err != nil {
			tb.Fatal(err)
		}
		nids[p] = nid
	}
	for _, p := range perm {
		rid := types.RelID(snowflake.ID(int64(p+1)<<15 | 1))
		r := types.NewRelationship(rid, 7, nids[p], nids[(p+1)%n])
		_ = r.SetProperty("k", int64(1))
		if err := ms.PutRelationship(r); err != nil {
			tb.Fatal(err)
		}
		rids[p] = rid
	}
	return ms, nids, rids
}

// bytesPerCall returns the heap bytes one call of fn allocates, averaged.
func bytesPerCall(fn func()) uint64 {
	const calls = 20
	fn() // warm
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < calls; i++ {
		fn()
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / calls
}

func TestPostingsPageCostIndependentOfKeySize(t *testing.T) {
	n := 200_000
	if testing.Short() {
		n = 50_000
	}
	ms, nids, rids := fillOneKey(t, n)
	const limit = 16
	// The bound is what a page of 16 rows may allocate: the result slice and
	// iteration state, far below one 8-byte copy of the key (n*8 bytes).
	const bound = 4 << 10
	mid := nids[n/2]
	midRel := rids[n/2]
	doors := []struct {
		name string
		call func() int
	}{
		{"NodesByLabelAndProperties", func() int {
			out, err := ms.NodesByLabelAndProperties(1, map[string]any{"a": int64(1), "b": "x"}, QueryOpts{After: types.EntityID(mid), Limit: limit})
			if err != nil {
				t.Fatal(err)
			}
			return len(out)
		}},
		{"NodesByLabelAndProperty", func() int {
			out, err := ms.NodesByLabelAndProperty(1, "a", int64(1), QueryOpts{After: types.EntityID(mid), Limit: limit})
			if err != nil {
				t.Fatal(err)
			}
			return len(out)
		}},
		{"RelationshipsByTypeAndProperty", func() int {
			out, err := ms.RelationshipsByTypeAndProperty(7, "k", int64(1), QueryOpts{After: types.EntityID(midRel), Limit: limit})
			if err != nil {
				t.Fatal(err)
			}
			return len(out)
		}},
	}
	for _, d := range doors {
		if got := d.call(); got != limit {
			t.Fatalf("%s: page holds %d rows, want %d", d.name, got, limit)
		}
		b := bytesPerCall(func() { d.call() })
		t.Logf("%s: %d B per page of %d over a key of %d", d.name, b, limit, n)
		if b > bound {
			t.Errorf("%s: one page of %d allocates %d B over a key of %d members (bound %d B): the page copies or sorts the key", d.name, limit, b, n, bound)
		}
	}
}

// postingIDs (test-only) returns a posting list's IDs ascending.
func postingIDs(p *indexpkg.IDPostings) []types.NodeID {
	var out []types.NodeID
	for id := range p.All() {
		out = append(out, types.NodeID(id))
	}
	return out
}

// lookupSet (test-only) returns value's IDs as a set, nil when none.
func lookupSet(pi *indexpkg.PropertyIndex, value any) map[snowflake.ID]struct{} {
	p := pi.Postings(value)
	if p.Len() == 0 {
		return nil
	}
	out := make(map[snowflake.ID]struct{}, p.Len())
	for id := range p.All() {
		out[id] = struct{}{}
	}
	return out
}
