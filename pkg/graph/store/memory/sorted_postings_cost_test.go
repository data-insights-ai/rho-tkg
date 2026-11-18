package memory

import (
	"slices"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// For ai-soc, an equality page with After/Limit reads its page from
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

// postingsPageDoor is one equality door that pages: ids returns the IDs of
// its page of limit after the key's middle member, call how many rows it holds.
type postingsPageDoor struct {
	name string
	ids  func() []int64
	call func() int
}

// postingsPageDoors are the equality doors that page, each reading its page
// of limit after the key's middle member.
func postingsPageDoors(t *testing.T, ms *Store, nids []types.NodeID, rids []types.RelID, limit int) []postingsPageDoor {
	mid := nids[len(nids)/2]
	midRel := rids[len(rids)/2]
	nodeIDs := func(out []*types.Node, err error) []int64 {
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]int64, len(out))
		for i, n := range out {
			ids[i] = int64(n.ID())
		}
		return ids
	}
	doors := []postingsPageDoor{
		{name: "NodesByLabelAndProperties", ids: func() []int64 {
			return nodeIDs(ms.NodesByLabelAndProperties(1, map[string]any{"a": int64(1), "b": "x"}, QueryOpts{After: types.EntityID(mid), Limit: limit}))
		}},
		{name: "NodesByLabelAndProperty", ids: func() []int64 {
			return nodeIDs(ms.NodesByLabelAndProperty(1, "a", int64(1), QueryOpts{After: types.EntityID(mid), Limit: limit}))
		}},
		{name: "RelationshipsByTypeAndProperty", ids: func() []int64 {
			out, err := ms.RelationshipsByTypeAndProperty(7, "k", int64(1), QueryOpts{After: types.EntityID(midRel), Limit: limit})
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]int64, len(out))
			for i, r := range out {
				ids[i] = int64(r.ID())
			}
			return ids
		}},
	}
	for i := range doors {
		ids := doors[i].ids
		doors[i].call = func() int { return len(ids()) }
	}
	return doors
}

// A page after the middle member of a key filled in shuffled ID order holds
// exactly the limit members that follow it in ID order, on every equality
// door. The bytes such a page allocates are
// TestPostingsPageCostIndependentOfKeySize, at the measurement level
// (sorted_postings_cost_measure_test.go, MEMGATE).
func TestPostingsPageFromTheMiddleOfAKey(t *testing.T) {
	const n = 200 // the page does not depend on the key size
	ms, nids, rids := fillOneKey(t, n)
	const limit = 16
	var wantNodes, wantRels []int64
	for p := n/2 + 1; p <= n/2+limit; p++ {
		wantNodes = append(wantNodes, int64(nids[p]))
		wantRels = append(wantRels, int64(rids[p]))
	}
	for _, d := range postingsPageDoors(t, ms, nids, rids, limit) {
		want := wantNodes
		if d.name == "RelationshipsByTypeAndProperty" {
			want = wantRels
		}
		if got := d.ids(); !slices.Equal(got, want) {
			t.Errorf("%s: page %v, want the %d IDs after the middle member %v", d.name, got, limit, want)
		}
	}
}

// idsAfter starts past every copy of the cursor, before the first ID, and
// yields nothing past the last; empty input yields nothing.
func TestIDsAfterSkipsEveryCopyOfTheCursor(t *testing.T) {
	ids := []types.NodeID{2, 4, 4, 4, 7}
	cases := []struct {
		after types.EntityID
		want  []snowflake.ID
	}{
		{0, []snowflake.ID{2, 4, 4, 4, 7}},
		{1, []snowflake.ID{2, 4, 4, 4, 7}},
		{2, []snowflake.ID{4, 4, 4, 7}},
		{4, []snowflake.ID{7}},
		{5, []snowflake.ID{7}},
		{7, nil},
		{9, nil},
	}
	for _, c := range cases {
		var got []snowflake.ID
		for id := range idsAfter(ids, c.after) {
			got = append(got, id)
		}
		if !slices.Equal(got, c.want) {
			t.Errorf("after %d: %v, want %v", c.after, got, c.want)
		}
	}
	for range idsAfter([]types.NodeID(nil), 3) {
		t.Fatal("empty input yielded an ID")
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
