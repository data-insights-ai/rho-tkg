package graph_test

import (
	"context"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 50 (cost): ForEachRelEffectiveAtNodes with one seed reads the rows
// of the relationships that ever named the seed (its adjacency and
// ex-adjacency) — not every relationship with history, nor every deleted one.
// On badger with a one-entry cache every row read is a cache miss, so the
// misses of one call count the relationships it visits.
//
// Catches: the all-history candidate fold (O(H) per call), the global
// deleted-ID fold (O(deleted) per call), and a gather that drops the seed's
// deleted or re-imported earlier lives (the exact answer).
func TestEffectiveAtNodesVisitsSeedAdjacencyOnly(t *testing.T) {
	ctx := context.Background()
	st, err := badger.New(badger.Config{Dir: t.TempDir(), FlushInterval: time.Hour, CacheCapacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: st})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	node := func() types.NodeID {
		n, err := g.Nodes().Add(ctx, []string{"Host"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return n.ID()
	}
	rel := func(s, e types.NodeID) types.RelID {
		r, err := g.Rels().AddByID(ctx, "FLOW", s, e, nil)
		if err != nil {
			t.Fatal(err)
		}
		return r.ID()
	}
	const seedRels, others = 5, 400
	seed := node()
	for i := 0; i < seedRels; i++ {
		rel(seed, node())
	}
	// H: relationships away from the seed, every one with a history row, a
	// quarter of them deleted.
	pool := []types.NodeID{node(), node(), node(), node()}
	for i := 0; i < others; i++ {
		id := rel(pool[i%4], pool[(i+1)%4])
		if _, err := g.Rels().Update(ctx, id, map[string]any{"i": int64(i)}); err != nil {
			t.Fatal(err)
		}
		if i%4 == 0 {
			if err := g.Rels().Delete(ctx, id); err != nil {
				t.Fatal(err)
			}
		}
	}
	// One relationship of the seed deleted, one moved off the seed by a
	// re-import: both earlier lives are in the answer at the pin.
	deleted := rel(seed, pool[0])
	moved := rel(seed, pool[1])
	pin, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Rels().Delete(ctx, deleted); err != nil {
		t.Fatal(err)
	}
	if err := g.Rels().Delete(ctx, moved); err != nil {
		t.Fatal(err)
	}
	start, err := g.Nodes().Get(ctx, pool[2])
	if err != nil {
		t.Fatal(err)
	}
	end, err := g.Nodes().Get(ctx, pool[1])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels().Import(ctx, moved, "FLOW", start, end, nil); err != nil {
		t.Fatal(err)
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	before := st.RelCacheMisses()
	got := map[types.RelID]bool{}
	if err := g.Temporal().ForEachRelEffectiveAtNodes([]types.NodeID{seed}, temporal.Outgoing, nil, pin, func(s temporal.RelSegment) bool {
		got[s.Rel.ID()] = true
		return true
	}); err != nil {
		t.Fatal(err)
	}
	misses := st.RelCacheMisses() - before
	t.Logf("one AtNodes call: %d relationship rows read (seed %d relationships, %d with history elsewhere)", misses, seedRels, others)
	if want := seedRels + 2; len(got) != want || !got[deleted] || !got[moved] {
		t.Fatalf("AtNodes(seed, pin) streamed %d relationships (deleted %v, moved %v); want %d with both", len(got), got[deleted], got[moved], want)
	}
	// The seed's adjacency rows are read twice (gather, then timeline), its
	// deleted and moved one once each; a little slack for the store.
	if bound := int64(2*seedRels + 2 + 4); misses > bound {
		t.Fatalf("one AtNodes call over a seed of %d relationships read %d relationship rows (bound %d): it visits relationships that never named the seed (%d with history elsewhere, %d of them deleted)", seedRels, misses, bound, others, others/4)
	}
}
