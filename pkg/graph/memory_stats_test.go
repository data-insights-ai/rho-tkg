package graph_test

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

// TestTierMemoryStats pins the g.Tier().MemoryStats door (MEMACCT): on a
// tiered graph the reference shard's account holds the nodes and the
// relationship just written and its memtable arena, and reading it counts no
// shard access; a closed graph and a graph not on a tiered store refuse it.
// Catches a door that does not reach the store.
func TestTierMemoryStats(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	ctx := context.Background()
	g := newTieredGraph(t)
	a, err := g.Nodes().Add(ctx, []string{"Case"}, nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	b, err := g.Nodes().Add(ctx, []string{"Case"}, nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := g.Rels().Add(ctx, "LINKS", a, b, nil); err != nil {
		t.Fatalf("Rels().Add: %v", err)
	}
	before, err := g.Tier().ShardAccessStats()
	if err != nil {
		t.Fatal(err)
	}
	st, err := g.Tier().MemoryStats()
	if err != nil {
		t.Fatalf("MemoryStats: %v", err)
	}
	if after, _ := g.Tier().ShardAccessStats(); after != before {
		t.Fatalf("MemoryStats counted shard accesses: %+v then %+v", before, after)
	}
	if st.Reference.IDSets <= 0 || st.Reference.MemTableArena <= 0 || st.Reference.Bytes() <= st.Reference.MemTableArena {
		t.Fatalf("MemoryStats reference = %+v, want its ID sets and memtable arena", st.Reference)
	}
	g.Close()
	if _, err := g.Tier().MemoryStats(); !errors.Is(err, graphpkg.ErrGraphClosed) {
		t.Fatalf("MemoryStats on a closed graph = %v, want ErrGraphClosed", err)
	}
	mem, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 12})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	defer mem.Close()
	if _, err := mem.Tier().MemoryStats(); !errors.Is(err, graphpkg.ErrNotTieredStore) {
		t.Fatalf("MemoryStats on a non-tiered graph = %v, want ErrNotTieredStore", err)
	}
}
