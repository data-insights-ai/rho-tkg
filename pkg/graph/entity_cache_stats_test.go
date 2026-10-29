package graph_test

import (
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

// TestTierEntityCacheStats pins the g.Tier().EntityCacheStats door
// (CACHESTATS): on a tiered graph the reference shard's row holds the nodes
// and the relationship writes just cached; a closed graph and a graph not on
// a tiered store refuse it. Catches a door that does not reach the store.
func TestTierEntityCacheStats(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g := newTieredGraph(t)
	a, err := g.Nodes().Add(ctx, []string{"Case"}, map[string]any{"k": "v"})
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
	rows, err := g.Tier().EntityCacheStats()
	if err != nil {
		t.Fatalf("EntityCacheStats: %v", err)
	}
	if len(rows) == 0 || rows[0].Name != "reference" || rows[0].Nodes.DirtyEntries+rows[0].Nodes.CleanEntries < 2 ||
		rows[0].Rels.DirtyEntries+rows[0].Rels.CleanEntries != 1 || rows[0].Rels.DirtyBytes+rows[0].Rels.CleanBytes <= 160 {
		t.Fatalf("EntityCacheStats = %+v, want the reference shard's two nodes and one relationship first", rows)
	}
	g.Close()
	if _, err := g.Tier().EntityCacheStats(); !errors.Is(err, graphpkg.ErrGraphClosed) {
		t.Fatalf("EntityCacheStats on a closed graph = %v, want ErrGraphClosed", err)
	}
	mem, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 12})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	defer mem.Close()
	if _, err := mem.Tier().EntityCacheStats(); !errors.Is(err, graphpkg.ErrNotTieredStore) {
		t.Fatalf("EntityCacheStats on a non-tiered graph = %v, want ErrNotTieredStore", err)
	}
}
