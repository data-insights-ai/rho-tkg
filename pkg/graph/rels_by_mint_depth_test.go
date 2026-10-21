package graph_test

import (
	"context"
	"slices"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRelsByMint_AdjacencyAtHonoursDepth (row 94): through the graph doors
// that take QueryOpts, a reference node's mint-placed rels are scoped by
// opts.Depth, with and without a temporal filter, ex-adjacency included.
func TestRelsByMint_AdjacencyAtHonoursDepth(t *testing.T) {
	ts, err := tiered.New(tiered.Config{
		InMemory:      true,
		RefLabels:     []string{"Ref"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
		RelsByMint:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: ts})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()
	a, err := g.Nodes().Add(ctx, []string{"Ref"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes().Add(ctx, []string{"Ref"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r1, err := g.Rels().AddByID(ctx, "LINK", a.ID(), b.ID(), nil)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	r2, err := g.Rels().AddByID(ctx, "LINK", a.ID(), b.ID(), nil)
	if err != nil {
		t.Fatal(err)
	}

	rels := func(opts storepkg.QueryOpts) []types.RelID {
		t.Helper()
		var ids []types.RelID
		if err := g.Rels().ForEachAdjacentRelAt(a.ID(), "", false, opts, func(r *types.Relationship) bool {
			ids = append(ids, r.ID())
			return true
		}); err != nil {
			t.Fatalf("ForEachAdjacentRelAt(%+v): %v", opts, err)
		}
		return ids
	}
	ends := func(opts storepkg.QueryOpts) []types.RelID {
		t.Helper()
		var ids []types.RelID
		if err := g.Rels().ForEachAdjacentEndpointAt(b.ID(), "LINK", true, opts, func(id types.RelID, _ types.NodeID) bool {
			ids = append(ids, id)
			return true
		}); err != nil {
			t.Fatalf("ForEachAdjacentEndpointAt(%+v): %v", opts, err)
		}
		return ids
	}
	now := types.InstantFromTime(time.Now())
	all, hot := []types.RelID{r1.ID(), r2.ID()}, []types.RelID{r2.ID()}
	cases := []struct {
		name string
		got  []types.RelID
		want []types.RelID
	}{
		{"rel all", rels(storepkg.QueryOpts{}), all},
		{"rel warm", rels(storepkg.QueryOpts{Depth: storepkg.DepthWarm}), all},
		{"rel hot", rels(storepkg.QueryOpts{Depth: storepkg.DepthHot}), hot},
		{"rel hot ValidAt", rels(storepkg.QueryOpts{Depth: storepkg.DepthHot, ValidAt: now}), hot},
		{"rel all ValidAt", rels(storepkg.QueryOpts{ValidAt: now}), all},
		{"endpoint hot", ends(storepkg.QueryOpts{Depth: storepkg.DepthHot}), hot},
		{"endpoint all", ends(storepkg.QueryOpts{}), all},
	}
	for _, tc := range cases {
		if !slices.Equal(tc.got, tc.want) {
			t.Fatalf("%s = %v, want %v", tc.name, tc.got, tc.want)
		}
	}

	// A deleted mint rel is ex-adjacency on its (warm) mint shard.
	pin := types.InstantFromTime(time.Now())
	time.Sleep(2 * time.Millisecond)
	if err := g.Rels().Delete(ctx, r1.ID()); err != nil {
		t.Fatal(err)
	}
	if got := rels(storepkg.QueryOpts{TxAt: pin}); !slices.Equal(got, all) {
		t.Fatalf("TxAt before the delete, DepthAll = %v, want %v", got, all)
	}
	if got := rels(storepkg.QueryOpts{TxAt: pin, Depth: storepkg.DepthHot}); !slices.Equal(got, hot) {
		t.Fatalf("TxAt before the delete, DepthHot = %v, want %v", got, hot)
	}
}
