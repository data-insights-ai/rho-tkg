package graph_test

import (
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRelHistoryCountByType: how many relationships of one type have history rows
// (HistoryCounts.Rels cannot tell types apart), across an update, close, delete
// (keeps its history) and writes to another type; unknown type is 0; stores
// without the per-type answer report ok=false, never a substitute count.
func TestRelHistoryCountByType(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		labels := []string{"P"}
		if b.tiered {
			labels = []string{"Ref"}
		}
		a, _ := g.Nodes().Add(ctx, labels, nil)
		c, _ := g.Nodes().Add(ctx, labels, nil)
		add := func(typ string) *types.Relationship {
			r, err := g.Rels().Add(ctx, typ, a, c, map[string]any{"w": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			return r
		}
		supported := b.name == "memory" || b.name == "badger" || b.tiered // tiered since backlog 58
		count := func(step, typ string, want int) {
			t.Helper()
			got, ok, err := g.Stats().RelHistoryCountByType(typ)
			if err != nil {
				t.Fatalf("%s: %v", step, err)
			}
			if !supported {
				if ok || got != 0 {
					t.Fatalf("%s: %s = (%d, %v), want (0, false): the store states no per-type count", step, typ, got, ok)
				}
				return
			}
			if !ok || got != want {
				t.Fatalf("%s: %s = (%d, %v), want (%d, true)", step, typ, got, ok, want)
			}
		}
		r1, r2 := add("R"), add("R")
		tick := add("TICK")
		count("fresh", "R", 0)
		count("fresh", "TICK", 0)
		count("fresh", "NO_SUCH_TYPE", 0)
		for v := range 3 {
			if _, err := g.Rels().Update(ctx, tick.ID(), map[string]any{"w": int64(v)}); err != nil {
				t.Fatal(err)
			}
		}
		count("another type updated", "R", 0)
		count("another type updated", "TICK", 1)
		if _, err := g.Rels().Update(ctx, r1.ID(), map[string]any{"w": int64(9)}); err != nil {
			t.Fatal(err)
		}
		count("one R updated", "R", 1)
		if err := g.Rels().Delete(ctx, r2.ID()); err != nil {
			t.Fatal(err)
		}
		count("one R deleted", "R", 2)
		count("one R deleted", "TICK", 1)
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := g.Stats().RelHistoryCountByType("R"); !errors.Is(err, graphpkg.ErrGraphClosed) {
			t.Fatalf("closed: %v", err)
		}
	})
}
