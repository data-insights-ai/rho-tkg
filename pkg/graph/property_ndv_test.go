package graph_test

import (
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestStatsPropertyNDV (round 5 request 1): g.Stats().PropertyNDV /
// RelPropertyNDV return PropertyStats' NDV and Count, two-phase across a
// write that removes the maximum, on every backend that states them; sharded
// (both) and tiered (relationships) report ErrCapabilityNotSupported, never a
// substitute; an unknown label or type is the zero value; a closed graph is
// ErrGraphClosed.
func TestStatsPropertyNDV(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		label := "P"
		if b.tiered {
			label = "Ref"
		}
		var nodes []*types.Node
		for _, v := range []int64{1, 5, 5, 100} {
			n, err := g.Nodes().Add(ctx, []string{label}, map[string]any{"v": v})
			if err != nil {
				t.Fatal(err)
			}
			nodes = append(nodes, n)
			if _, err := g.Rels().Add(ctx, "R", n, n, map[string]any{"w": v * 3}); err != nil {
				t.Fatal(err)
			}
		}
		nodeSupported := b.name != "sharded"
		relSupported := b.name == "memory" || b.name == "badger"
		check := func(phase string, wantCount int64) {
			t.Helper()
			got, err := g.Stats().PropertyNDV(label, "v")
			if !nodeSupported {
				if !errors.Is(err, storepkg.ErrCapabilityNotSupported) || got != (storepkg.PropertyNDV{}) {
					t.Fatalf("%s: PropertyNDV = %+v, %v; want ErrCapabilityNotSupported", phase, got, err)
				}
			} else {
				stats, serr := g.Stats().PropertyStats(label, "v")
				if err != nil || serr != nil {
					t.Fatalf("%s: %v / %v", phase, err, serr)
				}
				if got != (storepkg.PropertyNDV{NDV: stats.NDV, Count: stats.Count}) || got.Count != wantCount {
					t.Fatalf("%s: PropertyNDV = %+v, PropertyStats = %+v, want count %d", phase, got, stats, wantCount)
				}
			}
			rel, err := g.Stats().RelPropertyNDV("R", "w")
			if !relSupported {
				if !errors.Is(err, storepkg.ErrCapabilityNotSupported) || rel != (storepkg.PropertyNDV{}) {
					t.Fatalf("%s: RelPropertyNDV = %+v, %v; want ErrCapabilityNotSupported", phase, rel, err)
				}
				return
			}
			relStats, serr := g.Stats().RelPropertyStats("R", "w")
			if err != nil || serr != nil {
				t.Fatalf("%s: %v / %v", phase, err, serr)
			}
			if rel != (storepkg.PropertyNDV{NDV: relStats.NDV, Count: relStats.Count}) || rel.Count != wantCount {
				t.Fatalf("%s: RelPropertyNDV = %+v, RelPropertyStats = %+v, want count %d", phase, rel, relStats, wantCount)
			}
		}
		check("before", 4)
		if err := g.Nodes().Delete(ctx, nodes[3].ID()); err != nil {
			t.Fatal(err)
		}
		check("after deleting the maximum", 3)
		if got, err := g.Stats().PropertyNDV("NoSuchLabel", "v"); nodeSupported && (err != nil || got != (storepkg.PropertyNDV{})) {
			t.Fatalf("unknown label = %+v, %v", got, err)
		}
		if got, err := g.Stats().RelPropertyNDV("NO_SUCH_TYPE", "w"); relSupported && (err != nil || got != (storepkg.PropertyNDV{})) {
			t.Fatalf("unknown type = %+v, %v", got, err)
		}
		if _, err := g.Stats().PropertyNDV("", "v"); err == nil {
			t.Fatal("empty label accepted")
		}
		if _, err := g.Stats().RelPropertyNDV("R", "tkg_valid_from"); err == nil {
			t.Fatal("shadow key accepted")
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Stats().PropertyNDV(label, "v"); !errors.Is(err, graphpkg.ErrGraphClosed) {
			t.Fatalf("closed PropertyNDV: %v", err)
		}
		if _, err := g.Stats().RelPropertyNDV("R", "w"); !errors.Is(err, graphpkg.ErrGraphClosed) {
			t.Fatalf("closed RelPropertyNDV: %v", err)
		}
	})
}
