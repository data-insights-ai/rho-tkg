package core

import (
	"context"
	"testing"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RelOps.ForEachAdjacentEndpointAt has two arms: the native inline-stamp store
// capability (badger) and the decode-then-filter fallback (memory, no native
// capability). Both must return EXACTLY the edges the canonical decode path
// (Outgoing + MatchesTemporalFilter) returns at every query time. This is the
// facade-level parity gate complementing the store-level divergence gate.
func TestForEachAdjacentEndpointAt_ParityBothBackends(t *testing.T) {
	t.Parallel()

	backends := []struct {
		name  string
		build func(t *testing.T) *Core
	}{
		{name: "memory_fallback", build: func(t *testing.T) *Core {
			g, err := New(Config{Store: memory.New()})
			if err != nil {
				t.Fatalf("New(memory): %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })
			return g
		}},
		{name: "badger_native", build: func(t *testing.T) *Core {
			bs, err := badger.New(badger.Config{InMemory: true})
			if err != nil {
				t.Fatalf("badger.New: %v", err)
			}
			g, err := New(Config{Store: bs})
			if err != nil {
				t.Fatalf("New(badger): %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })
			return g
		}},
	}

	for _, be := range backends {
		t.Run(be.name, func(t *testing.T) {
			ctx := context.Background()
			g := be.build(t)

			hub, err := g.Nodes.Add(ctx, []string{"Hub"}, nil)
			if err != nil {
				t.Fatalf("add hub: %v", err)
			}
			const numTargets = 5
			targets := make([]*types.Node, numTargets)
			for i := 0; i < numTargets; i++ {
				targets[i], err = g.Nodes.Add(ctx, []string{"T"}, map[string]any{"i": int64(i)})
				if err != nil {
					t.Fatalf("add target %d: %v", i, err)
				}
			}

			// Edges with varied valid intervals (tkg_valid_to 0 / absent = open).
			type spec struct {
				target    int
				vf, vt    int64
				openEnded bool
			}
			specs := []spec{
				{0, 100, 200, false},
				{1, 100, 0, true},
				{2, 150, 400, false},
				{3, 50, 120, false},
				{4, 300, 0, true},
				{0, 250, 500, false}, // second edge to target 0
			}
			for _, s := range specs {
				props := map[string]any{"tkg_valid_from": s.vf}
				if !s.openEnded {
					props["tkg_valid_to"] = s.vt
				}
				if _, err := g.Rels.Add(ctx, "LINK", hub, targets[s.target], props); err != nil {
					t.Fatalf("add edge %+v: %v", s, err)
				}
			}

			for _, qt := range []int64{1, 60, 100, 110, 130, 175, 250, 350, 450, 600} {
				opts := storepkg.QueryOpts{ValidAt: types.Instant(qt)}

				// Oracle: Outgoing + canonical filter.
				rels, err := g.Rels.Outgoing(hub.ID(), "LINK")
				if err != nil {
					t.Fatalf("Outgoing: %v", err)
				}
				oracle := map[types.RelID]types.NodeID{}
				for _, r := range rels {
					if storeutil.MatchesTemporalFilter(r.InternalID().SnowflakeID(), r.Temporal(), opts) {
						oracle[r.InternalID()] = r.EndNodeID()
					}
				}

				got := map[types.RelID]types.NodeID{}
				if err := g.Rels.ForEachAdjacentEndpointAt(hub.ID(), "LINK", false, opts, func(rel types.RelID, other types.NodeID) bool {
					got[rel] = other
					return true
				}); err != nil {
					t.Fatalf("ForEachAdjacentEndpointAt t=%d: %v", qt, err)
				}

				if len(got) != len(oracle) {
					t.Fatalf("t=%d: size mismatch got=%d oracle=%d\ngot=%v\noracle=%v", qt, len(got), len(oracle), got, oracle)
				}
				for rel, end := range oracle {
					if g2, ok := got[rel]; !ok || g2 != end {
						t.Fatalf("t=%d: rel %d want end %d got %d present=%v", qt, rel, end, g2, ok)
					}
				}
			}
		})
	}
}
