package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkNodeEarlyPinPointDepth mirrors a depth-scaling BenchmarkBitemporalDepth
// EARLY-pin point-read arm: a node gains version depth via updates, and the
// as-of point door is pinned at a NowTx captured right after creation — so the
// reverse walk must skip every newer version to reach the pinned one. The consumer
// reported ~70x growth depth 1→100 on badger (a full msgpack decode per walked
// version); the v2 fixed-tail peek classifies each walked version without a
// decode.
func BenchmarkNodeEarlyPinPointDepth(b *testing.B) {
	for _, depth := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("badger/depth=%d", depth), func(b *testing.B) {
			g, err := New(Config{BadgerInMemory: true})
			if err != nil {
				b.Fatalf("New: %v", err)
			}
			defer g.Close()
			ctx := context.Background()

			n, err := g.Nodes.Add(ctx, []string{"P"}, map[string]any{"w": 0})
			if err != nil {
				b.Fatalf("add: %v", err)
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				b.Fatalf("NowTx: %v", err)
			}
			for v := 1; v < depth; v++ {
				if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"w": v}); err != nil {
					b.Fatalf("update: %v", err)
				}
			}
			time.Sleep(250 * time.Millisecond) // drain the async write buffer

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := g.Temporal.NodeAsOf(n.ID(), pin)
				if err != nil {
					b.Fatalf("NodeAsOf: %v", err)
				}
				if v, _ := got.GetProperty("w"); v != int64(0) && v != 0 {
					b.Fatalf("early pin resolved wrong version: w=%v", v)
				}
			}
		})
	}
}

// BenchmarkNodeValidTimeEarlyWindowDepth mirrors a depth-scaling
// BenchmarkBitemporalDepthValidTime: each node gains versions with ASCENDING
// explicit valid_from windows (adjacent tiling), then an OLD validity window
// is resolved — the point read and the label AT TIME scan. Consumer reported
// ~55x (point) / ~345x (scan) growth depth 1→100 on badger; the
// selection-skeleton fast path (TemporalMetaHistoryCapability) resolves the
// winner on temporal metadata and decodes only the winning row.
func BenchmarkNodeValidTimeEarlyWindowDepth(b *testing.B) {
	for _, depth := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("badger/depth=%d", depth), func(b *testing.B) {
			g, err := New(Config{BadgerInMemory: true})
			if err != nil {
				b.Fatalf("New: %v", err)
			}
			defer g.Close()
			ctx := context.Background()

			const nodes = 100
			base := types.Instant(1_000_000)
			window := types.Instant(1_000)
			ids := make([]types.NodeID, 0, nodes)
			for i := 0; i < nodes; i++ {
				n, err := g.Nodes.Add(ctx, []string{"D"}, map[string]any{
					"w": 0, "tkg_valid_from": base,
				})
				if err != nil {
					b.Fatalf("add: %v", err)
				}
				ids = append(ids, n.ID())
				for v := 1; v < depth; v++ {
					if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{
						"w": v, "tkg_valid_from": base + types.Instant(v)*window,
					}); err != nil {
						b.Fatalf("update: %v", err)
					}
				}
			}
			oldT := base + window/2 // inside the OLDEST window
			time.Sleep(250 * time.Millisecond)

			b.Run("point", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					n, err := g.Temporal.NodeAt(ids[i%nodes], oldT)
					if err != nil {
						b.Fatalf("NodeAt: %v", err)
					}
					if v, _ := n.GetProperty("w"); fmt.Sprintf("%v", v) != "0" {
						b.Fatalf("resolved wrong version: w=%v", v)
					}
				}
			})
			b.Run("scan", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					out, err := g.Temporal.NodesByLabelAt("D", oldT)
					if err != nil {
						b.Fatalf("NodesByLabelAt: %v", err)
					}
					if len(out) != nodes {
						b.Fatalf("scan returned %d, want %d", len(out), nodes)
					}
				}
			})
		})
	}
}

// BenchmarkRelHeadPinExpandDepth mirrors a depth-scaling
// BenchmarkBitemporalDepthRels shape: relationships gain version depth via
// updates while their endpoint nodes stay at one version, then the pinned
// one-hop expand door (OutgoingForNodesAtPin) is measured at a HEAD pin (a
// NowTx captured after every update). Consumer reported ~8x growth from depth 1
// to depth 100 on badger while node head-pins stayed flat; this benchmark
// exists to reproduce and localize that cost inside rho-tkg.
func BenchmarkRelHeadPinExpandDepth(b *testing.B) {
	for _, backend := range []struct {
		name string
		cfg  Config
	}{
		{"memory", Config{}},
		{"badger", Config{BadgerInMemory: true}},
	} {
		for _, depth := range []int{1, 10, 100} {
			b.Run(fmt.Sprintf("%s/depth=%d", backend.name, depth), func(b *testing.B) {
				g, err := New(backend.cfg)
				if err != nil {
					b.Fatalf("New: %v", err)
				}
				defer g.Close()
				ctx := context.Background()

				const seeds = 20
				const relsPerSeed = 4
				seedIDs := make([]types.NodeID, 0, seeds)
				relIDs := make([]types.RelID, 0, seeds*relsPerSeed)
				for i := 0; i < seeds; i++ {
					s, err := g.Nodes.Add(ctx, []string{"P"}, nil)
					if err != nil {
						b.Fatalf("add seed: %v", err)
					}
					seedIDs = append(seedIDs, s.ID())
					for j := 0; j < relsPerSeed; j++ {
						t, err := g.Nodes.Add(ctx, []string{"P"}, nil)
						if err != nil {
							b.Fatalf("add target: %v", err)
						}
						r, err := g.Rels.Add(ctx, "R", s, t, map[string]any{"w": 0})
						if err != nil {
							b.Fatalf("add rel: %v", err)
						}
						relIDs = append(relIDs, r.ID())
					}
				}
				// Rels gain depth; endpoint nodes stay at one version.
				for v := 1; v < depth; v++ {
					for _, id := range relIDs {
						if _, err := g.Rels.Update(ctx, id, map[string]any{"w": v}); err != nil {
							b.Fatalf("update rel: %v", err)
						}
					}
				}
				pin, err := g.Temporal.NowTx()
				if err != nil {
					b.Fatalf("NowTx: %v", err)
				}
				// Let the async write buffer flush (100ms tick) so the
				// measurement reflects steady-state reads, not the
				// pending-buffer overlay cost of just-written history.
				time.Sleep(250 * time.Millisecond)

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					out, err := g.Rels.OutgoingForNodesAtPin(seedIDs, "R", pin)
					if err != nil {
						b.Fatalf("OutgoingForNodesAtPin: %v", err)
					}
					if len(out) != seeds {
						b.Fatalf("expected %d seed entries, got %d", seeds, len(out))
					}
				}
			})
		}
	}
}
