package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// historyBenchUpdates numbers BenchmarkHistoryCounts' updates across rounds,
// so every update writes a new value (a version).
var historyBenchUpdates int

// BenchmarkHistoryCounts: 20,000 nodes of which 5,000 were updated. "walk"
// is what a caller pays without the door (the store's walk of the IDs with
// history), "stated" a HistoryCounts call with no history change since the
// last one, "afterUpdate" a call after one update (badger counts again).
func BenchmarkHistoryCounts(b *testing.B) {
	for _, be := range []struct {
		name string
		open func() (graphpkg.Config, historyIterator)
	}{
		{"memory", func() (graphpkg.Config, historyIterator) {
			st := memory.New()
			return graphpkg.Config{Store: st}, st
		}},
		{"badger", func() (graphpkg.Config, historyIterator) {
			st, err := badger.New(badger.Config{InMemory: true})
			if err != nil {
				b.Fatal(err)
			}
			return graphpkg.Config{Store: st}, st
		}},
	} {
		cfg, it := be.open()
		g, err := graphpkg.New(cfg)
		if err != nil {
			b.Fatal(err)
		}
		ctx := context.Background()
		ids := make([]types.NodeID, 0, 20000)
		for i := range 20000 {
			n, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"i": int64(i)})
			if err != nil {
				b.Fatal(err)
			}
			ids = append(ids, n.ID())
		}
		for i := range 5000 {
			if _, err := g.Nodes().Update(ctx, ids[i*4], map[string]any{"i": int64(-i - 1)}); err != nil {
				b.Fatal(err)
			}
		}
		b.Run(be.name+"/walk", func(b *testing.B) {
			for b.Loop() {
				n := 0
				if err := it.ForEachNodeHistoryID(func(types.NodeID) bool { n++; return true }); err != nil || n != 5000 {
					b.Fatal(n, err)
				}
			}
		})
		b.Run(be.name+"/stated", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				c, ok, err := g.Stats().HistoryCounts()
				if err != nil || !ok || c.Nodes != 5000 {
					b.Fatal(c, ok, err)
				}
			}
		})
		b.Run(be.name+"/afterUpdate", func(b *testing.B) {
			for b.Loop() {
				b.StopTimer()
				k := historyBenchUpdates
				historyBenchUpdates++
				if _, err := g.Nodes().Update(ctx, ids[(k%5000)*4], map[string]any{"i": int64(1_000_000 + k)}); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				c, ok, err := g.Stats().HistoryCounts()
				if err != nil || !ok || c.Nodes != 5000 {
					b.Fatal(c, ok, err)
				}
			}
		})
		_ = g.Close()
	}
}
