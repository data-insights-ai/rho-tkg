package graph_test

import (
	"context"
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkDocValuesMultiCold (round 7 request 6): the A:B columns of two
// numeric keys over 200,000 :A nodes, 100,000 of them also :B (sigma-tkgd's
// DocValuesMultiLabelAggSkewed cold row): each iteration writes one :B-only
// node (the intersection's snapshot goes stale, A's does not on badger),
// refreshes A's columns (as a query over A does), then streams the A:B
// columns, which are rebuilt. ns/op is the whole iteration.
func BenchmarkDocValuesMultiCold(b *testing.B) {
	const n = 200_000
	ctx := context.Background()
	keys := []string{"x", "y"}
	for _, backend := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{SnowflakeNodeID: 0}},
		{"badger", graphpkg.Config{SnowflakeNodeID: 0, BadgerInMemory: true}},
	} {
		g, err := graphpkg.New(backend.cfg)
		if err != nil {
			b.Fatal(err)
		}
		for _, labels := range [][]string{{"A", "B"}, {"A"}} {
			for lo := 0; lo < n/2; lo += 8192 {
				props := make([]map[string]any, min(8192, n/2-lo))
				for i := range props {
					props[i] = map[string]any{"x": int64(lo + i), "y": float64(i) / 3}
				}
				if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error { _, err := tx.AddNodes(labels, props); return err }); err != nil {
					b.Fatal(err)
				}
			}
		}
		b.Run(fmt.Sprintf("%s/rows=%d", backend.name, n/2), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := g.Nodes().Add(ctx, []string{"B"}, map[string]any{"x": int64(1)}); err != nil {
					b.Fatal(err)
				}
				if _, ok, err := g.Nodes().ForEachDocValues("A", keys, func(types.NodeID, []any, []bool) bool { return true }); err != nil || !ok {
					b.Fatal(ok, err)
				}
				rows := 0
				if _, ok, err := g.Nodes().ForEachDocValuesMulti([]string{"A", "B"}, keys, func(types.NodeID, []any, []bool) bool { rows++; return true }); err != nil || !ok || rows != n/2 {
					b.Fatal(ok, err, rows)
				}
			}
		})
		_ = g.Close()
	}
}
