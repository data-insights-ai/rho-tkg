package store

import (
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func benchNodes(n int) []*types.Node {
	nodes := make([]*types.Node, n)
	for i := range nodes {
		nd := types.NewNode(types.NodeID(i+1), 7, nil)
		_ = nd.SetProperty("qty", int64(i))
		_ = nd.SetProperty("city", fmt.Sprintf("c%d", i%17))
		nd.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(i + 1)})
		nodes[i] = nd
	}
	return nodes
}

// BenchmarkScanColumnsFromNodes_RowPath guards the shared row path's per-value loop.
//
// It exists because a refactor that shared that loop between the node and
// relationship drivers cost 10-23% and NOTHING CAUGHT IT — the path had no benchmark
// at all, the full suite stayed green, and the regression was found only because
// someone asked whether the change hurt performance. Measure this interleaved against
// the pre-change tree (alternate the two binaries; a single before-then-after run
// drifts ~6% on its own and benchstat cannot see that).
func BenchmarkScanColumnsFromNodes_RowPath(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		nodes := benchNodes(n)
		props := []string{"qty", "city"}
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := ScanColumnsFromNodes(nodes, props, func(bt *ColumnBatch) bool {
					return true
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkScanColumnsFromRels_RowPath is the relationship sibling, so a change to
// either copy of the per-value loop has a number attached to it.
func BenchmarkScanColumnsFromRels_RowPath(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		rels := make([]*types.Relationship, n)
		for i := range rels {
			r := types.NewRelationship(types.RelID(i+1), 7, types.NodeID(1), types.NodeID(2))
			_ = r.SetProperty("qty", int64(i))
			_ = r.SetProperty("city", fmt.Sprintf("c%d", i%17))
			r.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(i + 1)})
			rels[i] = r
		}
		props := []string{"qty", "city"}
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := ScanColumnsFromRels(rels, props, func(bt *RelColumnBatch) bool {
					return true
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
