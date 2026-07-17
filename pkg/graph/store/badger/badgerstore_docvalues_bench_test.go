package badger

import (
	"fmt"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A/B for the DocValues cold-build getProp strategy: the old column-major build
// re-fetched each node once per (column × pass) via GetNode (~M×2×N GetNodes and
// LRU thrash on a label larger than the cache); the new bulkNodePropGetter
// decodes each node exactly once. Nodes flushed; 20k > the 10k default cache, so
// the per-node path is badger-read + cache-thrash dominated.

func benchSeedDocValuesNodes(b *testing.B, bs *Store, n, cols int) ([]types.NodeID, []string) {
	b.Helper()
	ids := make([]types.NodeID, 0, n)
	keys := make([]string, cols)
	for k := 0; k < cols; k++ {
		keys[k] = fmt.Sprintf("p%d", k)
	}
	for i := 0; i < n; i++ {
		nid := types.NodeID(snowflake.ID(i + 1))
		nd := types.NewNode(nid, 1, nil)
		for k := 0; k < cols; k++ {
			if err := nd.SetProperty(keys[k], float64(i*cols+k)); err != nil {
				b.Fatalf("SetProperty: %v", err)
			}
		}
		if err := bs.PutNode(nd); err != nil {
			b.Fatalf("PutNode: %v", err)
		}
		ids = append(ids, nid)
	}
	if err := bs.flush(); err != nil {
		b.Fatalf("flush: %v", err)
	}
	return ids, keys
}

func BenchmarkDocValuesColdBuildPerNode(b *testing.B) {
	bs := newBenchmarkBadgerStore(b)
	b.Cleanup(func() { _ = bs.Close() })
	ids, keys := benchSeedDocValuesNodes(b, bs, 20_000, 8)

	// The OLD strategy: a fresh GetNode per (id, key), no bulk materialization.
	perNode := func(id types.NodeID, key string) (any, bool) {
		nd, err := bs.GetNode(id)
		if err != nil || nd == nil {
			return nil, false
		}
		return nd.GetProperty(key)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		col := indexpkg.BuildLabelDocValues(uint64(i+1), ids, keys, perNode)
		if col == nil {
			b.Fatal("nil column")
		}
	}
}

func BenchmarkDocValuesColdBuildBulk(b *testing.B) {
	bs := newBenchmarkBadgerStore(b)
	b.Cleanup(func() { _ = bs.Close() })
	ids, keys := benchSeedDocValuesNodes(b, bs, 20_000, 8)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		col := indexpkg.BuildLabelDocValues(uint64(i+1), ids, keys, bs.bulkNodePropGetter(ids))
		if col == nil {
			b.Fatal("nil column")
		}
	}
}
