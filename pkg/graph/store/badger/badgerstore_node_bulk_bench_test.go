package badger

import (
	"fmt"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A/B for the single-iterator bulk label scan (forEachNodeBulk) vs the old
// per-node point-get loop (prefetchNodeScan). Both no-fill (scan discipline), so
// the cache state from seeding is identical for both — the only difference is
// one read transaction + one forward-seeking iterator vs N distinct db.View +
// Txn.Get. Nodes are flushed, and 50k > the 10k default cache, so the scan is
// badger-read-dominated (the case the door targets: MATCH (n:L) RETURN n).

func benchSeedNodesForScan(b *testing.B, bs *Store, n int) []types.NodeID {
	b.Helper()
	ids := make([]types.NodeID, 0, n)
	for i := 0; i < n; i++ {
		nid := types.NodeID(snowflake.ID(i + 1))
		nd := types.NewNode(nid, 1, nil)
		for k := 0; k < 8; k++ {
			if err := nd.SetProperty(fmt.Sprintf("p%d", k), fmt.Sprintf("value-%d-%d", i, k)); err != nil {
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
	return ids
}

func BenchmarkNodeScanPointGet(b *testing.B) {
	bs := newBenchmarkBadgerStore(b)
	b.Cleanup(func() { _ = bs.Close() })
	ids := benchSeedNodesForScan(b, bs, 50_000)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got := 0
		for _, nid := range ids {
			n, err := bs.prefetchNodeScan(nid)
			if err != nil {
				b.Fatalf("prefetchNodeScan: %v", err)
			}
			if n != nil {
				got++
			}
		}
		if got != len(ids) {
			b.Fatalf("scanned %d, want %d", got, len(ids))
		}
	}
}

func BenchmarkNodeScanBulk(b *testing.B) {
	bs := newBenchmarkBadgerStore(b)
	b.Cleanup(func() { _ = bs.Close() })
	ids := benchSeedNodesForScan(b, bs, 50_000)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got := 0
		if err := bs.forEachNodeBulk(ids, func(n *types.Node) bool {
			got++
			return true
		}); err != nil {
			b.Fatalf("forEachNodeBulk: %v", err)
		}
		if got != len(ids) {
			b.Fatalf("scanned %d, want %d", got, len(ids))
		}
	}
}

// BenchmarkNodeScanBulkParallel measures the parallel-decode collector against the
// serial BenchmarkNodeScanBulk above (same seed). The badger fetch stays serial; only
// the CPU-bound msgpack decode fans across GOMAXPROCS. Gates whether parallel decode
// is worth wiring for full unbounded label scans (measure, don't guess).
func BenchmarkNodeScanBulkParallel(b *testing.B) {
	bs := newBenchmarkBadgerStore(b)
	b.Cleanup(func() { _ = bs.Close() })
	ids := benchSeedNodesForScan(b, bs, 50_000)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := bs.collectNodesBulkParallel(ids)
		if err != nil {
			b.Fatalf("collectNodesBulkParallel: %v", err)
		}
		if len(got) != len(ids) {
			b.Fatalf("scanned %d, want %d", len(got), len(ids))
		}
	}
}

// BenchmarkForEachNodeByLabelStream measures the streaming label door (BACKLOG 3) —
// it now rides forEachNodeBulk (one iterator pass) instead of N per-node Txn.Gets, so
// it runs at bulk-scan speed while materializing nothing.
func BenchmarkForEachNodeByLabelStream(b *testing.B) {
	bs := newBenchmarkBadgerStore(b)
	b.Cleanup(func() { _ = bs.Close() })
	_ = benchSeedNodesForScan(b, bs, 50_000)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got := 0
		if err := bs.ForEachNodeByLabel(1, QueryOpts{}, func(*types.Node) bool {
			got++
			return true
		}); err != nil {
			b.Fatalf("ForEachNodeByLabel: %v", err)
		}
		if got != 50_000 {
			b.Fatalf("streamed %d, want 50000", got)
		}
	}
}
