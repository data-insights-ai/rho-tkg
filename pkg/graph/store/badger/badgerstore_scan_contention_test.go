package badger

import (
	"sync/atomic"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkConcurrentLabelScan measures the concurrent-label-scan throughput the
// sigma BF1/BF3 workload stresses: many workers repeatedly scanning the same
// label, every node served from the entity cache (capacity >= cardinality, so
// this isolates cache-access contention, not miss/decode cost). Run it against
// the sharded default and TKG_CACHE_SHARDS=1 (the pre-sharding single-mutex
// baseline) to A/B the sharded-cache win:
//
//	go test -run=^$ -bench=BenchmarkConcurrentLabelScan -benchmem ./pkg/graph/store/badger/
//	TKG_CACHE_SHARDS=1 go test -run=^$ -bench=BenchmarkConcurrentLabelScan ...
//
// Reported ns/op is per FULL scan pass; the custom nodes/s metric is the useful
// one for the A/B.
func BenchmarkConcurrentLabelScan(b *testing.B) {
	const n = 20_000
	const label = uint16(1)
	bs, err := New(Config{InMemory: true, CacheCapacity: n + 1})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	b.Cleanup(func() { bs.Close() })

	for i := 1; i <= n; i++ {
		node := types.NewNode(types.NodeID(i), label, nil)
		if err := node.SetProperty("k", int64(i)); err != nil {
			b.Fatalf("prop %d: %v", i, err)
		}
		if err := bs.PutNode(node); err != nil {
			b.Fatalf("put %d: %v", i, err)
		}
	}
	if err := bs.Flush(); err != nil { // clean entries: the steady-state scan regime
		b.Fatalf("flush: %v", err)
	}
	// Warm the cache so the benchmark loop measures hits, not first-touch fills.
	if _, err := bs.NodesByLabel(label, QueryOpts{}); err != nil {
		b.Fatalf("warm: %v", err)
	}

	var scanned int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		local := int64(0)
		for pb.Next() {
			nodes, err := bs.NodesByLabel(label, QueryOpts{})
			if err != nil {
				b.Fatalf("scan: %v", err)
			}
			local += int64(len(nodes))
		}
		atomic.AddInt64(&scanned, local)
	})
	b.StopTimer()
	// nodes/s: total nodes fetched across all workers / wall seconds.
	b.ReportMetric(float64(scanned)/b.Elapsed().Seconds(), "nodes/s")
}
