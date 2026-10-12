package bench

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkEffectiveScanCold (backlog 55, ai-soc restart replay / sigma
// restore): one ForEachRelEffectiveByType over N relationships (100 K by
// default; BENCH_EFFECTIVE_SCAN_RELS overrides), every one updated once (one
// history row, two segments), on an on-disk badger store closed after the
// build and reopened with a 1,024-entry cache, so the scan reads almost every
// current row and every history chain from badger, as a from-empty advance
// after a restart does. ns/rel is the per-relationship cost.
func BenchmarkEffectiveScanCold(b *testing.B) {
	n := 100_000
	if v, err := strconv.Atoi(os.Getenv("BENCH_EFFECTIVE_SCAN_RELS")); err == nil && v > 0 {
		n = v
	}
	dir := b.TempDir()
	open := func() *graph.Graph {
		st, err := badger.New(badger.Config{Dir: dir, CacheCapacity: 1024, FlushInterval: 100 * time.Millisecond})
		if err != nil {
			b.Fatalf("badger.New: %v", err)
		}
		g, err := graph.New(graph.Config{Store: st, SnowflakeNodeID: 13})
		if err != nil {
			b.Fatalf("graph.New: %v", err)
		}
		return g
	}
	g := open()
	ctx := benchCtx()
	nodes := addLabeledNodes(b, g, "Host", 1000)
	for i := 0; i < n; i++ {
		r, err := g.Rels().AddByID(ctx, "FLOW", nodes[i%len(nodes)], nodes[(i*7+1)%len(nodes)],
			map[string]any{"tkg_valid_from": types.Instant(1000), "seq": i})
		if err != nil {
			b.Fatalf("add rel %d: %v", i, err)
		}
		if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"seq": -i}); err != nil {
			b.Fatalf("update rel %d: %v", i, err)
		}
	}
	if err := g.Close(); err != nil {
		b.Fatalf("Close: %v", err)
	}
	g = open()
	b.Cleanup(func() { _ = g.Close() })
	pin, err := g.Temporal().NowTx()
	if err != nil {
		b.Fatalf("NowTx: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		segs := 0
		if err := g.Temporal().ForEachRelEffectiveByType("FLOW", pin, func(temporal.RelSegment) bool {
			segs++
			return true
		}); err != nil || segs < n {
			b.Fatalf("scan: %d segments, %v; want >= %d", segs, err, n)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n), "ns/rel")
}
