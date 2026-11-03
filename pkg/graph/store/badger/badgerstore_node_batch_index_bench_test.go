package badger

import (
	"fmt"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkPutNodesBatch1000 (BATCHIDX, ai-soc 3 Nov: AddNode 16 us a row at
// 3.47 M nodes vs 4.5 us empty, property-index inserts 106 ms of a 219 ms
// cut): one PutNodesBatch of 1,000 new nodes, ascending IDs, into a store
// already holding existing nodes of the same label, with RAM property indexes
// on a repeated string, an ascending number (range counts) and a distinct
// string. Reported per batch and per row (ns/row).
//
//	go test -run '^$' -bench BenchmarkPutNodesBatch1000 -benchtime 200x ./pkg/graph/store/badger/
func BenchmarkPutNodesBatch1000(b *testing.B) {
	const batch = 1000
	for _, existing := range []int{0, 1_000_000, 3_000_000} {
		b.Run(fmt.Sprintf("existing=%d", existing), func(b *testing.B) {
			bs, err := New(Config{InMemory: true, FlushInterval: time.Hour})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { bs.Close() })
			const label = 3
			if err := bs.CreatePropertyIndex(label, "kind"); err != nil {
				b.Fatal(err)
			}
			if err := bs.CreatePropertyIndexWithOptions(label, "ts", storecontract.PropertyIndexOptions{RangeCounts: true}); err != nil {
				b.Fatal(err)
			}
			if err := bs.CreatePropertyIndex(label, "ref"); err != nil {
				b.Fatal(err)
			}
			next := int64(1 << 30)
			mkBatch := func(n int) []*types.Node {
				nodes := make([]*types.Node, n)
				for i := range nodes {
					next++
					nd := types.NewNode(types.NodeID(snowflake.ID(next)), label, nil)
					_ = nd.SetProperty("kind", fmt.Sprint("k", next%8))
					_ = nd.SetProperty("ts", next)
					_ = nd.SetProperty("ref", fmt.Sprintf("r%012d", next))
					nodes[i] = nd
				}
				return nodes
			}
			for done := 0; done < existing; done += 10_000 {
				if err := bs.PutNodesBatch(mkBatch(min(10_000, existing-done))); err != nil {
					b.Fatal(err)
				}
			}
			if err := bs.Flush(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				nodes := mkBatch(batch)
				b.StartTimer()
				if err := bs.PutNodesBatch(nodes); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batch), "ns/row")
		})
	}
}
