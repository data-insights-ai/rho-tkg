package graph_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkLendByIDs (sigma-tkgd round 7 request 3): a parallel record scan
// over 300,000 nodes of one label: the label's IDs in batches of 4,096,
// handed to 1, 4 or 16 goroutines, each lending its batches with one
// LendByIDs call per batch ("batch") or one Lend per ID ("lend"). The rows
// are read once before timing; on badger the store's cache keeps 10,000, so
// most reads are cold. Reports ns per lent node.
func BenchmarkLendByIDs(b *testing.B) {
	const n, batch = 300_000, 4096
	ctx := context.Background()
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
		for lo := 0; lo < n; lo += 8192 {
			props := make([]map[string]any, min(8192, n-lo))
			for i := range props {
				props[i] = map[string]any{"v": int64(lo + i), "s": "name"}
			}
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error { _, err := tx.AddNodes([]string{"P"}, props); return err }); err != nil {
				b.Fatal(err)
			}
		}
		ids := make([]types.NodeID, 0, n)
		if err := g.Nodes().ForEachByLabel("P", graphpkg.QueryOpts{}, func(nd *types.Node) bool { ids = append(ids, nd.ID()); return true }); err != nil {
			b.Fatal(err)
		}
		var batches [][]types.NodeID
		for lo := 0; lo < len(ids); lo += batch {
			batches = append(batches, ids[lo:min(lo+batch, len(ids))])
		}
		for _, workers := range []int{1, 4, 16} {
			for _, mode := range []string{"lend", "batch"} {
				b.Run(fmt.Sprintf("%s/workers=%d/%s", backend.name, workers, mode), func(b *testing.B) {
					var lent atomic.Int64
					for b.Loop() {
						var next atomic.Int64
						var wg sync.WaitGroup
						for range workers {
							wg.Go(func() {
								for {
									k := int(next.Add(1)) - 1
									if k >= len(batches) {
										return
									}
									if mode == "batch" {
										if err := g.Nodes().LendByIDs(ctx, batches[k], func(int, *types.Node) bool { lent.Add(1); return true }); err != nil {
											b.Error(err)
											return
										}
										continue
									}
									for _, id := range batches[k] {
										if _, err := g.Nodes().Lend(ctx, id); err != nil {
											b.Error(err)
											return
										}
										lent.Add(1)
									}
								}
							})
						}
						wg.Wait()
					}
					if lent.Load() != int64(b.N*n) {
						b.Fatalf("lent %d rows, want %d", lent.Load(), b.N*n)
					}
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/node")
				})
			}
		}
		if err := g.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
