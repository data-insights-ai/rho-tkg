package graph_test

import (
	"context"
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkForEachIDOrdered (round 7 request 5): a grouping by an indexed
// property reads (ID, value) of 300,000 nodes in value order (1,000 distinct
// int64 values; the string twin over 1,000 distinct strings), through the
// ordered row door reading the value from each row ("rows") against the ID
// door ("ids"). Reports ns per node.
func BenchmarkForEachIDOrdered(b *testing.B) {
	const n = 300_000
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
		for _, key := range []string{"v", "s"} {
			if err := g.Index().CreateProperty("P", key); err != nil {
				b.Fatal(err)
			}
		}
		for lo := 0; lo < n; lo += 8192 {
			props := make([]map[string]any, min(8192, n-lo))
			for i := range props {
				props[i] = map[string]any{"v": int64((lo + i) % 1000), "s": fmt.Sprintf("k%04d", (lo+i)%1000)}
			}
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error { _, err := tx.AddNodes([]string{"P"}, props); return err }); err != nil {
				b.Fatal(err)
			}
		}
		for _, kind := range []string{"range", "prefix"} {
			for _, mode := range []string{"rows", "ids"} {
				b.Run(fmt.Sprintf("%s/%s/%s", backend.name, kind, mode), func(b *testing.B) {
					var seen int
					for b.Loop() {
						seen = 0
						var err error
						switch kind + "/" + mode {
						case "range/rows":
							err = g.Nodes().ForEachByLabelPropertyRangeOrdered("P", "v", 0, 1000, true, true, false, storepkg.QueryOpts{}, func(nd *types.Node) bool {
								_, _ = nd.GetProperty("v")
								seen++
								return true
							})
						case "range/ids":
							err = g.Nodes().ForEachIDByLabelPropertyRangeOrdered(ctx, "P", "v", 0, 1000, true, true, false, storepkg.QueryOpts{}, func(types.NodeID, any) bool { seen++; return true })
						case "prefix/rows":
							err = g.Nodes().ForEachByLabelPropertyPrefix("P", "s", "k", false, storepkg.QueryOpts{}, func(nd *types.Node) bool {
								_, _ = nd.GetProperty("s")
								seen++
								return true
							})
						case "prefix/ids":
							err = g.Nodes().ForEachIDByLabelPropertyPrefix(ctx, "P", "s", "k", false, storepkg.QueryOpts{}, func(types.NodeID, any) bool { seen++; return true })
						}
						if err != nil || seen != n {
							b.Fatalf("seen %d err %v", seen, err)
						}
					}
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/node")
				})
			}
		}
		_ = g.Close()
	}
}
