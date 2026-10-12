package graph_test

import (
	"context"
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkColumnScanAfterPropertyWrite: a badger label of n :P nodes with
// an int "v" and a string "city"; before each timed scan (timer stopped) one
// node holding neither extreme gets a new "t" value (sigma-tkgd's cold
// harness touch, round 5 request 4: the scan then rebuilt the label's whole
// column snapshot, 4.5 s on 1,000,000 nodes). The timed part is a
// ScanNodeColumns over (v, city) summing v.
func BenchmarkColumnScanAfterPropertyWrite(b *testing.B) {
	for _, n := range []int{100_000, 400_000} {
		b.Run(fmt.Sprintf("badger/n=%d", n), func(b *testing.B) {
			g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 0, BadgerInMemory: true})
			if err != nil {
				b.Fatal(err)
			}
			defer g.Close()
			ctx := context.Background()
			batch, err := g.Batch().New()
			if err != nil {
				b.Fatal(err)
			}
			var mid *types.Node
			for i := range n {
				nd, err := batch.AddNode([]string{"P"}, map[string]any{"v": int64(i), "city": fmt.Sprintf("c%d", i%2000)})
				if err != nil {
					b.Fatal(err)
				}
				if i == n/2 {
					mid = nd
				}
			}
			if _, err := batch.Execute(); err != nil {
				b.Fatal(err)
			}
			scan := func() {
				var sum int64
				rows := 0
				if _, err := g.ScanNodeColumns("P", []string{"v", "city"}, storepkg.QueryOpts{}, func(cb *storepkg.ColumnBatch) bool {
					for _, v := range cb.Ints[0] {
						sum += v
					}
					rows += len(cb.IDs)
					return true
				}); err != nil {
					b.Fatal(err)
				}
				if rows != n {
					b.Fatalf("%d rows", rows)
				}
			}
			scan() // build the snapshot once
			tick := int64(0)
			for b.Loop() {
				b.StopTimer()
				tick++
				if err := g.Nodes().SetProperty(ctx, mid.ID(), "t", tick); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				scan()
			}
		})
	}
}
