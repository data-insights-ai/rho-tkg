package graph_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkRangeAtPin (round 8 request 2): a numeric range over every
// valid-time version as recorded at a pin (ValidStart 1, ValidEnd MaxInt64,
// TxAt), selecting 1% of 50,000 nodes that each have two versions (the door
// tests each node's resolved version, the newer one; backlog 16). "fold" is
// the label without a property index (every member resolved, as before);
// "sidecar" the same data under an indexed label (the membership sidecar's
// range candidates); "count" RangeCardinalityAt on the indexed label (before: RangeCardinality
// exact=false, the caller folded).
func BenchmarkRangeAtPin(b *testing.B) {
	const n = 50_000
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
		if err := g.Index().CreateProperty("Idx", "v"); err != nil {
			b.Fatal(err)
		}
		var pin types.Instant
		for _, label := range []string{"Idx", "Plain"} {
			for lo := 0; lo < n; lo += 4096 {
				props := make([]map[string]any, min(4096, n-lo))
				for i := range props {
					props[i] = map[string]any{"v": float64(lo + i), "tkg_valid_from": types.Instant(1000)}
				}
				var nodes []*types.Node
				if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
					var err error
					nodes, err = tx.AddNodes([]string{label}, props)
					return err
				}); err != nil {
					b.Fatal(err)
				}
				for i, nd := range nodes {
					up, err := g.Nodes().Update(ctx, nd.ID(), map[string]any{"v": float64(n + lo + i), "tkg_valid_from": types.Instant(2000)})
					if err != nil {
						b.Fatal(err)
					}
					pin, _, _ = up.TxStamps()
				}
			}
		}
		opts := storepkg.QueryOpts{ValidStart: 1, ValidEnd: math.MaxInt64, TxAt: pin}
		const lo, hi = n + 1000, n + 1499 // 500 resolved (second) versions; the first versions' values lie below n
		for _, mode := range []string{"fold", "sidecar", "count"} {
			b.Run(fmt.Sprintf("%s/%s", backend.name, mode), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var got int64
					switch mode {
					case "count":
						c, exact, err := g.Nodes().RangeCardinalityAt("Idx", "v", lo, hi, true, true, opts)
						if err != nil || !exact {
							b.Fatal(exact, err)
						}
						got = c
					default:
						label := "Idx"
						if mode == "fold" {
							label = "Plain"
						}
						if err := g.Nodes().ForEachByLabelPropertyRange(label, "v", lo, hi, true, true, opts, func(*types.Node) bool {
							got++
							return true
						}); err != nil {
							b.Fatal(err)
						}
					}
					if got != hi-lo+1 {
						b.Fatalf("%d rows", got)
					}
				}
			})
		}
		_ = g.Close()
	}
}
