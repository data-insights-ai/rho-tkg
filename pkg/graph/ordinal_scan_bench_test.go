package graph_test

import (
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkForEachByOrdinals (round 8 request 4): 100,000 nodes of one label,
// a bitset selecting 1% of their ordinals. "filter" is the scan a caller had
// (ForEachByLabel, dropping rows outside the set), "ordinals" the restricted
// door; current rows and at a transaction pin (TxPin).
func BenchmarkForEachByOrdinals(b *testing.B) {
	const n = 100_000
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
				props[i] = map[string]any{"v": int64(lo + i)}
			}
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error { _, err := tx.AddNodes([]string{"P"}, props); return err }); err != nil {
				b.Fatal(err)
			}
		}
		maxOrd, _, err := g.Nodes().MaxOrdinal()
		if err != nil {
			b.Fatal(err)
		}
		set := make([]uint64, maxOrd/64+1)
		for o := uint32(100); o <= maxOrd; o += 100 {
			set[o/64] |= 1 << (o % 64)
		}
		var pin types.Instant
		if err := g.Nodes().ForEachByLabel("P", storepkg.QueryOpts{}, func(nd *types.Node) bool {
			pin, _, _ = nd.TxStamps()
			return true
		}); err != nil {
			b.Fatal(err)
		}
		for _, coord := range []struct {
			name string
			opts storepkg.QueryOpts
		}{{"current", storepkg.QueryOpts{}}, {"txpin", storepkg.QueryOpts{TxPin: pin}}} {
			for _, mode := range []string{"filter", "ordinals"} {
				b.Run(fmt.Sprintf("%s/%s/%s", backend.name, coord.name, mode), func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						got := 0
						var err error
						if mode == "filter" {
							err = g.Nodes().ForEachByLabel("P", coord.opts, func(nd *types.Node) bool {
								if storepkg.OrdinalSetHas(set, nd.Ordinal()) {
									got++
								}
								return true
							})
						} else {
							err = g.Nodes().ForEachByLabelOrdinals("P", set, coord.opts, func(*types.Node) bool {
								got++
								return true
							})
						}
						if err != nil || got != int(maxOrd/100) {
							b.Fatal(got, err)
						}
					}
				})
			}
		}
		_ = g.Close()
	}
}
