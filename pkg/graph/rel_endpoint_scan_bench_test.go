package graph_test

import (
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkRelEndpointColumnScan: the endpoint column scan of a star type
// (64 hubs, 4,096 spokes each: 262,144 relationships), no property, no filter
// — the pass sigma-tkgd's counted scan makes over a type's endpoints
// (StarCandidatesSkewedCold, round 6 request 10). "valid-at" reads the same
// type at an instant every row covers.
func BenchmarkRelEndpointColumnScan(b *testing.B) {
	const hubs, spokes = 64, 4096
	for _, backend := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{SnowflakeNodeID: 0}},
		{"badger", graphpkg.Config{SnowflakeNodeID: 0, BadgerInMemory: true}},
	} {
		b.Run(backend.name, func(b *testing.B) {
			g, err := graphpkg.New(backend.cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer g.Close()
			batch, err := g.Batch().New()
			if err != nil {
				b.Fatal(err)
			}
			hubNodes := make([]*types.Node, hubs)
			for i := range hubNodes {
				if hubNodes[i], err = batch.AddNode([]string{"Hub"}, nil); err != nil {
					b.Fatal(err)
				}
			}
			for h := range hubs {
				for range spokes {
					s, err := batch.AddNode([]string{"Spoke"}, nil)
					if err != nil {
						b.Fatal(err)
					}
					if _, err := batch.AddRelationship("SPOKE", hubNodes[h], s, nil); err != nil {
						b.Fatal(err)
					}
				}
			}
			if _, err := batch.Execute(); err != nil {
				b.Fatal(err)
			}
			for _, arm := range []struct {
				name string
				opts graphpkg.QueryOpts
			}{
				{"unfiltered", graphpkg.QueryOpts{}},
				{"valid-at", graphpkg.QueryOpts{ValidAt: types.Instant(1) << 52}},
			} {
				b.Run(arm.name, func(b *testing.B) {
					scan := func() {
						var rows int
						var sum uint64
						ok, err := g.ScanRelColumns("SPOKE", nil, arm.opts, func(rb *graphpkg.RelColumnBatch) bool {
							for i := range rb.IDs {
								sum += uint64(rb.StartOrdinals[i]) + uint64(rb.EndOrdinals[i])
							}
							rows += len(rb.IDs)
							return true
						})
						if err != nil || !ok || rows != hubs*spokes {
							b.Fatalf("%d rows, %v %v", rows, ok, err)
						}
						_ = sum
					}
					scan() // the snapshot is built once; the loop measures the scan
					b.ReportAllocs()
					for b.Loop() {
						scan()
					}
				})
			}
		})
	}
}
