package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkScanColumnsTemporalOpts measures the column scans per QueryOpts shape
// (20,000 nodes and 20,000 relationships, every tenth updated, one property):
// "none" is the unchanged current-row fast path; "ValidAt" and "TxPin" the
// history-aware path (item C), next to ByLabel / ByType with the same opts, which
// is the cost the column scan now shares.
func BenchmarkScanColumnsTemporalOpts(b *testing.B) {
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
			ctx := context.Background()
			const n = 20000
			batch, err := g.Batch().New()
			if err != nil {
				b.Fatal(err)
			}
			nodes := make([]*types.Node, n)
			for i := range nodes {
				if nodes[i], err = batch.AddNode([]string{"P"}, map[string]any{"v": int64(i), "tkg_valid_from": types.Instant(1000)}); err != nil {
					b.Fatal(err)
				}
			}
			rels := make([]*types.Relationship, n)
			for i := range rels {
				if rels[i], err = batch.AddRelationship("KNOWS", nodes[i], nodes[(i*7+1)%n], map[string]any{"v": int64(i), "tkg_valid_from": types.Instant(1000)}); err != nil {
					b.Fatal(err)
				}
			}
			if _, err := batch.Execute(); err != nil {
				b.Fatal(err)
			}
			var pin types.Instant
			for i := 0; i < n; i += 10 {
				if _, err := g.Nodes().Update(ctx, nodes[i].ID(), map[string]any{"v": int64(-i), "tkg_valid_from": types.Instant(2000)}); err != nil {
					b.Fatal(err)
				}
				r, err := g.Rels().Update(ctx, rels[i].ID(), map[string]any{"v": int64(-i), "tkg_valid_from": types.Instant(2000)})
				if err != nil {
					b.Fatal(err)
				}
				pin = r.Temporal().TxFrom
			}
			for _, oc := range []struct {
				name string
				opts graphpkg.QueryOpts
			}{
				{"none", graphpkg.QueryOpts{}},
				{"ValidAt", graphpkg.QueryOpts{ValidAt: 1500}},
				{"TxPin", graphpkg.QueryOpts{TxPin: pin}},
			} {
				b.Run("ScanNodeColumns/"+oc.name, func(b *testing.B) {
					for b.Loop() {
						rows := 0
						if _, err := g.ScanNodeColumns("P", []string{"v"}, oc.opts, func(cb *graphpkg.ColumnBatch) bool {
							rows += len(cb.IDs)
							return true
						}); err != nil {
							b.Fatal(err)
						}
						if rows != n {
							b.Fatalf("rows %d, want %d", rows, n)
						}
					}
				})
				b.Run("ByLabel/"+oc.name, func(b *testing.B) {
					for b.Loop() {
						if _, err := g.Nodes().ByLabel("P", oc.opts); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("ScanRelColumns/"+oc.name, func(b *testing.B) {
					for b.Loop() {
						rows := 0
						if _, err := g.ScanRelColumns("KNOWS", []string{"v"}, oc.opts, func(rb *graphpkg.RelColumnBatch) bool {
							rows += len(rb.IDs)
							return true
						}); err != nil {
							b.Fatal(err)
						}
						if rows != n {
							b.Fatalf("rows %d, want %d", rows, n)
						}
					}
				})
				b.Run("ByType/"+oc.name, func(b *testing.B) {
					for b.Loop() {
						if _, err := g.Rels().ByType("KNOWS", oc.opts); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
