package graph_test

import (
	"math/rand"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkLabelScanAt: 300,000 :Event nodes without history and mixed valid
// intervals, ForEachByLabel at ValidAt 5000 (NoSort) and CountByLabelAt, without
// and with the label's valid-time index.
func BenchmarkLabelScanAt(b *testing.B) {
	for _, backend := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{SnowflakeNodeID: 1}},
		{"badger", graphpkg.Config{SnowflakeNodeID: 1, BadgerInMemory: true}},
	} {
		for _, indexed := range []bool{false, true} {
			name := backend.name + "/off"
			if indexed {
				name = backend.name + "/idx"
			}
			b.Run(name, func(b *testing.B) {
				g, err := graphpkg.New(backend.cfg)
				if err != nil {
					b.Fatal(err)
				}
				defer g.Close()
				r := rand.New(rand.NewSource(19))
				batch, err := g.Batch().New()
				if err != nil {
					b.Fatal(err)
				}
				for i := range 300_000 {
					props := map[string]any{"k": int64(i)}
					switch roll := r.Intn(10); {
					case roll < 4:
						props["tkg_valid_from"], props["tkg_valid_to"] = types.Instant(1000), types.Instant(2000)
					case roll < 6:
						props["tkg_valid_from"] = types.Instant(1000 + r.Int63n(3000))
					case roll < 8:
						props["tkg_valid_from"], props["tkg_valid_to"] = types.Instant(4000), types.Instant(4001+r.Int63n(3000))
					case roll < 9:
						props["tkg_valid_from"] = types.Instant(6000)
					}
					if _, err := batch.AddNode([]string{"Event"}, props); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := batch.Execute(); err != nil {
					b.Fatal(err)
				}
				if indexed {
					if err := g.Index().CreateTemporal("Event"); err != nil {
						b.Fatal(err)
					}
				}
				opts := graphpkg.QueryOpts{ValidAt: 5000, NoSort: true}
				want, err := g.Nodes().CountByLabelAt("Event", opts)
				if err != nil {
					b.Fatal(err)
				}
				b.Run("foreach", func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						n := 0
						if err := g.Nodes().ForEachByLabel("Event", opts, func(*types.Node) bool {
							n++
							return true
						}); err != nil || n != want {
							b.Fatalf("%d rows, want %d: %v", n, want, err)
						}
					}
				})
				b.Run("count", func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						if n, err := g.Nodes().CountByLabelAt("Event", opts); err != nil || n != want {
							b.Fatalf("%d, want %d: %v", n, want, err)
						}
					}
				})
			})
		}
	}
}
