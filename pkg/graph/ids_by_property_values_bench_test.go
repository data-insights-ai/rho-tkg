package graph_test

import (
	"context"
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// BenchmarkIDsByLabelAndPropertyValuesMemory (RHOMEMIDS): sigma's read-back
// shape on the memory store. DerivedPage rows carry a list "blocks" under an
// Elements index (one posting per value) and a scalar "page" under a plain
// index; a call asks the IDs of 1 or 100 block values, at 5k, 20k and 80k
// rows, with the indexes declared before the rows or built over them.
func BenchmarkIDsByLabelAndPropertyValuesMemory(b *testing.B) {
	ctx := context.Background()
	for _, setup := range []string{"indexFirst", "indexAfterRows"} {
		for _, rows := range []int{5_000, 20_000, 80_000} {
			g, err := graphpkg.New(graphpkg.Config{})
			if err != nil {
				b.Fatal(err)
			}
			declare := func() {
				if err := g.Index().CreatePropertyWithOptions("DerivedPage", "blocks", graphpkg.PropertyIndexOptions{Elements: true}); err != nil {
					b.Fatal(err)
				}
				if err := g.Index().CreateProperty("DerivedPage", "page"); err != nil {
					b.Fatal(err)
				}
			}
			if setup == "indexFirst" {
				declare()
			}
			for i := range rows {
				base := int64(i) * 4
				if _, err := g.Nodes().Add(ctx, []string{"DerivedPage"}, map[string]any{
					"blocks": []int64{base, base + 1, base + 2, base + 3},
					"page":   int64(i),
				}); err != nil {
					b.Fatal(err)
				}
			}
			if setup == "indexAfterRows" {
				declare()
			}
			for _, perCall := range []int{1, 100} {
				b.Run(fmt.Sprintf("%s/rows=%d/values=%d", setup, rows, perCall), func(b *testing.B) {
					values := make([]any, perCall)
					b.ReportAllocs()
					b.ResetTimer()
					for i := range b.N {
						for j := range values {
							values[j] = int64((i*perCall+j)*7919) % int64(rows*4)
						}
						ids, err := g.Nodes().IDsByLabelAndPropertyValues("DerivedPage", "blocks", values, storepkg.QueryOpts{})
						if err != nil {
							b.Fatal(err)
						}
						if len(ids) == 0 {
							b.Fatal("no IDs")
						}
					}
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*perCall)/1e3, "us/value")
				})
			}
			_ = g.Close()
		}
	}
}
