package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

// BenchmarkPropertyIndexElementsWrite (backlog 63): the write cost of one
// Derived row carrying ov_support with 5 rel IDs, under no index, a plain
// index on a scalar key of the same row, and an Elements index on ov_support.
func BenchmarkPropertyIndexElementsWrite(b *testing.B) {
	ctx := context.Background()
	for _, store := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{}},
		{"badger", graphpkg.Config{BadgerInMemory: true}},
	} {
		for _, mode := range []string{"none", "scalar", "elements"} {
			b.Run(store.name+"/"+mode, func(b *testing.B) {
				g, err := graphpkg.New(store.cfg)
				if err != nil {
					b.Fatal(err)
				}
				defer g.Close()
				switch mode {
				case "scalar":
					err = g.Index().CreateProperty("Derived", "rel")
				case "elements":
					err = g.Index().CreatePropertyWithOptions("Derived", "ov_support", graphpkg.PropertyIndexOptions{Elements: true})
				}
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					base := int64(i) * 5
					props := map[string]any{
						"ov_support": []int64{base, base + 1, base + 2, base + 3, base + 4},
						"rel":        base,
					}
					if _, err := g.Nodes().Add(ctx, []string{"Derived"}, props); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
