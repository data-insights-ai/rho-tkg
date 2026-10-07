package graph_test

import (
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

// BenchmarkTxCreateNodes: 1,000 nodes of one label with three properties
// created inside one transaction (committed), per backend: one AddNode per
// node against one AddNodes call. The shape of UNWIND ... CREATE inside a
// statement's transaction (sigma-tkgd C5a item 2c, C5b store request 1).
func BenchmarkTxCreateNodes(b *testing.B) {
	const n = 1000
	props := make([]map[string]any, n)
	for i := range props {
		props[i] = map[string]any{"i": int64(i), "name": "a name", "score": float64(i) / 2}
	}
	for _, backend := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{SnowflakeNodeID: 0}},
		{"badger", graphpkg.Config{SnowflakeNodeID: 0, BadgerInMemory: true}},
	} {
		for _, mode := range []string{"AddNode", "AddNodes"} {
			b.Run(backend.name+"/"+mode, func(b *testing.B) {
				g, err := graphpkg.New(backend.cfg)
				if err != nil {
					b.Fatal(err)
				}
				defer g.Close()
				b.ReportAllocs()
				for b.Loop() {
					err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
						if mode == "AddNodes" {
							_, err := tx.AddNodes([]string{"Person"}, props)
							return err
						}
						for _, p := range props {
							if _, err := tx.AddNode([]string{"Person"}, p); err != nil {
								return err
							}
						}
						return nil
					})
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
