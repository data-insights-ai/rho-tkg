package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkTxCreateRelationships: 1,000 relationships of one type with three
// properties between 1,000 existing nodes, created inside one transaction
// (committed), per backend: one AddRelationshipByID per relationship against
// one AddRelationships call. The shape of UNWIND ... CREATE (a)-[:T]->(b)
// inside a statement's transaction (sigma-tkgd, the adoption of v4.42.0, open
// question 3).
func BenchmarkTxCreateRelationships(b *testing.B) {
	const n = 1000
	for _, backend := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{SnowflakeNodeID: 0}},
		{"badger", graphpkg.Config{SnowflakeNodeID: 0, BadgerInMemory: true}},
	} {
		for _, mode := range []string{"AddRelationshipByID", "AddRelationships"} {
			b.Run(backend.name+"/"+mode, func(b *testing.B) {
				g, err := graphpkg.New(backend.cfg)
				if err != nil {
					b.Fatal(err)
				}
				defer g.Close()
				ids := make([]types.NodeID, n)
				for i := range ids {
					nd, err := g.Nodes().Add(context.Background(), []string{"Person"}, nil)
					if err != nil {
						b.Fatal(err)
					}
					ids[i] = nd.ID()
				}
				specs := make([]graphpkg.RelCreate, n)
				for i := range specs {
					specs[i] = graphpkg.RelCreate{
						StartID: ids[i],
						EndID:   ids[(i*7+1)%n],
						Props:   map[string]any{"i": int64(i), "name": "a name", "score": float64(i) / 2},
					}
				}
				b.ReportAllocs()
				for b.Loop() {
					err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
						if mode == "AddRelationships" {
							_, err := tx.AddRelationships("KNOWS", specs)
							return err
						}
						for _, s := range specs {
							if _, err := tx.AddRelationshipByID("KNOWS", s.StartID, s.EndID, s.Props); err != nil {
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
