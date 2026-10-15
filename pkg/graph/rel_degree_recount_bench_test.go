package graph_test

import (
	"context"
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkRelTypeDegreeStatsRecount (round 7 request 2, the reopen
// criterion of round 5 request 5): RelTypeDegreeStats of type T0 on a graph of
// three types of n relationships each (n/4 start and n/4 end nodes per type),
// per backend: "cached" (no write since the last call), "after-own-write"
// (one relationship of T0 created before each call: the recount the
// maintained counts would remove), "after-other-write" (one of T1: the
// per-type stamp keeps the cached stats) and "tracked-after-own-write" (T0
// opted in with Stats().TrackRelTypeDegrees: the create is applied to the
// kept counts). ns/op is one write plus one call.
func BenchmarkRelTypeDegreeStatsRecount(b *testing.B) {
	ctx := context.Background()
	for _, backend := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{SnowflakeNodeID: 0}},
		{"badger", graphpkg.Config{SnowflakeNodeID: 0, BadgerInMemory: true}},
	} {
		for _, n := range []int{10_000, 100_000} {
			g, err := graphpkg.New(backend.cfg)
			if err != nil {
				b.Fatal(err)
			}
			props := make([]map[string]any, n/2)
			for i := range props {
				props[i] = map[string]any{"i": int64(i)}
			}
			var nodes []*types.Node
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				var err error
				nodes, err = tx.AddNodes([]string{"N"}, props)
				return err
			}); err != nil {
				b.Fatal(err)
			}
			for typ := range 3 {
				for lo := 0; lo < n; lo += 8192 {
					specs := make([]graphpkg.RelCreate, 0, 8192)
					for i := lo; i < min(lo+8192, n); i++ {
						specs = append(specs, graphpkg.RelCreate{StartID: nodes[i%(n/4)].ID(), EndID: nodes[n/4+(i*7)%(n/4)].ID()})
					}
					if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
						_, err := tx.AddRelationships(fmt.Sprintf("T%d", typ), specs)
						return err
					}); err != nil {
						b.Fatal(err)
					}
				}
			}
			for _, mode := range []string{"cached", "after-own-write", "after-other-write", "tracked-after-own-write"} {
				b.Run(fmt.Sprintf("%s/rels=3x%d/%s", backend.name, n, mode), func(b *testing.B) {
					if mode == "tracked-after-own-write" {
						if err := g.Stats().TrackRelTypeDegrees("T0"); err != nil {
							b.Fatal(err)
						}
					}
					if _, err := g.Stats().RelTypeDegreeStats("T0"); err != nil {
						b.Fatal(err)
					}
					i := 0
					for b.Loop() {
						i++
						switch mode {
						case "after-own-write", "after-other-write", "tracked-after-own-write":
							typ := "T0"
							if mode == "after-other-write" {
								typ = "T1"
							}
							if _, err := g.Rels().Add(ctx, typ, nodes[i%(n/4)], nodes[n/4+i%(n/4)], nil); err != nil {
								b.Fatal(err)
							}
						}
						st, err := g.Stats().RelTypeDegreeStats("T0")
						if err != nil || st.Rels < int64(n) {
							b.Fatalf("stats %+v err %v", st, err)
						}
					}
				})
			}
			if err := g.Close(); err != nil {
				b.Fatal(err)
			}
		}
	}
}
