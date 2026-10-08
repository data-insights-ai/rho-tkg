package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkRelEndpointOrdinals: a CSR-style build by node ordinal (each
// relationship's (start ordinal, end ordinal) pair appended, the out-degree
// counted) over 36,000 relationships of 20,000 nodes, per backend. Before:
// the endpoints' ordinals by a Lend per endpoint, after the column scan
// (columns+lend) or the row scan (rows+lend). After: the column batch's
// StartOrdinals/EndOrdinals (columns+ordinals) or the rows' StartOrdinal /
// EndOrdinal (rows+ordinals).
func BenchmarkRelEndpointOrdinals(b *testing.B) {
	for _, backend := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{SnowflakeNodeID: 0}},
		{"badger", graphpkg.Config{SnowflakeNodeID: 0, BadgerInMemory: true}},
	} {
		b.Run(backend.name, func(b *testing.B) {
			g, ids := endpointOrdinalFixture(b, backend.cfg)
			ctx := context.Background()
			lend := func(id types.NodeID) uint32 {
				n, err := g.Nodes().Lend(ctx, id)
				if err != nil {
					b.Fatal(err)
				}
				return n.Ordinal()
			}
			build := func(b *testing.B, fill func(add func(s, e uint32)) int) {
				pairs := make([][2]uint32, 0, len(ids))
				deg := make([]uint32, 20001+len(ids))
				for b.Loop() {
					pairs = pairs[:0]
					clear(deg)
					seen := fill(func(s, e uint32) {
						pairs = append(pairs, [2]uint32{s, e})
						deg[s]++
					})
					if seen != len(ids) {
						b.Fatalf("%d rows", seen)
					}
				}
			}
			columns := func(b *testing.B, each func(rb *graphpkg.RelColumnBatch, i int, add func(s, e uint32))) {
				build(b, func(add func(s, e uint32)) int {
					seen := 0
					ok, err := g.ScanRelColumns("KNOWS", []string{"w"}, graphpkg.QueryOpts{}, func(rb *graphpkg.RelColumnBatch) bool {
						for i := range rb.IDs {
							each(rb, i, add)
						}
						seen += len(rb.IDs)
						return true
					})
					if err != nil || !ok {
						b.Fatal(ok, err)
					}
					return seen
				})
			}
			rows := func(b *testing.B, each func(r *types.Relationship, add func(s, e uint32))) {
				build(b, func(add func(s, e uint32)) int {
					seen := 0
					if err := g.Rels().ForEachByType("KNOWS", graphpkg.QueryOpts{}, func(r *types.Relationship) bool {
						each(r, add)
						seen++
						return true
					}); err != nil {
						b.Fatal(err)
					}
					return seen
				})
			}
			b.Run("columns+lend", func(b *testing.B) {
				columns(b, func(rb *graphpkg.RelColumnBatch, i int, add func(s, e uint32)) {
					add(lend(rb.StartIDs[i]), lend(rb.EndIDs[i]))
				})
			})
			b.Run("columns+ordinals", func(b *testing.B) {
				columns(b, func(rb *graphpkg.RelColumnBatch, i int, add func(s, e uint32)) {
					add(rb.StartOrdinals[i], rb.EndOrdinals[i])
				})
			})
			b.Run("rows+lend", func(b *testing.B) {
				rows(b, func(r *types.Relationship, add func(s, e uint32)) {
					add(lend(r.StartNodeID()), lend(r.EndNodeID()))
				})
			})
			b.Run("rows+ordinals", func(b *testing.B) {
				rows(b, func(r *types.Relationship, add func(s, e uint32)) {
					add(r.StartOrdinal(), r.EndOrdinal())
				})
			})
		})
	}
}
