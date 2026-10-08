package graph_test

import (
	"context"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// endpointOrdinalFixture builds 20,000 nodes and 36,000 KNOWS relationships
// (one property) in one batch.
func endpointOrdinalFixture(b *testing.B, cfg graphpkg.Config) (*graphpkg.Graph, []types.RelID) {
	b.Helper()
	g, err := graphpkg.New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = g.Close() })
	const nodes, rels = 20000, 36000
	batch, err := g.Batch().New()
	if err != nil {
		b.Fatal(err)
	}
	created := make([]*types.Node, nodes)
	for i := range created {
		if created[i], err = batch.AddNode([]string{"P"}, nil); err != nil {
			b.Fatal(err)
		}
	}
	ids := make([]*types.Relationship, 0, rels)
	for i := range rels {
		r, err := batch.AddRelationship("KNOWS", created[i%nodes], created[(i*7+1)%nodes], map[string]any{"w": int64(i)})
		if err != nil {
			b.Fatal(err)
		}
		ids = append(ids, r)
	}
	if _, err := batch.Execute(); err != nil {
		b.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	out := make([]types.RelID, len(ids))
	for i, r := range ids {
		out[i] = r.ID()
	}
	return g, out
}

// BenchmarkRelDecodeGet: badger with an 8-entry cache, so each Rels().Get of
// 36,000 relationships decodes its row: the cost a decode pays for the row's
// ordinals (its own and, since the endpoint ordinals, its endpoints').
func BenchmarkRelDecodeGet(b *testing.B) {
	g, ids := endpointOrdinalFixture(b, graphpkg.Config{SnowflakeNodeID: 0, BadgerInMemory: true, CacheCapacity: 8})
	ctx := context.Background()
	for b.Loop() {
		for _, id := range ids {
			if _, err := g.Rels().Get(ctx, id); err != nil {
				b.Fatal(err)
			}
		}
	}
}
