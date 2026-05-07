package graph

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

const (
	baselineFixtureSize = 2048
	baselineBatchSize   = 100
)

type graphBaselineFixture struct {
	g         *Graph
	nodes     []*types.Node
	rels      []*types.Relationship
	nodeIDs   []types.NodeID
	relIDs    []types.RelID
	queryTime types.Instant
	relTime   types.Instant
}

func newBaselineMemoryGraph(b *testing.B) *Graph {
	b.Helper()
	g, err := New(Config{SnowflakeNodeID: 2, Store: memory.New()})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = g.Close() })
	return g
}

func newBaselineBadgerGraph(b *testing.B, syncWrites bool) *Graph {
	b.Helper()
	g, err := New(Config{
		SnowflakeNodeID: 3,
		BadgerDir:       b.TempDir(),
		SyncWrites:      syncWrites,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = g.Close() })
	return g
}

func newBaselineTieredGraph(b *testing.B) *Graph {
	b.Helper()
	ts, err := tiered.New(tiered.Config{
		InMemory:    true,
		RefLabels:   []string{"Case", "User", "Organization"},
		ShardWindow: 7 * 24 * time.Hour,
	})
	if err != nil {
		b.Fatal(err)
	}
	g, err := New(Config{SnowflakeNodeID: 4, Store: ts})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = g.Close() })
	return g
}

func baselineNodeProps(i int) map[string]any {
	return map[string]any{
		"seq":       i,
		"group":     fmt.Sprintf("g%d", i%16),
		"status":    []string{"active", "draft"}[i%2],
		"embedding": []float32{float32(i % 7), float32(i % 11), float32(i % 13)},
	}
}

func newGraphBaselineFixture(b *testing.B, g *Graph, size int) graphBaselineFixture {
	b.Helper()
	nodes := make([]*types.Node, 0, size)
	nodeIDs := make([]types.NodeID, 0, size)
	for i := 0; i < size; i++ {
		n, err := g.AddNode([]string{"Person"}, baselineNodeProps(i))
		if err != nil {
			b.Fatal(err)
		}
		nodes = append(nodes, n)
		nodeIDs = append(nodeIDs, n.ID())
	}

	rels := make([]*types.Relationship, 0, size)
	relIDs := make([]types.RelID, 0, size)
	for i := 0; i < size; i++ {
		r, err := g.AddRelationship("KNOWS", nodes[i], nodes[(i+1)%size], map[string]any{"weight": i % 5})
		if err != nil {
			b.Fatal(err)
		}
		rels = append(rels, r)
		relIDs = append(relIDs, r.ID())
	}

	if err := g.CreatePropertyIndex("Person", "group"); err != nil {
		b.Fatal(err)
	}
	if err := g.CreateTemporalIndex("Person"); err != nil {
		b.Fatal(err)
	}
	if err := g.CreateVectorIndex("Person", "embedding", 3, storepkg.DistanceEuclidean); err != nil {
		b.Fatal(err)
	}

	return graphBaselineFixture{
		g:         g,
		nodes:     nodes,
		rels:      rels,
		nodeIDs:   nodeIDs,
		relIDs:    relIDs,
		queryTime: g.nodeValidFrom(nodes[size/2]),
		relTime:   g.relValidFrom(rels[size/2]),
	}
}

func BenchmarkGraphBaseline_Reads_MemoryStore(b *testing.B) {
	f := newGraphBaselineFixture(b, newBaselineMemoryGraph(b), baselineFixtureSize)

	b.Run("GetNode", func(b *testing.B) {
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			n, err := f.g.GetNode(f.nodeIDs[i%len(f.nodeIDs)])
			if err != nil || n == nil {
				b.Fatalf("GetNode: %v", err)
			}
			i++
		}
	})

	b.Run("GetRelationship", func(b *testing.B) {
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			r, err := f.g.GetRelationship(f.relIDs[i%len(f.relIDs)])
			if err != nil || r == nil {
				b.Fatalf("GetRelationship: %v", err)
			}
			i++
		}
	})

	b.Run("NodesByLabelLimit64", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.NodesByLabel("Person", storepkg.QueryOpts{Limit: 64})
			if err != nil || len(nodes) != 64 {
				b.Fatalf("NodesByLabel: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("RelationshipsByTypeLimit64", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rels, err := f.g.RelationshipsByType("KNOWS", storepkg.QueryOpts{Limit: 64})
			if err != nil || len(rels) != 64 {
				b.Fatalf("RelationshipsByType: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("OutgoingRelationships", func(b *testing.B) {
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			rels, err := f.g.OutgoingRelationships(f.nodeIDs[i%len(f.nodeIDs)], "KNOWS")
			if err != nil || len(rels) == 0 {
				b.Fatalf("OutgoingRelationships: len=%d err=%v", len(rels), err)
			}
			i++
		}
	})

	b.Run("IncomingRelationshipsForNodes16", func(b *testing.B) {
		nodeIDs := f.nodeIDs[:16]
		b.ReportAllocs()
		for b.Loop() {
			rels, err := f.g.IncomingRelationshipsForNodes(nodeIDs, "KNOWS")
			if err != nil || len(rels) != len(nodeIDs) {
				b.Fatalf("IncomingRelationshipsForNodes: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("NodesByLabelAndPropertyIndexed", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.NodesByLabelAndProperty("Person", "group", "g7", storepkg.QueryOpts{Limit: 64})
			if err != nil || len(nodes) != 64 {
				b.Fatalf("NodesByLabelAndProperty: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("Counts", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			n, err := f.g.NodeCountByLabel("Person")
			if err != nil || n != len(f.nodes) {
				b.Fatalf("NodeCountByLabel: n=%d err=%v", n, err)
			}
			r, err := f.g.RelCountByType("KNOWS")
			if err != nil || r != len(f.rels) {
				b.Fatalf("RelCountByType: n=%d err=%v", r, err)
			}
		}
	})

	b.Run("SearchNearestNodes", func(b *testing.B) {
		query := []float32{1, 2, 3}
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.SearchNearestNodes("Person", "embedding", query, 8, storepkg.QueryOpts{})
			if err != nil || len(nodes) != 8 {
				b.Fatalf("SearchNearestNodes: len=%d err=%v", len(nodes), err)
			}
		}
	})
}

func BenchmarkGraphBaseline_Writes_MemoryStore(b *testing.B) {
	b.Run("ImportNodeWithID", func(b *testing.B) {
		g := newBaselineMemoryGraph(b)
		ids := make([]types.NodeID, b.N)
		for i := range ids {
			ids[i] = g.NextNodeID()
		}
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := g.ImportNodeWithID(ctx, ids[i], []string{"Imported"}, baselineNodeProps(i)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("UpdateNode", func(b *testing.B) {
		g := newBaselineMemoryGraph(b)
		ids := make([]types.NodeID, b.N)
		for i := range ids {
			n, err := g.AddNode([]string{"Person"}, baselineNodeProps(i))
			if err != nil {
				b.Fatal(err)
			}
			ids[i] = n.ID()
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := g.UpdateNode(ids[i], map[string]any{"status": "updated"}); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("CompareAndSetProperty", func(b *testing.B) {
		g := newBaselineMemoryGraph(b)
		ids := make([]types.NodeID, b.N)
		for i := range ids {
			n, err := g.AddNode([]string{"Person"}, map[string]any{"state": "open"})
			if err != nil {
				b.Fatal(err)
			}
			ids[i] = n.ID()
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ok, err := g.CompareAndSetProperty(ids[i], "state", "open", "closed")
			if err != nil || !ok {
				b.Fatalf("CompareAndSetProperty: ok=%v err=%v", ok, err)
			}
		}
	})

	b.Run("UpdateRelationship", func(b *testing.B) {
		g := newBaselineMemoryGraph(b)
		relIDs := make([]types.RelID, b.N)
		for i := range relIDs {
			a, _ := g.AddNode([]string{"Person"}, map[string]any{"seq": i})
			c, _ := g.AddNode([]string{"Person"}, map[string]any{"seq": i + b.N})
			r, err := g.AddRelationship("KNOWS", a, c, map[string]any{"weight": 1})
			if err != nil {
				b.Fatal(err)
			}
			relIDs[i] = r.ID()
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := g.UpdateRelationship(relIDs[i], map[string]any{"weight": 2}); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("DeleteRelationship", func(b *testing.B) {
		g := newBaselineMemoryGraph(b)
		relIDs := make([]types.RelID, b.N)
		for i := range relIDs {
			a, _ := g.AddNode([]string{"Person"}, map[string]any{"seq": i})
			c, _ := g.AddNode([]string{"Person"}, map[string]any{"seq": i + b.N})
			r, err := g.AddRelationship("KNOWS", a, c, nil)
			if err != nil {
				b.Fatal(err)
			}
			relIDs[i] = r.ID()
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := g.DeleteRelationship(relIDs[i]); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("DeleteNodeNoRels", func(b *testing.B) {
		g := newBaselineMemoryGraph(b)
		nodeIDs := make([]types.NodeID, b.N)
		for i := range nodeIDs {
			n, err := g.AddNode([]string{"Person"}, map[string]any{"seq": i})
			if err != nil {
				b.Fatal(err)
			}
			nodeIDs[i] = n.ID()
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := g.DeleteNode(nodeIDs[i]); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGraphBaseline_Temporal_MemoryStore(b *testing.B) {
	f := newGraphBaselineFixture(b, newBaselineMemoryGraph(b), baselineFixtureSize)
	versionedID := f.nodeIDs[0]
	if _, err := f.g.UpdateNode(versionedID, map[string]any{"status": "historical"}); err != nil {
		b.Fatal(err)
	}

	b.Run("GetNodeAt", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			n, err := f.g.GetNodeAt(versionedID, f.queryTime)
			if err != nil || n == nil {
				b.Fatalf("GetNodeAt: %v", err)
			}
		}
	})

	b.Run("GetNodesByLabelValidAt", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.GetNodesByLabelValidAt("Person", f.queryTime)
			if err != nil || len(nodes) == 0 {
				b.Fatalf("GetNodesByLabelValidAt: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("NodesByLabelPropertyAndTime", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.NodesByLabelPropertyAndTime("Person", "status", "active", f.queryTime)
			if err != nil || len(nodes) == 0 {
				b.Fatalf("NodesByLabelPropertyAndTime: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("GetRelationshipsValidAt", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rels, err := f.g.GetRelationshipsValidAt(f.relTime)
			if err != nil || len(rels) == 0 {
				b.Fatalf("GetRelationshipsValidAt: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("Snapshot", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			snap, err := f.g.Snapshot(f.queryTime)
			if err != nil || snap.NodeCount == 0 {
				b.Fatalf("Snapshot: nodes=%d err=%v", snap.NodeCount, err)
			}
		}
	})
}

func BenchmarkGraphBaseline_BatchAndTx_MemoryStore(b *testing.B) {
	b.Run("BatchAddNode100", func(b *testing.B) {
		g := newBaselineMemoryGraph(b)
		b.ReportAllocs()
		for b.Loop() {
			batch := NewBatchBuilder(g)
			for i := 0; i < baselineBatchSize; i++ {
				if _, err := batch.AddNode([]string{"BatchNode"}, baselineNodeProps(i)); err != nil {
					b.Fatal(err)
				}
			}
			result, err := batch.Execute()
			if err != nil || result.Failed != 0 || result.Created != baselineBatchSize {
				b.Fatalf("batch: result=%+v err=%v", result, err)
			}
		}
	})

	b.Run("TxAddNode100", func(b *testing.B) {
		g := newBaselineMemoryGraph(b)
		b.ReportAllocs()
		for b.Loop() {
			tx := g.BeginTx()
			for i := 0; i < baselineBatchSize; i++ {
				if _, err := tx.AddNode([]string{"TxNode"}, baselineNodeProps(i)); err != nil {
					_ = tx.Rollback()
					b.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGraphBaseline_StoreBackends(b *testing.B) {
	b.Run("BadgerAsyncAddNode", func(b *testing.B) {
		g := newBaselineBadgerGraph(b, false)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := g.AddNode([]string{"Person"}, baselineNodeProps(i)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("BadgerSyncAddNode", func(b *testing.B) {
		g := newBaselineBadgerGraph(b, true)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := g.AddNode([]string{"Person"}, baselineNodeProps(i)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("BadgerReadNodesByLabelLimit64", func(b *testing.B) {
		f := newGraphBaselineFixture(b, newBaselineBadgerGraph(b, false), baselineFixtureSize)
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.NodesByLabel("Person", storepkg.QueryOpts{Limit: 64})
			if err != nil || len(nodes) != 64 {
				b.Fatalf("NodesByLabel: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("TieredRefAddNode", func(b *testing.B) {
		g := newBaselineTieredGraph(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := g.AddNode([]string{"Case"}, map[string]any{"seq": i}); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("TieredEventAddNode", func(b *testing.B) {
		g := newBaselineTieredGraph(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := g.AddNode([]string{"Signal"}, map[string]any{"seq": i}); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("TieredCrossShardRelationship", func(b *testing.B) {
		g := newBaselineTieredGraph(b)
		type pair struct {
			start *types.Node
			end   *types.Node
		}
		pairs := make([]pair, b.N)
		for i := range pairs {
			start, err := g.AddNode([]string{"Signal"}, map[string]any{"seq": i})
			if err != nil {
				b.Fatal(err)
			}
			end, err := g.AddNode([]string{"Case"}, map[string]any{"seq": i})
			if err != nil {
				b.Fatal(err)
			}
			pairs[i] = pair{start: start, end: end}
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := g.AddRelationship("ABOUT", pairs[i].start, pairs[i].end, map[string]any{"weight": 1}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
