package graph

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

const (
	defaultProductionNodeCount          = 10000
	defaultProductionFanout             = 5
	defaultProductionHubDegree          = 1000
	defaultProductionHistoryNodes       = 128
	defaultProductionHistoryDays        = 30
	defaultProductionExportNodes        = 2000
	defaultProductionExportFanout       = 2
	defaultProductionBatchNodeSize      = 256
	defaultProductionTieredCases        = 1024
	defaultProductionTieredWarmSignals  = 2048
	defaultProductionTieredHotSignals   = 2048
	defaultProductionPublicSurfaceNodes = 2048
)

type graphProductionFixture struct {
	g          *Graph
	nodes      []*types.Node
	nodeIDs    []snowflake.ID
	relIDs     []snowflake.ID
	hubID      snowflake.ID
	groupValue string
}

func benchEnvInt(b *testing.B, name string, def, min, max int) int {
	b.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		b.Fatalf("%s must be an integer, got %q", name, raw)
	}
	if value < min || value > max {
		b.Fatalf("%s=%d outside supported range [%d,%d]", name, value, min, max)
	}
	return value
}

func productionNodeCount(b *testing.B) int {
	return benchEnvInt(b, "TKG_BENCH_NODES", defaultProductionNodeCount, 1, 1_000_000)
}

func productionFanout(b *testing.B) int {
	return benchEnvInt(b, "TKG_BENCH_FANOUT", defaultProductionFanout, 1, 100)
}

func productionHubDegree(b *testing.B) int {
	return benchEnvInt(b, "TKG_BENCH_HUB_DEGREE", defaultProductionHubDegree, 1, 100_000)
}

func productionHistoryNodes(b *testing.B) int {
	return benchEnvInt(b, "TKG_BENCH_HISTORY_NODES", defaultProductionHistoryNodes, 2, 10_000)
}

func productionHistoryDays(b *testing.B) int {
	return benchEnvInt(b, "TKG_BENCH_HISTORY_DAYS", defaultProductionHistoryDays, 1, 10_000)
}

func productionExportNodes(b *testing.B) int {
	return benchEnvInt(b, "TKG_BENCH_EXPORT_NODES", defaultProductionExportNodes, 1, 100_000)
}

func productionExportFanout(b *testing.B) int {
	return benchEnvInt(b, "TKG_BENCH_EXPORT_FANOUT", defaultProductionExportFanout, 1, 50)
}

func productionBatchNodeSize(b *testing.B) int {
	return benchEnvInt(b, "TKG_BENCH_BATCH_NODES", defaultProductionBatchNodeSize, 1, 10_000)
}

func productionTieredCases(b *testing.B) int {
	return benchEnvInt(b, "TKG_BENCH_TIERED_CASES", defaultProductionTieredCases, 1, 100_000)
}

func productionTieredWarmSignals(b *testing.B) int {
	return benchEnvInt(b, "TKG_BENCH_TIERED_WARM_SIGNALS", defaultProductionTieredWarmSignals, 1, 1_000_000)
}

func productionTieredHotSignals(b *testing.B) int {
	return benchEnvInt(b, "TKG_BENCH_TIERED_HOT_SIGNALS", defaultProductionTieredHotSignals, 1, 1_000_000)
}

func productionPublicSurfaceNodes(b *testing.B) int {
	return benchEnvInt(b, "TKG_BENCH_SURFACE_NODES", defaultProductionPublicSurfaceNodes, 1, 100_000)
}

func productionProps(i int) map[string]any {
	return map[string]any{
		"seq":       i,
		"group":     fmt.Sprintf("tenant-%03d", i%128),
		"status":    []string{"active", "review", "archived", "blocked"}[i%4],
		"score":     i % 1000,
		"embedding": []float32{float32(i % 17), float32(i % 23), float32(i % 29), float32(i % 31)},
	}
}

func newGraphProductionFixture(b *testing.B, nodeCount, fanout, hubDegree int) graphProductionFixture {
	b.Helper()
	if nodeCount < hubDegree+1 {
		nodeCount = hubDegree + 1
	}
	g := newBaselineMemoryGraph(b)

	nodes := make([]*types.Node, 0, nodeCount)
	nodeIDs := make([]snowflake.ID, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		labels := []string{"Person"}
		if i%10 == 0 {
			labels = []string{"Person", "Customer"}
		}
		n, err := g.AddNode(labels, productionProps(i))
		if err != nil {
			b.Fatal(err)
		}
		nodes = append(nodes, n)
		nodeIDs = append(nodeIDs, n.InternalID().SnowflakeID())
	}

	relIDs := make([]snowflake.ID, 0, nodeCount*fanout+hubDegree)
	for i := 0; i < nodeCount; i++ {
		for j := 1; j <= fanout; j++ {
			r, err := g.AddRelationship("FOLLOWS", nodes[i], nodes[(i+j)%nodeCount], map[string]any{
				"weight": j,
				"kind":   "regular",
			})
			if err != nil {
				b.Fatal(err)
			}
			relIDs = append(relIDs, r.InternalID().SnowflakeID())
		}
	}
	for i := 1; i <= hubDegree; i++ {
		r, err := g.AddRelationship("MENTIONS", nodes[0], nodes[i], map[string]any{
			"weight": i % 7,
			"kind":   "hub",
		})
		if err != nil {
			b.Fatal(err)
		}
		relIDs = append(relIDs, r.InternalID().SnowflakeID())
	}

	if err := g.CreatePropertyIndex("Person", "group"); err != nil {
		b.Fatal(err)
	}
	if err := g.CreateTemporalIndex("Person"); err != nil {
		b.Fatal(err)
	}
	if err := g.CreateVectorIndex("Person", "embedding", 4, DistanceEuclidean); err != nil {
		b.Fatal(err)
	}

	return graphProductionFixture{
		g:          g,
		nodes:      nodes,
		nodeIDs:    nodeIDs,
		relIDs:     relIDs,
		hubID:      nodeIDs[0],
		groupValue: "tenant-007",
	}
}

func BenchmarkGraphProduction_LargeGraphReads_MemoryStore(b *testing.B) {
	hubDegree := productionHubDegree(b)
	f := newGraphProductionFixture(b,
		productionNodeCount(b),
		productionFanout(b),
		hubDegree,
	)

	b.Run("NodesByLabelPage1000", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.NodesByLabel("Person", QueryOpts{Limit: 1000})
			if err != nil || len(nodes) != 1000 {
				b.Fatalf("NodesByLabel: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("RelationshipsByTypePage1000", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rels, err := f.g.RelationshipsByType("FOLLOWS", QueryOpts{Limit: 1000})
			if err != nil || len(rels) != 1000 {
				b.Fatalf("RelationshipsByType: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("AllNodesPage1024", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.AllNodes(QueryOpts{Limit: 1024})
			if err != nil || len(nodes) != 1024 {
				b.Fatalf("AllNodes: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("AllRelationshipsPage1024", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rels, err := f.g.AllRelationships(QueryOpts{Limit: 1024})
			if err != nil || len(rels) != 1024 {
				b.Fatalf("AllRelationships: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("GetNodesByIDs1000", func(b *testing.B) {
		ids := f.nodeIDs[:1000]
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.GetNodesByIDs(ids)
			if err != nil || len(nodes) != len(ids) {
				b.Fatalf("GetNodesByIDs: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("GetRelationshipsByIDs1000", func(b *testing.B) {
		ids := f.relIDs[:1000]
		b.ReportAllocs()
		for b.Loop() {
			rels, err := f.g.GetRelationshipsByIDs(ids)
			if err != nil || len(rels) != len(ids) {
				b.Fatalf("GetRelationshipsByIDs: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("HighDegreeOutgoingHub", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rels, err := f.g.OutgoingRelationships(f.hubID, "")
			if err != nil || len(rels) < hubDegree {
				b.Fatalf("OutgoingRelationships: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("OutgoingRelationshipsForNodes128", func(b *testing.B) {
		ids := f.nodeIDs[:128]
		b.ReportAllocs()
		for b.Loop() {
			rels, err := f.g.OutgoingRelationshipsForNodes(ids, "FOLLOWS")
			if err != nil || len(rels) != len(ids) {
				b.Fatalf("OutgoingRelationshipsForNodes: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("IncomingRelationshipsForNodes128", func(b *testing.B) {
		ids := f.nodeIDs[1:129]
		b.ReportAllocs()
		for b.Loop() {
			rels, err := f.g.IncomingRelationshipsForNodes(ids, "FOLLOWS")
			if err != nil || len(rels) != len(ids) {
				b.Fatalf("IncomingRelationshipsForNodes: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("PropertyIndexTenant", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.NodesByLabelAndProperty("Person", "group", f.groupValue, QueryOpts{Limit: 64})
			if err != nil || len(nodes) != 64 {
				b.Fatalf("NodesByLabelAndProperty: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("VectorKNN", func(b *testing.B) {
		query := []float32{1, 2, 3, 4}
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.SearchNearestNodes("Person", "embedding", query, 20, QueryOpts{})
			if err != nil || len(nodes) != 20 {
				b.Fatalf("SearchNearestNodes: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("StatsAndCounts", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = f.g.Stats()
			if _, err := f.g.AllLabelCounts(); err != nil {
				b.Fatal(err)
			}
			if _, err := f.g.AllRelTypeCounts(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

type graphHistoryProductionFixture struct {
	g          *Graph
	nodeIDs    []snowflake.ID
	relIDs     []snowflake.ID
	queryTime  types.Instant
	asOfTime   types.Instant
	relAsOf    types.Instant
	updatedID  snowflake.ID
	updatedRel snowflake.ID
	historyLen int
}

func newGraphHistoryProductionFixture(b *testing.B) graphHistoryProductionFixture {
	b.Helper()
	g := newBaselineMemoryGraph(b)
	nodeCount := productionHistoryNodes(b)
	historyDays := productionHistoryDays(b)
	nodes := make([]*types.Node, 0, nodeCount)
	nodeIDs := make([]snowflake.ID, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		n, err := g.AddNode([]string{"Account"}, map[string]any{
			"account": i,
			"day":     0,
			"balance": i * 100,
		})
		if err != nil {
			b.Fatal(err)
		}
		nodes = append(nodes, n)
		nodeIDs = append(nodeIDs, n.InternalID().SnowflakeID())
	}

	relIDs := make([]snowflake.ID, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		r, err := g.AddRelationship("ACCOUNT_LINK", nodes[i], nodes[(i+1)%nodeCount], map[string]any{
			"day":    0,
			"amount": i * 10,
		})
		if err != nil {
			b.Fatal(err)
		}
		relIDs = append(relIDs, r.InternalID().SnowflakeID())
	}
	if err := g.CreatePropertyIndex("Account", "account"); err != nil {
		b.Fatal(err)
	}
	if err := g.CreateTemporalIndex("Account"); err != nil {
		b.Fatal(err)
	}

	queryTime := types.Instant(time.Now().UnixMilli())
	time.Sleep(2 * time.Millisecond)

	for day := 1; day <= historyDays; day++ {
		for i, id := range nodeIDs {
			if _, err := g.UpdateNode(id, map[string]any{
				"day":     day,
				"balance": i*100 + day,
			}); err != nil {
				b.Fatal(err)
			}
		}
		for i, id := range relIDs {
			if _, err := g.UpdateRelationship(id, map[string]any{
				"day":    day,
				"amount": i*10 + day,
			}); err != nil {
				b.Fatal(err)
			}
		}
	}

	current, err := g.GetNode(nodeIDs[0])
	if err != nil {
		b.Fatal(err)
	}
	asOf := current.Temporal().TxFrom
	currentRel, err := g.GetRelationship(relIDs[0])
	if err != nil {
		b.Fatal(err)
	}

	return graphHistoryProductionFixture{
		g:          g,
		nodeIDs:    nodeIDs,
		relIDs:     relIDs,
		queryTime:  queryTime,
		asOfTime:   asOf,
		relAsOf:    currentRel.Temporal().TxFrom,
		updatedID:  nodeIDs[0],
		updatedRel: relIDs[0],
		historyLen: historyDays,
	}
}

func BenchmarkGraphProduction_HistoricalDailyUpdates_MemoryStore(b *testing.B) {
	f := newGraphHistoryProductionFixture(b)

	b.Run("GetNodeHistoryVersions", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			history, err := f.g.GetNodeHistory(f.updatedID)
			if err != nil || len(history) != f.historyLen {
				b.Fatalf("GetNodeHistory: len=%d err=%v", len(history), err)
			}
		}
	})

	b.Run("GetNodeAtGenesis", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			n, err := f.g.GetNodeAt(f.updatedID, f.queryTime)
			if err != nil || n == nil {
				b.Fatalf("GetNodeAt: %v", err)
			}
		}
	})

	b.Run("GetNodeAsOfCurrent", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			n, err := f.g.GetNodeAsOf(f.updatedID, f.asOfTime)
			if err != nil || n == nil {
				b.Fatalf("GetNodeAsOf: %v", err)
			}
		}
	})

	b.Run("NodesByLabelValidAtGenesis", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.NodesByLabel("Account", QueryOpts{ValidAt: f.queryTime, Limit: len(f.nodeIDs)})
			if err != nil || len(nodes) != len(f.nodeIDs) {
				b.Fatalf("NodesByLabel ValidAt: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("NodesByLabelAndPropertyValidAtGenesis", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.NodesByLabelAndProperty("Account", "account", 0, QueryOpts{ValidAt: f.queryTime, Limit: 1})
			if err != nil || len(nodes) != 1 {
				b.Fatalf("NodesByLabelAndProperty ValidAt: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("GetNodesByLabelValidAtGenesis", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.GetNodesByLabelValidAt("Account", f.queryTime)
			if err != nil || len(nodes) != len(f.nodeIDs) {
				b.Fatalf("GetNodesByLabelValidAt: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("GetNodesValidAtGenesis", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.GetNodesValidAt(f.queryTime)
			if err != nil || len(nodes) == 0 {
				b.Fatalf("GetNodesValidAt: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("GetNodesAsOfCurrent", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.GetNodesAsOf(f.asOfTime)
			if err != nil || len(nodes) == 0 {
				b.Fatalf("GetNodesAsOf: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("GetRelationshipHistoryVersions", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			history, err := f.g.GetRelHistory(f.updatedRel)
			if err != nil || len(history) != f.historyLen {
				b.Fatalf("GetRelHistory: len=%d err=%v", len(history), err)
			}
		}
	})

	b.Run("GetRelationshipAtGenesis", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			r, err := f.g.GetRelAt(f.updatedRel, f.queryTime)
			if err != nil || r == nil {
				b.Fatalf("GetRelAt: %v", err)
			}
		}
	})

	b.Run("GetRelAsOfCurrent", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			r, err := f.g.GetRelAsOf(f.updatedRel, f.relAsOf)
			if err != nil || r == nil {
				b.Fatalf("GetRelAsOf: %v", err)
			}
		}
	})

	b.Run("RelationshipsByTypeValidAtGenesis", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rels, err := f.g.RelationshipsByType("ACCOUNT_LINK", QueryOpts{ValidAt: f.queryTime, Limit: len(f.relIDs)})
			if err != nil || len(rels) != len(f.relIDs) {
				b.Fatalf("RelationshipsByType ValidAt: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("GetRelationshipsValidAtGenesis", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rels, err := f.g.GetRelationshipsValidAt(f.queryTime)
			if err != nil || len(rels) == 0 {
				b.Fatalf("GetRelationshipsValidAt: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("GetRelsAsOfCurrent", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rels, err := f.g.GetRelsAsOf(f.relAsOf)
			if err != nil || len(rels) == 0 {
				b.Fatalf("GetRelsAsOf: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("SnapshotGenesis", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			snapshot, err := f.g.Snapshot(f.queryTime)
			if err != nil || snapshot.NodeCount == 0 {
				b.Fatalf("Snapshot: nodes=%d err=%v", snapshot.NodeCount, err)
			}
		}
	})
}

func BenchmarkGraphProduction_ExportImport_MemoryStore(b *testing.B) {
	f := newGraphProductionFixture(b, productionExportNodes(b), productionExportFanout(b), 256)
	var exportBytes bytes.Buffer
	if err := f.g.ExportGraph(&exportBytes); err != nil {
		b.Fatal(err)
	}
	payload := exportBytes.Bytes()

	b.Run("ExportGraph", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var out bytes.Buffer
			if err := f.g.ExportGraph(&out); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("ImportGraph", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			g, err := New(Config{Store: NewMemoryStore()})
			if err != nil {
				b.Fatal(err)
			}
			if err := g.ImportGraph(bytes.NewReader(payload)); err != nil {
				_ = g.Close()
				b.Fatal(err)
			}
			_ = g.Close()
		}
	})
}

func BenchmarkGraphProduction_Events_MemoryStore(b *testing.B) {
	b.Run("SyncEventBusAddNode", func(b *testing.B) {
		g := newBaselineMemoryGraph(b)
		bus := NewEventBus()
		var count atomic.Int64
		bus.Subscribe(func(Event) {
			count.Add(1)
		})
		g.SetEventBus(bus)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := g.AddNode([]string{"EventNode"}, map[string]any{"seq": i}); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("AsyncEventBusAddNodeDropLatest", func(b *testing.B) {
		g := newBaselineMemoryGraph(b)
		bus := NewAsyncEventBus(AsyncEventBusConfig{
			Workers:      4,
			QueueSize:    8192,
			Backpressure: BackpressureDropLatest,
		})
		b.Cleanup(func() { bus.Close() })
		bus.Subscribe(func(Event) {})
		g.SetAsyncEventBus(bus)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := g.AddNode([]string{"AsyncEventNode"}, map[string]any{"seq": i}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGraphProduction_BatchAndTxWriteShapes_MemoryStore(b *testing.B) {
	batchSize := productionBatchNodeSize(b)

	b.Run("BatchCreateNodesAndRels", func(b *testing.B) {
		g := newBaselineMemoryGraph(b)
		b.ReportAllocs()
		for b.Loop() {
			batch := NewBatchBuilder(g)
			nodes := make([]*types.Node, batchSize)
			for i := range nodes {
				n, err := batch.AddNode([]string{"BatchPerson"}, productionProps(i))
				if err != nil {
					b.Fatal(err)
				}
				nodes[i] = n
			}
			for i := 1; i < len(nodes); i++ {
				if _, err := batch.AddRelationship("BATCH_REL", nodes[i-1], nodes[i], map[string]any{"seq": i}); err != nil {
					b.Fatal(err)
				}
			}
			result, err := batch.Execute()
			if err != nil || result.Failed != 0 {
				b.Fatalf("batch: result=%+v err=%v", result, err)
			}
		}
	})

	b.Run("TxUpdateNodes", func(b *testing.B) {
		g := newBaselineMemoryGraph(b)
		ids := make([]snowflake.ID, batchSize)
		for i := range ids {
			n, err := g.AddNode([]string{"TxPerson"}, productionProps(i))
			if err != nil {
				b.Fatal(err)
			}
			ids[i] = n.InternalID().SnowflakeID()
		}
		b.ReportAllocs()
		for b.Loop() {
			tx := g.BeginTx()
			for i, id := range ids {
				if _, err := tx.UpdateNode(id, map[string]any{"score": i}); err != nil {
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

func BenchmarkGraphProduction_TieredStore_MultiShard(b *testing.B) {
	g := newBaselineTieredGraph(b)
	cases := make([]*types.Node, productionTieredCases(b))
	for i := range cases {
		n, err := g.AddNode([]string{"Case"}, map[string]any{"seq": i})
		if err != nil {
			b.Fatal(err)
		}
		cases[i] = n
	}
	for i := 0; i < productionTieredWarmSignals(b); i++ {
		if _, err := g.AddNode([]string{"Signal"}, map[string]any{"seq": i, "phase": "warm"}); err != nil {
			b.Fatal(err)
		}
	}
	if err := g.ForceRotate(); err != nil {
		b.Fatal(err)
	}
	signals := make([]*types.Node, productionTieredHotSignals(b))
	for i := range signals {
		n, err := g.AddNode([]string{"Signal"}, map[string]any{"seq": i, "phase": "hot"})
		if err != nil {
			b.Fatal(err)
		}
		signals[i] = n
	}
	for i, signal := range signals {
		if _, err := g.AddRelationship("ABOUT", signal, cases[i%len(cases)], map[string]any{"seq": i}); err != nil {
			b.Fatal(err)
		}
	}

	b.Run("NodesByLabelDepthAll", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := g.NodesByLabel("Signal", QueryOpts{Limit: 1000, Depth: DepthAll})
			if err != nil || len(nodes) != 1000 {
				b.Fatalf("NodesByLabel DepthAll: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("NodesByLabelDepthHot", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := g.NodesByLabel("Signal", QueryOpts{Limit: 1000, Depth: DepthHot})
			if err != nil || len(nodes) != 1000 {
				b.Fatalf("NodesByLabel DepthHot: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("RelationshipsByTypeDepthAll", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rels, err := g.RelationshipsByType("ABOUT", QueryOpts{Limit: 1000, Depth: DepthAll})
			if err != nil || len(rels) != 1000 {
				b.Fatalf("RelationshipsByType: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("CrossShardAddRelationship", func(b *testing.B) {
		pairs := make([]struct {
			signal *types.Node
			cas    *types.Node
		}, b.N)
		for i := range pairs {
			signal, err := g.AddNode([]string{"Signal"}, map[string]any{"seq": i, "phase": "bench"})
			if err != nil {
				b.Fatal(err)
			}
			pairs[i] = struct {
				signal *types.Node
				cas    *types.Node
			}{signal: signal, cas: cases[i%len(cases)]}
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := g.AddRelationship("ABOUT_BENCH", pairs[i].signal, pairs[i].cas, map[string]any{"seq": i}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGraphProduction_PublicMethodSurface_MemoryStore(b *testing.B) {
	f := newGraphProductionFixture(b, productionPublicSurfaceNodes(b), 3, 256)

	b.Run("RegistryAndResolution", func(b *testing.B) {
		node := f.nodes[0]
		rel := f.relIDs[0]
		relationship, err := f.g.GetRelationship(rel)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for b.Loop() {
			if _, err := f.g.GetOrCreateLabel("Person"); err != nil {
				b.Fatal(err)
			}
			if _, err := f.g.GetOrCreateRelType("FOLLOWS"); err != nil {
				b.Fatal(err)
			}
			_ = f.g.NodeLabels(node)
			_ = f.g.NodePrimaryLabel(node)
			_ = f.g.NodeHasLabel(node, "Person")
			_ = f.g.RelationshipType(relationship)
			_ = f.g.RelationshipHasType(relationship, "FOLLOWS")
		}
	})

	b.Run("VersionNavigation", func(b *testing.B) {
		id := f.nodeIDs[0]
		if _, err := f.g.UpdateNode(id, map[string]any{"status": "versioned"}); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for b.Loop() {
			if _, err := f.g.GetPreviousNodeVersion(id, 1); err != nil {
				b.Fatal(err)
			}
			if _, err := f.g.GetNextNodeVersion(id, 0); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("ContextReads", func(b *testing.B) {
		ctx := context.Background()
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			if _, err := f.g.GetNodeWithContext(ctx, f.nodeIDs[i%len(f.nodeIDs)]); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
