package core

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"

	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
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
	g          *Core
	nodes      []*types.Node
	nodeIDs    []types.NodeID
	relIDs     []types.RelID
	hubID      types.NodeID
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
	nodeIDs := make([]types.NodeID, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		labels := []string{"Person"}
		if i%10 == 0 {
			labels = []string{"Person", "Customer"}
		}
		n, err := g.Nodes.Add(context.Background(), labels, productionProps(i))
		if err != nil {
			b.Fatal(err)
		}
		nodes = append(nodes, n)
		nodeIDs = append(nodeIDs, n.ID())
	}

	relIDs := make([]types.RelID, 0, nodeCount*fanout+hubDegree)
	for i := 0; i < nodeCount; i++ {
		for j := 1; j <= fanout; j++ {
			r, err := g.Rels.Add(context.Background(), "FOLLOWS", nodes[i], nodes[(i+j)%nodeCount], map[string]any{
				"weight": j,
				"kind":   "regular",
			})
			if err != nil {
				b.Fatal(err)
			}
			relIDs = append(relIDs, r.ID())
		}
	}
	for i := 1; i <= hubDegree; i++ {
		r, err := g.Rels.Add(context.Background(), "MENTIONS", nodes[0], nodes[i], map[string]any{
			"weight": i % 7,
			"kind":   "hub",
		})
		if err != nil {
			b.Fatal(err)
		}
		relIDs = append(relIDs, r.ID())
	}

	if err := g.Index.CreateProperty("Person", "group"); err != nil {
		b.Fatal(err)
	}
	if err := g.Index.CreateTemporal("Person"); err != nil {
		b.Fatal(err)
	}
	if err := g.Index.CreateVector("Person", "embedding", 4, storepkg.DistanceEuclidean); err != nil {
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
			nodes, err := f.g.Nodes.ByLabel("Person", storepkg.QueryOpts{Limit: 1000})
			if err != nil || len(nodes) != 1000 {
				b.Fatalf("NodesByLabel: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("RelationshipsByTypePage1000", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rels, err := f.g.Rels.ByType("FOLLOWS", storepkg.QueryOpts{Limit: 1000})
			if err != nil || len(rels) != 1000 {
				b.Fatalf("RelationshipsByType: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("AllNodesPage1024", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.Nodes.All(storepkg.QueryOpts{Limit: 1024})
			if err != nil || len(nodes) != 1024 {
				b.Fatalf("AllNodes: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("AllRelationshipsPage1024", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rels, err := f.g.Rels.All(storepkg.QueryOpts{Limit: 1024})
			if err != nil || len(rels) != 1024 {
				b.Fatalf("AllRelationships: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("GetNodesByIDs1000", func(b *testing.B) {
		ids := f.nodeIDs[:1000]
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.Nodes.GetByIDs(ids)
			if err != nil || len(nodes) != len(ids) {
				b.Fatalf("GetNodesByIDs: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("GetRelationshipsByIDs1000", func(b *testing.B) {
		ids := f.relIDs[:1000]
		b.ReportAllocs()
		for b.Loop() {
			rels, err := f.g.Rels.GetByIDs(ids)
			if err != nil || len(rels) != len(ids) {
				b.Fatalf("GetRelationshipsByIDs: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("HighDegreeOutgoingHub", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rels, err := f.g.Rels.Outgoing(f.hubID, "")
			if err != nil || len(rels) < hubDegree {
				b.Fatalf("OutgoingRelationships: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("OutgoingRelationshipsForNodes128", func(b *testing.B) {
		ids := f.nodeIDs[:128]
		b.ReportAllocs()
		for b.Loop() {
			rels, err := f.g.Rels.OutgoingForNodes(ids, "FOLLOWS")
			if err != nil || len(rels) != len(ids) {
				b.Fatalf("OutgoingRelationshipsForNodes: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("IncomingRelationshipsForNodes128", func(b *testing.B) {
		ids := f.nodeIDs[1:129]
		b.ReportAllocs()
		for b.Loop() {
			rels, err := f.g.Rels.IncomingForNodes(ids, "FOLLOWS")
			if err != nil || len(rels) != len(ids) {
				b.Fatalf("IncomingRelationshipsForNodes: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("PropertyIndexTenant", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.Nodes.ByLabelAndProperty("Person", "group", f.groupValue, storepkg.QueryOpts{Limit: 64})
			if err != nil || len(nodes) != 64 {
				b.Fatalf("NodesByLabelAndProperty: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("VectorKNN", func(b *testing.B) {
		query := []float32{1, 2, 3, 4}
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.Index.SearchNearest("Person", "embedding", query, 20, storepkg.QueryOpts{})
			if err != nil || len(nodes) != 20 {
				b.Fatalf("SearchNearestNodes: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("StatsAndCounts", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = f.g.Stats.Get()
			if _, err := f.g.Stats.AllLabelCounts(); err != nil {
				b.Fatal(err)
			}
			if _, err := f.g.Stats.AllRelTypeCounts(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

type graphHistoryProductionFixture struct {
	g          *Core
	nodeIDs    []types.NodeID
	relIDs     []types.RelID
	queryTime  types.Instant
	asOfTime   types.Instant
	relAsOf    types.Instant
	updatedID  types.NodeID
	updatedRel types.RelID
	historyLen int
}

func newGraphHistoryProductionFixture(b *testing.B) graphHistoryProductionFixture {
	b.Helper()
	g := newBaselineMemoryGraph(b)
	clk := useTestClock(b, g)
	nodeCount := productionHistoryNodes(b)
	historyDays := productionHistoryDays(b)
	nodes := make([]*types.Node, 0, nodeCount)
	nodeIDs := make([]types.NodeID, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		n, err := g.Nodes.Add(context.Background(), []string{"Account"}, map[string]any{
			"account": i,
			"day":     0,
			"balance": i * 100,
		})
		if err != nil {
			b.Fatal(err)
		}
		nodes = append(nodes, n)
		nodeIDs = append(nodeIDs, n.ID())
	}

	relIDs := make([]types.RelID, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		r, err := g.Rels.Add(context.Background(), "ACCOUNT_LINK", nodes[i], nodes[(i+1)%nodeCount], map[string]any{
			"day":    0,
			"amount": i * 10,
		})
		if err != nil {
			b.Fatal(err)
		}
		relIDs = append(relIDs, r.ID())
	}
	if err := g.Index.CreateProperty("Account", "account"); err != nil {
		b.Fatal(err)
	}
	if err := g.Index.CreateTemporal("Account"); err != nil {
		b.Fatal(err)
	}

	queryTime := types.Instant(time.Now().UnixMilli())
	clk.Advance(2 * time.Millisecond)

	for day := 1; day <= historyDays; day++ {
		for i, id := range nodeIDs {
			if _, err := g.Nodes.Update(context.Background(), id, map[string]any{
				"day":     day,
				"balance": i*100 + day,
			}); err != nil {
				b.Fatal(err)
			}
		}
		for i, id := range relIDs {
			if _, err := g.Rels.Update(context.Background(), id, map[string]any{
				"day":    day,
				"amount": i*10 + day,
			}); err != nil {
				b.Fatal(err)
			}
		}
	}

	current, err := g.Nodes.Get(context.Background(), nodeIDs[0])
	if err != nil {
		b.Fatal(err)
	}
	asOf := current.Temporal().TxFrom
	currentRel, err := g.Rels.Get(context.Background(), relIDs[0])
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
			history, err := f.g.Nodes.History(f.updatedID)
			if err != nil || len(history) != f.historyLen {
				b.Fatalf("GetNodeHistory: len=%d err=%v", len(history), err)
			}
		}
	})

	b.Run("GetNodeAtGenesis", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			n, err := f.g.Temporal.NodeAt(f.updatedID, f.queryTime)
			if err != nil || n == nil {
				b.Fatalf("GetNodeAt: %v", err)
			}
		}
	})

	b.Run("GetNodeAsOfCurrent", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			n, err := f.g.Temporal.NodeAsOf(f.updatedID, f.asOfTime)
			if err != nil || n == nil {
				b.Fatalf("GetNodeAsOf: %v", err)
			}
		}
	})

	b.Run("NodesByLabelValidAtGenesis", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.Nodes.ByLabel("Account", storepkg.QueryOpts{ValidAt: f.queryTime, Limit: len(f.nodeIDs)})
			if err != nil || len(nodes) != len(f.nodeIDs) {
				b.Fatalf("NodesByLabel ValidAt: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("NodesByLabelAndPropertyValidAtGenesis", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.Nodes.ByLabelAndProperty("Account", "account", 0, storepkg.QueryOpts{ValidAt: f.queryTime, Limit: 1})
			if err != nil || len(nodes) != 1 {
				b.Fatalf("NodesByLabelAndProperty ValidAt: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("GetNodesByLabelValidAtGenesis", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.Temporal.NodesByLabelAt("Account", f.queryTime)
			if err != nil || len(nodes) != len(f.nodeIDs) {
				b.Fatalf("GetNodesByLabelValidAt: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("GetNodesValidAtGenesis", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.Temporal.NodesAt(f.queryTime)
			if err != nil || len(nodes) == 0 {
				b.Fatalf("GetNodesValidAt: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("GetNodesAsOfCurrent", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := f.g.Temporal.NodesAsOf(f.asOfTime)
			if err != nil || len(nodes) == 0 {
				b.Fatalf("GetNodesAsOf: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("GetRelationshipHistoryVersions", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			history, err := f.g.Rels.History(f.updatedRel)
			if err != nil || len(history) != f.historyLen {
				b.Fatalf("GetRelHistory: len=%d err=%v", len(history), err)
			}
		}
	})

	b.Run("GetRelationshipAtGenesis", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			r, err := f.g.Temporal.RelAt(f.updatedRel, f.queryTime)
			if err != nil || r == nil {
				b.Fatalf("GetRelAt: %v", err)
			}
		}
	})

	b.Run("GetRelAsOfCurrent", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			r, err := f.g.Temporal.RelAsOf(f.updatedRel, f.relAsOf)
			if err != nil || r == nil {
				b.Fatalf("GetRelAsOf: %v", err)
			}
		}
	})

	b.Run("RelationshipsByTypeValidAtGenesis", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rels, err := f.g.Rels.ByType("ACCOUNT_LINK", storepkg.QueryOpts{ValidAt: f.queryTime, Limit: len(f.relIDs)})
			if err != nil || len(rels) != len(f.relIDs) {
				b.Fatalf("RelationshipsByType ValidAt: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("GetRelationshipsValidAtGenesis", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rels, err := f.g.Temporal.RelsAt(f.queryTime)
			if err != nil || len(rels) == 0 {
				b.Fatalf("GetRelationshipsValidAt: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("GetRelsAsOfCurrent", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rels, err := f.g.Temporal.RelsAsOf(f.relAsOf)
			if err != nil || len(rels) == 0 {
				b.Fatalf("GetRelsAsOf: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("SnapshotGenesis", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			snapshot, err := f.g.Temporal.Snapshot(f.queryTime)
			if err != nil || snapshot.NodeCount == 0 {
				b.Fatalf("Snapshot: nodes=%d err=%v", snapshot.NodeCount, err)
			}
		}
	})
}

func BenchmarkGraphProduction_ExportImport_MemoryStore(b *testing.B) {
	f := newGraphProductionFixture(b, productionExportNodes(b), productionExportFanout(b), 256)
	var exportBytes bytes.Buffer
	if err := f.g.IO.Export(&exportBytes); err != nil {
		b.Fatal(err)
	}
	payload := exportBytes.Bytes()

	b.Run("ExportGraph", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var out bytes.Buffer
			if err := f.g.IO.Export(&out); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("ImportGraph", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			g, err := New(Config{Store: memory.New()})
			if err != nil {
				b.Fatal(err)
			}
			if err := g.IO.Import(bytes.NewReader(payload), tkgio.ImportOptions{}); err != nil {
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
		bus := eventspkg.NewEventBus()
		var count atomic.Int64
		bus.Subscribe(func(eventspkg.Event) {
			count.Add(1)
		})
		_ = g.Events.SetSync(bus)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := g.Nodes.Add(context.Background(), []string{"EventNode"}, map[string]any{"seq": i}); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("AsyncEventBusAddNodeDropLatest", func(b *testing.B) {
		g := newBaselineMemoryGraph(b)
		bus := eventspkg.NewAsyncEventBus(eventspkg.AsyncEventBusConfig{
			Workers:      4,
			QueueSize:    8192,
			Backpressure: eventspkg.BackpressureDropLatest,
		})
		b.Cleanup(func() { bus.Close() })
		bus.Subscribe(func(eventspkg.Event) {})
		_ = g.Events.SetAsync(bus)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := g.Nodes.Add(context.Background(), []string{"AsyncEventNode"}, map[string]any{"seq": i}); err != nil {
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
			batch, _ := NewBatchBuilder(g)
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
		ids := make([]types.NodeID, batchSize)
		for i := range ids {
			n, err := g.Nodes.Add(context.Background(), []string{"TxPerson"}, productionProps(i))
			if err != nil {
				b.Fatal(err)
			}
			ids[i] = n.ID()
		}
		b.ReportAllocs()
		for b.Loop() {
			tx, _ := g.BeginTx()
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
		n, err := g.Nodes.Add(context.Background(), []string{"Case"}, map[string]any{"seq": i})
		if err != nil {
			b.Fatal(err)
		}
		cases[i] = n
	}
	for i := 0; i < productionTieredWarmSignals(b); i++ {
		if _, err := g.Nodes.Add(context.Background(), []string{"Signal"}, map[string]any{"seq": i, "phase": "warm"}); err != nil {
			b.Fatal(err)
		}
	}
	if err := g.Admin.ForceRotate(); err != nil {
		b.Fatal(err)
	}
	signals := make([]*types.Node, productionTieredHotSignals(b))
	for i := range signals {
		n, err := g.Nodes.Add(context.Background(), []string{"Signal"}, map[string]any{"seq": i, "phase": "hot"})
		if err != nil {
			b.Fatal(err)
		}
		signals[i] = n
	}
	aboutRelIDs := make([]types.RelID, len(signals))
	for i, signal := range signals {
		rel, err := g.Rels.Add(context.Background(), "ABOUT", signal, cases[i%len(cases)], map[string]any{"seq": i})
		if err != nil {
			b.Fatal(err)
		}
		aboutRelIDs[i] = rel.ID()
	}

	mixedNodeIDs := make([]types.NodeID, 0, 1000)
	for i := 0; i < 500; i++ {
		mixedNodeIDs = append(mixedNodeIDs, cases[i%len(cases)].ID())
	}
	for i := 0; i < 500; i++ {
		mixedNodeIDs = append(mixedNodeIDs, signals[i%len(signals)].ID())
	}
	mixedRelIDs := make([]types.RelID, 0, 1000)
	for i := 0; i < 1000; i++ {
		mixedRelIDs = append(mixedRelIDs, aboutRelIDs[i%len(aboutRelIDs)])
	}
	outgoingSignalIDs := make([]types.NodeID, 0, 1000)
	outgoingSignalSeen := make(map[types.NodeID]struct{}, min(1000, len(signals)))
	for i := 0; i < 1000; i++ {
		id := signals[i%len(signals)].ID()
		outgoingSignalIDs = append(outgoingSignalIDs, id)
		outgoingSignalSeen[id] = struct{}{}
	}
	incomingCaseIDs := make([]types.NodeID, 0, 1000)
	incomingCaseSeen := make(map[types.NodeID]struct{}, min(1000, len(cases)))
	for i := 0; i < 1000; i++ {
		signalIdx := i % len(aboutRelIDs)
		id := cases[signalIdx%len(cases)].ID()
		incomingCaseIDs = append(incomingCaseIDs, id)
		incomingCaseSeen[id] = struct{}{}
	}

	b.Run("NodesByLabelDepthAll", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := g.Nodes.ByLabel("Signal", storepkg.QueryOpts{Limit: 1000, Depth: storepkg.DepthAll})
			if err != nil || len(nodes) != 1000 {
				b.Fatalf("NodesByLabel storepkg.DepthAll: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("NodesByLabelDepthHot", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := g.Nodes.ByLabel("Signal", storepkg.QueryOpts{Limit: 1000, Depth: storepkg.DepthHot})
			if err != nil || len(nodes) != 1000 {
				b.Fatalf("NodesByLabel storepkg.DepthHot: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("RelationshipsByTypeDepthAll", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rels, err := g.Rels.ByType("ABOUT", storepkg.QueryOpts{Limit: 1000, Depth: storepkg.DepthAll})
			if err != nil || len(rels) != 1000 {
				b.Fatalf("RelationshipsByType: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("GetNodesByIDsMixedShards1000", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			nodes, err := g.Nodes.GetByIDs(mixedNodeIDs)
			if err != nil || len(nodes) != len(mixedNodeIDs) {
				b.Fatalf("GetNodesByIDs tiered: len=%d err=%v", len(nodes), err)
			}
		}
	})

	b.Run("GetRelationshipsByIDsMixedShards1000", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rels, err := g.Rels.GetByIDs(mixedRelIDs)
			if err != nil || len(rels) != len(mixedRelIDs) {
				b.Fatalf("GetRelationshipsByIDs tiered: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("OutgoingRelationshipsForNodesSignals1000", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rels, err := g.Rels.OutgoingForNodes(outgoingSignalIDs, "ABOUT")
			if err != nil || len(rels) != len(outgoingSignalSeen) {
				b.Fatalf("OutgoingRelationshipsForNodes tiered: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("IncomingRelationshipsForNodesCases1000", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rels, err := g.Rels.IncomingForNodes(incomingCaseIDs, "ABOUT")
			if err != nil || len(rels) != len(incomingCaseSeen) {
				b.Fatalf("IncomingRelationshipsForNodes tiered: len=%d err=%v", len(rels), err)
			}
		}
	})

	b.Run("CrossShardAddRelationship", func(b *testing.B) {
		poolSize := benchmarkEndpointPoolSize(b.N)
		if poolSize > len(signals) {
			poolSize = len(signals)
		}
		pairs := make([]struct {
			signal *types.Node
			cas    *types.Node
		}, poolSize)
		for i := range pairs {
			pairs[i] = struct {
				signal *types.Node
				cas    *types.Node
			}{signal: signals[i%len(signals)], cas: cases[i%len(cases)]}
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pair := pairs[i%len(pairs)]
			if _, err := g.Rels.Add(context.Background(), "ABOUT_BENCH", pair.signal, pair.cas, map[string]any{"seq": i}); err != nil {
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
		relationship, err := f.g.Rels.Get(context.Background(), rel)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for b.Loop() {
			if _, err := f.g.Resolve.GetOrCreateLabel("Person"); err != nil {
				b.Fatal(err)
			}
			if _, err := f.g.Resolve.GetOrCreateRelType("FOLLOWS"); err != nil {
				b.Fatal(err)
			}
			_ = f.g.Nodes.Labels(node)
			_ = f.g.Nodes.PrimaryLabel(node)
			_ = f.g.Nodes.HasLabel(node, "Person")
			_ = f.g.Rels.Type(relationship)
			_ = f.g.Rels.HasType(relationship, "FOLLOWS")
		}
	})

	b.Run("VersionNavigation", func(b *testing.B) {
		id := f.nodeIDs[0]
		if _, err := f.g.Nodes.Update(context.Background(), id, map[string]any{"status": "versioned"}); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for b.Loop() {
			if _, err := f.g.Nodes.VersionBefore(id, 1); err != nil {
				b.Fatal(err)
			}
			if _, err := f.g.Nodes.VersionAfter(id, 0); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("ContextReads", func(b *testing.B) {
		ctx := context.Background()
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			if _, err := f.g.Nodes.Get(ctx, f.nodeIDs[i%len(f.nodeIDs)]); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
