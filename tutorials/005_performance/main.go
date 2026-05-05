// Tutorial 005: Performance & Storage
//
// Benchmarks tkg/v3 write throughput, query performance, and index acceleration
// across three backends (MemoryStore, BadgerStore in-memory, BadgerStore on-disk)
// and four index types:
//   - Property index:  NodesByLabelAndProperty  — O(1) hash lookup vs O(N) label scan
//   - Temporal index:  GetNodesByLabelValidAt   — binary search vs O(N) temporal filter
//   - Vector index:    SearchNearestNodes        — k-NN brute-force in-memory
//   - BatchBuilder:    bulk inserts              — single g.mu.Lock vs N individual locks
//
// Run: go run ./tutorials/005_performance/
package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// commas formats an integer with thousand separators: 1234567 -> "1,234,567".
func commas(n int64) string {
	if n < 0 {
		return "-" + commas(-n)
	}
	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 {
		return s
	}
	var buf []byte
	pre := len(s) % 3
	if pre > 0 {
		buf = append(buf, s[:pre]...)
	}
	for i := pre; i < len(s); i += 3 {
		if len(buf) > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, s[i:i+3]...)
	}
	return string(buf)
}

// randomVec returns a random unit-ish float32 vector of the given dimension.
func randomVec(dims int, rng *rand.Rand) []float32 {
	v := make([]float32, dims)
	for i := range v {
		v[i] = rng.Float32()
	}
	return v
}

func main() {
	const (
		nodeCount = 10_000
		relCount  = 50_000
	)

	// ----------------------------------------------------------------
	fmt.Println("=== 1. MemoryStore Benchmark ===")
	// ----------------------------------------------------------------

	gMem, err := graph.New(graph.Config{SnowflakeNodeID: 1})
	if err != nil {
		log.Fatal(err)
	}
	memElapsed, memNodeOps, memRelOps := benchmarkBackend("MemoryStore", gMem, nodeCount, relCount)

	fmt.Printf("  Elapsed:     %v\n", memElapsed)
	fmt.Printf("  Node write:  %s ops/sec\n", commas(int64(memNodeOps)))
	fmt.Printf("  Rel write:   %s ops/sec\n", commas(int64(memRelOps)))

	// ----------------------------------------------------------------
	fmt.Println("\n=== 2. BadgerStore In-Memory Benchmark ===")
	// ----------------------------------------------------------------

	bsMem, err := graph.NewBadgerStore(graph.BadgerStoreConfig{InMemory: true})
	if err != nil {
		log.Fatal(err)
	}
	gBadgerMem, err := graph.New(graph.Config{SnowflakeNodeID: 2, Store: bsMem})
	if err != nil {
		log.Fatal(err)
	}
	bmElapsed, bmNodeOps, bmRelOps := benchmarkBackend("BadgerInMemory", gBadgerMem, nodeCount, relCount)

	fmt.Printf("  Elapsed:     %v\n", bmElapsed)
	fmt.Printf("  Node write:  %s ops/sec\n", commas(int64(bmNodeOps)))
	fmt.Printf("  Rel write:   %s ops/sec\n", commas(int64(bmRelOps)))

	// ----------------------------------------------------------------
	fmt.Println("\n=== 3. BadgerStore On-Disk Benchmark ===")
	// ----------------------------------------------------------------

	tmpDir, err := os.MkdirTemp("", "tkg-005-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	bs, err := graph.NewBadgerStore(graph.BadgerStoreConfig{Dir: tmpDir})
	if err != nil {
		log.Fatal(err)
	}
	gDisk, err := graph.New(graph.Config{SnowflakeNodeID: 3, Store: bs})
	if err != nil {
		log.Fatal(err)
	}
	diskElapsed, diskNodeOps, diskRelOps := benchmarkBackend("BadgerOnDisk", gDisk, nodeCount, relCount)

	fmt.Printf("  Elapsed:     %v\n", diskElapsed)
	fmt.Printf("  Node write:  %s ops/sec\n", commas(int64(diskNodeOps)))
	fmt.Printf("  Rel write:   %s ops/sec\n", commas(int64(diskRelOps)))

	// ----------------------------------------------------------------
	fmt.Println("\n=== 4. Memory Usage ===")
	// ----------------------------------------------------------------

	memUsageMem := measureMemoryUsage(func() *graph.Graph {
		g, err := graph.New(graph.Config{SnowflakeNodeID: 4})
		if err != nil {
			log.Fatal(err)
		}
		return g
	}, nodeCount, relCount)

	memUsageBadger := measureMemoryUsage(func() *graph.Graph {
		bs, err := graph.NewBadgerStore(graph.BadgerStoreConfig{InMemory: true})
		if err != nil {
			log.Fatal(err)
		}
		g, err := graph.New(graph.Config{SnowflakeNodeID: 5, Store: bs})
		if err != nil {
			log.Fatal(err)
		}
		return g
	}, nodeCount, relCount)

	fmt.Printf("  MemoryStore:     %s KB heap for %s nodes + %s rels\n", commas(int64(memUsageMem/1024)), commas(nodeCount), commas(relCount))    // #nosec G115 — heap KB fits int64
	fmt.Printf("  BadgerInMemory:  %s KB heap for %s nodes + %s rels\n", commas(int64(memUsageBadger/1024)), commas(nodeCount), commas(relCount)) // #nosec G115 — heap KB fits int64

	// ----------------------------------------------------------------
	fmt.Println("\n=== 5. Storage Usage ===")
	// ----------------------------------------------------------------

	diskSize := dirSize(tmpDir)
	fmt.Printf("  On-disk Badger:  %.2f MB for %s nodes + %s rels\n",
		float64(diskSize)/(1024*1024), commas(nodeCount), commas(relCount))

	// ----------------------------------------------------------------
	fmt.Println("\n=== 6. Point Lookup Performance ===")
	// ----------------------------------------------------------------

	bsQuery, err := graph.NewBadgerStore(graph.BadgerStoreConfig{InMemory: true})
	if err != nil {
		log.Fatal(err)
	}
	gQuery, err := graph.New(graph.Config{SnowflakeNodeID: 6, Store: bsQuery})
	if err != nil {
		_ = bsQuery.Close()
		log.Fatal(err)
	}
	defer func() {
		if err := gQuery.Close(); err != nil {
			log.Printf("error closing query graph: %v", err)
		}
	}()

	qNodes := make([]*types.Node, nodeCount)
	for i := range nodeCount {
		label := fmt.Sprintf("Type%d", i%10)
		n, err := gQuery.AddNode([]string{label}, nil)
		if err != nil {
			log.Fatal(err)
		}
		qNodes[i] = n
	}
	qRels := make([]*types.Relationship, relCount)
	for i := range relCount {
		sIdx := i % nodeCount
		eIdx := (i*7 + 3) % nodeCount
		if sIdx == eIdx {
			eIdx = (eIdx + 1) % nodeCount
		}
		r, err := gQuery.AddRelationship("EDGE", qNodes[sIdx], qNodes[eIdx], nil)
		if err != nil {
			log.Fatal(err)
		}
		qRels[i] = r
	}

	const lookupCount = 10_000
	start := time.Now()
	for i := range lookupCount {
		id := qNodes[i%nodeCount].ID()
		if _, err := gQuery.GetNode(id); err != nil {
			log.Fatal(err)
		}
	}
	getNodeElapsed := time.Since(start)
	getNodeOps := int64(float64(lookupCount) / getNodeElapsed.Seconds())
	getNodeNs := getNodeElapsed.Nanoseconds() / lookupCount

	start = time.Now()
	for i := range lookupCount {
		id := qRels[i%relCount].ID()
		if _, err := gQuery.GetRelationship(id); err != nil {
			log.Fatal(err)
		}
	}
	getRelElapsed := time.Since(start)
	getRelOps := int64(float64(lookupCount) / getRelElapsed.Seconds())
	getRelNs := getRelElapsed.Nanoseconds() / lookupCount

	fmt.Printf("  GetNode:           %s ops/sec  (%d ns/op)\n", commas(getNodeOps), getNodeNs)
	fmt.Printf("  GetRelationship:   %s ops/sec  (%d ns/op)\n", commas(getRelOps), getRelNs)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 7. Index-Free Query Performance ===")
	// ----------------------------------------------------------------

	// OutgoingRelationships — adjacency scan, no temporal filter.
	const outQueryCount = 10_000
	var outTotalEntities int64
	start = time.Now()
	for i := range outQueryCount {
		id := qNodes[i%nodeCount].ID()
		rels, err := gQuery.OutgoingRelationships(id, "")
		if err != nil {
			log.Fatal(err)
		}
		outTotalEntities += int64(len(rels))
	}
	outElapsed := time.Since(start)
	outOps := int64(float64(outQueryCount) / outElapsed.Seconds())
	outAvgSize := float64(outTotalEntities) / outQueryCount
	outEntPerSec := int64(float64(outTotalEntities) / outElapsed.Seconds())

	// NodesByLabel — full label scan, no temporal filter.
	const labelQueryCount = 1000
	var labelTotalEntities int64
	start = time.Now()
	for i := range labelQueryCount {
		label := fmt.Sprintf("Type%d", i%10)
		nodes, err := gQuery.NodesByLabel(label, graph.QueryOpts{})
		if err != nil {
			log.Fatal(err)
		}
		labelTotalEntities += int64(len(nodes))
	}
	labelElapsed := time.Since(start)
	labelOps := int64(float64(labelQueryCount) / labelElapsed.Seconds())
	labelAvgSize := float64(labelTotalEntities) / labelQueryCount
	labelEntPerSec := int64(float64(labelTotalEntities) / labelElapsed.Seconds())

	fmt.Printf("  OutgoingRelationships: %s queries/sec  (~%.0f rels/query,  ~%s entities/sec)\n",
		commas(outOps), outAvgSize, commas(outEntPerSec))
	fmt.Printf("  NodesByLabel:          %s queries/sec  (~%.0f nodes/query, ~%s entities/sec)\n",
		commas(labelOps), labelAvgSize, commas(labelEntPerSec))

	// ----------------------------------------------------------------
	fmt.Println("\n=== 8. BatchBuilder vs Single Writes ===")
	// ----------------------------------------------------------------
	// BatchBuilder queues N nodes with eager validation, then persists them
	// under a single g.mu.Lock using PutNodesBatch (one store lock for all).
	// Standalone AddNode acquires g.mu.RLock + entity lock per node.

	const batchBenchN = 5_000

	gSingle, err := graph.New(graph.Config{SnowflakeNodeID: 7})
	if err != nil {
		log.Fatal(err)
	}

	// Baseline: N standalone AddNode calls.
	singleStart := time.Now()
	for i := range batchBenchN {
		if _, err := gSingle.AddNode([]string{"Single"}, map[string]any{"idx": i}); err != nil {
			log.Fatalf("AddNode: %v", err)
		}
	}
	singleDur := time.Since(singleStart)
	singleNodeOps := int64(float64(batchBenchN) / singleDur.Seconds())

	// BatchBuilder: queue all nodes (validation + ID gen), then Execute.
	b := graph.NewBatchBuilder(gSingle)
	for i := range batchBenchN {
		if _, err := b.AddNode([]string{"Batch"}, map[string]any{"idx": i}); err != nil {
			log.Fatalf("batch AddNode: %v", err)
		}
	}
	batchAllStart := time.Now()
	bResult, err := b.Execute()
	if err != nil {
		log.Fatal(err)
	}
	batchDur := time.Since(batchAllStart)
	batchNodeOps := int64(float64(bResult.Created) / batchDur.Seconds())

	if err := gSingle.Close(); err != nil {
		log.Fatal(err)
	}

	batchSpeedup := float64(batchNodeOps) / float64(singleNodeOps)
	fmt.Printf("  Single AddNode:  %s ops/sec\n", commas(singleNodeOps))
	fmt.Printf("  BatchBuilder:    %s ops/sec  (×%.1fx vs single)\n", commas(batchNodeOps), batchSpeedup)
	fmt.Printf("  Batch created:   %d nodes, %d failed\n", bResult.Created, bResult.Failed)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 9. Property Index ===")
	// ----------------------------------------------------------------
	// NodesByLabelAndProperty without a property index falls back to a
	// full label scan (O(N)). With CreatePropertyIndex, it uses a hash
	// map — O(1) lookup returning only matching IDs.
	//
	// Setup: 10K "Product" nodes, score in [0, 100). 100 nodes per score value.
	// Query: score == 42  →  expected ~100 matches (1% selectivity).

	const (
		propN      = 10_000
		propValues = 100
		propQ      = 500
		queryScore = 42
	)

	gProp, err := graph.New(graph.Config{SnowflakeNodeID: 8})
	if err != nil {
		log.Fatal(err)
	}
	for i := range propN {
		if _, err := gProp.AddNode([]string{"Product"}, map[string]any{"score": i % propValues}); err != nil {
			log.Fatalf("AddNode: %v", err)
		}
	}

	// Without property index: falls back to label scan + property filter.
	start = time.Now()
	for range propQ {
		if _, err := gProp.NodesByLabelAndProperty("Product", "score", queryScore, graph.QueryOpts{}); err != nil {
			log.Fatalf("NodesByLabelAndProperty: %v", err)
		}
	}
	propNoIdxDur := time.Since(start)
	propNoIdxOps := int64(float64(propQ) / propNoIdxDur.Seconds())

	// Create property index — backfills from all existing 10K nodes.
	if err := gProp.CreatePropertyIndex("Product", "score"); err != nil {
		log.Fatalf("CreatePropertyIndex: %v", err)
	}

	// With property index: O(1) hash lookup.
	start = time.Now()
	for range propQ {
		if _, err := gProp.NodesByLabelAndProperty("Product", "score", queryScore, graph.QueryOpts{}); err != nil {
			log.Fatalf("NodesByLabelAndProperty: %v", err)
		}
	}
	propWithIdxDur := time.Since(start)
	propWithIdxOps := int64(float64(propQ) / propWithIdxDur.Seconds())

	if err := gProp.Close(); err != nil {
		log.Fatal(err)
	}

	propSpeedup := float64(propWithIdxOps) / float64(propNoIdxOps)
	fmt.Printf("  NodesByLabelAndProperty without index: %s ops/sec\n", commas(propNoIdxOps))
	fmt.Printf("  NodesByLabelAndProperty with index:    %s ops/sec  (×%.1fx)\n", commas(propWithIdxOps), propSpeedup)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 10. Temporal Index ===")
	// ----------------------------------------------------------------
	// GetNodesByLabelValidAt without a temporal index performs a full label
	// scan + per-node validity check (O(N)). With CreateTemporalIndex, the
	// interval index uses binary search to find the valid-at boundary, then
	// filters by ValidTo — skipping ineligible nodes without loading them.
	//
	// Scenario: 50K "Event" nodes. 90% are closed (ValidTo = 1 hour ago),
	// simulating expired events. Query at now → ~5K active events (10%).
	// Without index: sort + scan + deep-copy all 50K, filter down to 5K.
	// With index: scan 50K index entries (sequential), deep-copy only 5K.

	const (
		tempN      = 50_000
		tempClosed = tempN * 9 / 10 // 90% closed
		tempQ      = 100
	)

	gTemp, err := graph.New(graph.Config{SnowflakeNodeID: 9})
	if err != nil {
		log.Fatal(err)
	}

	// Create all Event nodes, then close 90% with a past timestamp.
	closeTime := types.Instant(time.Now().Add(-time.Hour).UnixMilli())
	tempNodes := make([]*types.Node, tempN)
	for i := range tempN {
		n, err := gTemp.AddNode([]string{"Event"}, nil)
		if err != nil {
			log.Fatalf("AddNode: %v", err)
		}
		tempNodes[i] = n
	}
	for i := range tempClosed {
		id := tempNodes[i].ID()
		if err := gTemp.CloseNodeVersion(id, closeTime); err != nil {
			log.Fatalf("CloseNodeVersion: %v", err)
		}
	}

	// Query 1 second in the future to ensure all nodes have ValidFrom <= t.
	nowQuery := types.Instant(time.Now().Add(time.Second).UnixMilli())

	// Without temporal index: full label scan + per-node ValidTo check.
	start = time.Now()
	for range tempQ {
		if _, err := gTemp.GetNodesByLabelValidAt("Event", nowQuery); err != nil {
			log.Fatalf("GetNodesByLabelValidAt: %v", err)
		}
	}
	tempNoIdxDur := time.Since(start)
	tempNoIdxOps := int64(float64(tempQ) / tempNoIdxDur.Seconds())

	// Create temporal index — backfills from all 50K existing nodes.
	if err := gTemp.CreateTemporalIndex("Event"); err != nil {
		log.Fatalf("CreateTemporalIndex: %v", err)
	}

	// With temporal index: sequential scan of index entries, deep-copy only matching.
	start = time.Now()
	for range tempQ {
		if _, err := gTemp.GetNodesByLabelValidAt("Event", nowQuery); err != nil {
			log.Fatalf("GetNodesByLabelValidAt: %v", err)
		}
	}
	tempWithIdxDur := time.Since(start)
	tempWithIdxOps := int64(float64(tempQ) / tempWithIdxDur.Seconds())

	if err := gTemp.Close(); err != nil {
		log.Fatal(err)
	}

	tempSpeedup := float64(tempWithIdxOps) / float64(tempNoIdxOps)
	fmt.Printf("  GetNodesByLabelValidAt without index: %s ops/sec  (%s nodes, 10%% selectivity)\n",
		commas(tempNoIdxOps), commas(tempN))
	fmt.Printf("  GetNodesByLabelValidAt with index:    %s ops/sec  (×%.1fx)\n",
		commas(tempWithIdxOps), tempSpeedup)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 11. Vector Index (k-NN) ===")
	// ----------------------------------------------------------------
	// CreateVectorIndex builds a brute-force k-nearest-neighbor index on a
	// float32 property. SearchNearestNodes scans all N vectors in memory —
	// O(N × dims). Suitable for datasets up to ~100K nodes.
	//
	// Setup: 2K "Doc" nodes with 64-dim embeddings. k=10 nearest neighbors.

	const (
		vecN    = 2_000
		vecDims = 64
		vecK    = 10
		vecQ    = 500
	)

	rng := rand.New(rand.NewSource(42)) // #nosec G404 — deterministic seed for reproducible benchmark data

	gVec, err := graph.New(graph.Config{SnowflakeNodeID: 10})
	if err != nil {
		log.Fatal(err)
	}
	for range vecN {
		emb := randomVec(vecDims, rng)
		if _, err := gVec.AddNode([]string{"Doc"}, map[string]any{"embedding": emb}); err != nil {
			log.Fatalf("AddNode: %v", err)
		}
	}

	if err := gVec.CreateVectorIndex("Doc", "embedding", vecDims, graph.DistanceCosine); err != nil {
		log.Fatalf("CreateVectorIndex: %v", err)
	}

	query := randomVec(vecDims, rng)
	start = time.Now()
	for range vecQ {
		results, err := gVec.SearchNearestNodes("Doc", "embedding", query, vecK, graph.QueryOpts{})
		if err != nil {
			log.Fatalf("SearchNearestNodes: %v", err)
		}
		_ = results
	}
	vecDur := time.Since(start)
	vecOps := int64(float64(vecQ) / vecDur.Seconds())
	vecNs := vecDur.Nanoseconds() / vecQ

	if err := gVec.Close(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("  SearchNearestNodes: %s ops/sec  (%d ns/op)  [N=%d, dims=%d, k=%d, cosine]\n",
		commas(vecOps), vecNs, vecN, vecDims, vecK)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 12. Summary ===")
	// ----------------------------------------------------------------

	fmt.Println()
	fmt.Printf("  %-24s %12s %12s %12s\n", "Backend", "Node ops/s", "Rel ops/s", "Elapsed")
	fmt.Printf("  %-24s %12s %12s %12s\n", "------", "----------", "---------", "-------")
	fmt.Printf("  %-24s %12s %12s %12v\n", "MemoryStore", commas(int64(memNodeOps)), commas(int64(memRelOps)), memElapsed.Round(time.Millisecond))
	fmt.Printf("  %-24s %12s %12s %12v\n", "BadgerStore (memory)", commas(int64(bmNodeOps)), commas(int64(bmRelOps)), bmElapsed.Round(time.Millisecond))
	fmt.Printf("  %-24s %12s %12s %12v\n", "BadgerStore (disk)", commas(int64(diskNodeOps)), commas(int64(diskRelOps)), diskElapsed.Round(time.Millisecond))
	fmt.Println()
	fmt.Printf("  %-24s %12s\n", "Memory (heap)", "KB")
	fmt.Printf("  %-24s %12s\n", "------", "--")
	fmt.Printf("  %-24s %12s\n", "MemoryStore", commas(int64(memUsageMem/1024)))             // #nosec G115 — heap KB fits int64
	fmt.Printf("  %-24s %12s\n", "BadgerStore (memory)", commas(int64(memUsageBadger/1024))) // #nosec G115 — heap KB fits int64
	fmt.Println()
	fmt.Printf("  %-24s %12s\n", "Storage", "Size")
	fmt.Printf("  %-24s %12s\n", "------", "----")
	fmt.Printf("  %-24s %10.2f MB\n", "BadgerStore (disk)", float64(diskSize)/(1024*1024))
	fmt.Println()
	fmt.Printf("  %-24s %12s %14s\n", "Query (no index)", "ops/sec", "entities/sec")
	fmt.Printf("  %-24s %12s %14s\n", "------", "-------", "------------")
	fmt.Printf("  %-24s %12s %14s\n", "GetNode", commas(getNodeOps), "-")
	fmt.Printf("  %-24s %12s %14s\n", "GetRelationship", commas(getRelOps), "-")
	fmt.Printf("  %-24s %12s %14s\n", "OutgoingRels", commas(outOps), commas(outEntPerSec))
	fmt.Printf("  %-24s %12s %14s\n", "NodesByLabel", commas(labelOps), commas(labelEntPerSec))
	fmt.Println()
	fmt.Printf("  %-24s %12s %12s %10s\n", "Index Acceleration", "Without", "With", "Speedup")
	fmt.Printf("  %-24s %12s %12s %10s\n", "------", "-------", "----", "-------")
	fmt.Printf("  %-24s %12s %12s %9.1fx\n", "BatchBuilder", commas(singleNodeOps), commas(batchNodeOps), batchSpeedup)
	fmt.Printf("  %-24s %12s %12s %9.1fx\n", "PropertyIndex", commas(propNoIdxOps), commas(propWithIdxOps), propSpeedup)
	fmt.Printf("  %-24s %12s %12s %9.1fx\n", "TemporalIndex (10%)", commas(tempNoIdxOps), commas(tempWithIdxOps), tempSpeedup)
	fmt.Println()
	fmt.Printf("  %-24s %12s %10s\n", "Vector Index", "ops/sec", "ns/op")
	fmt.Printf("  %-24s %12s %10s\n", "------", "-------", "----")
	fmt.Printf("  %-24s %12s %10d\n", "SearchNearestNodes", commas(vecOps), vecNs)

	fmt.Println("\n=== Done ===")
}

// benchmarkBackend populates a graph with nodeCount nodes and relCount
// relationships, returning elapsed time and throughput.
func benchmarkBackend(name string, g *graph.Graph, nodeCount, relCount int) (time.Duration, int, int) {
	start := time.Now()

	nodes := make([]*types.Node, nodeCount)
	for i := range nodeCount {
		label := fmt.Sprintf("Type%d", i%10)
		n, err := g.AddNode([]string{label}, nil)
		if err != nil {
			log.Fatalf("[%s] AddNode %d: %v", name, i, err)
		}
		nodes[i] = n
	}
	nodeElapsed := time.Since(start)

	relStart := time.Now()
	for i := range relCount {
		sIdx := i % nodeCount
		eIdx := (i*7 + 3) % nodeCount
		if sIdx == eIdx {
			eIdx = (eIdx + 1) % nodeCount
		}
		if _, err := g.AddRelationship("EDGE", nodes[sIdx], nodes[eIdx], nil); err != nil {
			log.Fatalf("[%s] AddRelationship %d: %v", name, i, err)
		}
	}
	relElapsed := time.Since(relStart)

	if err := g.Close(); err != nil {
		log.Fatalf("[%s] Close: %v", name, err)
	}
	total := time.Since(start)

	nodeOps := int(float64(nodeCount) / nodeElapsed.Seconds())
	relOps := int(float64(relCount) / relElapsed.Seconds())

	return total, nodeOps, relOps
}

// measureMemoryUsage returns the approximate heap increase in bytes after
// creating nodeCount nodes and relCount relationships.
func measureMemoryUsage(newGraph func() *graph.Graph, nodeCount, relCount int) uint64 {
	runtime.GC()
	debug.FreeOSMemory()
	runtime.GC()

	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	g := newGraph()
	nodes := make([]*types.Node, nodeCount)
	for i := range nodeCount {
		label := fmt.Sprintf("Type%d", i%10)
		n, err := g.AddNode([]string{label}, nil)
		if err != nil {
			log.Fatal(err)
		}
		nodes[i] = n
	}
	for i := range relCount {
		sIdx := i % nodeCount
		eIdx := (i*7 + 3) % nodeCount
		if sIdx == eIdx {
			eIdx = (eIdx + 1) % nodeCount
		}
		if _, err := g.AddRelationship("EDGE", nodes[sIdx], nodes[eIdx], nil); err != nil {
			log.Fatal(err)
		}
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	if err := g.Close(); err != nil {
		log.Fatal(err)
	}

	if after.HeapAlloc > before.HeapAlloc {
		return after.HeapAlloc - before.HeapAlloc
	}
	return 0
}

// dirSize walks a directory tree and returns the total file size in bytes.
func dirSize(path string) int64 {
	var total int64
	err := filepath.WalkDir(path, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			total += info.Size()
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	return total
}
