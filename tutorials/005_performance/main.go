// Tutorial 005: Performance & Storage
//
// Measures write throughput, heap and disk use, and read speed across three
// backends (the memory store, Badger in memory, Badger on disk), then the
// doors and indexes that make reads and writes cheaper:
//
//   - Point reads:     Nodes().Get (a private copy) against Nodes().Lend (the
//     store's frozen row, no copy)
//   - Bulk writes:     single Nodes().Add calls, one g.Batch(), and g.Ingest()
//     sessions that prepare in parallel
//   - Property index:  Nodes().ByLabelAndProperty with and without
//     Index().CreateProperty
//   - Read budget:     store.WithReadBudget bounds the work of one selective read
//   - Temporal index:  Temporal().NodesByLabelAt with and without
//     Index().CreateTemporal
//   - Vector index:    Index().SearchNearest over Index().CreateVector (HNSW)
//
// Figures depend on the machine; run it twice before drawing conclusions.
//
// Run: go run ./tutorials/005_performance/
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
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

// kib and mib convert a byte count to KiB (1 << 10) and MiB (1 << 20).
func kib(b uint64) int64                  { return int64(b >> 10) } // #nosec G115 -- a heap size in KiB fits int64
func mib(b int64) float64                 { return float64(b) / (1 << 20) }
func perSec(n int, d time.Duration) int64 { return int64(float64(n) / d.Seconds()) }

// randomVec returns a random float32 vector of the given dimension.
func randomVec(dims int, rng *rand.Rand) []float32 {
	v := make([]float32, dims)
	for i := range v {
		v[i] = rng.Float32()
	}
	return v
}

func main() {
	ctx := context.Background()
	const (
		nodeCount = 10_000
		relCount  = 50_000
	)

	// ----------------------------------------------------------------
	fmt.Println("=== 1. Memory Store Writes ===")
	// ----------------------------------------------------------------

	gMem, err := graph.New(graph.Config{SnowflakeNodeID: 1})
	if err != nil {
		log.Fatal(err)
	}
	memElapsed, memNodeOps, memRelOps := benchmarkBackend("memory", gMem, nodeCount, relCount)
	fmt.Printf("  Elapsed:     %v\n", memElapsed)
	fmt.Printf("  Node write:  %s ops/sec\n", commas(memNodeOps))
	fmt.Printf("  Rel write:   %s ops/sec\n", commas(memRelOps))

	// ----------------------------------------------------------------
	fmt.Println("\n=== 2. Badger In-Memory Writes ===")
	// ----------------------------------------------------------------

	gBadgerMem, err := graph.New(graph.Config{SnowflakeNodeID: 2, BadgerInMemory: true})
	if err != nil {
		log.Fatal(err)
	}
	bmElapsed, bmNodeOps, bmRelOps := benchmarkBackend("badger in memory", gBadgerMem, nodeCount, relCount)
	fmt.Printf("  Elapsed:     %v\n", bmElapsed)
	fmt.Printf("  Node write:  %s ops/sec\n", commas(bmNodeOps))
	fmt.Printf("  Rel write:   %s ops/sec\n", commas(bmRelOps))

	// ----------------------------------------------------------------
	fmt.Println("\n=== 3. Badger On-Disk Writes ===")
	// ----------------------------------------------------------------

	// The store lives in a temporary directory this program removes at exit.
	tmpDir, err := os.MkdirTemp("", "tkg-005-*")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	gDisk, err := graph.New(graph.Config{SnowflakeNodeID: 3, BadgerDir: tmpDir})
	if err != nil {
		log.Fatal(err)
	}
	diskElapsed, diskNodeOps, diskRelOps := benchmarkBackend("badger on disk", gDisk, nodeCount, relCount)
	fmt.Printf("  Elapsed:     %v\n", diskElapsed)
	fmt.Printf("  Node write:  %s ops/sec\n", commas(diskNodeOps))
	fmt.Printf("  Rel write:   %s ops/sec\n", commas(diskRelOps))

	// ----------------------------------------------------------------
	fmt.Println("\n=== 4. Heap Use ===")
	// ----------------------------------------------------------------

	memUsageMem := measureMemoryUsage(graph.Config{SnowflakeNodeID: 4}, nodeCount, relCount)
	memUsageBadger := measureMemoryUsage(graph.Config{SnowflakeNodeID: 5, BadgerInMemory: true}, nodeCount, relCount)
	fmt.Printf("  Memory store:      %s KiB live heap for %s nodes + %s rels\n",
		commas(kib(memUsageMem)), commas(nodeCount), commas(relCount))
	fmt.Printf("  Badger in memory:  %s KiB live heap for %s nodes + %s rels\n",
		commas(kib(memUsageBadger)), commas(nodeCount), commas(relCount))

	// ----------------------------------------------------------------
	fmt.Println("\n=== 5. Disk Use ===")
	// ----------------------------------------------------------------

	diskSize := dirSize(tmpDir)
	fmt.Printf("  Badger on disk:  %.2f MiB of files for %s nodes + %s rels (after Close)\n",
		mib(diskSize), commas(nodeCount), commas(relCount))

	// ----------------------------------------------------------------
	fmt.Println("\n=== 6. Point Reads: Get and Lend ===")
	// ----------------------------------------------------------------

	gQuery, err := graph.New(graph.Config{SnowflakeNodeID: 6, BadgerInMemory: true})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := gQuery.Close(); err != nil {
			log.Printf("error closing query graph: %v", err)
		}
	}()

	qNodes := make([]*types.Node, nodeCount)
	for i := range nodeCount {
		n, err := gQuery.Nodes().Add(ctx, []string{fmt.Sprintf("Type%d", i%10)}, map[string]any{"idx": int64(i)})
		if err != nil {
			log.Fatal(err)
		}
		qNodes[i] = n
	}
	qRels := make([]*types.Relationship, relCount)
	for i := range relCount {
		r, err := gQuery.Rels().Add(ctx, "EDGE", qNodes[i%nodeCount], qNodes[endIndex(i, nodeCount)], nil)
		if err != nil {
			log.Fatal(err)
		}
		qRels[i] = r
	}

	// Get returns a private, mutable copy of the current row. Lend returns
	// the store's own frozen row without copying it: read it, hold it, but
	// DeepCopy it before changing it.
	const lookupCount = 10_000
	start := time.Now()
	for i := range lookupCount {
		if _, err := gQuery.Nodes().Get(ctx, qNodes[i%nodeCount].ID()); err != nil {
			log.Fatal(err)
		}
	}
	getNodeDur := time.Since(start)

	start = time.Now()
	for i := range lookupCount {
		row, err := gQuery.Nodes().Lend(ctx, qNodes[i%nodeCount].ID())
		if err != nil {
			log.Fatal(err)
		}
		if _, ok := row.BorrowProperty("idx"); !ok {
			log.Fatal("lent row lost its property")
		}
	}
	lendNodeDur := time.Since(start)

	start = time.Now()
	for i := range lookupCount {
		if _, err := gQuery.Rels().Get(ctx, qRels[i%relCount].ID()); err != nil {
			log.Fatal(err)
		}
	}
	getRelDur := time.Since(start)

	getNodeOps, lendNodeOps, getRelOps := perSec(lookupCount, getNodeDur), perSec(lookupCount, lendNodeDur), perSec(lookupCount, getRelDur)
	fmt.Printf("  Nodes().Get:   %s ops/sec  (%d ns/op)\n", commas(getNodeOps), getNodeDur.Nanoseconds()/lookupCount)
	fmt.Printf("  Nodes().Lend:  %s ops/sec  (%d ns/op)\n", commas(lendNodeOps), lendNodeDur.Nanoseconds()/lookupCount)
	fmt.Printf("  Rels().Get:    %s ops/sec  (%d ns/op)\n", commas(getRelOps), getRelDur.Nanoseconds()/lookupCount)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 7. Adjacency and Label Scans (no index) ===")
	// ----------------------------------------------------------------

	const outQueryCount = 10_000
	var outTotal int
	start = time.Now()
	for i := range outQueryCount {
		rels, err := gQuery.Rels().Outgoing(qNodes[i%nodeCount].ID(), "")
		if err != nil {
			log.Fatal(err)
		}
		outTotal += len(rels)
	}
	outDur := time.Since(start)
	outOps, outEntPerSec := perSec(outQueryCount, outDur), perSec(outTotal, outDur)

	const labelQueryCount = 1000
	var labelTotal int
	start = time.Now()
	for i := range labelQueryCount {
		nodes, err := gQuery.Nodes().ByLabel(fmt.Sprintf("Type%d", i%10), graph.QueryOpts{})
		if err != nil {
			log.Fatal(err)
		}
		labelTotal += len(nodes)
	}
	labelDur := time.Since(start)
	labelOps, labelEntPerSec := perSec(labelQueryCount, labelDur), perSec(labelTotal, labelDur)

	fmt.Printf("  Rels().Outgoing:  %s queries/sec  (~%.0f rels/query,  ~%s entities/sec)\n",
		commas(outOps), float64(outTotal)/outQueryCount, commas(outEntPerSec))
	fmt.Printf("  Nodes().ByLabel:  %s queries/sec  (~%.0f nodes/query, ~%s entities/sec)\n",
		commas(labelOps), float64(labelTotal)/labelQueryCount, commas(labelEntPerSec))

	// ----------------------------------------------------------------
	fmt.Println("\n=== 8. Bulk Writes: Single Adds, Batch, Ingest ===")
	// ----------------------------------------------------------------

	// A single Add is one write with its own transaction instant. A batch
	// validates while it queues, then applies all its writes in one step,
	// atomic against readers, with a result per operation. An ingest session
	// prepares on the caller's goroutine (validation, hashing, ID minting),
	// so several sessions prepare in parallel while one applier writes their
	// groups. The doors differ in what they guarantee first; their speed
	// depends on the store and the load. On the memory store a single Add is
	// already cheap, so the ratios printed here can be near or below 1.
	const bulkN = 20_000

	gBulk, err := graph.New(graph.Config{SnowflakeNodeID: 7})
	if err != nil {
		log.Fatal(err)
	}

	start = time.Now()
	for i := range bulkN {
		if _, err := gBulk.Nodes().Add(ctx, []string{"Single"}, map[string]any{"idx": i}); err != nil {
			log.Fatalf("Nodes().Add: %v", err)
		}
	}
	singleOps := perSec(bulkN, time.Since(start))

	start = time.Now()
	bResult, err := gBulk.Batch().Run(func(bb *graph.BatchBuilder) error {
		for i := range bulkN {
			if _, err := bb.AddNode([]string{"Batch"}, map[string]any{"idx": i}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	batchOps := perSec(bResult.Created, time.Since(start))

	const (
		sessions  = 4
		groupSize = 1_000
	)
	start = time.Now()
	var wg sync.WaitGroup
	errs := make([]error, sessions)
	for s := range sessions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[s] = ingestNodes(gBulk, s, bulkN/sessions, groupSize)
		}()
	}
	wg.Wait()
	ingestDur := time.Since(start)
	if err := errors.Join(errs...); err != nil {
		log.Fatal(err)
	}
	ingested, err := gBulk.Nodes().CountByLabel("Ingest")
	if err != nil {
		log.Fatal(err)
	}
	ingestOps := perSec(ingested, ingestDur)

	if err := gBulk.Close(); err != nil {
		log.Fatal(err)
	}

	batchSpeedup := float64(batchOps) / float64(singleOps)
	ingestSpeedup := float64(ingestOps) / float64(singleOps)
	fmt.Printf("  Single Nodes().Add:       %s nodes/sec\n", commas(singleOps))
	fmt.Printf("  g.Batch().Run:            %s nodes/sec  (%.1fx single, queueing included; created %s, failed %d)\n",
		commas(batchOps), batchSpeedup, commas(int64(bResult.Created)), bResult.Failed)
	fmt.Printf("  g.Ingest(), %d sessions:   %s nodes/sec  (%.1fx single; %s nodes in groups of %s)\n",
		sessions, commas(ingestOps), ingestSpeedup, commas(int64(ingested)), commas(groupSize))

	// ----------------------------------------------------------------
	fmt.Println("\n=== 9. Property Index ===")
	// ----------------------------------------------------------------
	// Without an index, ByLabelAndProperty reads every node of the label and
	// filters. Index().CreateProperty keeps a map from value to nodes, so the
	// lookup reads only the matches; the answer is the same.
	//
	// Setup: 10,000 Product nodes, score in [0, 100): 100 nodes per value.

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
		if _, err := gProp.Nodes().Add(ctx, []string{"Product"}, map[string]any{"score": i % propValues}); err != nil {
			log.Fatalf("Nodes().Add: %v", err)
		}
	}

	start = time.Now()
	for range propQ {
		if _, err := gProp.Nodes().ByLabelAndProperty("Product", "score", queryScore, graph.QueryOpts{}); err != nil {
			log.Fatalf("ByLabelAndProperty: %v", err)
		}
	}
	propNoIdxOps := perSec(propQ, time.Since(start))

	// The index is filled from the existing nodes when it is created.
	if err := gProp.Index().CreateProperty("Product", "score"); err != nil {
		log.Fatalf("Index().CreateProperty: %v", err)
	}

	var matches int
	start = time.Now()
	for range propQ {
		rows, err := gProp.Nodes().ByLabelAndProperty("Product", "score", queryScore, graph.QueryOpts{})
		if err != nil {
			log.Fatalf("ByLabelAndProperty: %v", err)
		}
		matches = len(rows)
	}
	propWithIdxOps := perSec(propQ, time.Since(start))

	propSpeedup := float64(propWithIdxOps) / float64(propNoIdxOps)
	fmt.Printf("  ByLabelAndProperty without index: %s ops/sec\n", commas(propNoIdxOps))
	fmt.Printf("  ByLabelAndProperty with index:    %s ops/sec  (%.1fx, %d matches)\n",
		commas(propWithIdxOps), propSpeedup, matches)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 10. Read Budget ===")
	// ----------------------------------------------------------------
	// A read budget in the context bounds the work of one selective read,
	// not only the rows it returns. A door that supports budgets (the ...Ctx
	// doors) counts its work into the meter and, before the work that would
	// pass a limit, refuses with a *store.ReadBudgetError and no rows: the
	// answer is exact or refused. A meter counts every call made with its
	// context, and a refused context is spent, so take a fresh one per read.

	budgetCtx, meter := store.WithReadBudget(ctx, store.ReadBudget{Candidates: 50})
	_, err = gProp.Nodes().ByLabelAndPropertyCtx(budgetCtx, "Product", "score", queryScore, graph.QueryOpts{})
	var budgetErr *store.ReadBudgetError
	if !errors.As(err, &budgetErr) || !errors.Is(err, store.ErrReadBudget) {
		log.Fatalf("expected a read budget refusal, got %v", err)
	}
	// The index hands on its 100 matches, which pass the limit of 50: the
	// door returns the error and no rows. Work includes the work past the limit.
	fmt.Printf("  Candidates 50:  refused at %q by %s, work %d candidates\n",
		budgetErr.Limit, budgetErr.Door, meter.Work().Candidates)

	budgetCtx, meter = store.WithReadBudget(ctx, store.ReadBudget{Candidates: 200})
	rows, err := gProp.Nodes().ByLabelAndPropertyCtx(budgetCtx, "Product", "score", queryScore, graph.QueryOpts{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Candidates 200: %d rows, work %d candidates\n", len(rows), meter.Work().Candidates)

	// A key without an index falls back to the label's every node, and the
	// budget counts all of them before the scan reads a row.
	budgetCtx, meter = store.WithReadBudget(ctx, store.ReadBudget{Candidates: 200})
	_, err = gProp.Nodes().ByLabelAndPropertyCtx(budgetCtx, "Product", "name", "x", graph.QueryOpts{})
	fmt.Printf("  Candidates 200, key without an index: refused=%v, work %s candidates\n",
		errors.Is(err, store.ErrReadBudget), commas(int64(meter.Work().Candidates))) // #nosec G115 -- a count of nodes

	if err := gProp.Close(); err != nil {
		log.Fatal(err)
	}

	// ----------------------------------------------------------------
	fmt.Println("\n=== 11. Temporal Index ===")
	// ----------------------------------------------------------------
	// Without a temporal index, NodesByLabelAt reads every node of the label
	// and checks its valid interval. Index().CreateTemporal keeps an interval
	// index over the label's valid-time envelopes; a point query visits only
	// the intervals that contain the instant and reads only those nodes.
	//
	// Scenario: 50,000 Event nodes, 90% closed an hour ago (CloseVersion), so
	// a query at now matches the 10% still open.

	const (
		tempN      = 50_000
		tempClosed = tempN * 9 / 10
		tempQ      = 100
	)

	gTemp, err := graph.New(graph.Config{SnowflakeNodeID: 9})
	if err != nil {
		log.Fatal(err)
	}

	// A new store's first versions without a valid start are valid from the
	// unbounded past, so an end an hour ago is after their start.
	closeTime := types.InstantFromTime(time.Now().Add(-time.Hour))
	tempNodes := make([]*types.Node, tempN)
	for i := range tempN {
		n, err := gTemp.Nodes().Add(ctx, []string{"Event"}, nil)
		if err != nil {
			log.Fatalf("Nodes().Add: %v", err)
		}
		tempNodes[i] = n
	}
	for i := range tempClosed {
		if err := gTemp.Nodes().CloseVersion(ctx, tempNodes[i].ID(), closeTime); err != nil {
			log.Fatalf("Nodes().CloseVersion: %v", err)
		}
	}

	// Read at the graph clock, which every committed write is at or below.
	nowQuery, err := gTemp.Temporal().NowTx()
	if err != nil {
		log.Fatal(err)
	}

	var openCount int
	start = time.Now()
	for range tempQ {
		rows, err := gTemp.Temporal().NodesByLabelAt("Event", nowQuery)
		if err != nil {
			log.Fatalf("NodesByLabelAt: %v", err)
		}
		openCount = len(rows)
	}
	tempNoIdxOps := perSec(tempQ, time.Since(start))

	if err := gTemp.Index().CreateTemporal("Event"); err != nil {
		log.Fatalf("Index().CreateTemporal: %v", err)
	}

	start = time.Now()
	for range tempQ {
		if _, err := gTemp.Temporal().NodesByLabelAt("Event", nowQuery); err != nil {
			log.Fatalf("NodesByLabelAt: %v", err)
		}
	}
	tempWithIdxOps := perSec(tempQ, time.Since(start))

	if err := gTemp.Close(); err != nil {
		log.Fatal(err)
	}

	tempSpeedup := float64(tempWithIdxOps) / float64(tempNoIdxOps)
	fmt.Printf("  NodesByLabelAt without index: %s ops/sec  (%s nodes, %s valid now)\n",
		commas(tempNoIdxOps), commas(tempN), commas(int64(openCount)))
	fmt.Printf("  NodesByLabelAt with index:    %s ops/sec  (%.1fx)\n",
		commas(tempWithIdxOps), tempSpeedup)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 12. Vector Index (k-NN) ===")
	// ----------------------------------------------------------------
	// Index().CreateVector builds an approximate HNSW index (M=16,
	// EfConstruction=200, EfSearch=64) over a float vector property;
	// Index().CreateVectorWithOptions picks exact brute-force search or other
	// tuning. SearchNearest ranks by each node's current vector.
	//
	// Setup: 2,000 Doc nodes with 64-dimension embeddings, k = 10.

	const (
		vecN    = 2_000
		vecDims = 64
		vecK    = 10
		vecQ    = 500
	)

	rng := rand.New(rand.NewSource(42)) // #nosec G404 -- deterministic seed for reproducible benchmark data

	gVec, err := graph.New(graph.Config{SnowflakeNodeID: 10})
	if err != nil {
		log.Fatal(err)
	}
	for range vecN {
		if _, err := gVec.Nodes().Add(ctx, []string{"Doc"}, map[string]any{"embedding": randomVec(vecDims, rng)}); err != nil {
			log.Fatalf("Nodes().Add: %v", err)
		}
	}

	if err := gVec.Index().CreateVector("Doc", "embedding", vecDims, graph.DistanceCosine); err != nil {
		log.Fatalf("Index().CreateVector: %v", err)
	}

	query := randomVec(vecDims, rng)
	var hits int
	start = time.Now()
	for range vecQ {
		results, err := gVec.Index().SearchNearest("Doc", "embedding", query, vecK, graph.QueryOpts{})
		if err != nil {
			log.Fatalf("Index().SearchNearest: %v", err)
		}
		hits = len(results)
	}
	vecDur := time.Since(start)
	vecOps := perSec(vecQ, vecDur)
	vecNs := vecDur.Nanoseconds() / vecQ

	if err := gVec.Close(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("  SearchNearest: %s ops/sec  (%d ns/op)  [N=%d, dims=%d, k=%d, %d hits, cosine, HNSW]\n",
		commas(vecOps), vecNs, vecN, vecDims, vecK, hits)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 13. Summary ===")
	// ----------------------------------------------------------------

	fmt.Println()
	fmt.Printf("  %-24s %12s %12s %12s\n", "Backend", "Node ops/s", "Rel ops/s", "Elapsed")
	fmt.Printf("  %-24s %12s %12s %12s\n", "-------", "----------", "---------", "-------")
	fmt.Printf("  %-24s %12s %12s %12v\n", "Memory store", commas(memNodeOps), commas(memRelOps), memElapsed.Round(time.Millisecond))
	fmt.Printf("  %-24s %12s %12s %12v\n", "Badger (in memory)", commas(bmNodeOps), commas(bmRelOps), bmElapsed.Round(time.Millisecond))
	fmt.Printf("  %-24s %12s %12s %12v\n", "Badger (on disk)", commas(diskNodeOps), commas(diskRelOps), diskElapsed.Round(time.Millisecond))
	fmt.Println()
	fmt.Printf("  %-24s %12s\n", "Live heap", "KiB")
	fmt.Printf("  %-24s %12s\n", "---------", "---")
	fmt.Printf("  %-24s %12s\n", "Memory store", commas(kib(memUsageMem)))
	fmt.Printf("  %-24s %12s\n", "Badger (in memory)", commas(kib(memUsageBadger)))
	fmt.Println()
	fmt.Printf("  %-24s %12s\n", "Disk", "MiB")
	fmt.Printf("  %-24s %12s\n", "----", "---")
	fmt.Printf("  %-24s %12.2f\n", "Badger (on disk)", mib(diskSize))
	fmt.Println()
	fmt.Printf("  %-24s %12s %14s\n", "Read (Badger in memory)", "ops/sec", "entities/sec")
	fmt.Printf("  %-24s %12s %14s\n", "-----------------------", "-------", "------------")
	fmt.Printf("  %-24s %12s %14s\n", "Nodes().Get", commas(getNodeOps), "-")
	fmt.Printf("  %-24s %12s %14s\n", "Nodes().Lend", commas(lendNodeOps), "-")
	fmt.Printf("  %-24s %12s %14s\n", "Rels().Get", commas(getRelOps), "-")
	fmt.Printf("  %-24s %12s %14s\n", "Rels().Outgoing", commas(outOps), commas(outEntPerSec))
	fmt.Printf("  %-24s %12s %14s\n", "Nodes().ByLabel", commas(labelOps), commas(labelEntPerSec))
	fmt.Println()
	fmt.Printf("  %-24s %12s %12s %10s\n", "Compared", "Baseline", "Measured", "Ratio")
	fmt.Printf("  %-24s %12s %12s %10s\n", "--------", "--------", "--------", "-----")
	fmt.Printf("  %-24s %12s %12s %9.1fx\n", "Batch vs single Add", commas(singleOps), commas(batchOps), batchSpeedup)
	fmt.Printf("  %-24s %12s %12s %9.1fx\n", "Ingest vs single Add", commas(singleOps), commas(ingestOps), ingestSpeedup)
	fmt.Printf("  %-24s %12s %12s %9.1fx\n", "Property index", commas(propNoIdxOps), commas(propWithIdxOps), propSpeedup)
	fmt.Printf("  %-24s %12s %12s %9.1fx\n", "Temporal index (10%)", commas(tempNoIdxOps), commas(tempWithIdxOps), tempSpeedup)
	fmt.Println()
	fmt.Printf("  %-24s %12s %10s\n", "Vector index (HNSW)", "ops/sec", "ns/op")
	fmt.Printf("  %-24s %12s %10s\n", "-------------------", "-------", "-----")
	fmt.Printf("  %-24s %12s %10d\n", "Index().SearchNearest", commas(vecOps), vecNs)

	fmt.Println("\n=== Done ===")
}

// endIndex picks the end node of relationship i: spread over the nodes and
// never the start node, since self-loops are refused by default.
func endIndex(i, nodeCount int) int {
	e := (i*7 + 3) % nodeCount
	if e == i%nodeCount {
		e = (e + 1) % nodeCount
	}
	return e
}

// ingestNodes writes n Ingest nodes through one ingest session, submitting a
// group every groupSize nodes. Sync makes each Submit return once its group
// is applied and visible.
func ingestNodes(g *graph.Graph, session, n, groupSize int) error {
	s, err := g.Ingest().NewSession(ingest.IngestOptions{Sync: true, DeclareLabels: []string{"Ingest"}})
	if err != nil {
		return err
	}
	for i := range n {
		if _, err := s.AddNode([]string{"Ingest"}, map[string]any{"session": session, "idx": i}); err != nil {
			_ = s.Close()
			return err
		}
		if s.Pending() >= groupSize {
			if _, err := s.Submit(); err != nil {
				_ = s.Close()
				return err
			}
		}
	}
	return s.Close() // submits the rest
}

// benchmarkBackend populates a graph with nodeCount nodes and relCount
// relationships, closes it, and returns the elapsed time and throughput.
func benchmarkBackend(name string, g *graph.Graph, nodeCount, relCount int) (time.Duration, int64, int64) {
	ctx := context.Background()
	start := time.Now()

	nodes := make([]*types.Node, nodeCount)
	for i := range nodeCount {
		n, err := g.Nodes().Add(ctx, []string{fmt.Sprintf("Type%d", i%10)}, nil)
		if err != nil {
			log.Fatalf("[%s] Nodes().Add %d: %v", name, i, err)
		}
		nodes[i] = n
	}
	nodeDur := time.Since(start)

	relStart := time.Now()
	for i := range relCount {
		if _, err := g.Rels().Add(ctx, "EDGE", nodes[i%nodeCount], nodes[endIndex(i, nodeCount)], nil); err != nil {
			log.Fatalf("[%s] Rels().Add %d: %v", name, i, err)
		}
	}
	relDur := time.Since(relStart)

	if err := g.Close(); err != nil {
		log.Fatalf("[%s] Close: %v", name, err)
	}
	return time.Since(start), perSec(nodeCount, nodeDur), perSec(relCount, relDur)
}

// measureMemoryUsage returns the live heap growth in bytes after a graph
// opened with cfg holds nodeCount nodes and relCount relationships.
func measureMemoryUsage(cfg graph.Config, nodeCount, relCount int) uint64 {
	ctx := context.Background()
	runtime.GC()
	debug.FreeOSMemory()
	runtime.GC()

	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	g, err := graph.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	nodes := make([]*types.Node, nodeCount)
	for i := range nodeCount {
		n, err := g.Nodes().Add(ctx, []string{fmt.Sprintf("Type%d", i%10)}, nil)
		if err != nil {
			log.Fatal(err)
		}
		nodes[i] = n
	}
	for i := range relCount {
		if _, err := g.Rels().Add(ctx, "EDGE", nodes[i%nodeCount], nodes[endIndex(i, nodeCount)], nil); err != nil {
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
