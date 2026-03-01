// Tutorial 005: Performance & Storage
//
// Benchmarks tkg-v3 across three backends: MemoryStore (default),
// BadgerStore in-memory, and BadgerStore on-disk. Measures write throughput,
// query performance, memory usage, and on-disk storage footprint.
//
// Run: go run ./tutorials/005_performance/
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/graph"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
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

	fmt.Printf("  MemoryStore:     %s KB heap for %s nodes + %s rels\n", commas(int64(memUsageMem/1024)), commas(nodeCount), commas(relCount))
	fmt.Printf("  BadgerInMemory:  %s KB heap for %s nodes + %s rels\n", commas(int64(memUsageBadger/1024)), commas(nodeCount), commas(relCount))

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
		log.Fatal(err)
	}
	defer func() {
		if err := gQuery.Close(); err != nil {
			log.Printf("error closing query graph: %v", err)
		}
	}()

	// Populate query graph.
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

	// Benchmark GetNode — single-entity retrieval.
	const lookupCount = 10_000
	start := time.Now()
	for i := range lookupCount {
		id := qNodes[i%nodeCount].InternalID().SnowflakeID()
		if _, err := gQuery.GetNode(id); err != nil {
			log.Fatal(err)
		}
	}
	getNodeElapsed := time.Since(start)
	getNodeOps := int64(float64(lookupCount) / getNodeElapsed.Seconds())
	getNodeNs := getNodeElapsed.Nanoseconds() / lookupCount

	// Benchmark GetRelationship — single-entity retrieval.
	start = time.Now()
	for i := range lookupCount {
		id := qRels[i%relCount].InternalID().SnowflakeID()
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
	fmt.Println("\n=== 7. Query Performance ===")
	// ----------------------------------------------------------------

	// Benchmark OutgoingRelationships — adjacency query, small result sets.
	const outQueryCount = 10_000
	var outTotalEntities int64
	start = time.Now()
	for i := range outQueryCount {
		id := qNodes[i%nodeCount].InternalID().SnowflakeID()
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

	// Benchmark NodesByLabel — index scan, large result sets.
	const labelQueryCount = 1000
	var labelTotalEntities int64
	start = time.Now()
	for i := range labelQueryCount {
		label := fmt.Sprintf("Type%d", i%10)
		nodes, err := gQuery.NodesByLabel(label)
		if err != nil {
			log.Fatal(err)
		}
		labelTotalEntities += int64(len(nodes))
	}
	labelElapsed := time.Since(start)
	labelOps := int64(float64(labelQueryCount) / labelElapsed.Seconds())
	labelAvgSize := float64(labelTotalEntities) / labelQueryCount
	labelEntPerSec := int64(float64(labelTotalEntities) / labelElapsed.Seconds())

	fmt.Printf("  OutgoingRelationships: %s queries/sec  (~%.0f rels/query, ~%s entities/sec)\n",
		commas(outOps), outAvgSize, commas(outEntPerSec))
	fmt.Printf("  NodesByLabel:          %s queries/sec  (~%.0f nodes/query, ~%s entities/sec)\n",
		commas(labelOps), labelAvgSize, commas(labelEntPerSec))

	// ----------------------------------------------------------------
	fmt.Println("\n=== 8. Summary ===")
	// ----------------------------------------------------------------

	fmt.Println()
	fmt.Printf("  %-22s %12s %12s %12s\n", "Backend", "Node ops/s", "Rel ops/s", "Elapsed")
	fmt.Printf("  %-22s %12s %12s %12s\n", "------", "----------", "---------", "-------")
	fmt.Printf("  %-22s %12s %12s %12v\n", "MemoryStore", commas(int64(memNodeOps)), commas(int64(memRelOps)), memElapsed.Round(time.Millisecond))
	fmt.Printf("  %-22s %12s %12s %12v\n", "BadgerStore (memory)", commas(int64(bmNodeOps)), commas(int64(bmRelOps)), bmElapsed.Round(time.Millisecond))
	fmt.Printf("  %-22s %12s %12s %12v\n", "BadgerStore (disk)", commas(int64(diskNodeOps)), commas(int64(diskRelOps)), diskElapsed.Round(time.Millisecond))
	fmt.Println()
	fmt.Printf("  %-22s %12s\n", "Memory (heap)", "KB")
	fmt.Printf("  %-22s %12s\n", "------", "--")
	fmt.Printf("  %-22s %12s\n", "MemoryStore", commas(int64(memUsageMem/1024)))
	fmt.Printf("  %-22s %12s\n", "BadgerStore (memory)", commas(int64(memUsageBadger/1024)))
	fmt.Println()
	fmt.Printf("  %-22s %12s\n", "Storage", "Size")
	fmt.Printf("  %-22s %12s\n", "------", "----")
	fmt.Printf("  %-22s %10.2f MB\n", "BadgerStore (disk)", float64(diskSize)/(1024*1024))
	fmt.Println()
	fmt.Printf("  %-22s %12s %14s\n", "Query", "ops/sec", "entities/sec")
	fmt.Printf("  %-22s %12s %14s\n", "------", "-------", "------------")
	fmt.Printf("  %-22s %12s %14s\n", "GetNode", commas(getNodeOps), "-")
	fmt.Printf("  %-22s %12s %14s\n", "GetRelationship", commas(getRelOps), "-")
	fmt.Printf("  %-22s %12s %14s\n", "OutgoingRels", commas(outOps), commas(outEntPerSec))
	fmt.Printf("  %-22s %12s %14s\n", "NodesByLabel", commas(labelOps), commas(labelEntPerSec))

	fmt.Println("\n=== Done ===")
}

// benchmarkBackend populates a graph with nodeCount nodes and relCount
// relationships, returning elapsed time and throughput.
func benchmarkBackend(name string, g *graph.Graph, nodeCount, relCount int) (time.Duration, int, int) {
	start := time.Now()

	// Create nodes with labels Type0-Type9.
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

	// Create relationships with deterministic distribution.
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

	// Include Close() in timing — captures async flush for BadgerStore.
	// MemoryStore.Close() is a no-op, so this is safe for all backends.
	if err := g.Close(); err != nil {
		log.Fatalf("[%s] Close: %v", name, err)
	}
	total := time.Since(start)

	nodeOps := int(float64(nodeCount) / nodeElapsed.Seconds())
	relOps := int(float64(relCount) / relElapsed.Seconds())

	return total, nodeOps, relOps
}

// measureMemoryUsage creates nodeCount nodes and relCount relationships in a
// fresh graph and returns the approximate heap increase in bytes.
func measureMemoryUsage(newGraph func() *graph.Graph, nodeCount, relCount int) uint64 {
	// Aggressively clean up prior allocations (Badger goroutine stacks, etc.)
	// before measuring baseline. A single GC pass is insufficient because
	// finalizers require a second pass to collect their referents.
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
