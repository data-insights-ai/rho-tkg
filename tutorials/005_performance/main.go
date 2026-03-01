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
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/graph"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

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
	if err := gMem.Close(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("  Elapsed:     %v\n", memElapsed)
	fmt.Printf("  Node write:  %d ops/sec\n", memNodeOps)
	fmt.Printf("  Rel write:   %d ops/sec\n", memRelOps)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 2. BadgerStore In-Memory Benchmark ===")
	// ----------------------------------------------------------------

	gBadgerMem, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true})
	if err != nil {
		log.Fatal(err)
	}
	bmElapsed, bmNodeOps, bmRelOps := benchmarkBackend("BadgerInMemory", gBadgerMem, nodeCount, relCount)
	if err := gBadgerMem.Close(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("  Elapsed:     %v\n", bmElapsed)
	fmt.Printf("  Node write:  %d ops/sec\n", bmNodeOps)
	fmt.Printf("  Rel write:   %d ops/sec\n", bmRelOps)

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
	gDisk, err := graph.New(graph.Config{SnowflakeNodeID: 1, Store: bs})
	if err != nil {
		log.Fatal(err)
	}
	diskElapsed, diskNodeOps, diskRelOps := benchmarkBackend("BadgerOnDisk", gDisk, nodeCount, relCount)
	if err := gDisk.Close(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("  Elapsed:     %v\n", diskElapsed)
	fmt.Printf("  Node write:  %d ops/sec\n", diskNodeOps)
	fmt.Printf("  Rel write:   %d ops/sec\n", diskRelOps)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 4. Memory Usage ===")
	// ----------------------------------------------------------------

	memUsageMem := measureMemoryUsage(func() *graph.Graph {
		g, err := graph.New(graph.Config{SnowflakeNodeID: 1})
		if err != nil {
			log.Fatal(err)
		}
		return g
	}, nodeCount)

	memUsageBadger := measureMemoryUsage(func() *graph.Graph {
		g, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true})
		if err != nil {
			log.Fatal(err)
		}
		return g
	}, nodeCount)

	fmt.Printf("  MemoryStore:     %d KB heap for %d nodes\n", memUsageMem/1024, nodeCount)
	fmt.Printf("  BadgerInMemory:  %d KB heap for %d nodes\n", memUsageBadger/1024, nodeCount)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 5. Storage Usage ===")
	// ----------------------------------------------------------------

	diskSize := dirSize(tmpDir)
	fmt.Printf("  On-disk Badger:  %.2f MB for %d nodes + %d rels\n",
		float64(diskSize)/(1024*1024), nodeCount, relCount)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 6. Query Performance ===")
	// ----------------------------------------------------------------

	gQuery, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := gQuery.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	// Populate query graph.
	qNodes := make([]*queryNode, nodeCount)
	for i := range nodeCount {
		label := fmt.Sprintf("Type%d", i%10)
		n, err := gQuery.AddNode([]string{label}, nil)
		if err != nil {
			log.Fatal(err)
		}
		qNodes[i] = &queryNode{node: n}
	}
	for i := range relCount {
		sIdx := i % nodeCount
		eIdx := (i*7 + 3) % nodeCount
		if sIdx == eIdx {
			eIdx = (eIdx + 1) % nodeCount
		}
		if _, err := gQuery.AddRelationship("EDGE", qNodes[sIdx].node, qNodes[eIdx].node, nil); err != nil {
			log.Fatal(err)
		}
	}

	// Benchmark NodesByLabel.
	const queryCount = 1000
	start := time.Now()
	for i := range queryCount {
		label := fmt.Sprintf("Type%d", i%10)
		if _, err := gQuery.NodesByLabel(label); err != nil {
			log.Fatal(err)
		}
	}
	labelElapsed := time.Since(start)
	labelOps := int(float64(queryCount) / labelElapsed.Seconds())

	// Benchmark OutgoingRelationships.
	start = time.Now()
	for i := range queryCount {
		nodeIdx := i % nodeCount
		id := qNodes[nodeIdx].node.InternalID().SnowflakeID()
		if _, err := gQuery.OutgoingRelationships(id, ""); err != nil {
			log.Fatal(err)
		}
	}
	outElapsed := time.Since(start)
	outOps := int(float64(queryCount) / outElapsed.Seconds())

	fmt.Printf("  NodesByLabel:          %d queries/sec (%v for %d)\n", labelOps, labelElapsed, queryCount)
	fmt.Printf("  OutgoingRelationships: %d queries/sec (%v for %d)\n", outOps, outElapsed, queryCount)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 7. Summary ===")
	// ----------------------------------------------------------------

	fmt.Println()
	fmt.Printf("  %-22s %12s %12s %12s\n", "Backend", "Node ops/s", "Rel ops/s", "Elapsed")
	fmt.Printf("  %-22s %12s %12s %12s\n", "------", "----------", "---------", "-------")
	fmt.Printf("  %-22s %12d %12d %12v\n", "MemoryStore", memNodeOps, memRelOps, memElapsed.Round(time.Millisecond))
	fmt.Printf("  %-22s %12d %12d %12v\n", "BadgerStore (memory)", bmNodeOps, bmRelOps, bmElapsed.Round(time.Millisecond))
	fmt.Printf("  %-22s %12d %12d %12v\n", "BadgerStore (disk)", diskNodeOps, diskRelOps, diskElapsed.Round(time.Millisecond))
	fmt.Println()
	fmt.Printf("  %-22s %12s\n", "Memory (heap)", "KB")
	fmt.Printf("  %-22s %12s\n", "------", "--")
	fmt.Printf("  %-22s %12d\n", "MemoryStore", memUsageMem/1024)
	fmt.Printf("  %-22s %12d\n", "BadgerStore (memory)", memUsageBadger/1024)
	fmt.Println()
	fmt.Printf("  %-22s %12s\n", "Storage", "Size")
	fmt.Printf("  %-22s %12s\n", "------", "----")
	fmt.Printf("  %-22s %10.2f MB\n", "BadgerStore (disk)", float64(diskSize)/(1024*1024))

	fmt.Println("\n=== Done ===")
}

// queryNode wraps a graph node for the query benchmark population.
type queryNode struct {
	node *types.Node
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
	total := time.Since(start)

	nodeOps := int(float64(nodeCount) / nodeElapsed.Seconds())
	relOps := int(float64(relCount) / relElapsed.Seconds())

	return total, nodeOps, relOps
}

// measureMemoryUsage creates nodeCount nodes in a fresh graph and returns
// the approximate heap increase in bytes.
func measureMemoryUsage(newGraph func() *graph.Graph, nodeCount int) uint64 {
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	g := newGraph()
	for i := range nodeCount {
		label := fmt.Sprintf("Type%d", i%10)
		if _, err := g.AddNode([]string{label}, nil); err != nil {
			log.Fatal(err)
		}
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	if err := g.Close(); err != nil {
		log.Fatal(err)
	}

	if after.HeapInuse > before.HeapInuse {
		return after.HeapInuse - before.HeapInuse
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
