package types

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

const benchCount = 1_000_000

// ─── Scale runs ──────────────────────────────────────────────────────────────
//
// Long tier only: each test skips under -short, so neither the gate nor
// `make race` runs them; `go test ./...` and the major gate do. They build one
// million entities, print time and heap, and check a sample of the values they
// wrote. The per-entity footprint measurements live in
// footprint_measure_test.go (measure tag).

func TestBenchMillionNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping million-node benchmark in short mode")
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	start := time.Now()

	nodes := make([]*Node, benchCount)
	for i := range benchCount {
		id := snowflake.ID(i + 1)
		n := NewNode(NodeID(id), 1, []uint16{2, 3})
		_ = n.SetProperty("name", fmt.Sprintf("node_%d", i))
		_ = n.SetProperty("score", i)
		_ = n.SetProperty("active", i%2 == 0)
		nodes[i] = n
	}

	elapsed := time.Since(start)

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	heapBytes := after.HeapAlloc - before.HeapAlloc
	t.Logf("Created %d nodes in %v (%.0f ns/node)", benchCount, elapsed, float64(elapsed.Nanoseconds())/float64(benchCount))
	t.Logf("Heap delta: %.1f MB (%.0f bytes/node)", float64(heapBytes)/(1<<20), float64(heapBytes)/float64(benchCount))

	for _, idx := range []int{0, benchCount / 2, benchCount - 1} {
		n := nodes[idx]
		val, ok := n.GetProperty("score")
		if !ok || val != idx {
			t.Fatalf("node[%d].GetProperty(\"score\") = (%v, %v), want (%d, true)", idx, val, ok, idx)
		}
	}
	runtime.KeepAlive(nodes)
}

func TestBenchMillionRelationships(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping million-relationship benchmark in short mode")
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	start := time.Now()

	rels := make([]*Relationship, benchCount)
	for i := range benchCount {
		id := snowflake.ID(i + 1)
		startID := snowflake.ID(i*2 + 1)
		endID := snowflake.ID(i*2 + 2)
		r := NewRelationship(RelID(id), 1, NodeID(startID), NodeID(endID))
		_ = r.SetProperty("weight", float64(i))
		_ = r.SetProperty("label", fmt.Sprintf("rel_%d", i))
		_ = r.SetProperty("active", i%2 == 0)
		rels[i] = r
	}

	elapsed := time.Since(start)

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	heapBytes := after.HeapAlloc - before.HeapAlloc
	t.Logf("Created %d relationships in %v (%.0f ns/rel)", benchCount, elapsed, float64(elapsed.Nanoseconds())/float64(benchCount))
	t.Logf("Heap delta: %.1f MB (%.0f bytes/rel)", float64(heapBytes)/(1<<20), float64(heapBytes)/float64(benchCount))

	for _, idx := range []int{0, benchCount / 2, benchCount - 1} {
		r := rels[idx]
		val, ok := r.GetProperty("weight")
		if !ok || val != float64(idx) {
			t.Fatalf("rel[%d].GetProperty(\"weight\") = (%v, %v), want (%v, true)", idx, val, ok, float64(idx))
		}
	}
	runtime.KeepAlive(rels)
}

func TestBenchMillionNodeLookup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping million-node lookup benchmark in short mode")
	}

	nodes := make(map[NodeID]*Node, benchCount)
	ids := make([]NodeID, benchCount)
	for i := range benchCount {
		id := snowflake.ID(i + 1)
		n := NewNode(NodeID(id), 1, nil)
		_ = n.SetProperty("val", i)
		nid := n.ID()
		nodes[nid] = n
		ids[i] = nid
	}

	start := time.Now()
	for range benchCount {
		idx := rand.IntN(benchCount)
		n := nodes[ids[idx]]
		val, ok := n.GetProperty("val")
		if !ok || val != idx {
			t.Fatalf("lookup failed for index %d: got (%v, %v)", idx, val, ok)
		}
	}
	elapsed := time.Since(start)

	opsPerSec := float64(benchCount) / elapsed.Seconds()
	t.Logf("Performed %d random lookups in %v (%.0f ops/sec)", benchCount, elapsed, opsPerSec)
	runtime.KeepAlive(nodes)
}
