package types

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"testing"
	"time"
	"unsafe"

	snowflake "gitlab2024.bds421-cloud.com/bds421/rho/snowflake-2026"
)

const benchCount = 1_000_000

// ─── Scale benchmarks ────────────────────────────────────────────────────────

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
		n := NewNode(id, 1, []uint16{2, 3})
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
		r := NewRelationship(id, 1, startID, endID)
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

	nodes := make(map[nodeID]*Node, benchCount)
	ids := make([]nodeID, benchCount)
	for i := range benchCount {
		id := snowflake.ID(i + 1)
		n := NewNode(id, 1, nil)
		_ = n.SetProperty("val", i)
		nid := n.InternalID()
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

// ─── Memory footprint breakdown ──────────────────────────────────────────────
//
// Measures per-entity heap cost in configurations matching the spec estimates.
// Uses 100K entities per measurement. Data is kept alive via runtime.KeepAlive
// across the GC+ReadMemStats boundary to prevent premature collection.

const footprintCount = 100_000

// measureHeap allocates data via fn, forces GC, measures heap, and returns the
// delta. The returned data must be kept alive by the caller via runtime.KeepAlive.
func measureHeap(fn func() any) (any, uint64) {
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	data := fn()

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	runtime.KeepAlive(data)

	if after.HeapAlloc > before.HeapAlloc {
		return data, after.HeapAlloc - before.HeapAlloc
	}
	return data, 0
}

func TestBenchFootprintNodeBare(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping footprint benchmark in short mode")
	}

	data, heap := measureHeap(func() any {
		nodes := make([]*Node, footprintCount)
		for i := range footprintCount {
			nodes[i] = NewNode(snowflake.ID(i+1), 1, nil)
		}
		return nodes
	})

	perEntity := float64(heap) / float64(footprintCount)
	t.Logf("Node (bare, 1 label, 0 props):  %.0f bytes/node", perEntity)
	t.Logf("  sizeof(Node) = %d, heap measured = %.0f (delta = %.0f overhead from allocator)",
		unsafe.Sizeof(Node{}), perEntity, perEntity-float64(unsafe.Sizeof(Node{})))
	runtime.KeepAlive(data)
}

func TestBenchFootprintNodeNonTemporal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping footprint benchmark in short mode")
	}

	data, heap := measureHeap(func() any {
		nodes := make([]*Node, footprintCount)
		for i := range footprintCount {
			n := NewNode(snowflake.ID(i+1), 1, nil)
			_ = n.SetProperty("name", fmt.Sprintf("entity_%d", i))
			_ = n.SetProperty("desc", "a short description value")
			_ = n.SetProperty("score", i)
			_ = n.SetProperty("weight", float64(i)*0.5)
			_ = n.SetProperty("active", i%2 == 0)
			nodes[i] = n
		}
		return nodes
	})

	perEntity := float64(heap) / float64(footprintCount)
	specEstimate := 366.0
	t.Logf("Node (1 label, 5 props, no temporal):  %.0f bytes/node  (spec estimate: %.0fB)", perEntity, specEstimate)
	t.Logf("  Delta vs spec: %+.0f bytes (%.1f%%)", perEntity-specEstimate, (perEntity-specEstimate)/specEstimate*100)
	runtime.KeepAlive(data)
}

func TestBenchFootprintNodeFullTemporal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping footprint benchmark in short mode")
	}

	data, heap := measureHeap(func() any {
		nodes := make([]*Node, footprintCount)
		for i := range footprintCount {
			n := NewNode(snowflake.ID(i+1), 1, nil)
			_ = n.SetProperty("name", fmt.Sprintf("entity_%d", i))
			_ = n.SetProperty("desc", "a short description value")
			_ = n.SetProperty("score", i)
			_ = n.SetProperty("weight", float64(i)*0.5)
			_ = n.SetProperty("active", i%2 == 0)
			n.SetTemporal(&TemporalMetadata{
				ValidFrom: Instant(1740000000000), TxFrom: Instant(1740000000000),
				CreatedAt: Instant(1740000000000), UpdatedAt: Instant(1740000000001),
				CreatedBy: "system", UpdatedBy: "system",
			})
			n.SetIntegrity(&NodeIntegrity{
				Hash:     "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
				PrevHash: "f6e5d4c3b2a1f6e5d4c3b2a1f6e5d4c3b2a1f6e5d4c3b2a1f6e5d4c3b2a1f6e5",
			})
			nodes[i] = n
		}
		return nodes
	})

	perEntity := float64(heap) / float64(footprintCount)
	specEstimate := 494.0
	t.Logf("Node (1 label, 5 props, temporal+integrity):  %.0f bytes/node  (spec estimate: %.0fB)", perEntity, specEstimate)
	t.Logf("  Delta vs spec: %+.0f bytes (%.1f%%)", perEntity-specEstimate, (perEntity-specEstimate)/specEstimate*100)
	t.Logf("  Components:")
	t.Logf("    sizeof(Node)             = %d", unsafe.Sizeof(Node{}))
	t.Logf("    sizeof(TemporalMetadata) = %d", unsafe.Sizeof(TemporalMetadata{}))
	t.Logf("    sizeof(NodeIntegrity)    = %d", unsafe.Sizeof(NodeIntegrity{}))
	t.Logf("    struct subtotal          = %d", unsafe.Sizeof(Node{})+unsafe.Sizeof(TemporalMetadata{})+unsafe.Sizeof(NodeIntegrity{}))
	runtime.KeepAlive(data)
}

func TestBenchFootprintRelNonTemporal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping footprint benchmark in short mode")
	}

	data, heap := measureHeap(func() any {
		rels := make([]*Relationship, footprintCount)
		for i := range footprintCount {
			r := NewRelationship(snowflake.ID(i+1), 1, snowflake.ID(i*2+1), snowflake.ID(i*2+2))
			_ = r.SetProperty("weight", float64(i))
			_ = r.SetProperty("label", fmt.Sprintf("edge_%d", i))
			_ = r.SetProperty("score", i)
			_ = r.SetProperty("since", "2026-01-15")
			_ = r.SetProperty("active", i%2 == 0)
			rels[i] = r
		}
		return rels
	})

	perEntity := float64(heap) / float64(footprintCount)
	t.Logf("Relationship (5 props, no temporal):  %.0f bytes/rel", perEntity)
	t.Logf("  sizeof(Relationship) = %d", unsafe.Sizeof(Relationship{}))
	runtime.KeepAlive(data)
}

func TestBenchFootprintRelFullTemporal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping footprint benchmark in short mode")
	}

	data, heap := measureHeap(func() any {
		rels := make([]*Relationship, footprintCount)
		for i := range footprintCount {
			r := NewRelationship(snowflake.ID(i+1), 1, snowflake.ID(i*2+1), snowflake.ID(i*2+2))
			_ = r.SetProperty("weight", float64(i))
			_ = r.SetProperty("label", fmt.Sprintf("edge_%d", i))
			_ = r.SetProperty("score", i)
			_ = r.SetProperty("since", "2026-01-15")
			_ = r.SetProperty("active", i%2 == 0)
			r.SetTemporal(&TemporalMetadata{
				ValidFrom: Instant(1740000000000), TxFrom: Instant(1740000000000),
				CreatedAt: Instant(1740000000000), UpdatedAt: Instant(1740000000001),
				CreatedBy: "system", UpdatedBy: "system",
			})
			r.SetIntegrity(&RelIntegrity{
				Hash:     "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
				PrevHash: "f6e5d4c3b2a1f6e5d4c3b2a1f6e5d4c3b2a1f6e5d4c3b2a1f6e5d4c3b2a1f6e5",
			})
			rels[i] = r
		}
		return rels
	})

	perEntity := float64(heap) / float64(footprintCount)
	t.Logf("Relationship (5 props, temporal+integrity):  %.0f bytes/rel", perEntity)
	t.Logf("  Components:")
	t.Logf("    sizeof(Relationship)     = %d", unsafe.Sizeof(Relationship{}))
	t.Logf("    sizeof(TemporalMetadata) = %d", unsafe.Sizeof(TemporalMetadata{}))
	t.Logf("    sizeof(RelIntegrity)     = %d", unsafe.Sizeof(RelIntegrity{}))
	t.Logf("    struct subtotal          = %d", unsafe.Sizeof(Relationship{})+unsafe.Sizeof(TemporalMetadata{})+unsafe.Sizeof(RelIntegrity{}))
	runtime.KeepAlive(data)
}

func TestBenchFootprintTemporalOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping footprint benchmark in short mode")
	}

	data, heap := measureHeap(func() any {
		tms := make([]*TemporalMetadata, footprintCount)
		for i := range footprintCount {
			tms[i] = &TemporalMetadata{
				ValidFrom: Instant(1740000000000 + int64(i)),
				TxFrom:    Instant(1740000000000 + int64(i)),
				CreatedAt: Instant(1740000000000),
				UpdatedAt: Instant(1740000000000 + int64(i)),
				CreatedBy: "system",
				UpdatedBy: "system",
			}
		}
		return tms
	})

	perEntity := float64(heap) / float64(footprintCount)
	t.Logf("TemporalMetadata (with string fields):  %.0f bytes/alloc  (sizeof = %d)", perEntity, unsafe.Sizeof(TemporalMetadata{}))
	t.Logf("  Overhead vs sizeof: %+.0f bytes (allocator rounding + string backing)", perEntity-float64(unsafe.Sizeof(TemporalMetadata{})))
	runtime.KeepAlive(data)
}

func TestBenchFootprintIntegrityOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping footprint benchmark in short mode")
	}

	data, heap := measureHeap(func() any {
		igs := make([]*NodeIntegrity, footprintCount)
		for i := range footprintCount {
			igs[i] = &NodeIntegrity{
				Hash:     fmt.Sprintf("%064x", i),
				PrevHash: fmt.Sprintf("%064x", i+1),
			}
		}
		return igs
	})

	perEntity := float64(heap) / float64(footprintCount)
	t.Logf("NodeIntegrity (64-char hashes):  %.0f bytes/alloc  (sizeof = %d)", perEntity, unsafe.Sizeof(NodeIntegrity{}))
	t.Logf("  Overhead vs sizeof: %+.0f bytes (allocator rounding + string backing for 2x64B hash strings)", perEntity-float64(unsafe.Sizeof(NodeIntegrity{})))
	runtime.KeepAlive(data)
}

func TestBenchFootprintPropertySlice5(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping footprint benchmark in short mode")
	}

	data, heap := measureHeap(func() any {
		slices := make([]PropertySlice, footprintCount)
		for i := range footprintCount {
			var ps PropertySlice
			_ = ps.Set("active", true)
			_ = ps.Set("desc", "a short description value")
			_ = ps.Set("name", fmt.Sprintf("entity_%d", i))
			_ = ps.Set("score", i)
			_ = ps.Set("weight", float64(i)*0.5)
			slices[i] = ps
		}
		return slices
	})

	perSlice := float64(heap) / float64(footprintCount)
	t.Logf("PropertySlice (5 props: 2 string, 1 int, 1 float64, 1 bool):  %.0f bytes/slice", perSlice)
	t.Logf("  Property struct sizeof = %d, 5x = %d", unsafe.Sizeof(Property{}), 5*unsafe.Sizeof(Property{}))
	runtime.KeepAlive(data)
}

// ─── Summary report ──────────────────────────────────────────────────────────

func TestBenchFootprintSummary(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping footprint summary in short mode")
	}

	const n = footprintCount

	// All data slices kept alive until the end via runtime.KeepAlive.

	// 1. Bare nodes (1 label, 0 props)
	d1, heapBareNode := measureHeap(func() any {
		nodes := make([]*Node, n)
		for i := range n {
			nodes[i] = NewNode(snowflake.ID(i+1), 1, nil)
		}
		return nodes
	})

	// 2. Nodes with 5 props, no temporal
	d2, heapNonTempNode := measureHeap(func() any {
		nodes := make([]*Node, n)
		for i := range n {
			nd := NewNode(snowflake.ID(i+1), 1, nil)
			_ = nd.SetProperty("name", fmt.Sprintf("entity_%d", i))
			_ = nd.SetProperty("desc", "a short description value")
			_ = nd.SetProperty("score", i)
			_ = nd.SetProperty("weight", float64(i)*0.5)
			_ = nd.SetProperty("active", i%2 == 0)
			nodes[i] = nd
		}
		return nodes
	})

	// 3. Nodes with 5 props + temporal + integrity
	d3, heapFullNode := measureHeap(func() any {
		nodes := make([]*Node, n)
		for i := range n {
			nd := NewNode(snowflake.ID(i+1), 1, nil)
			_ = nd.SetProperty("name", fmt.Sprintf("entity_%d", i))
			_ = nd.SetProperty("desc", "a short description value")
			_ = nd.SetProperty("score", i)
			_ = nd.SetProperty("weight", float64(i)*0.5)
			_ = nd.SetProperty("active", i%2 == 0)
			nd.SetTemporal(&TemporalMetadata{
				ValidFrom: Instant(1740000000000), TxFrom: Instant(1740000000000),
				CreatedAt: Instant(1740000000000), UpdatedAt: Instant(1740000000001),
				CreatedBy: "system", UpdatedBy: "system",
			})
			nd.SetIntegrity(&NodeIntegrity{
				Hash:     "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
				PrevHash: "f6e5d4c3b2a1f6e5d4c3b2a1f6e5d4c3b2a1f6e5d4c3b2a1f6e5d4c3b2a1f6e5",
			})
			nodes[i] = nd
		}
		return nodes
	})

	// 4. Bare relationships (0 props)
	d4, heapBareRel := measureHeap(func() any {
		rels := make([]*Relationship, n)
		for i := range n {
			rels[i] = NewRelationship(snowflake.ID(i+1), 1, snowflake.ID(i*2+1), snowflake.ID(i*2+2))
		}
		return rels
	})

	// 5. Relationships with 5 props, no temporal
	d5, heapNonTempRel := measureHeap(func() any {
		rels := make([]*Relationship, n)
		for i := range n {
			r := NewRelationship(snowflake.ID(i+1), 1, snowflake.ID(i*2+1), snowflake.ID(i*2+2))
			_ = r.SetProperty("weight", float64(i))
			_ = r.SetProperty("label", fmt.Sprintf("edge_%d", i))
			_ = r.SetProperty("score", i)
			_ = r.SetProperty("since", "2026-01-15")
			_ = r.SetProperty("active", i%2 == 0)
			rels[i] = r
		}
		return rels
	})

	// 6. Relationships with 5 props + temporal + integrity
	d6, heapFullRel := measureHeap(func() any {
		rels := make([]*Relationship, n)
		for i := range n {
			r := NewRelationship(snowflake.ID(i+1), 1, snowflake.ID(i*2+1), snowflake.ID(i*2+2))
			_ = r.SetProperty("weight", float64(i))
			_ = r.SetProperty("label", fmt.Sprintf("edge_%d", i))
			_ = r.SetProperty("score", i)
			_ = r.SetProperty("since", "2026-01-15")
			_ = r.SetProperty("active", i%2 == 0)
			r.SetTemporal(&TemporalMetadata{
				ValidFrom: Instant(1740000000000), TxFrom: Instant(1740000000000),
				CreatedAt: Instant(1740000000000), UpdatedAt: Instant(1740000000001),
				CreatedBy: "system", UpdatedBy: "system",
			})
			r.SetIntegrity(&RelIntegrity{
				Hash:     "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
				PrevHash: "f6e5d4c3b2a1f6e5d4c3b2a1f6e5d4c3b2a1f6e5d4c3b2a1f6e5d4c3b2a1f6e5",
			})
			rels[i] = r
		}
		return rels
	})

	// Compute per-entity bytes.
	bareNodeB := float64(heapBareNode) / float64(n)
	nonTempNodeB := float64(heapNonTempNode) / float64(n)
	fullNodeB := float64(heapFullNode) / float64(n)
	bareRelB := float64(heapBareRel) / float64(n)
	nonTempRelB := float64(heapNonTempRel) / float64(n)
	fullRelB := float64(heapFullRel) / float64(n)

	// Derived costs.
	propsNodeCost := nonTempNodeB - bareNodeB
	temporalNodeCost := fullNodeB - nonTempNodeB
	propsRelCost := nonTempRelB - bareRelB
	temporalRelCost := fullRelB - nonTempRelB

	t.Logf("")
	t.Logf("╔══════════════════════════════════════════════════════════════════════╗")
	t.Logf("║           MEMORY FOOTPRINT SUMMARY  (%dk entities each)            ║", n/1000)
	t.Logf("╠══════════════════════════════════════════════════════════════════════╣")
	t.Logf("║                                                                    ║")
	t.Logf("║  Struct sizes (compile-time, unsafe.Sizeof):                       ║")
	t.Logf("║    Node              = %3dB                                        ║", unsafe.Sizeof(Node{}))
	t.Logf("║    Relationship      = %3dB                                        ║", unsafe.Sizeof(Relationship{}))
	t.Logf("║    TemporalMetadata  = %3dB                                        ║", unsafe.Sizeof(TemporalMetadata{}))
	t.Logf("║    NodeIntegrity     = %3dB                                        ║", unsafe.Sizeof(NodeIntegrity{}))
	t.Logf("║    RelIntegrity      = %3dB                                        ║", unsafe.Sizeof(RelIntegrity{}))
	t.Logf("║    Property          = %3dB                                        ║", unsafe.Sizeof(Property{}))
	t.Logf("║                                                                    ║")
	t.Logf("╠══════════════════════════════════════════════════════════════════════╣")
	t.Logf("║  NODE (1 label)                     Measured   Spec Est   Delta     ║")
	t.Logf("║  ─────────────────────────────────────────────────────────────────  ║")
	t.Logf("║  Bare (struct only)             %7.0fB                           ║", bareNodeB)
	t.Logf("║  + 5 properties (non-temporal)  %7.0fB     366B    %+5.0fB        ║", nonTempNodeB, nonTempNodeB-366)
	t.Logf("║  + temporal + integrity (full)  %7.0fB     494B    %+5.0fB        ║", fullNodeB, fullNodeB-494)
	t.Logf("║  ─────────────────────────────────────────────────────────────────  ║")
	t.Logf("║  Property cost (5 props)        %7.0fB     ~245B                  ║", propsNodeCost)
	t.Logf("║  Temporal+integrity cost        %7.0fB     ~128B                  ║", temporalNodeCost)
	t.Logf("║                                                                    ║")
	t.Logf("╠══════════════════════════════════════════════════════════════════════╣")
	t.Logf("║  RELATIONSHIP                       Measured   Spec Est   Delta     ║")
	t.Logf("║  ─────────────────────────────────────────────────────────────────  ║")
	t.Logf("║  Bare (struct only)             %7.0fB                           ║", bareRelB)
	t.Logf("║  + 5 properties (non-temporal)  %7.0fB                           ║", nonTempRelB)
	t.Logf("║  + temporal + integrity (full)  %7.0fB                           ║", fullRelB)
	t.Logf("║  ─────────────────────────────────────────────────────────────────  ║")
	t.Logf("║  Property cost (5 props)        %7.0fB     ~245B                  ║", propsRelCost)
	t.Logf("║  Temporal+integrity cost        %7.0fB     ~128B                  ║", temporalRelCost)
	t.Logf("║                                                                    ║")
	t.Logf("╠══════════════════════════════════════════════════════════════════════╣")
	t.Logf("║  1M PROJECTION                                                     ║")
	t.Logf("║  ─────────────────────────────────────────────────────────────────  ║")
	t.Logf("║  1M nodes  (non-temporal):   %7.1f MB                             ║", nonTempNodeB*1_000_000/(1<<20))
	t.Logf("║  1M nodes  (full temporal):  %7.1f MB                             ║", fullNodeB*1_000_000/(1<<20))
	t.Logf("║  1M rels   (non-temporal):   %7.1f MB                             ║", nonTempRelB*1_000_000/(1<<20))
	t.Logf("║  1M rels   (full temporal):  %7.1f MB                             ║", fullRelB*1_000_000/(1<<20))
	t.Logf("║  1M nodes + 1M rels (full):  %7.1f MB                             ║", (fullNodeB+fullRelB)*1_000_000/(1<<20))
	t.Logf("║                                                                    ║")
	t.Logf("╚══════════════════════════════════════════════════════════════════════╝")

	// Keep all data alive so GC doesn't collect during sequential measurements.
	runtime.KeepAlive(d1)
	runtime.KeepAlive(d2)
	runtime.KeepAlive(d3)
	runtime.KeepAlive(d4)
	runtime.KeepAlive(d5)
	runtime.KeepAlive(d6)
}
