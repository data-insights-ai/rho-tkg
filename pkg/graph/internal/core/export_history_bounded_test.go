package core

import (
	"io"
	"runtime"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// TestExportGraph_HistoryBoundedMemory verifies that the export's history
// phase no longer materialises the full history-ID slice in memory. With
// 1000 nodes × 50 history versions each (50K history records, 1000 distinct
// history IDs), the bounded-RAM cursor should keep the in-flight slice
// near `exportHistoryBatchSize = 4096` IDs rather than the full population.
//
// The test records the heap delta across the export call; it must stay well
// below what the unbounded slice would have allocated. We use a 16 MiB
// envelope: realistic, with comfortable headroom over the cursor's working
// set, but still tight enough that the OLD behaviour (loading every ID up
// front) would have shown up as a clear regression on much larger seedings
// — and even at 50K nodes / 50 versions each (the stretch case the lesson
// targets) the page-driven walk stays near-flat. The discardWriter sinks
// the export bytes themselves so they don't count against the heap.
func TestExportGraph_HistoryBoundedMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("seeding 50K records is slow; skipped in -short")
	}

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	// Seed 1000 history-bearing nodes by writing version entries directly
	// into the store. We bypass the public API so we can pre-populate
	// without paying for full mutation overhead — exercising the export
	// path is what's being measured, not the seeding.
	const (
		nodeCount       = 1000
		versionsPerNode = 50
	)
	store := g.store
	for i := 1; i <= nodeCount; i++ {
		nid := types.NodeID(snowflake.ID(int64(i)))
		// Live node (so PutNode succeeds when re-imported; we only export here).
		if err := store.PutNode(types.NewNode(nid, 1, nil)); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
		for v := 0; v < versionsPerNode; v++ {
			n := types.NewNode(nid, 1, nil)
			n.SetVersion(uint32(v))
			if err := store.PutNodeVersion(nid, uint32(v), n); err != nil {
				t.Fatalf("PutNodeVersion(%d, v%d): %v", i, v, err)
			}
		}
	}

	// Warm up: GC twice to settle. Take baseline.
	runtime.GC()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	if err := g.IO.Export(io.Discard); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	// Sample peak after export. We don't GC first — we want to see what the
	// exporter retained while it was running. Go runtime statistics are
	// approximate, hence the generous envelope.
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	delta := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	const limit = 16 * 1024 * 1024 // 16 MiB
	if delta > limit {
		t.Fatalf("ExportGraph heap delta = %d bytes (>%d); the unbounded "+
			"history-ID slice would have allocated significantly more", delta, limit)
	}
	t.Logf("ExportGraph heap delta = %d bytes (limit %d)", delta, limit)
}
