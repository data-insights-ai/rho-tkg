package core

import (
	"io"
	"runtime"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// TestExportGraph_HistoryBoundedMemory verifies that the export's history
// phase no longer retains the full history-ID slice past return. With
// 1000 nodes × 50 history versions each, an unbounded loader would
// retain all 50K history entries (~tens of MiB) on the heap until the
// caller's deferred close runs. The cursor-paginated implementation
// retains nothing past the loop iteration; after a forced GC the only
// remaining allocations are the seeded entities themselves.
//
// What the test measures: heap-alloc delta AFTER a forced post-export
// GC, comparing the retained heap before vs after the export call.
// Without the GC the measurement was flaky because runtime.HeapAlloc
// captures every still-live allocation including transients the GC
// hadn't seen yet — under load that pushed the delta past the budget.
// With GC the transient cursor state is reclaimed before measurement,
// so the test reflects "did the exporter leak anything" rather than
// "what was the peak transient heap during export".
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

	// Warm up: GC twice to settle. Take baseline of retained heap.
	runtime.GC()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	if err := g.IO.Export(io.Discard); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	// Force GC so the measurement captures only what the exporter
	// retained past return — not transient cursor state, not freshly
	// allocated msgpack buffers, not anything the GC was about to
	// reclaim. Without this, parallel test goroutines and Go's
	// concurrent-mark cycles make HeapAlloc deltas noisy.
	runtime.GC()
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	delta := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	const limit = 16 * 1024 * 1024 // 16 MiB
	if delta > limit {
		t.Fatalf("ExportGraph retained heap delta = %d bytes (>%d); the unbounded "+
			"history-ID slice would have allocated significantly more", delta, limit)
	}
	t.Logf("ExportGraph retained heap delta = %d bytes (limit %d)", delta, limit)
}
