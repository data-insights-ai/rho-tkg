package core

import (
	"bytes"
	"errors"
	"io"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// exportHistoryIDPageStore counts the node-history ID reads Export makes: the
// paged AllNodeHistoryIDsFrom calls, the largest page it asked for and got,
// and any call to the unbounded AllNodeHistoryIDs.
type exportHistoryIDPageStore struct {
	*memory.Store
	pageCalls      atomic.Int64
	maxLimit       atomic.Int64
	maxPage        atomic.Int64
	unboundedCalls atomic.Int64
}

func (s *exportHistoryIDPageStore) AllNodeHistoryIDsFrom(after types.NodeID, limit int) ([]types.NodeID, error) {
	s.pageCalls.Add(1)
	if int64(limit) > s.maxLimit.Load() {
		s.maxLimit.Store(int64(limit))
	}
	ids, err := s.Store.AllNodeHistoryIDsFrom(after, limit)
	if int64(len(ids)) > s.maxPage.Load() {
		s.maxPage.Store(int64(len(ids)))
	}
	return ids, err
}

func (s *exportHistoryIDPageStore) AllNodeHistoryIDs() ([]types.NodeID, error) {
	s.unboundedCalls.Add(1)
	return s.Store.AllNodeHistoryIDs()
}

// TestExportGraph_HistoryBoundedMemory: Export holds at most
// exportHistoryBatchSize node-history IDs at once. With one more
// history-bearing node than a page, it reads the history ID set in exactly two
// pages of at most exportHistoryBatchSize IDs, never calls the unbounded
// AllNodeHistoryIDs, and still writes every history record. (Paging each
// entity's versions is TestExportGraph_HistoryVersionPagingSplitsDeepEntity.)
func TestExportGraph_HistoryBoundedMemory(t *testing.T) {
	t.Parallel()
	store := &exportHistoryIDPageStore{Store: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	// Seed through the store directly: one current row and one history
	// version per node.
	const nodeCount = exportHistoryBatchSize + 1
	for i := 1; i <= nodeCount; i++ {
		nid := types.NodeID(i)
		if err := store.PutNode(types.NewNode(nid, 1, nil)); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
		n := types.NewNode(nid, 1, nil)
		if err := store.PutNodeVersion(nid, 0, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", i, err)
		}
	}

	var out bytes.Buffer
	if err := g.IO.Export(&out); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if got := store.unboundedCalls.Load(); got != 0 {
		t.Fatalf("Export called the unbounded AllNodeHistoryIDs %d times", got)
	}
	if got := store.pageCalls.Load(); got != 2 {
		t.Fatalf("AllNodeHistoryIDsFrom calls = %d, want 2 for %d history nodes", got, nodeCount)
	}
	if got := store.maxLimit.Load(); got != exportHistoryBatchSize {
		t.Fatalf("largest AllNodeHistoryIDsFrom limit = %d, want %d", got, exportHistoryBatchSize)
	}
	if got := store.maxPage.Load(); got != exportHistoryBatchSize {
		t.Fatalf("largest history ID page = %d, want %d", got, exportHistoryBatchSize)
	}

	histRecords := 0
	for {
		tag, _, err := readExportRecord(&out)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("read export record: %v", err)
		}
		if tag == exportTagNodeHist {
			histRecords++
		}
	}
	if histRecords != nodeCount {
		t.Fatalf("node history records = %d, want %d", histRecords, nodeCount)
	}
}

// TestExportGraph_HistoryRetainedHeapBounded verifies that the export's history
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
//
// The paging itself is counted by TestExportGraph_HistoryBoundedMemory and
// TestExportGraph_HistoryVersionPagingSplitsDeepEntity.
func TestExportGraph_HistoryRetainedHeapBounded(t *testing.T) {
	// Not parallel: the heap delta is process-wide, so no parallel test of
	// this package runs while it measures.

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
		nid := types.NodeID(i)
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
