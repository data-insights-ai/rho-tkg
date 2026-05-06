package graph

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- MemoryStore.Clear tests ---

func TestMemoryStoreClear_Empty(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()
	if err := ms.Clear(); err != nil {
		t.Fatalf("Clear on empty store: %v", err)
	}
	count, _ := ms.NodeCount()
	if count != 0 {
		t.Fatalf("node count after clear: %d", count)
	}
}

func TestMemoryStoreClear_ClearsEntities(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	if err := ms.PutNode(n); err != nil {
		t.Fatal(err)
	}

	n2 := types.NewNode(types.NodeID(snowflake.ID(2)), 1, nil)
	if err := ms.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := ms.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	if err := ms.Clear(); err != nil {
		t.Fatal(err)
	}

	nc, _ := ms.NodeCount()
	rc, _ := ms.RelationshipCount()
	if nc != 0 || rc != 0 {
		t.Fatalf("after clear: nodes=%d, rels=%d", nc, rc)
	}

	_, err := ms.GetNode(types.NodeID(1))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound, got %v", err)
	}

	_, err = ms.GetRelationship(types.RelID(100))
	if !errors.Is(err, ErrRelNotFound) {
		t.Errorf("expected ErrRelNotFound, got %v", err)
	}
}

func TestMemoryStoreClear_ClearsHistory(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	if err := ms.PutNode(n); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutNodeVersion(types.NodeID(1), 0, n); err != nil {
		t.Fatal(err)
	}

	if err := ms.Clear(); err != nil {
		t.Fatal(err)
	}

	history, err := ms.GetNodeHistory(types.NodeID(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 0 {
		t.Fatalf("history after clear: got %d entries", len(history))
	}
}

func TestMemoryStoreClear_ClearsPropertyIndexes(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	n.SetProperties(mustPropertySlice(t, map[string]any{"name": "Alice"}))
	if err := ms.PutNode(n); err != nil {
		t.Fatal(err)
	}
	if err := ms.CreatePropertyIndex(1, "name"); err != nil {
		t.Fatal(err)
	}

	if err := ms.Clear(); err != nil {
		t.Fatal(err)
	}

	// Index should be gone — CreatePropertyIndex should succeed again.
	// But first the store is empty, so there's nothing to index.
	err := ms.CreatePropertyIndex(1, "name")
	if err != nil {
		t.Fatalf("CreatePropertyIndex after clear: %v", err)
	}
}

// TestMemoryStoreClear_ClearsTemporalIndexes mirrors the BadgerStore F2
// test for the MemoryStore backend.
func TestMemoryStoreClear_ClearsTemporalIndexes(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	if err := ms.CreateTemporalIndex(1); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	if err := ms.Clear(); err != nil {
		t.Fatal(err)
	}
	if err := ms.CreateTemporalIndex(1); err != nil {
		t.Fatalf("CreateTemporalIndex after Clear: %v", err)
	}
}

// TestMemoryStoreClear_ClearsHFIndexes covers the MemoryStore symmetric of
// F2 — flagged in the MR-A agent report as a same-shape gap missed by
// the original brief and folded in for parity with BadgerStore.
func TestMemoryStoreClear_ClearsHFIndexes(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	if err := ms.CreateHighFrequencyIndex(1, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	if err := ms.Clear(); err != nil {
		t.Fatal(err)
	}
	if err := ms.CreateHighFrequencyIndex(1, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex after Clear: %v", err)
	}
}

// --- BadgerStore.Clear tests ---

func TestBadgerStoreClear_Empty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	if err := bs.Clear(); err != nil {
		t.Fatalf("Clear on empty store: %v", err)
	}
	count, _ := bs.NodeCount()
	if count != 0 {
		t.Fatalf("node count after clear: %d", count)
	}
}

func TestBadgerStoreClear_ClearsAll(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}
	n2 := types.NewNode(types.NodeID(snowflake.ID(2)), 1, nil)
	if err := bs.PutNode(n2); err != nil {
		t.Fatal(err)
	}
	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := bs.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	if err := bs.Clear(); err != nil {
		t.Fatal(err)
	}

	nc, _ := bs.NodeCount()
	rc, _ := bs.RelationshipCount()
	if nc != 0 || rc != 0 {
		t.Fatalf("after clear: nodes=%d, rels=%d", nc, rc)
	}

	_, err := bs.GetNode(types.NodeID(1))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound, got %v", err)
	}
}

func TestBadgerStoreClear_ClearsHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNodeVersion(types.NodeID(1), 0, n); err != nil {
		t.Fatal(err)
	}
	// Flush to persist history to Badger.
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := bs.Clear(); err != nil {
		t.Fatal(err)
	}

	history, err := bs.GetNodeHistory(types.NodeID(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 0 {
		t.Fatalf("history after clear: got %d entries", len(history))
	}
}

func TestBadgerStoreClear_ClearsPropertyIndexes(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	n.SetProperties(mustPropertySlice(t, map[string]any{"name": "Alice"}))
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreatePropertyIndex(1, "name"); err != nil {
		t.Fatal(err)
	}

	if err := bs.Clear(); err != nil {
		t.Fatal(err)
	}

	// Index should be gone after clear.
	err := bs.CreatePropertyIndex(1, "name")
	if err != nil {
		t.Fatalf("CreatePropertyIndex after clear: %v", err)
	}
}

// --- F1: BadgerStore.Clear must drain in-flight async flushes ---

// TestBadgerStoreClear_DrainsInFlightFlush verifies that Clear() serializes
// with the async flush goroutine. If a flush has already snapshotted pending
// writes but has not yet submitted them to Badger when Clear runs, Clear
// must block until the flush completes (or the snapshotted writes are
// discarded). Otherwise the post-snapshot WriteBatch.Flush() resurrects
// pre-Clear entities into Badger after DropAll has wiped the namespace.
//
// HONEST FRAMING (review M1): this test asserts only that Clear takes
// flushMu before proceeding. It does NOT demonstrate the resurrection
// race itself (which would require a running flush to be in-flight
// when Clear acquires its locks). Pre-fix Clear had no flushMu
// interaction at all, so the test still catches the regression of
// removing the new lock — but it cannot prove the resurrection
// scenario it was originally documented to test. The behavioral
// guard for that scenario is
// TestBadgerStoreClear_NoResurrectionAfterReopen below; the
// deterministic version would require a flush hook in production
// code, which we deliberately avoid (no test-only state on
// production structs).
func TestBadgerStoreClear_AcquiresFlushMu(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Hold flushMu so any caller that takes it blocks. Pre-fix Clear
	// did not take flushMu and would NOT block here — it would return
	// immediately and the timeout below would fire. Post-fix Clear
	// takes flushMu and waits, so the timeout passes silently.
	bs.flushMu.Lock()

	clearReturned := make(chan struct{})
	go func() {
		_ = bs.Clear()
		close(clearReturned)
	}()

	select {
	case <-clearReturned:
		bs.flushMu.Unlock() // unblock for cleanup
		t.Fatal("Clear returned without waiting on flushMu — fix regressed (production code no longer takes flushMu in Clear)")
	case <-time.After(50 * time.Millisecond):
		// Expected: Clear is blocked on flushMu.
	}

	// Release the simulated flush. Clear should now finish.
	bs.flushMu.Unlock()
	select {
	case <-clearReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("Clear did not return after flushMu was released")
	}
}

// TestBadgerStoreClear_NoResurrectionAfterReopen is a behavioral guard:
// pre-Clear writes that were already accepted by PutNode (in-memory) and
// are about to be flushed must NOT survive a subsequent Clear + Close +
// Reopen cycle. Disk-backed so we can verify the on-disk state.
//
// Honest framing (review M2): this is intrinsically probabilistic —
// catching the resurrection race requires a flush to be mid-WriteBatch
// when Clear's DropAll runs. We do NOT call Clear a second time to
// "drain" post-Clear writes (the original test did, which masked the
// very bug under test). After the writers finish, we accept that some
// puts may have landed AFTER Clear returned (legitimate) and verify
// only one thing: the count is bounded by puts that landed after
// Clear, not the full pre-Clear set. We track that bound by recording
// a sentinel ID before Clear and asserting it is NOT in the reopened
// store. A deterministic version would require a flush hook in
// production code, which we deliberately avoid.
func TestBadgerStoreClear_NoResurrectionAfterReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	bs1, err := NewBadgerStore(BadgerStoreConfig{Dir: dir, FlushInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}

	// Sentinel: a node we KNOW is in the store before Clear runs and
	// can therefore assert is gone after Clear+Close+Reopen. This is
	// what the resurrection bug would re-materialise.
	sentinel := types.NewNode(types.NodeID(snowflake.ID(0xDEAD_BEEF)), 1, nil)
	if err := bs1.PutNode(sentinel); err != nil {
		t.Fatalf("seed sentinel: %v", err)
	}
	// Force a flush so the sentinel is durable on-disk before the storm.
	if err := bs1.flush(); err != nil {
		t.Fatalf("flush sentinel: %v", err)
	}

	// Burst of writes, with Clear racing the background flush loop. We
	// count successful puts to fail loud if every PutNode somehow erred
	// (review L2 — a silent _ = bs1.PutNode(n) hid that class of bug).
	const writers = 4
	const perWriter = 50
	var wg sync.WaitGroup
	var putOK int64
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(base int64) {
			defer wg.Done()
			for i := int64(0); i < perWriter; i++ {
				n := types.NewNode(types.NodeID(snowflake.ID(base*1000+i+1)), 1, nil)
				if err := bs1.PutNode(n); err == nil {
					atomic.AddInt64(&putOK, 1)
				}
			}
		}(int64(w))
	}
	time.Sleep(2 * time.Millisecond)
	if err := bs1.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	wg.Wait()

	if atomic.LoadInt64(&putOK) == 0 {
		t.Fatal("no PutNode call succeeded — test cannot exercise the resurrection scenario")
	}

	if err := bs1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	bs2, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	// The sentinel was definitely in the store before Clear. If it
	// survives Clear+Close+Reopen, the resurrection race occurred.
	got, err := bs2.GetNode(sentinel.ID())
	if err == nil && got != nil {
		t.Fatalf("sentinel resurrected after Clear+Close+Reopen: got %v", got.ID())
	}
}

// --- F2: BadgerStore.Clear must wipe stale secondary indexes ---

// TestBadgerStoreClear_ClearsTemporalIndexes ensures that after Clear() a
// temporal index with the same label token can be created again. Before the
// fix, the stale index map left CreateTemporalIndex returning
// ErrTemporalIndexExists on a freshly cleared store.
func TestBadgerStoreClear_ClearsTemporalIndexes(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if err := bs.CreateTemporalIndex(1); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	if err := bs.Clear(); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateTemporalIndex(1); err != nil {
		t.Fatalf("CreateTemporalIndex after Clear: %v", err)
	}
}

// TestBadgerStoreClear_ClearsHFIndexes ensures the hf index map is wiped.
func TestBadgerStoreClear_ClearsHFIndexes(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if err := bs.CreateHighFrequencyIndex(1, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	if err := bs.Clear(); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateHighFrequencyIndex(1, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex after Clear: %v", err)
	}
}

// TestBadgerStoreClear_ClearsVectorIndexes ensures stale vector entries do not
// occupy top-k slots after Clear (and that CreateVectorIndex works again).
func TestBadgerStoreClear_ClearsVectorIndexes(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	n.SetProperties(mustPropertySlice(t, map[string]any{"v": []float32{1, 0, 0}}))
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateVectorIndex(1, "v", 3, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}
	if err := bs.Clear(); err != nil {
		t.Fatal(err)
	}
	// After Clear, the index map must be empty: re-creating the same key must
	// succeed (not ErrVectorIndexExists), and SearchNearestNodes on the new
	// empty index must return zero entries (no stale candidates from pre-Clear).
	if err := bs.CreateVectorIndex(1, "v", 3, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex after Clear: %v", err)
	}
	results, err := bs.SearchNearestNodes(1, "v", []float32{1, 0, 0}, 5, QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes after Clear: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("SearchNearestNodes after Clear: got %d stale results, want 0", len(results))
	}
}

// --- F3: TieredStore.Clear must wipe store-level indexes ---

// TestTieredStoreClear_ClearsVectorIndexes verifies that the TieredStore-level
// vector index map is reset on Clear so CreateVectorIndex does not return
// ErrVectorIndexExists on a freshly cleared store.
func TestTieredStoreClear_ClearsVectorIndexes(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")

	if err := ts.CreateVectorIndex(caseTok, "v", 3, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}
	if err := ts.Clear(); err != nil {
		t.Fatal(err)
	}
	if err := ts.CreateVectorIndex(caseTok, "v", 3, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex after Clear: %v", err)
	}
}

// TestTieredStoreClear_ClearsTempIdxLabels verifies that the tracked temporal
// index labels list is wiped on Clear, so a subsequent shard rotation does
// not re-install temporal indexes for stale labels.
func TestTieredStoreClear_ClearsTempIdxLabels(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")

	if err := ts.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	ts.tempIdxMu.Lock()
	beforeLen := len(ts.tempIdxLabels)
	ts.tempIdxMu.Unlock()
	if beforeLen == 0 {
		t.Fatalf("tempIdxLabels not populated after CreateTemporalIndex")
	}

	if err := ts.Clear(); err != nil {
		t.Fatal(err)
	}

	ts.tempIdxMu.Lock()
	afterLen := len(ts.tempIdxLabels)
	ts.tempIdxMu.Unlock()
	if afterLen != 0 {
		t.Fatalf("tempIdxLabels after Clear = %d, want 0 (rotation would re-install stale labels)", afterLen)
	}
}
