package badger

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Store.Clear tests ---

func TestBadgerStoreClear_Empty(t *testing.T) {
	testtier.Slow(t)
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
	testtier.Slow(t)
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
	testtier.Slow(t)
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

// --- Store.Clear must drain in-flight async flushes ---

// TestBadgerStoreClear_NoResurrectionAfterReopen calls Clear while a flush is
// in flight: the flush has taken its snapshot of the pending buffer (node C)
// and not yet committed it, which is where Config.BeforeFlushRows runs. Clear
// must wait for that flush (flushMu) and then wipe its rows; a Clear that ran
// inside the window would let the commit resurrect C after the wipe. Node A
// is durable before Clear, node B is put inside the window so it is pending
// when Clear runs, node D is put after Clear. After Close and reopen only D
// may exist.
//
// The hook gives Clear 50 ms to finish inside the window; a Clear that does
// not wait for the flush is caught when it completes within that bound. A
// correct Clear never completes there, so the bound cannot fail a good build.
func TestBadgerStoreClear_NoResurrectionAfterReopen(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	dir := t.TempDir()

	const nodeA, nodeB, nodeC, nodeD = 1, 2, 3, 4
	var (
		bs        *Store
		armed     atomic.Bool
		clearDone = make(chan error, 1)
		inWindow  atomic.Bool // Clear returned while the flush was in flight
		putBErr   error
	)
	bs, err := New(Config{
		Dir:           dir,
		FlushInterval: time.Hour, // only the explicit Flush and Close flush
		BeforeFlushRows: func(bool) error {
			if !armed.CompareAndSwap(true, false) {
				return nil
			}
			putBErr = bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(nodeB)), 1, nil))
			go func() { clearDone <- bs.Clear() }()
			select {
			case err := <-clearDone:
				inWindow.Store(true)
				clearDone <- err
			case <-time.After(50 * time.Millisecond):
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	putTestNode(t, bs, nodeA, 1, nil)
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush A: %v", err)
	}
	putTestNode(t, bs, nodeC, 1, nil)
	armed.Store(true)
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush C: %v", err)
	}
	if err := <-clearDone; err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if putBErr != nil {
		t.Fatalf("PutNode(B) in the flush window: %v", putBErr)
	}
	if inWindow.Load() {
		t.Fatal("Clear returned while a flush was in flight (it must wait on flushMu)")
	}
	putTestNode(t, bs, nodeD, 1, nil)
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()
	for _, id := range []int64{nodeA, nodeB, nodeC} {
		if got, err := bs2.GetNode(types.NodeID(id)); !errors.Is(err, ErrNodeNotFound) {
			t.Errorf("node %d after Clear, Close and reopen = %v, %v; want ErrNodeNotFound", id, got, err)
		}
	}
	if _, err := bs2.GetNode(types.NodeID(nodeD)); err != nil {
		t.Errorf("node %d put after Clear: %v", nodeD, err)
	}
	if nc, _ := bs2.NodeCount(); nc != 1 {
		t.Errorf("NodeCount after reopen = %d, want 1", nc)
	}
}

// --- Store.Clear must wipe stale secondary indexes ---

// TestBadgerStoreClear_ClearsTemporalIndexes ensures that after Clear() a
// temporal index with the same label token can be created again. Before the
// fix, the stale index map left CreateTemporalIndex returning
// ErrTemporalIndexExists on a freshly cleared store.
func TestBadgerStoreClear_ClearsTemporalIndexes(t *testing.T) {
	testtier.Slow(t)
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
	testtier.Slow(t)
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
	testtier.Slow(t)
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
