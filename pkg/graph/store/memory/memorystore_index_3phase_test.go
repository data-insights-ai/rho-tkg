package memory

import (
	"sync"
	"sync/atomic"
	"testing"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 17h: CreatePropertyIndex/CreateRelPropertyIndex were rewritten from
// "hold ms.mu.Lock() across the whole scan-and-build" to the 3-phase pattern
// already proven out by the badger backend — install an empty live index and
// snapshot candidate IDs under Lock, scan with only brief per-row RLocks (no
// lock held across the scan), then merge under Lock again, reconciling
// concurrent writes via the index's Mutated tracking. These tests target the
// two properties a pure "still returns the same index" test can't see: that
// the lock is actually released during the scan, and that the reconciliation
// itself is correct.

// TestCreatePropertyIndex_ReleasesLockDuringScan proves Phase 2 does not hold
// ms.mu continuously: a concurrent goroutine polling TryLock while
// CreatePropertyIndex scans a large label must succeed at least once. Under
// the pre-fix single-Lock-for-the-whole-scan code this could never succeed
// until the scan (and the whole call) finished.
func TestCreatePropertyIndex_ReleasesLockDuringScan(t *testing.T) {
	ms := New()
	const n = 20_000
	for i := 1; i <= n; i++ {
		if err := ms.PutNode(memNode(int64(i), 10)); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}

	var tryLockSuccesses atomic.Int64
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			if ms.mu.TryLock() {
				tryLockSuccesses.Add(1)
				ms.mu.Unlock()
			}
		}
	}()

	if err := ms.CreatePropertyIndex(10, "prop"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}
	close(done)
	wg.Wait()

	// Threshold, not just >0: a lucky TryLock in the tail-end race window
	// between CreatePropertyIndex's return (Unlock) and close(done) can hit
	// once or twice even under the pre-fix single-Lock-for-the-whole-scan
	// code (observed up to ~23 over repeated pre-fix runs); genuine per-row
	// lock release during a 20,000-node Phase 2 scan produces thousands of
	// successes (observed 2,400+). 100 sits comfortably between the two.
	if got := tryLockSuccesses.Load(); got < 100 {
		t.Fatalf("TryLock succeeded only %d times while CreatePropertyIndex scanned %d nodes — "+
			"the exclusive lock was held for the whole scan (BACKLOG 17h regression)", got, n)
	}
}

// TestCreateRelPropertyIndex_ReleasesLockDuringScan mirrors the node test for
// the relationship-type-keyed door.
func TestCreateRelPropertyIndex_ReleasesLockDuringScan(t *testing.T) {
	ms := New()
	const n = 20_000
	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode(1): %v", err)
	}
	if err := ms.PutNode(memNode(2, 10)); err != nil {
		t.Fatalf("PutNode(2): %v", err)
	}
	for i := 1; i <= n; i++ {
		r := types.NewRelationship(types.RelID(1000+i), 5, types.NodeID(1), types.NodeID(2))
		if err := ms.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship(%d): %v", i, err)
		}
	}

	var tryLockSuccesses atomic.Int64
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			if ms.mu.TryLock() {
				tryLockSuccesses.Add(1)
				ms.mu.Unlock()
			}
		}
	}()

	if err := ms.CreateRelPropertyIndex(5, "prop"); err != nil {
		t.Fatalf("CreateRelPropertyIndex: %v", err)
	}
	close(done)
	wg.Wait()

	// Threshold rationale: see TestCreatePropertyIndex_ReleasesLockDuringScan.
	if got := tryLockSuccesses.Load(); got < 100 {
		t.Fatalf("TryLock succeeded only %d times while CreateRelPropertyIndex scanned %d relationships — "+
			"the exclusive lock was held for the whole scan (BACKLOG 17h regression)", got, n)
	}
}

// TestCreatePropertyIndex_ConcurrentMutationDuringScanIsReconciled proves
// Phase 3's Mutated-based reconciliation is actually correct, not just
// "doesn't crash": while CreatePropertyIndex scans a large label, a
// concurrent goroutine deletes some of the snapshotted nodes and updates
// others' indexed property value. The finished index must reflect the FINAL
// state — no stale entries for deleted nodes, no stale (pre-update) values
// for updated nodes — never the Phase-2-snapshot-time state.
func TestCreatePropertyIndex_ConcurrentMutationDuringScanIsReconciled(t *testing.T) {
	ms := New()
	const n = 20_000
	for i := 1; i <= n; i++ {
		n := memNode(int64(i), 10)
		if err := n.SetProperty("prop", int64(1)); err != nil {
			t.Fatalf("SetProperty(%d): %v", i, err)
		}
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}

	// Concurrently: delete the first 2,000 nodes, update the next 2,000 to a
	// new property value, racing CreatePropertyIndex's scan.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(1); i <= 2_000; i++ {
			if err := ms.DeleteNode(types.NodeID(i)); err != nil {
				t.Errorf("DeleteNode(%d): %v", i, err)
				return
			}
		}
		for i := int64(2_001); i <= 4_000; i++ {
			updated := memNode(i, 10)
			if err := updated.SetProperty("prop", int64(999)); err != nil {
				t.Errorf("SetProperty(%d): %v", i, err)
				return
			}
			if err := ms.ReplaceNode(updated); err != nil {
				t.Errorf("ReplaceNode(%d): %v", i, err)
				return
			}
		}
	}()

	if err := ms.CreatePropertyIndex(10, "prop"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}
	wg.Wait()

	ms.mu.RLock()
	idx := ms.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: 10, PropertyKey: "prop"}]
	ms.mu.RUnlock()
	if idx == nil {
		t.Fatal("index missing after CreatePropertyIndex")
	}

	for i := int64(1); i <= 2_000; i++ {
		ids := idx.NodeIDs(int64(1))
		for _, id := range ids {
			if id == types.NodeID(i) {
				t.Fatalf("deleted node %d still present in index under its old value", i)
			}
		}
		ids999 := idx.NodeIDs(int64(999))
		for _, id := range ids999 {
			if id == types.NodeID(i) {
				t.Fatalf("deleted node %d present in index under the updated value", i)
			}
		}
	}
	for i := int64(2_001); i <= 4_000; i++ {
		found := false
		for _, id := range idx.NodeIDs(int64(999)) {
			if id == types.NodeID(i) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("updated node %d missing from index under its new value 999", i)
		}
		for _, id := range idx.NodeIDs(int64(1)) {
			if id == types.NodeID(i) {
				t.Fatalf("updated node %d still present in index under its stale pre-update value 1", i)
			}
		}
	}
	// A node untouched by the concurrent goroutine keeps its original value.
	for _, id := range idx.NodeIDs(int64(1)) {
		if id == types.NodeID(n) {
			return
		}
	}
	t.Fatalf("untouched node %d missing from index under its original value 1", n)
}
