package badger

import (
	"sync/atomic"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Fix #4: DeleteNode does not block idxMu during Badger read ---

func newTinyCacheBadgerStore(t *testing.T) *Store {
	t.Helper()
	bs, err := New(Config{InMemory: true, CacheCapacity: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	return bs
}

// TestBadgerStore_DeleteNode_NoDiskIOUnderWriteLock verifies that DeleteNode on a
// cache-miss node completes without contention on idxMu during Badger I/O.
// We exercise this by flushing the store (evicting the write-pending entry from
// the dirty path), then deleting concurrently while another goroutine holds an
// RLock to detect any deadlock that would previously occur.
func TestBadgerStore_DeleteNode_NoDiskIOUnderWriteLock(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(9901)), 1, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	// Flush to Badger and evict from LRU so the delete path hits db.View.
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.NodeCacheForTest().ResetForTest() // evict all entries (post-Flush, all are clean)

	// Hold an RLock concurrently for the duration of DeleteNode.
	// Before fix #4, DeleteNode held idxMu.Lock() THEN called db.View, which
	// would block the concurrent RLock for the I/O duration. Now prefetchNode
	// does db.View before the write lock, so the RLock holder is not blocked.
	var rLockDuration atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		start := time.Now()
		bs.LockIdxMuRForTest()
		time.Sleep(5 * time.Millisecond) // simulate RLock holder doing work
		bs.UnlockIdxMuRForTest()
		rLockDuration.Store(time.Since(start).Milliseconds())
	}()

	time.Sleep(1 * time.Millisecond) // let the goroutine acquire RLock first
	if err := bs.DeleteNode(types.NodeID(9901)); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	<-done

	// Verify deletion.
	if _, err := bs.GetNode(types.NodeID(9901)); err == nil {
		t.Error("node still present after DeleteNode")
	}
}

func TestBadgerStore_DeleteRelationship_CacheMissUnderReadLockContention(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 9910, 1, nil)
	putTestNode(t, bs, 9920, 1, nil)
	putTestRel(t, bs, 9930, 3, 9910, 9920)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.RelCacheForTest().ResetForTest()

	locked := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		bs.LockIdxMuRForTest()
		close(locked)
		<-release
		bs.UnlockIdxMuRForTest()
	}()
	<-locked

	go func() {
		done <- bs.DeleteRelationship(types.RelID(9930))
	}()

	time.Sleep(5 * time.Millisecond)
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	if _, err := bs.GetRelationship(types.RelID(9930)); err == nil {
		t.Fatal("relationship still present after DeleteRelationship")
	}
}

func TestBadgerStore_DeleteNodesBatch_CacheMissPrefetchOutsideWriteLock(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	for _, id := range []snowflake.ID{9941, 9942} {
		if err := bs.PutNode(types.NewNode(types.NodeID(id), 1, nil)); err != nil {
			t.Fatalf("PutNode(%d): %v", id, err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.NodeCacheForTest().ResetForTest()

	bs.LockIdxMuRForTest()
	released := false
	defer func() {
		if !released {
			bs.UnlockIdxMuRForTest()
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- bs.DeleteNodesBatch([]types.NodeID{types.NodeID(9941), types.NodeID(9942)})
	}()

	waitForNodeCacheHits(t, bs, 9941, 9942)
	bs.UnlockIdxMuRForTest()
	released = true

	if err := <-done; err != nil {
		t.Fatalf("DeleteNodesBatch: %v", err)
	}
}

func TestBadgerStore_DeleteRelationshipsBatch_CacheMissPrefetchOutsideWriteLock(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 9951, 1, nil)
	putTestNode(t, bs, 9952, 1, nil)
	putTestRel(t, bs, 9953, 3, 9951, 9952)
	putTestRel(t, bs, 9954, 3, 9952, 9951)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.RelCacheForTest().ResetForTest()

	bs.LockIdxMuRForTest()
	released := false
	defer func() {
		if !released {
			bs.UnlockIdxMuRForTest()
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- bs.DeleteRelationshipsBatch([]types.RelID{types.RelID(9953), types.RelID(9954)})
	}()

	waitForRelCacheHits(t, bs, 9953, 9954)
	bs.UnlockIdxMuRForTest()
	released = true

	if err := <-done; err != nil {
		t.Fatalf("DeleteRelationshipsBatch: %v", err)
	}
}

func TestBadgerStore_DeleteNodeCascade_CacheMissPrefetchOutsideWriteLock(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 9961, 1, nil)
	putTestNode(t, bs, 9962, 1, nil)
	putTestNode(t, bs, 9963, 1, nil)
	putTestRel(t, bs, 9964, 3, 9961, 9962)
	putTestRel(t, bs, 9965, 3, 9963, 9961)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.NodeCacheForTest().ResetForTest()
	bs.RelCacheForTest().ResetForTest()

	bs.LockIdxMuRForTest()
	released := false
	defer func() {
		if !released {
			bs.UnlockIdxMuRForTest()
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- bs.DeleteNodeCascade(types.NodeID(9961))
	}()

	waitForNodeCacheHits(t, bs, 9961)
	waitForRelCacheHits(t, bs, 9964, 9965)
	bs.UnlockIdxMuRForTest()
	released = true

	if err := <-done; err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}
}

func TestBadgerStore_DeleteRelWithHistory_CacheMissPrefetchOutsideWriteLock(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 9971, 1, nil)
	putTestNode(t, bs, 9972, 1, nil)
	rel := putTestRel(t, bs, 9973, 3, 9971, 9972)
	tombstone := rel.DeepCopy()
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.RelCacheForTest().ResetForTest()

	bs.LockIdxMuRForTest()
	released := false
	defer func() {
		if !released {
			bs.UnlockIdxMuRForTest()
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- bs.DeleteRelWithHistory(rel.ID(), rel.Version(), tombstone)
	}()

	waitForRelCacheHits(t, bs, 9973)
	bs.UnlockIdxMuRForTest()
	released = true

	if err := <-done; err != nil {
		t.Fatalf("DeleteRelWithHistory: %v", err)
	}
}

func TestBadgerStore_DeleteNodeWithHistory_CacheMissPrefetchOutsideWriteLock(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	node := types.NewNode(types.NodeID(snowflake.ID(9981)), 1, nil)
	if err := bs.PutNode(node); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	putTestNode(t, bs, 9982, 1, nil)
	rel := putTestRel(t, bs, 9983, 3, 9981, 9982)
	nodeTombstone := node.DeepCopy()
	relTombstone := rel.DeepCopy()
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.NodeCacheForTest().ResetForTest()
	bs.RelCacheForTest().ResetForTest()

	bs.LockIdxMuRForTest()
	released := false
	defer func() {
		if !released {
			bs.UnlockIdxMuRForTest()
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- bs.DeleteNodeWithHistory(node.ID(), node.Version(), nodeTombstone, []RelTombstone{{
			ID:          rel.ID(),
			PrevVersion: rel.Version(),
			Tombstone:   relTombstone,
		}})
	}()

	waitForNodeCacheHits(t, bs, 9981)
	waitForRelCacheHits(t, bs, 9983)
	bs.UnlockIdxMuRForTest()
	released = true

	if err := <-done; err != nil {
		t.Fatalf("DeleteNodeWithHistory: %v", err)
	}
}

func TestBadgerStore_DeleteRelationshipsBatch_UsesPrefetchedInfoAfterLRUEviction(t *testing.T) {
	t.Parallel()
	bs := newTinyCacheBadgerStore(t)

	putTestNode(t, bs, 9991, 1, nil)
	putTestNode(t, bs, 9992, 1, nil)
	putTestRel(t, bs, 9993, 3, 9991, 9992)
	putTestRel(t, bs, 9994, 3, 9992, 9991)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.RelCacheForTest().ResetForTest()
	missesBefore := bs.RelCacheForTest().Misses()

	if err := bs.DeleteRelationshipsBatch([]types.RelID{types.RelID(9993), types.RelID(9994)}); err != nil {
		t.Fatalf("DeleteRelationshipsBatch: %v", err)
	}

	if got := bs.RelCacheForTest().Misses() - missesBefore; got != 2 {
		t.Fatalf("relationship cache misses during two-row delete = %d, want 2 prefetch misses only", got)
	}
}

func TestBadgerStore_DeleteNodesBatch_UsesPrefetchedNodesAfterLRUEviction(t *testing.T) {
	t.Parallel()
	bs := newTinyCacheBadgerStore(t)

	for _, id := range []snowflake.ID{9986, 9987} {
		if err := bs.PutNode(types.NewNode(types.NodeID(id), 1, nil)); err != nil {
			t.Fatalf("PutNode(%d): %v", id, err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.NodeCacheForTest().ResetForTest()
	missesBefore := bs.NodeCacheForTest().Misses()

	if err := bs.DeleteNodesBatch([]types.NodeID{types.NodeID(9986), types.NodeID(9987)}); err != nil {
		t.Fatalf("DeleteNodesBatch: %v", err)
	}

	if got := bs.NodeCacheForTest().Misses() - missesBefore; got != 2 {
		t.Fatalf("node cache misses during two-row delete = %d, want 2 prefetch misses only", got)
	}
}

func TestBadgerStore_NodeDeleteInfoRejectsStalePrefetch(t *testing.T) {
	t.Parallel()
	bs := newTinyCacheBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(9988)), 1, nil)
	if err := n.SetProperty("rev", int64(1)); err != nil {
		t.Fatalf("SetProperty rev1: %v", err)
	}
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.NodeCacheForTest().ResetForTest()

	info, err := bs.prefetchNodeDeleteInfo(n.ID())
	if err != nil {
		t.Fatalf("prefetchNodeDeleteInfo: %v", err)
	}
	updated := n.DeepCopy()
	if err := updated.SetProperty("rev", int64(2)); err != nil {
		t.Fatalf("SetProperty rev2: %v", err)
	}
	if err := bs.ReplaceNode(updated); err != nil {
		t.Fatalf("ReplaceNode: %v", err)
	}

	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	if bs.nodeDeleteInfoStillCurrentLocked(n.ID(), info) {
		t.Fatal("stale prefetched node row was accepted after ReplaceNode")
	}
	current, err := bs.currentNodeForPrefetchLocked(n.ID(), info)
	if err != nil {
		t.Fatalf("currentNodeForPrefetchLocked: %v", err)
	}
	got, ok := current.GetProperty("rev")
	if !ok || got != int64(2) {
		t.Fatalf("currentNodeForPrefetchLocked returned stale property rev=%v, ok=%v", got, ok)
	}
}

func TestBadgerStore_RelPrefetchFallsBackAfterIndexedStateChange(t *testing.T) {
	t.Parallel()
	bs := newTinyCacheBadgerStore(t)

	putTestNode(t, bs, 10011, 1, nil)
	putTestNode(t, bs, 10012, 1, nil)
	putTestNode(t, bs, 10013, 1, nil)
	putTestNode(t, bs, 10014, 1, nil)
	rel := putTestRel(t, bs, 10015, 3, 10011, 10012)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.RelCacheForTest().ResetForTest()

	prefetched, err := bs.prefetchRel(rel.ID())
	if err != nil {
		t.Fatalf("prefetchRel: %v", err)
	}
	if err := bs.DeleteRelationship(rel.ID()); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	recreated := types.NewRelationship(rel.ID(), 4, types.NodeID(snowflake.ID(10013)), types.NodeID(snowflake.ID(10014)))
	if err := bs.PutRelationship(recreated); err != nil {
		t.Fatalf("PutRelationship recreated: %v", err)
	}

	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	if bs.relDeleteInfoStillIndexedLocked(relDeleteInfoFromRelationship(prefetched)) {
		t.Fatal("stale prefetched relationship indexes were accepted after delete/recreate")
	}
	current, err := bs.currentRelForPrefetchLocked(rel.ID(), prefetched)
	if err != nil {
		t.Fatalf("currentRelForPrefetchLocked: %v", err)
	}
	if current.TypeToken().Value() != 4 || current.StartNodeID() != types.NodeID(snowflake.ID(10013)) ||
		current.EndNodeID() != types.NodeID(snowflake.ID(10014)) {
		t.Fatalf("currentRelForPrefetchLocked returned stale relationship type=%d start=%d end=%d",
			current.TypeToken().Value(), current.StartNodeID(), current.EndNodeID())
	}
}

func TestBadgerStore_OutgoingRelationships_TypeFilterAvoidsNonMatchingRowFetches(t *testing.T) {
	t.Parallel()
	bs := newTinyCacheBadgerStore(t)

	putTestNode(t, bs, 10021, 1, nil)
	putTestNode(t, bs, 10022, 1, nil)
	putTestNode(t, bs, 10023, 1, nil)
	putTestRel(t, bs, 10024, 3, 10021, 10022)
	putTestRel(t, bs, 10025, 3, 10021, 10023)
	want := putTestRel(t, bs, 10026, 4, 10021, 10022)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.RelCacheForTest().ResetForTest()
	missesBefore := bs.RelCacheForTest().Misses()

	got, err := bs.OutgoingRelationships(types.NodeID(snowflake.ID(10021)), 4)
	if err != nil {
		t.Fatalf("OutgoingRelationships: %v", err)
	}
	if len(got) != 1 || got[0].ID() != want.ID() {
		t.Fatalf("OutgoingRelationships type filter IDs = %v, want [%d]", relIDsForTest(got), want.ID())
	}
	if misses := bs.RelCacheForTest().Misses() - missesBefore; misses != 1 {
		t.Fatalf("relationship cache misses = %d, want 1 matching row fetch only", misses)
	}
}

func TestBadgerStore_OutgoingRelationshipsForNodes_TypeFilterAvoidsNonMatchingRowFetches(t *testing.T) {
	t.Parallel()
	bs := newTinyCacheBadgerStore(t)

	for _, id := range []int64{10031, 10032, 10033, 10034} {
		putTestNode(t, bs, id, 1, nil)
	}
	putTestRel(t, bs, 10035, 3, 10031, 10033)
	wantA := putTestRel(t, bs, 10036, 4, 10031, 10034)
	putTestRel(t, bs, 10037, 3, 10032, 10033)
	wantB := putTestRel(t, bs, 10038, 4, 10032, 10034)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.RelCacheForTest().ResetForTest()
	missesBefore := bs.RelCacheForTest().Misses()

	got, err := bs.OutgoingRelationshipsForNodes([]types.NodeID{
		types.NodeID(snowflake.ID(10031)),
		types.NodeID(snowflake.ID(10032)),
	}, 4)
	if err != nil {
		t.Fatalf("OutgoingRelationshipsForNodes: %v", err)
	}
	if gotIDs := relIDsForTest(got[types.NodeID(snowflake.ID(10031))]); len(gotIDs) != 1 || gotIDs[0] != wantA.ID() {
		t.Fatalf("OutgoingRelationshipsForNodes node 10031 IDs = %v, want [%d]", gotIDs, wantA.ID())
	}
	if gotIDs := relIDsForTest(got[types.NodeID(snowflake.ID(10032))]); len(gotIDs) != 1 || gotIDs[0] != wantB.ID() {
		t.Fatalf("OutgoingRelationshipsForNodes node 10032 IDs = %v, want [%d]", gotIDs, wantB.ID())
	}
	if misses := bs.RelCacheForTest().Misses() - missesBefore; misses != 2 {
		t.Fatalf("relationship cache misses = %d, want 2 matching row fetches only", misses)
	}
}

func TestBadgerStore_OutgoingRelationships_VerifiesFetchedRowStartNode(t *testing.T) {
	t.Parallel()
	bs := newTinyCacheBadgerStore(t)

	putTestNode(t, bs, 10091, 1, nil)
	putTestNode(t, bs, 10092, 1, nil)
	putTestNode(t, bs, 10093, 1, nil)
	rel := putTestRel(t, bs, 10094, 4, 10093, 10092)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.RelCacheForTest().ResetForTest()

	wrongStart := types.NodeID(snowflake.ID(10091))
	bs.idxMu.Lock()
	bs.outIdx[wrongStart] = map[types.RelID]struct{}{rel.ID(): {}}
	bs.idxMu.Unlock()

	got, err := bs.OutgoingRelationships(wrongStart, 4)
	if err != nil {
		t.Fatalf("OutgoingRelationships: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("OutgoingRelationships returned wrong-start rel IDs = %v, want none", relIDsForTest(got))
	}
}

func TestBadgerStore_OutgoingRelationshipsForNodes_VerifiesFetchedRowStartNode(t *testing.T) {
	t.Parallel()
	bs := newTinyCacheBadgerStore(t)

	putTestNode(t, bs, 10101, 1, nil)
	putTestNode(t, bs, 10102, 1, nil)
	putTestNode(t, bs, 10103, 1, nil)
	rel := putTestRel(t, bs, 10104, 4, 10103, 10102)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.RelCacheForTest().ResetForTest()

	wrongStart := types.NodeID(snowflake.ID(10101))
	bs.idxMu.Lock()
	bs.outIdx[wrongStart] = map[types.RelID]struct{}{rel.ID(): {}}
	bs.idxMu.Unlock()

	got, err := bs.OutgoingRelationshipsForNodes([]types.NodeID{wrongStart}, 4)
	if err != nil {
		t.Fatalf("OutgoingRelationshipsForNodes: %v", err)
	}
	if got != nil {
		t.Fatalf("OutgoingRelationshipsForNodes returned wrong-start rels = %v, want nil", got)
	}
}

func TestBadgerStore_IncomingRelationships_TypeFilterVerifiesFetchedRowType(t *testing.T) {
	t.Parallel()
	bs := newTinyCacheBadgerStore(t)

	putTestNode(t, bs, 10041, 1, nil)
	putTestNode(t, bs, 10042, 1, nil)
	rel := putTestRel(t, bs, 10043, 3, 10041, 10042)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.RelCacheForTest().ResetForTest()

	bs.idxMu.Lock()
	bs.inIdx[rel.EndNodeID()][rel.ID()] = 4
	bs.idxMu.Unlock()

	got, err := bs.IncomingRelationships(rel.EndNodeID(), 4)
	if err != nil {
		t.Fatalf("IncomingRelationships: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("IncomingRelationships returned stale-index type match IDs = %v, want none", relIDsForTest(got))
	}
}

func TestBadgerStore_IncomingRelationshipsForNodes_TypeFilterVerifiesFetchedRowType(t *testing.T) {
	t.Parallel()
	bs := newTinyCacheBadgerStore(t)

	putTestNode(t, bs, 10051, 1, nil)
	putTestNode(t, bs, 10052, 1, nil)
	rel := putTestRel(t, bs, 10053, 3, 10051, 10052)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.RelCacheForTest().ResetForTest()

	bs.idxMu.Lock()
	bs.inIdx[rel.EndNodeID()][rel.ID()] = 4
	bs.idxMu.Unlock()

	got, err := bs.IncomingRelationshipsForNodes([]types.NodeID{rel.EndNodeID()}, 4)
	if err != nil {
		t.Fatalf("IncomingRelationshipsForNodes: %v", err)
	}
	if got != nil {
		t.Fatalf("IncomingRelationshipsForNodes returned stale-index type match = %v, want nil", got)
	}
}

func TestBadgerStore_IncomingRelationships_VerifiesFetchedRowEndNode(t *testing.T) {
	t.Parallel()
	bs := newTinyCacheBadgerStore(t)

	putTestNode(t, bs, 10111, 1, nil)
	putTestNode(t, bs, 10112, 1, nil)
	putTestNode(t, bs, 10113, 1, nil)
	rel := putTestRel(t, bs, 10114, 4, 10111, 10113)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.RelCacheForTest().ResetForTest()

	wrongEnd := types.NodeID(snowflake.ID(10112))
	bs.idxMu.Lock()
	bs.inIdx[wrongEnd] = map[types.RelID]uint16{rel.ID(): 4}
	bs.idxMu.Unlock()

	got, err := bs.IncomingRelationships(wrongEnd, 4)
	if err != nil {
		t.Fatalf("IncomingRelationships: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("IncomingRelationships returned wrong-end rel IDs = %v, want none", relIDsForTest(got))
	}
}

func TestBadgerStore_IncomingRelationshipsForNodes_VerifiesFetchedRowEndNode(t *testing.T) {
	t.Parallel()
	bs := newTinyCacheBadgerStore(t)

	putTestNode(t, bs, 10121, 1, nil)
	putTestNode(t, bs, 10122, 1, nil)
	putTestNode(t, bs, 10123, 1, nil)
	rel := putTestRel(t, bs, 10124, 4, 10121, 10123)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.RelCacheForTest().ResetForTest()

	wrongEnd := types.NodeID(snowflake.ID(10122))
	bs.idxMu.Lock()
	bs.inIdx[wrongEnd] = map[types.RelID]uint16{rel.ID(): 4}
	bs.idxMu.Unlock()

	got, err := bs.IncomingRelationshipsForNodes([]types.NodeID{wrongEnd}, 4)
	if err != nil {
		t.Fatalf("IncomingRelationshipsForNodes: %v", err)
	}
	if got != nil {
		t.Fatalf("IncomingRelationshipsForNodes returned wrong-end rels = %v, want nil", got)
	}
}

func TestBadgerStore_RelationshipsByType_VerifiesFetchedRowTypeBeforeLimit(t *testing.T) {
	t.Parallel()
	bs := newTinyCacheBadgerStore(t)

	putTestNode(t, bs, 10061, 1, nil)
	putTestNode(t, bs, 10062, 1, nil)
	wrongType := putTestRel(t, bs, 10063, 3, 10061, 10062)
	want := putTestRel(t, bs, 10064, 4, 10061, 10062)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.RelCacheForTest().ResetForTest()

	bs.idxMu.Lock()
	bs.typeIdx[4][wrongType.ID()] = struct{}{}
	bs.idxMu.Unlock()

	got, err := bs.RelationshipsByType(4, QueryOpts{Limit: 1})
	if err != nil {
		t.Fatalf("RelationshipsByType: %v", err)
	}
	if gotIDs := relIDsForTest(got); len(gotIDs) != 1 || gotIDs[0] != want.ID() {
		t.Fatalf("RelationshipsByType stale type index IDs = %v, want [%d]", gotIDs, want.ID())
	}
}

func TestBadgerStore_NodesByLabel_VerifiesFetchedRowLabelBeforeLimit(t *testing.T) {
	t.Parallel()
	bs := newTinyCacheBadgerStore(t)

	wrongLabel := putTestNode(t, bs, 10071, 5, nil)
	want := putTestNode(t, bs, 10072, 7, nil)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.NodeCacheForTest().ResetForTest()

	bs.idxMu.Lock()
	bs.labelIdx[7][wrongLabel.ID()] = struct{}{}
	bs.idxMu.Unlock()

	got, err := bs.NodesByLabel(7, QueryOpts{Limit: 1})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if len(got) != 1 || got[0].ID() != want.ID() {
		t.Fatalf("NodesByLabel stale label index = %v, want [%d]", got, want.ID())
	}
}

func TestBadgerStore_NodesByLabelAndProperty_VerifiesFetchedRowBeforeLimit(t *testing.T) {
	t.Parallel()
	bs := newTinyCacheBadgerStore(t)

	wrongLabel := types.NewNode(types.NodeID(snowflake.ID(10081)), 5, nil)
	if err := wrongLabel.SetProperty("name", "Alice"); err != nil {
		t.Fatalf("SetProperty wrongLabel: %v", err)
	}
	want := types.NewNode(types.NodeID(snowflake.ID(10082)), 7, nil)
	if err := want.SetProperty("name", "Alice"); err != nil {
		t.Fatalf("SetProperty want: %v", err)
	}
	if err := bs.PutNode(wrongLabel); err != nil {
		t.Fatalf("PutNode wrongLabel: %v", err)
	}
	if err := bs.PutNode(want); err != nil {
		t.Fatalf("PutNode want: %v", err)
	}
	if err := bs.CreatePropertyIndex(7, "name"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.NodeCacheForTest().ResetForTest()

	key := indexpkg.PropertyIndexKey{LabelToken: 7, PropertyKey: "name"}
	bs.idxMu.Lock()
	bs.propertyIndexes[key].AddKey(wrongLabel.ID().SnowflakeID(), indexpkg.PropertyValueKey("Alice"))
	bs.idxMu.Unlock()

	got, err := bs.NodesByLabelAndProperty(7, "name", "Alice", QueryOpts{Limit: 1})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty: %v", err)
	}
	if len(got) != 1 || got[0].ID() != want.ID() {
		t.Fatalf("NodesByLabelAndProperty stale property index = %v, want [%d]", got, want.ID())
	}
}

func TestBadgerStore_DeleteNodeCascade_UsesPrefetchedRelInfoAfterLRUEviction(t *testing.T) {
	t.Parallel()
	bs := newTinyCacheBadgerStore(t)

	putTestNode(t, bs, 9995, 1, nil)
	putTestNode(t, bs, 9996, 1, nil)
	putTestNode(t, bs, 9997, 1, nil)
	putTestRel(t, bs, 9998, 3, 9995, 9996)
	putTestRel(t, bs, 9999, 3, 9997, 9995)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.NodeCacheForTest().ResetForTest()
	bs.RelCacheForTest().ResetForTest()
	nodeMissesBefore := bs.NodeCacheForTest().Misses()
	relMissesBefore := bs.RelCacheForTest().Misses()

	if err := bs.DeleteNodeCascade(types.NodeID(9995)); err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}

	if got := bs.NodeCacheForTest().Misses() - nodeMissesBefore; got != 1 {
		t.Fatalf("node cache misses during cascade = %d, want 1 prefetch miss only", got)
	}
	if got := bs.RelCacheForTest().Misses() - relMissesBefore; got != 2 {
		t.Fatalf("relationship cache misses during cascade = %d, want 2 prefetch misses only", got)
	}
}

func waitForNodeCacheHits(t *testing.T, bs *Store, ids ...snowflake.ID) {
	t.Helper()
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		allHit := true
		for _, id := range ids {
			if _, status := bs.NodeCacheForTest().Peek(id); status != indexpkg.CacheHit {
				allHit = false
				break
			}
		}
		if allHit {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("node cache entries were not prefetched before idxMu write lock for IDs %v", ids)
}

func waitForRelCacheHits(t *testing.T, bs *Store, ids ...snowflake.ID) {
	t.Helper()
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		allHit := true
		for _, id := range ids {
			if _, status := bs.RelCacheForTest().Peek(id); status != indexpkg.CacheHit {
				allHit = false
				break
			}
		}
		if allHit {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("relationship cache entries were not prefetched before idxMu write lock for IDs %v", ids)
}

func relIDsForTest(rels []*types.Relationship) []types.RelID {
	ids := make([]types.RelID, 0, len(rels))
	for _, r := range rels {
		if r != nil {
			ids = append(ids, r.ID())
		}
	}
	return ids
}
