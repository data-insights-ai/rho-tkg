package badger

import (
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Same commit window as badgerstore_labeltxmembers_commitwindow_test.go, for the
// two other readers that scanned Badger before reading the write buffer: the
// lazy belief-watermark builds (a missed row leaves the watermark too low for
// the life of the store, which opens the current-row fast path in
// nodeCurrentAnswersAt / relCurrentAnswersAt for an entity whose newest belief
// is a history row) and the orphan-rel index purge (a missed index key never
// gets its delete and stays on disk as an orphan).

// commitParkedDuringHook parks pending into flushing and arranges for the hook
// to commit it inside the reader's scan->overlay window.
func commitParkedDuringHook(t *testing.T, bs *Store) {
	t.Helper()
	parkPendingIntoFlushing(t, bs)
	var once sync.Once
	bs.historyScanTestHook = func() {
		once.Do(func() { commitFlushingToBadger(t, bs) })
	}
	t.Cleanup(func() { bs.historyScanTestHook = nil })
}

// SYSTEM PROPERTY (node): the belief watermark is the maximum TxFrom across the
// whole chain even when the flush holding the newest history row commits
// during the lazy build.
func TestNodeBeliefWatermark_NoDropAcrossFlushCommit(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	cur := bgNode(1, []uint16{10}, 100)
	cur.SetVersion(1)
	if err := bs.PutNode(cur); err != nil {
		t.Fatalf("PutNode(current): %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// A bounded correction: a history row with a NEWER TxFrom than the
	// untouched current row.
	corr := bgNode(1, []uint16{10}, 300)
	corr.SetVersion(0)
	if err := bs.PutNodeVersion(types.NodeID(1), 0, corr); err != nil {
		t.Fatalf("PutNodeVersion(correction): %v", err)
	}
	commitParkedDuringHook(t, bs)

	wm, ok := bs.NodeBeliefWatermark(types.NodeID(1))
	if !ok || wm != 300 {
		t.Fatalf("NodeBeliefWatermark(1) = (%d, %v), want (300, true): the build missed the correction row "+
			"its flush committed between the Badger scan and the overlay read", wm, ok)
	}
}

// SYSTEM PROPERTY (rel mirror).
func TestRelBeliefWatermark_NoDropAcrossFlushCommit(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	cur := types.NewRelationship(types.RelID(100), 7, types.NodeID(1), types.NodeID(2))
	cur.SetTemporal(&types.TemporalMetadata{TxFrom: 100, ValidFrom: 100})
	cur.SetVersion(1)
	if err := bs.PutRelationship(cur); err != nil {
		t.Fatalf("PutRelationship(current): %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	corr := types.NewRelationship(types.RelID(100), 7, types.NodeID(1), types.NodeID(2))
	corr.SetTemporal(&types.TemporalMetadata{TxFrom: 300, ValidFrom: 100})
	corr.SetVersion(0)
	if err := bs.PutRelVersion(types.RelID(100), 0, corr); err != nil {
		t.Fatalf("PutRelVersion(correction): %v", err)
	}
	commitParkedDuringHook(t, bs)

	wm, ok := bs.RelBeliefWatermark(types.RelID(100))
	if !ok || wm != 300 {
		t.Fatalf("RelBeliefWatermark(100) = (%d, %v), want (300, true)", wm, ok)
	}
}

// SYSTEM PROPERTY: the orphan-rel purge queues a delete for every index key of
// the rel, including keys whose flush commits during the purge's Badger scan.
func TestPurgeOrphanRelIndexes_NoOrphanAcrossFlushCommit(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush(endpoints): %v", err)
	}
	rel := types.NewRelationship(types.RelID(100), 7, types.NodeID(1), types.NodeID(2))
	if err := bs.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	commitParkedDuringHook(t, bs)

	bs.idxMu.Lock()
	err := bs.purgeOrphanRelIDLocked(types.RelID(100))
	bs.idxMu.Unlock()
	if err != nil {
		t.Fatalf("purgeOrphanRelIDLocked: %v", err)
	}
	bs.historyScanTestHook = nil
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	left, err := bs.relationshipIndexKeysForRel(types.RelID(100).SnowflakeID())
	if err != nil {
		t.Fatalf("relationshipIndexKeysForRel: %v", err)
	}
	if len(left) != 0 {
		t.Fatalf("purge left %d index keys for rel 100 on disk (%x): they were committed by the parked flush "+
			"between the purge's Badger scan and its `flushing` read, so no delete was queued", len(left), left)
	}
}
