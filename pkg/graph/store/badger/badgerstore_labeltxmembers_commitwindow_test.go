package badger

import (
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// These tests pin the SCAN->OVERLAY commit window in the lazy K1 membership
// sidecar builds (ensureLabelTxMembersBuilt / ensureRelTypeTxMembersBuilt) —
// the same race lesson 64 closed in getNodeHistoryByPrefix. The build holds
// idxMu.Lock, but flush() releases its idxMu.RLock right after parking
// `pending` into `flushing`, so a flush that was already parked still commits
// to Badger and clears `flushing` while the build runs. A build that scans
// Badger FIRST and reads the overlay SECOND misses every row that flush
// committed in between: not in its older Badger snapshot, no longer in
// `flushing`. The build then marks itself done, so the member is missing for
// the life of the store and a pinned ByLabel/ByType scan silently omits a
// node/rel whose label/type is carried only by history or a deleted row.
//
// Field sighting: TestBitemporalOracle_BadgerCommitWindow failed ~1% of runs
// under load (Nodes.ByLabel{TxPin} / Rels.ByType dropping an entity). The hook
// makes the interleaving deterministic.

// SYSTEM PROPERTY (node): a label carried only by a history row parked in
// `flushing` is a sidecar member even when that flush commits mid-build.
func TestLabelTxMembership_NoDropAcrossFlushCommit(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	const tokOld, tokNew uint16 = 10, 11

	// The current row carries only tokNew; tokOld lives ONLY in the v0 history
	// row, so the defensive union with the current label index cannot rescue it.
	cur := bgNode(1, []uint16{tokNew}, 200)
	cur.SetVersion(1)
	if err := bs.PutNode(cur); err != nil {
		t.Fatalf("PutNode(current): %v", err)
	}
	v0 := bgNode(1, []uint16{tokOld}, 100)
	v0.SetVersion(0)
	if err := bs.PutNodeVersion(types.NodeID(1), 0, v0); err != nil {
		t.Fatalf("PutNodeVersion(v0): %v", err)
	}

	parkPendingIntoFlushing(t, bs)

	var once sync.Once
	bs.historyScanTestHook = func() {
		once.Do(func() { commitFlushingToBadger(t, bs) })
	}
	defer func() { bs.historyScanTestHook = nil }()

	members := labelMembers(t, bs, tokOld)
	tx, ok := members[types.NodeID(1)]
	if !ok {
		t.Fatalf("label %d sidecar dropped node 1 across the flush commit window: members=%v. "+
			"The build scanned Badger before the parked flush committed v0 and read the overlay after `flushing` was cleared.",
			tokOld, members)
	}
	if tx != 100 {
		t.Fatalf("node 1 firstTxFrom = %d, want 100", tx)
	}
	if flushingLen(bs) != 0 {
		t.Fatalf("hook did not fire: flushing still holds %d ops", flushingLen(bs))
	}
}

// SYSTEM PROPERTY (rel mirror).
func TestRelTypeTxMembership_NoDropAcrossFlushCommit(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	const knows uint16 = 7

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush(endpoints): %v", err)
	}

	cur := types.NewRelationship(types.RelID(100), knows, types.NodeID(1), types.NodeID(2))
	cur.SetTemporal(&types.TemporalMetadata{TxFrom: 200, ValidFrom: 200})
	cur.SetVersion(1)
	if err := bs.PutRelationship(cur); err != nil {
		t.Fatalf("PutRelationship(current): %v", err)
	}
	v0 := types.NewRelationship(types.RelID(100), knows, types.NodeID(1), types.NodeID(2))
	v0.SetTemporal(&types.TemporalMetadata{TxFrom: 100, ValidFrom: 100})
	v0.SetVersion(0)
	if err := bs.PutRelVersion(types.RelID(100), 0, v0); err != nil {
		t.Fatalf("PutRelVersion(v0): %v", err)
	}

	parkPendingIntoFlushing(t, bs)

	var once sync.Once
	bs.historyScanTestHook = func() {
		once.Do(func() { commitFlushingToBadger(t, bs) })
	}
	defer func() { bs.historyScanTestHook = nil }()

	members := make(map[types.RelID]types.Instant)
	if err := bs.ForEachRelTypeTxMember(knows, func(id types.RelID, firstTx types.Instant) bool {
		members[id] = firstTx
		return true
	}); err != nil {
		t.Fatalf("ForEachRelTypeTxMember: %v", err)
	}
	tx, ok := members[types.RelID(100)]
	if !ok {
		t.Fatalf("rel-type %d sidecar dropped rel 100 across the flush commit window: members=%v", knows, members)
	}
	if tx != 100 {
		t.Fatalf("rel 100 firstTxFrom = %d, want 100", tx)
	}
	if flushingLen(bs) != 0 {
		t.Fatalf("hook did not fire: flushing still holds %d ops", flushingLen(bs))
	}
}

// versionInsertDuringBuild runs insert on another goroutine from inside the
// build (the hook fires while the build holds idxMu.Lock, after its overlay
// capture and Badger scan, before it marks itself built) and gives it up to
// 200ms to finish there. A version door that enqueues without idxMu finishes
// inside the build: its row is behind the build's overlay capture and it saw
// built=false, so nobody records it. A door that serialises against the build
// blocks here, runs after the build and records incrementally. The wait only
// bounds the red case (a slow machine can at worst hide the defect); the
// green assertion does not depend on it.
func versionInsertDuringBuild(t *testing.T, bs *Store, insert func() error) (wait func()) {
	t.Helper()
	done := make(chan error, 1)
	var once sync.Once
	bs.historyScanTestHook = func() {
		once.Do(func() {
			go func() { done <- insert() }()
			select {
			case err := <-done:
				done <- err // hand it back to wait()
			case <-time.After(200 * time.Millisecond):
			}
		})
	}
	return func() {
		bs.historyScanTestHook = nil
		if err := <-done; err != nil {
			t.Fatalf("version insert: %v", err)
		}
	}
}

// SYSTEM PROPERTY (node): a history version inserted while the lazy label
// sidecar build is in progress is a member once both have finished.
func TestLabelTxMembership_NoDropForVersionInsertDuringBuild(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	const tokOld, tokNew uint16 = 10, 11
	cur := bgNode(1, []uint16{tokNew}, 200)
	cur.SetVersion(1)
	if err := bs.PutNode(cur); err != nil {
		t.Fatalf("PutNode(current): %v", err)
	}
	v0 := bgNode(1, []uint16{tokOld}, 100)
	v0.SetVersion(0)
	wait := versionInsertDuringBuild(t, bs, func() error { return bs.PutNodeVersion(types.NodeID(1), 0, v0) })

	_ = labelMembers(t, bs, tokNew) // triggers the build; the hook inserts v0 mid-build
	wait()

	members := labelMembers(t, bs, tokOld)
	if _, ok := members[types.NodeID(1)]; !ok {
		t.Fatalf("label %d sidecar lost node 1: its v0 history row was enqueued during the build without idxMu, "+
			"behind the build's overlay capture, while built was still false: members=%v", tokOld, members)
	}
}

// SYSTEM PROPERTY (rel mirror).
func TestRelTypeTxMembership_NoDropForVersionInsertDuringBuild(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	const knows, likes uint16 = 7, 8
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	other := types.NewRelationship(types.RelID(50), likes, types.NodeID(1), types.NodeID(2))
	other.SetTemporal(&types.TemporalMetadata{TxFrom: 50, ValidFrom: 50})
	if err := bs.PutRelationship(other); err != nil {
		t.Fatalf("PutRelationship(other): %v", err)
	}
	// Rel 100 exists only as a history version (a deleted rel reconstructed by
	// version inserts), so only the version door can make it a member.
	v0 := types.NewRelationship(types.RelID(100), knows, types.NodeID(1), types.NodeID(2))
	v0.SetTemporal(&types.TemporalMetadata{TxFrom: 100, ValidFrom: 100})
	v0.SetVersion(0)
	wait := versionInsertDuringBuild(t, bs, func() error { return bs.PutRelVersion(types.RelID(100), 0, v0) })

	if err := bs.ForEachRelTypeTxMember(likes, func(types.RelID, types.Instant) bool { return true }); err != nil {
		t.Fatalf("ForEachRelTypeTxMember(likes): %v", err)
	}
	wait()

	members := make(map[types.RelID]types.Instant)
	if err := bs.ForEachRelTypeTxMember(knows, func(id types.RelID, firstTx types.Instant) bool {
		members[id] = firstTx
		return true
	}); err != nil {
		t.Fatalf("ForEachRelTypeTxMember(knows): %v", err)
	}
	if _, ok := members[types.RelID(100)]; !ok {
		t.Fatalf("rel-type %d sidecar lost rel 100 inserted by PutRelVersion during the build: members=%v", knows, members)
	}
}
