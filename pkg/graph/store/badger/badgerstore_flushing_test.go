package badger

import (
	"errors"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// These tests pin the `flushing` overlay (the snapshot a flush() parks while it
// is mid-committing a WriteBatch). flush() releases idxMu BEFORE wb.Flush(), so
// for the whole commit window a just-written row is gone from `pending` and not
// yet in a Badger View. Every overlay reader must consult `flushing` too, and
// the snapshot must be cleared on success / Clear / failed-flush — otherwise it
// both DROPS in-flight rows and (when stale) RESURRECTS cleared ones.

// newFlushParkStore builds a store whose background flush loop will not fire
// during a sub-second test (FlushInterval = 1h, no SyncWrites), so writes stay
// resident in `pending` until the test parks them.
func newFlushParkStore(t *testing.T, mut func(*Config)) *Store {
	t.Helper()
	cfg := Config{InMemory: true, FlushInterval: time.Hour}
	if mut != nil {
		mut(&cfg)
	}
	bs, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { bs.Close() })
	return bs
}

// parkPendingIntoFlushing reproduces the flush() commit window deterministically:
// it performs the exact pending->flushing swap flush() does before wb.Flush(),
// WITHOUT committing to Badger. After this the buffered rows live ONLY in
// `flushing` — gone from `pending`, not yet in a Badger View — which is precisely
// the window in which an overlay reader that consults only `pending` drops them.
func parkPendingIntoFlushing(t *testing.T, bs *Store) {
	t.Helper()
	bs.wbMu.Lock()
	defer bs.wbMu.Unlock()
	if len(bs.pending) == 0 {
		t.Fatal("parkPendingIntoFlushing: pending is empty — write something (without Flush) first")
	}
	bs.flushing = bs.pending
	bs.pending = make(map[string]writeOp)
}

func flushingLen(bs *Store) int {
	bs.wbMu.Lock()
	defer bs.wbMu.Unlock()
	return len(bs.flushing)
}

func pendingLen(bs *Store) int {
	bs.wbMu.Lock()
	defer bs.wbMu.Unlock()
	return len(bs.pending)
}

// R1 — single-key version lookups must see a version parked in `flushing`.
func TestFlushingOverlay_GetNodeVersion_SeesParkedRow(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	n := types.NewNode(types.NodeID(1), 10, nil)
	n.SetVersion(3)
	if err := bs.PutNodeVersion(types.NodeID(1), 3, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}
	parkPendingIntoFlushing(t, bs)

	got, err := bs.GetNodeVersion(types.NodeID(1), 3)
	if err != nil {
		t.Fatalf("GetNodeVersion in the commit window: %v (regression: reader ignored flushing)", err)
	}
	if got == nil {
		t.Fatal("GetNodeVersion returned nil for a version parked in flushing")
	}
}

func TestFlushingOverlay_GetRelVersion_SeesParkedRow(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	r := types.NewRelationship(types.RelID(1), 5, types.NodeID(1), types.NodeID(2))
	r.SetVersion(3)
	if err := bs.PutRelVersion(types.RelID(1), 3, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}
	parkPendingIntoFlushing(t, bs)

	got, err := bs.GetRelVersion(types.RelID(1), 3)
	if err != nil {
		t.Fatalf("GetRelVersion in the commit window: %v (regression: reader ignored flushing)", err)
	}
	if got == nil {
		t.Fatal("GetRelVersion returned nil for a version parked in flushing")
	}
}

// R2 — history-ID scans + Max*HistoryID must see flushing-parked versions, and
// must agree with each other (the inconsistency hunt-24 flagged).
func TestFlushingOverlay_HistoryIDScans_SeeParkedRows(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	ids := []int64{100, 200}
	for _, id := range ids {
		n := types.NewNode(types.NodeID(snowflake.ID(id)), 10, nil)
		if err := bs.PutNodeVersion(types.NodeID(snowflake.ID(id)), 0, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", id, err)
		}
	}
	parkPendingIntoFlushing(t, bs)

	hist, err := bs.AllNodeHistoryIDs()
	if err != nil {
		t.Fatalf("AllNodeHistoryIDs: %v", err)
	}
	seen := map[types.NodeID]bool{}
	for _, id := range hist {
		seen[id] = true
	}
	for _, id := range ids {
		if !seen[types.NodeID(snowflake.ID(id))] {
			t.Fatalf("AllNodeHistoryIDs missing %d during the commit window (ignored flushing): got %v", id, hist)
		}
	}

	maxID, err := bs.MaxNodeHistoryID()
	if err != nil {
		t.Fatalf("MaxNodeHistoryID: %v", err)
	}
	if maxID != types.NodeID(200) {
		t.Fatalf("MaxNodeHistoryID = %d during the commit window, want 200 (ignored flushing / disagrees with AllNodeHistoryIDs)", maxID)
	}
}

// R2 — full version-chain read (getNodeHistoryByPrefix) must see parked versions.
func TestFlushingOverlay_GetNodeHistory_SeesParkedVersions(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	for _, v := range []uint32{0, 1, 2} {
		n := types.NewNode(types.NodeID(7), 10, nil)
		n.SetVersion(v)
		if err := bs.PutNodeVersion(types.NodeID(7), v, n); err != nil {
			t.Fatalf("PutNodeVersion(v=%d): %v", v, err)
		}
	}
	parkPendingIntoFlushing(t, bs)

	hist, err := bs.GetNodeHistory(types.NodeID(7))
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(hist) != 3 {
		t.Fatalf("GetNodeHistory returned %d versions during the commit window, want 3 (dropped flushing-parked versions)", len(hist))
	}
}

func TestFlushingOverlay_GetRelHistory_SeesParkedVersions(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	for _, v := range []uint32{0, 1} {
		r := types.NewRelationship(types.RelID(9), 5, types.NodeID(1), types.NodeID(2))
		r.SetVersion(v)
		if err := bs.PutRelVersion(types.RelID(9), v, r); err != nil {
			t.Fatalf("PutRelVersion(v=%d): %v", v, err)
		}
	}
	parkPendingIntoFlushing(t, bs)

	hist, err := bs.GetRelHistory(types.RelID(9))
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(hist) != 2 {
		t.Fatalf("GetRelHistory returned %d versions during the commit window, want 2 (dropped flushing-parked versions)", len(hist))
	}
}

// R3 — disk-resident label overlay must see a node whose label-index SET is
// parked in flushing.
func TestFlushingOverlay_LabelDisk_SeesParkedNode(t *testing.T) {
	bs := newFlushParkStore(t, func(c *Config) { c.LabelIndexOnDisk = true })
	putTestNode(t, bs, 1, 10, nil)
	parkPendingIntoFlushing(t, bs)

	bs.idxMu.RLock()
	nids, err := bs.labelNodeIDsSnapshotLocked(10)
	bs.idxMu.RUnlock()
	if err != nil {
		t.Fatalf("labelNodeIDsSnapshotLocked: %v", err)
	}
	if len(nids) != 1 || nids[0] != types.NodeID(1) {
		t.Fatalf("label snapshot = %v during the commit window, want [1] (disk overlay ignored flushing)", nids)
	}
}

// R3 — disk-resident adjacency overlay must see a rel whose adjacency SET is
// parked in flushing.
func TestFlushingOverlay_AdjacencyDisk_SeesParkedRel(t *testing.T) {
	bs := newFlushParkStore(t, func(c *Config) { c.AdjacencyIndexOnDisk = true })
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestRel(t, bs, 100, 5, 1, 2)
	parkPendingIntoFlushing(t, bs)

	bs.idxMu.RLock()
	rids, err := bs.adjacentRelIDsSnapshotLocked(types.NodeID(1), 0, false)
	bs.idxMu.RUnlock()
	if err != nil {
		t.Fatalf("adjacentRelIDsSnapshotLocked: %v", err)
	}
	found := false
	for _, rid := range rids {
		if rid == types.RelID(100) {
			found = true
		}
	}
	if !found {
		t.Fatalf("outgoing adjacency = %v during the commit window, want to include rel 100 (disk overlay ignored flushing)", rids)
	}
}

// M1 — a SUCCESSFUL flush must clear `flushing` (the comment promised it; the
// code did not). A lingering snapshot is what M2 turns into a Clear resurrection.
func TestFlushingOverlay_SuccessfulFlushClearsFlushing(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	putTestNode(t, bs, 1, 10, nil)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if n := flushingLen(bs); n != 0 {
		t.Fatalf("flushing retained %d entries after a successful flush (leak — comment claims it is cleared)", n)
	}
}

// M2 — Clear() must reset `flushing` so a leaked/in-flight snapshot cannot
// resurrect history IDs the wipe just removed.
func TestFlushingOverlay_ClearResetsFlushing_NoResurrection(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	n := types.NewNode(types.NodeID(100), 10, nil)
	if err := bs.PutNodeVersion(types.NodeID(100), 0, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}
	parkPendingIntoFlushing(t, bs) // a snapshot left parked (as a leaked flush would)

	if err := bs.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if nf := flushingLen(bs); nf != 0 {
		t.Fatalf("Clear left %d flushing entries (resurrection risk)", nf)
	}
	hist, err := bs.AllNodeHistoryIDs()
	if err != nil {
		t.Fatalf("AllNodeHistoryIDs: %v", err)
	}
	if len(hist) != 0 {
		t.Fatalf("phantom history IDs survived Clear: %v (flushing resurrected them)", hist)
	}
}

// T3 — a FAILED flush (requeue path) must clear `flushing` and restore the ops
// to `pending`, losing nothing.
func TestFlushingOverlay_FailedFlushClearsFlushingAndRequeues(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	putTestNode(t, bs, 1, 10, nil)
	before := pendingLen(bs)
	if before == 0 {
		t.Fatal("precondition: pending should hold the node op")
	}

	bs.dbClosed.Store(true) // force flush() to fail at the dbClosed guard
	err := bs.flush()
	if !errors.Is(err, badgerv4.ErrDBClosed) {
		t.Fatalf("flush() err = %v, want ErrDBClosed", err)
	}
	if nf := flushingLen(bs); nf != 0 {
		t.Fatalf("failed flush left %d flushing entries (requeue must clear)", nf)
	}
	if np := pendingLen(bs); np < before {
		t.Fatalf("failed flush lost ops: pending %d < %d before (requeue must restore)", np, before)
	}
	bs.dbClosed.Store(false) // allow t.Cleanup Close() to proceed
}

// Mutator parity — the cascade orphan-purge computes its delete set from a Badger
// View + buffers; an index key parked in `flushing` (gone from pending, not yet
// committed) must still get a delete queued, else it orphans once the in-flight
// commit lands it.
func TestFlushingOverlay_CascadeScrub_DeletesParkedIndexKeys(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestRel(t, bs, 100, 5, 1, 2)
	parkPendingIntoFlushing(t, bs)

	bs.idxMu.Lock()
	err := bs.purgeOrphanRelIDLocked(types.RelID(100))
	bs.idxMu.Unlock()
	if err != nil {
		t.Fatalf("purgeOrphanRelIDLocked: %v", err)
	}

	bs.wbMu.Lock()
	defer bs.wbMu.Unlock()
	n := 0
	for k, op := range bs.pending {
		if op.opType == writeOpDelete && relationshipIndexKeyMatchesRelID([]byte(k), snowflake.ID(100)) {
			n++
		}
	}
	if n == 0 {
		t.Fatal("cascade scrub queued no delete for a rel index key parked in flushing (orphaned persisted key)")
	}
}

// Mutator parity — incoming-repair (entity gone, exact key unknown) scans pending
// + a Badger View; an incoming-adjacency SET parked in `flushing` must still get a
// delete queued.
func TestFlushingOverlay_IncomingRepair_DeletesParkedKey(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestRel(t, bs, 100, 5, 1, 2) // writes the KeyIn key for end node 2
	parkPendingIntoFlushing(t, bs)

	if err := bs.DeleteIncomingByRelID(snowflake.ID(2), snowflake.ID(100)); err != nil {
		t.Fatalf("DeleteIncomingByRelID: %v", err)
	}

	bs.wbMu.Lock()
	defer bs.wbMu.Unlock()
	found := false
	for k, op := range bs.pending {
		kb := []byte(k)
		if len(kb) == storepkg.SizeAdjacency && kb[0] == storepkg.KeyIn && op.opType == writeOpDelete &&
			storepkg.ParseIDFromKey(kb, 1) == snowflake.ID(2) && storepkg.ParseRelIDFromAdjKey(kb) == snowflake.ID(100) {
			found = true
		}
	}
	if !found {
		t.Fatal("incoming-repair queued no delete for the incoming key parked in flushing (orphaned persisted key)")
	}
}
