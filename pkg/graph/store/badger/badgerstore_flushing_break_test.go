package badger

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// These tests pin the `flushing` overlay against the version-chain readers
// (NodeAsOf/RelAsOf via reverseScanHistoryVersion->pendingHistoryVersionOverlay,
// TruncateNodeHistory/TrimNodeHistoryFrom retention counting, GetNodeHistory /
// GetNodeVersion pending-wins masking) and against the disk adjacency overlay +
// cascade/incoming index scrub, end to end. Each test reproduces the flush()
// commit window with parkPendingIntoFlushing (rows live ONLY in `flushing` — gone
// from `pending`, not yet in a Badger View). A reader/mutator that consults only
// `pending` regresses each property below.

// SYSTEM PROPERTY: a transaction-time point read must see a superseded version
// that is parked in `flushing` mid-commit, not fall through to ErrVersionNotFound
// or return the (non-matching) current row. reverseScanHistoryVersion merges the
// pending-history overlay (flushing ++ pending), so the parked v0 is decisive.
func TestFlushingOverlay_NodeAsOf_SeesParkedSupersededVersion(t *testing.T) {
	bs := newFlushParkStore(t, nil)

	const (
		t0 = types.Instant(100) // v0 valid: recorded at t0, superseded at t1
		t1 = types.Instant(200) // v1 valid: recorded at t1, still open
	)

	// Current row v1 (open in tx-time: TxFrom=t1, TxTo=0). Written through the
	// cache so GetNode sees it; it does NOT match a query at t0 (TxFrom=200 > 100),
	// forcing NodeAsOf into the history arm.
	cur := types.NewNode(types.NodeID(1), 10, nil)
	cur.SetVersion(1)
	cur.SetTemporal(&types.TemporalMetadata{TxFrom: t1, TxTo: 0})
	if err := bs.PutNode(cur); err != nil {
		t.Fatalf("PutNode(current): %v", err)
	}

	// Superseded history version v0 (TxFrom=t0, TxTo=t1).
	v0 := types.NewNode(types.NodeID(1), 10, nil)
	v0.SetVersion(0)
	v0.SetTemporal(&types.TemporalMetadata{TxFrom: t0, TxTo: t1})
	if err := bs.PutNodeVersion(types.NodeID(1), 0, v0); err != nil {
		t.Fatalf("PutNodeVersion(v0): %v", err)
	}

	parkPendingIntoFlushing(t, bs)

	got, err := bs.NodeAsOf(types.NodeID(1), t0)
	if err != nil {
		t.Fatalf("NodeAsOf(t0) in the commit window: %v (regression: reverse scan ignored flushing)", err)
	}
	if got == nil {
		t.Fatal("NodeAsOf(t0) = nil, want the parked superseded v0")
	}
	if got.Version() != 0 {
		t.Fatalf("NodeAsOf(t0) returned version %d, want 0 (the superseded version parked in flushing)", got.Version())
	}
	if tm := got.Temporal(); tm == nil || tm.TxFrom != t0 || tm.TxTo != t1 {
		t.Fatalf("NodeAsOf(t0) temporal = %+v, want {TxFrom:%d TxTo:%d}", tm, t0, t1)
	}
}

// SYSTEM PROPERTY (rel mirror of the node case above).
func TestFlushingOverlay_RelAsOf_SeesParkedSupersededVersion(t *testing.T) {
	bs := newFlushParkStore(t, nil)

	const (
		t0 = types.Instant(100)
		t1 = types.Instant(200)
	)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)

	cur := types.NewRelationship(types.RelID(100), 5, types.NodeID(1), types.NodeID(2))
	cur.SetVersion(1)
	cur.SetTemporal(&types.TemporalMetadata{TxFrom: t1, TxTo: 0})
	if err := bs.PutRelationship(cur); err != nil {
		t.Fatalf("PutRelationship(current): %v", err)
	}

	v0 := types.NewRelationship(types.RelID(100), 5, types.NodeID(1), types.NodeID(2))
	v0.SetVersion(0)
	v0.SetTemporal(&types.TemporalMetadata{TxFrom: t0, TxTo: t1})
	if err := bs.PutRelVersion(types.RelID(100), 0, v0); err != nil {
		t.Fatalf("PutRelVersion(v0): %v", err)
	}

	parkPendingIntoFlushing(t, bs)

	got, err := bs.RelAsOf(types.RelID(100), t0)
	if err != nil {
		t.Fatalf("RelAsOf(t0) in the commit window: %v (regression: reverse scan ignored flushing)", err)
	}
	if got == nil {
		t.Fatal("RelAsOf(t0) = nil, want the parked superseded v0")
	}
	if got.Version() != 0 {
		t.Fatalf("RelAsOf(t0) returned version %d, want 0 (superseded version parked in flushing)", got.Version())
	}
	if tm := got.Temporal(); tm == nil || tm.TxFrom != t0 || tm.TxTo != t1 {
		t.Fatalf("RelAsOf(t0) temporal = %+v, want {TxFrom:%d TxTo:%d}", tm, t0, t1)
	}
}

// SYSTEM PROPERTY: TruncateNodeHistory retention counting must include versions
// parked in `flushing`. With 3 parked versions and keepVersions=2, the oldest
// (v0) must be deleted — so GetNodeHistory must be exactly [v1, v2]. If truncate
// counted only `pending` it would see 0 versions and delete nothing.
func TestFlushingOverlay_Truncate_CountsParkedVersions(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	for _, v := range []uint32{0, 1, 2} {
		n := types.NewNode(types.NodeID(7), 10, nil)
		n.SetVersion(v)
		if err := bs.PutNodeVersion(types.NodeID(7), v, n); err != nil {
			t.Fatalf("PutNodeVersion(v=%d): %v", v, err)
		}
	}
	parkPendingIntoFlushing(t, bs)

	if err := bs.TruncateNodeHistory(types.NodeID(7), 2); err != nil {
		t.Fatalf("TruncateNodeHistory: %v", err)
	}

	hist, err := bs.GetNodeHistory(types.NodeID(7))
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	gotVers := make([]uint32, len(hist))
	for i, n := range hist {
		gotVers[i] = n.Version()
	}
	if len(gotVers) != 2 || gotVers[0] != 1 || gotVers[1] != 2 {
		t.Fatalf("post-truncate versions = %v, want [1 2] (truncate must count the 3 parked versions and drop v0)", gotVers)
	}
}

// SYSTEM PROPERTY: TrimNodeHistoryFrom must delete a version parked in `flushing`
// that is >= minVersion. With versions {0,1,2} parked and minVersion=2, v2 must
// be removed — GetNodeHistory must become [v0, v1]. A trim that counted only
// `pending` would leave v2 intact.
func TestFlushingOverlay_Trim_DeletesParkedVersion(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	for _, v := range []uint32{0, 1, 2} {
		n := types.NewNode(types.NodeID(7), 10, nil)
		n.SetVersion(v)
		if err := bs.PutNodeVersion(types.NodeID(7), v, n); err != nil {
			t.Fatalf("PutNodeVersion(v=%d): %v", v, err)
		}
	}
	parkPendingIntoFlushing(t, bs)

	if err := bs.TrimNodeHistoryFrom(types.NodeID(7), 2); err != nil {
		t.Fatalf("TrimNodeHistoryFrom: %v", err)
	}

	hist, err := bs.GetNodeHistory(types.NodeID(7))
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	gotVers := make([]uint32, len(hist))
	for i, n := range hist {
		gotVers[i] = n.Version()
	}
	if len(gotVers) != 2 || gotVers[0] != 0 || gotVers[1] != 1 {
		t.Fatalf("post-trim versions = %v, want [0 1] (trim must delete the parked v2 >= minVersion)", gotVers)
	}
	// And the trimmed version must read back as not-found.
	if _, err := bs.GetNodeVersion(types.NodeID(7), 2); !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("GetNodeVersion(7,2) after trim = %v, want ErrVersionNotFound", err)
	}
}

// SYSTEM PROPERTY: a pending DELETE must MASK a flushing SET for the same history
// key (rangePending visits flushing then pending; pending wins). Three versions
// parked in flushing, then a delete of v1 queued into pending: GetNodeHistory must
// be [v0, v2] and GetNodeVersion(7,1) must be ErrVersionNotFound. If pending did
// not win over flushing, v1 would resurface.
func TestFlushingOverlay_PendingDeleteWinsOverFlushingSet(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	for _, v := range []uint32{0, 1, 2} {
		n := types.NewNode(types.NodeID(7), 10, nil)
		n.SetVersion(v)
		if err := bs.PutNodeVersion(types.NodeID(7), v, n); err != nil {
			t.Fatalf("PutNodeVersion(v=%d): %v", v, err)
		}
	}
	parkPendingIntoFlushing(t, bs) // 3 SETs now live ONLY in flushing

	// Queue a DELETE of v1 into pending (mid-commit masking).
	bs.appendOps(writeOp{opType: writeOpDelete, key: storepkg.HistNodeKey(snowflake.ID(7), 1)})

	hist, err := bs.GetNodeHistory(types.NodeID(7))
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	gotVers := make([]uint32, len(hist))
	for i, n := range hist {
		gotVers[i] = n.Version()
	}
	if len(gotVers) != 2 || gotVers[0] != 0 || gotVers[1] != 2 {
		t.Fatalf("history = %v, want [0 2] (pending DELETE must mask the flushing SET for v1)", gotVers)
	}
	if _, err := bs.GetNodeVersion(types.NodeID(7), 1); !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("GetNodeVersion(7,1) = %v, want ErrVersionNotFound (pending DELETE must win over flushing SET)", err)
	}
}

// SYSTEM PROPERTY: the disk-resident adjacency overlay must surface a rel whose
// index SETs are parked in `flushing`, across the incoming, typed-outgoing, and
// metas readers — and the type filter must still exclude non-matching types.
func TestFlushingOverlay_AdjacencyDisk_IncomingSeesParkedRel(t *testing.T) {
	bs := newFlushParkStore(t, func(c *Config) { c.AdjacencyIndexOnDisk = true })
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestRel(t, bs, 100, 5, 1, 2)
	parkPendingIntoFlushing(t, bs)

	bs.idxMu.RLock()
	rids, err := bs.adjacentRelIDsSnapshotLocked(types.NodeID(2), 0, true)
	bs.idxMu.RUnlock()
	if err != nil {
		t.Fatalf("adjacentRelIDsSnapshotLocked(incoming): %v", err)
	}
	if !containsRelID(rids, types.RelID(100)) {
		t.Fatalf("incoming adjacency for node 2 = %v during commit window, want to include rel 100 (disk overlay ignored flushing)", rids)
	}
}

func TestFlushingOverlay_AdjacencyDisk_TypedSeesParkedRel(t *testing.T) {
	bs := newFlushParkStore(t, func(c *Config) { c.AdjacencyIndexOnDisk = true })
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestRel(t, bs, 100, 5, 1, 2)
	parkPendingIntoFlushing(t, bs)

	bs.idxMu.RLock()
	matching, mErr := bs.adjacentRelIDsSnapshotLocked(types.NodeID(1), 5, false)
	wrongType, wErr := bs.adjacentRelIDsSnapshotLocked(types.NodeID(1), 999, false)
	bs.idxMu.RUnlock()
	if mErr != nil {
		t.Fatalf("adjacentRelIDsSnapshotLocked(type=5): %v", mErr)
	}
	if wErr != nil {
		t.Fatalf("adjacentRelIDsSnapshotLocked(type=999): %v", wErr)
	}
	if !containsRelID(matching, types.RelID(100)) {
		t.Fatalf("typed (type=5) outgoing adjacency = %v, want to include rel 100 (disk overlay ignored flushing)", matching)
	}
	if containsRelID(wrongType, types.RelID(100)) {
		t.Fatalf("typed (type=999) outgoing adjacency = %v, must NOT include rel 100 (type filter dropped against the flushing overlay)", wrongType)
	}
}

func TestFlushingOverlay_AdjacencyDisk_MetasSeeParkedRel(t *testing.T) {
	bs := newFlushParkStore(t, func(c *Config) { c.AdjacencyIndexOnDisk = true })
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestRel(t, bs, 100, 5, 1, 2)
	parkPendingIntoFlushing(t, bs)

	bs.idxMu.RLock()
	metas, err := bs.adjacentRelMetasSnapshotLocked(types.NodeID(1), 0, false)
	bs.idxMu.RUnlock()
	if err != nil {
		t.Fatalf("adjacentRelMetasSnapshotLocked: %v", err)
	}
	var found *adjMeta
	for i := range metas {
		if metas[i].rel == types.RelID(100) {
			found = &metas[i]
		}
	}
	if found == nil {
		t.Fatalf("metas = %+v during commit window, want to include rel 100 (disk overlay ignored flushing)", metas)
	}
	if found.other != types.NodeID(2) {
		t.Fatalf("outgoing meta other-endpoint for rel 100 = %d, want 2 (end node)", found.other)
	}
}

// SYSTEM PROPERTY (end-to-end): a cascade orphan-purge of a rel whose index keys
// were parked in `flushing` mid-commit must ensure the PERSISTED key is gone once
// the in-flight commit lands. We reproduce the window deterministically: the
// flushing SETs commit to Badger (simulating the in-flight wb.Flush landing),
// then the purge-queued pending DELETEs are flushed. The persisted index key must
// then be ABSENT — not merely have a delete queued.
func TestFlushingOverlay_CascadeScrub_RemovesPersistedIndexKey_EndToEnd(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestRel(t, bs, 100, 5, 1, 2)
	parkPendingIntoFlushing(t, bs)

	// Purge runs WHILE the SETs are parked in flushing: it must queue deletes for
	// those parked index keys into pending.
	bs.idxMu.Lock()
	err := bs.purgeOrphanRelIDLocked(types.RelID(100))
	bs.idxMu.Unlock()
	if err != nil {
		t.Fatalf("purgeOrphanRelIDLocked: %v", err)
	}

	// Land the in-flight commit: copy the parked flushing SETs into Badger, then
	// clear flushing (exactly what a successful flush would do for that snapshot).
	landParkedFlushingSets(t, bs)

	// Now flush the purge-queued pending DELETEs.
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// The persisted relationship index keys for rel 100 must be GONE.
	keys, err := bs.relationshipIndexKeysForRel(snowflake.ID(100))
	if err != nil {
		t.Fatalf("relationshipIndexKeysForRel: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("rel 100 still has %d persisted index keys after cascade scrub + commit: %v (orphaned persisted key — flushing-parked key not scrubbed)", len(keys), keys)
	}
}

// SYSTEM PROPERTY (end-to-end, incoming-repair mirror): DeleteIncomingByRelID must
// ensure the PERSISTED incoming (0x06) key for a rel parked in `flushing` is gone
// once the in-flight commit lands.
func TestFlushingOverlay_IncomingRepair_RemovesPersistedIndexKey_EndToEnd(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestRel(t, bs, 100, 5, 1, 2) // writes the KeyIn key for end node 2
	parkPendingIntoFlushing(t, bs)

	if err := bs.DeleteIncomingByRelID(snowflake.ID(2), snowflake.ID(100)); err != nil {
		t.Fatalf("DeleteIncomingByRelID: %v", err)
	}

	landParkedFlushingSets(t, bs)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// The persisted incoming (0x06) key for (end=2, rel=100) must be gone.
	if got := countPersistedIncomingKeys(t, bs, snowflake.ID(2), snowflake.ID(100)); got != 0 {
		t.Fatalf("incoming key for (end=2, rel=100) persisted count = %d after repair + commit, want 0 (orphaned flushing-parked incoming key)", got)
	}
}

// ─── helpers ────────────────────────────────────────────────────────────────

func containsRelID(rids []types.RelID, want types.RelID) bool {
	for _, r := range rids {
		if r == want {
			return true
		}
	}
	return false
}

// landParkedFlushingSets simulates the in-flight flush commit landing: it writes
// the SET ops currently parked in `flushing` straight into Badger and clears the
// snapshot, exactly as a successful flush of that snapshot would. The
// purge-queued DELETEs remain in `pending` for the next Flush().
func landParkedFlushingSets(t *testing.T, bs *Store) {
	t.Helper()
	bs.wbMu.Lock()
	parked := bs.flushing
	bs.flushing = nil
	bs.wbMu.Unlock()
	if len(parked) == 0 {
		t.Fatal("landParkedFlushingSets: nothing parked in flushing")
	}
	if err := bs.db.Update(func(txn *badgerv4.Txn) error {
		for _, op := range parked {
			if op.opType != writeOpSet {
				continue
			}
			if err := txn.Set(op.key, op.value); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("land parked flushing sets into badger: %v", err)
	}
}

func countPersistedIncomingKeys(t *testing.T, bs *Store, endNodeID, relID snowflake.ID) int {
	t.Helper()
	prefix := make([]byte, 1+8)
	prefix[0] = storepkg.KeyIn
	storepkg.PutUint64(prefix, 1, int64(endNodeID))
	n := 0
	if err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) == storepkg.SizeAdjacency && storepkg.ParseRelIDFromAdjKey(key) == relID {
				n++
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("scan persisted incoming keys: %v", err)
	}
	return n
}
