package badger

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 18g: relDeleteInfoStillIndexedLocked's fast-path staleness check
// only verifies relIDs/typeIdx membership plus (in RAM-adjacency mode)
// outIdx/inIdx membership. In AdjacencyIndexOnDisk mode there is no RAM
// adjacency mirror to check, so the function falls back to relIDs+typeIdx
// membership ALONE — which is blind to a delete-then-recreate-with-the-
// same-rel-ID-but-different-endpoints race (lesson 22's classic case,
// possible via any door that accepts a caller-supplied rel ID). A batch/
// cascade delete that reused a prefetched RelDeleteInfo across such a race
// would delete the (new) relationship row while cleaning up adjacency for
// the STALE (old) endpoints — orphaning the new relationship's real
// adjacency entries and leaving a phantom delete op for endpoints that no
// longer own that rel ID.
//
// Fix: prefetchRelDeleteInfo now also snapshots relRevs[rid] (mirroring
// prefetchRelWithRev/BACKLOG 18b). relRevs is bumped on every create AND
// deleted entirely on every delete for that ID, so ANY write to that
// specific rel ID between prefetch and the locked re-check — including a
// delete+recreate with a reused ID — changes it. All three call sites now
// AND relDeleteInfoRevCurrentLocked into the fast-path gate.
func TestRelDeleteInfoStillIndexedLocked_DiskMode_BlindToEndpointReuse(t *testing.T) {
	bs, err := New(Config{InMemory: true, AdjacencyIndexOnDisk: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestNode(t, bs, 3, 10, nil)
	putTestNode(t, bs, 4, 10, nil)

	rid := types.RelID(snowflake.ID(100))
	orig := types.NewRelationship(rid, 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := bs.PutRelationship(orig); err != nil {
		t.Fatalf("PutRelationship(orig): %v", err)
	}

	// Snapshot BEFORE the concurrent delete+recreate — mirrors what the
	// batch/cascade delete doors capture during their unlocked prefetch phase.
	prefetched, err := bs.prefetchRelDeleteInfo(rid)
	if err != nil {
		t.Fatalf("prefetchRelDeleteInfo: %v", err)
	}
	if prefetched.StartID != snowflake.ID(1) || prefetched.EndID != snowflake.ID(2) {
		t.Fatalf("prefetched endpoints = %d->%d, want 1->2", prefetched.StartID, prefetched.EndID)
	}
	if prefetched.Rev == 0 {
		t.Fatal("prefetchRelDeleteInfo did not capture a nonzero rev")
	}

	// Concurrent delete+recreate with the SAME rel ID but DIFFERENT endpoints
	// and the SAME type — lands in the prefetch->lock window.
	if err := bs.DeleteRelationship(rid); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	reused := types.NewRelationship(rid, 5, types.NodeID(snowflake.ID(3)), types.NodeID(snowflake.ID(4)))
	if err := bs.PutRelationship(reused); err != nil {
		t.Fatalf("PutRelationship(reused): %v", err)
	}

	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	// The OLD fast-path check alone (structural membership only) is blind to
	// this race in disk mode: relIDs[rid] exists (the reused rel) and
	// typeIdx[5] contains rid (same type reused too), so it reports the
	// stale prefetch as "still indexed" even though its endpoints (1->2)
	// no longer match the live row (3->4).
	if !bs.relDeleteInfoStillIndexedLocked(prefetched) {
		t.Fatal("test setup invalid: relDeleteInfoStillIndexedLocked unexpectedly detected staleness on its own — the disk-mode blind spot this test targets no longer exists structurally, update the test")
	}

	// The FIX: the rev gate must catch what structural membership alone
	// cannot, since the reused row's create bumped relRevs[rid] to a new
	// value after the delete cleared it.
	if bs.relDeleteInfoRevCurrentLocked(prefetched) {
		t.Fatal("relDeleteInfoRevCurrentLocked = true for a stale prefetch across a delete+recreate-with-reused-ID race — BACKLOG 18g regression: a caller combining this with relDeleteInfoStillIndexedLocked would corrupt disk-mode adjacency for the reused relationship")
	}
}

// TestRelDeleteInfoRevCurrentLocked_UnchangedRevReusesPrefetch is the
// non-regression counterpart: when nothing wrote to the rel ID between
// prefetch and lock, the rev gate must still pass so the fast path (the
// entire point of prefetching outside the lock) is preserved.
func TestRelDeleteInfoRevCurrentLocked_UnchangedRevReusesPrefetch(t *testing.T) {
	bs := newTestBadgerStore(t)
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	rid := types.RelID(snowflake.ID(100))

	r := types.NewRelationship(rid, 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	prefetched, err := bs.prefetchRelDeleteInfo(rid)
	if err != nil {
		t.Fatalf("prefetchRelDeleteInfo: %v", err)
	}

	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	if !bs.relDeleteInfoRevCurrentLocked(prefetched) {
		t.Fatal("relDeleteInfoRevCurrentLocked = false with no concurrent write — fast path regressed")
	}
	if !bs.relDeleteInfoStillIndexedLocked(prefetched) {
		t.Fatal("relDeleteInfoStillIndexedLocked = false with no concurrent write — fast path regressed")
	}
}

// TestDeleteRelationshipsBatch_DiskMode_ReusedIDDoesNotOrphanNewAdjacency is
// the end-to-end door-level regression: DeleteRelationshipsBatch must never
// use a stale prefetch to clean up adjacency for a relationship ID that was
// deleted and recreated (different endpoints) before the batch reached its
// locked phase. Since there is no test hook to land the race exactly inside
// the real unlocked prefetch window, this drives the sequential-simulation
// pattern already established by TestCurrentRelForPrefetchLocked_* — delete
// and recreate BEFORE calling the door, so the door's OWN prefetch (fresh)
// reflects the reused row, and separately proves via the manual prefetch
// above that a stale snapshot taken beforehand is correctly rejected by the
// fix rather than silently reused.
func TestDeleteRelationshipsBatch_DiskMode_ReusedIDDoesNotOrphanNewAdjacency(t *testing.T) {
	bs, err := New(Config{InMemory: true, AdjacencyIndexOnDisk: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestNode(t, bs, 3, 10, nil)
	putTestNode(t, bs, 4, 10, nil)

	rid := types.RelID(snowflake.ID(100))
	orig := types.NewRelationship(rid, 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := bs.PutRelationship(orig); err != nil {
		t.Fatalf("PutRelationship(orig): %v", err)
	}
	if err := bs.DeleteRelationship(rid); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	reused := types.NewRelationship(rid, 5, types.NodeID(snowflake.ID(3)), types.NodeID(snowflake.ID(4)))
	if err := bs.PutRelationship(reused); err != nil {
		t.Fatalf("PutRelationship(reused): %v", err)
	}

	if err := bs.DeleteRelationshipsBatch([]types.RelID{rid}); err != nil {
		t.Fatalf("DeleteRelationshipsBatch: %v", err)
	}

	out, err := bs.OutgoingRelationships(types.NodeID(snowflake.ID(3)), 0)
	if err != nil {
		t.Fatalf("OutgoingRelationships(3): %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("node 3 still has %d outgoing relationship(s) after deleting reused rel %v — adjacency not cleaned", len(out), rid)
	}
	in, err := bs.IncomingRelationships(types.NodeID(snowflake.ID(4)), 0)
	if err != nil {
		t.Fatalf("IncomingRelationships(4): %v", err)
	}
	if len(in) != 0 {
		t.Fatalf("node 4 still has %d incoming relationship(s) after deleting reused rel %v — adjacency not cleaned", len(in), rid)
	}
	if _, err := bs.GetRelationship(rid); err == nil {
		t.Fatal("GetRelationship succeeded after DeleteRelationshipsBatch — relationship row was not deleted")
	}
}
