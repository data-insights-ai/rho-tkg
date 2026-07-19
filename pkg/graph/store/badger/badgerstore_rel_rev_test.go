package badger

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 18b: Node.ReplaceNode guards its prefetch-then-lock reuse fast path
// with nodeRevs — a counter bumped on EVERY node data write, so ANY concurrent
// mutation between the unlocked prefetch and the locked re-check is detected
// and the door falls back to a fresh locked read. Relationship.ReplaceRelationship
// had no equivalent: its fast-path guard (relDeleteInfoStillIndexedLocked) only
// checks STRUCTURAL identity (relIDs/typeIdx/adjacency membership), which is
// blind to a concurrent property-only update — a rel's type and endpoints are
// immutable, so a property change leaves every one of those membership sets
// completely unchanged. A concurrent writer changing a property in the window
// between ReplaceRelationship's unlocked prefetch and its idxMu.Lock() could
// have its change silently overwritten in the rel property index: the door
// would "remove" the STALE prefetched property value (which by then wasn't
// even the indexed one) and "add" its own new value, leaving the concurrent
// writer's real value orphaned in the index forever (a phantom entry) while
// never actually being removed.
//
// The fix adds relRevs/nextRelRev (mirroring nodeRevs/nextNodeRev) and a
// relPrefetchSnapshot carrying the rev observed at prefetch time;
// currentRelForPrefetchLocked now requires the rev to still match under the
// lock before reusing the prefetched row, exactly like nodeDeleteInfoStillCurrentLocked.

func TestCurrentRelForPrefetchLocked_StaleRevForcesFreshRead(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	rid := types.RelID(snowflake.ID(100))

	r := types.NewRelationship(rid, 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := r.SetProperty("status", "A"); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	// Snapshot BEFORE the concurrent write — mirrors what ReplaceRelationship's
	// unlocked prefetch captures.
	prefetched, err := bs.prefetchRelWithRev(rid)
	if err != nil {
		t.Fatalf("prefetchRelWithRev: %v", err)
	}
	if got, _ := prefetched.rel.GetProperty("status"); got != "A" {
		t.Fatalf("prefetch snapshot status = %v, want A", got)
	}

	// A concurrent write lands BEFORE the racing door acquires idxMu.Lock().
	r2 := r.DeepCopy()
	if err := r2.SetProperty("status", "B"); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := bs.ReplaceRelationship(r2); err != nil {
		t.Fatalf("concurrent ReplaceRelationship: %v", err)
	}

	// currentRelForPrefetchLocked must detect the stale rev and re-read fresh —
	// never reuse the pre-write snapshot.
	bs.idxMu.Lock()
	current, err := bs.currentRelForPrefetchLocked(rid, prefetched)
	bs.idxMu.Unlock()
	if err != nil {
		t.Fatalf("currentRelForPrefetchLocked: %v", err)
	}
	if got, _ := current.GetProperty("status"); got != "B" {
		t.Fatalf("currentRelForPrefetchLocked returned status = %v, want B (fresh read) — BACKLOG 18b regression: reused a stale prefetch across a concurrent write", got)
	}
}

func TestCurrentRelForPrefetchLocked_UnchangedRevReusesPrefetch(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	rid := types.RelID(snowflake.ID(100))

	r := types.NewRelationship(rid, 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	prefetched, err := bs.prefetchRelWithRev(rid)
	if err != nil {
		t.Fatalf("prefetchRelWithRev: %v", err)
	}

	// No concurrent write — the fast path must still reuse the prefetched row
	// (the whole point of prefetch-then-lock: avoid a second Badger read).
	bs.idxMu.Lock()
	current, err := bs.currentRelForPrefetchLocked(rid, prefetched)
	bs.idxMu.Unlock()
	if err != nil {
		t.Fatalf("currentRelForPrefetchLocked: %v", err)
	}
	if current != prefetched.rel {
		t.Fatal("currentRelForPrefetchLocked did not reuse the prefetched row when the rev was unchanged — fast path regressed")
	}
}

// TestReplaceRelationship_ConcurrentPropertyUpdateInPrefetchWindowIsNotStale is
// the end-to-end regression: it drives the actual race through ReplaceRelationship
// itself (not just the helper functions) using replaceRelPrefetchTestHook to
// deterministically land a concurrent property-changing ReplaceRelationship call
// inside the real prefetch->lock window, then asserts the rel property index
// reflects ONLY the true final state — no phantom stale entries.
func TestReplaceRelationship_ConcurrentPropertyUpdateInPrefetchWindowIsNotStale(t *testing.T) {
	bs := newTestBadgerStore(t)
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	rid := types.RelID(snowflake.ID(100))

	r := types.NewRelationship(rid, 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := r.SetProperty("status", "A"); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	if err := bs.CreateRelPropertyIndex(5, "status"); err != nil {
		t.Fatalf("CreateRelPropertyIndex: %v", err)
	}

	bs.replaceRelPrefetchTestHook = func() {
		bs.replaceRelPrefetchTestHook = nil // one-shot — the concurrent call below must not re-trigger it
		concurrent := types.NewRelationship(rid, 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
		if err := concurrent.SetProperty("status", "B"); err != nil {
			t.Errorf("SetProperty (concurrent): %v", err)
			return
		}
		if err := bs.ReplaceRelationship(concurrent); err != nil {
			t.Errorf("concurrent ReplaceRelationship: %v", err)
		}
	}

	final := types.NewRelationship(rid, 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := final.SetProperty("status", "C"); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := bs.ReplaceRelationship(final); err != nil {
		t.Fatalf("ReplaceRelationship: %v", err)
	}

	if got, err := bs.RelationshipsByTypeAndProperty(5, "status", "A", QueryOpts{}); err != nil {
		t.Fatalf("RelationshipsByTypeAndProperty(A): %v", err)
	} else if len(got) != 0 {
		t.Fatalf("phantom stale index entry: status=A still indexed after both replaces, got %d rows", len(got))
	}
	if got, err := bs.RelationshipsByTypeAndProperty(5, "status", "B", QueryOpts{}); err != nil {
		t.Fatalf("RelationshipsByTypeAndProperty(B): %v", err)
	} else if len(got) != 0 {
		t.Fatalf("phantom stale index entry: status=B still indexed after final replace to C, got %d rows — BACKLOG 18b regression", len(got))
	}
	got, err := bs.RelationshipsByTypeAndProperty(5, "status", "C", QueryOpts{})
	if err != nil {
		t.Fatalf("RelationshipsByTypeAndProperty(C): %v", err)
	}
	if len(got) != 1 || got[0].ID() != rid {
		t.Fatalf("status=C index = %v, want exactly rel %v", got, rid)
	}

	current, err := bs.GetRelationship(rid)
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if v, _ := current.GetProperty("status"); v != "C" {
		t.Fatalf("final relationship status = %v, want C", v)
	}
}
