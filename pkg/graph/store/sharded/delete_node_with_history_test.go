package sharded

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 20d: DeleteNodeWithHistory — arguably the most cross-shard-hazardous
// door in history.go, since it independently classifies every connected
// relationship tombstone into LOCAL (co-located with the node — deleted
// atomically alongside the node tombstone), REMOTE (a different local shard —
// deleted individually first via DeleteRelWithHistory), and FOREIGN-INCOMING
// STUB (a Model-A half-edge with no version chain, removed via
// DeleteRelationshipForeignIncoming before the node's own delete) — had ZERO
// direct test coverage, unlike every sibling mutation door
// (TestDeleteNodeCascadeCrossShard, TestDeleteNodesBatchCrossShard) which has
// a dedicated *CrossShard test.

// TestDeleteNodeWithHistoryCrossShard exercises all three tombstone
// categories in one call: a LOCAL rel (its own ID slot matches the node's
// shard), a REMOTE rel (its own ID slot is a DIFFERENT local shard), and
// verifies the node + both rels are gone with their history preserved, their
// neighbors survive, and the store is left fully consistent.
func TestDeleteNodeWithHistoryCrossShard(t *testing.T) {
	st := newMemStore(t, 0, 4)

	hub := mkNodeID(0, 1) // node's shard = 0
	n1 := mkNodeID(1, 1)
	n2 := mkNodeID(2, 1)
	putNode(t, st, hub, 10)
	putNode(t, st, n1, 10)
	putNode(t, st, n2, 10)

	// LOCAL: rel ID's own slot (0) matches hub's shard (0).
	localRel := putRel(t, st, mkRelID(0, 50), 5, hub, n1)
	// REMOTE: rel ID's own slot (2) is a DIFFERENT local shard than hub's (0),
	// even though one endpoint (hub) lives on shard 0.
	remoteRel := putRel(t, st, mkRelID(2, 50), 5, hub, n2)

	hubTombstone := types.NewNode(hub, 10, nil)
	localTombstone := localRel.DeepCopy()
	remoteTombstone := remoteRel.DeepCopy()

	err := st.DeleteNodeWithHistory(hub, 0, hubTombstone, []RelTombstone{
		{ID: localRel.ID(), PrevVersion: localRel.Version(), Tombstone: localTombstone},
		{ID: remoteRel.ID(), PrevVersion: remoteRel.Version(), Tombstone: remoteTombstone},
	})
	if err != nil {
		t.Fatalf("DeleteNodeWithHistory: %v", err)
	}

	// Hub is gone as a live row...
	if _, err := st.GetNode(hub); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode(hub) after delete: want ErrNodeNotFound, got %v", err)
	}
	// ...but its history is preserved (B32 — deleted-with-history keeps history queryable).
	hist, err := st.GetNodeHistory(hub)
	if err != nil {
		t.Fatalf("GetNodeHistory(hub): %v", err)
	}
	if len(hist) == 0 {
		t.Fatal("hub history is empty after DeleteNodeWithHistory — tombstone not recorded")
	}

	// Both rels gone as live rows, both with preserved history.
	for _, rid := range []types.RelID{localRel.ID(), remoteRel.ID()} {
		if _, err := st.GetRelationship(rid); !errors.Is(err, ErrRelNotFound) {
			t.Fatalf("GetRelationship(%d) after delete: want ErrRelNotFound, got %v", rid, err)
		}
		relHist, err := st.GetRelHistory(rid)
		if err != nil {
			t.Fatalf("GetRelHistory(%d): %v", rid, err)
		}
		if len(relHist) == 0 {
			t.Fatalf("rel %d history is empty after DeleteNodeWithHistory — tombstone not recorded", rid)
		}
	}

	// Neighbors survive with clean adjacency.
	for _, nb := range []types.NodeID{n1, n2} {
		if _, err := st.GetNode(nb); err != nil {
			t.Fatalf("neighbor %v must survive: %v", nb, err)
		}
		in, err := st.IncomingRelationships(nb, 0)
		if err != nil {
			t.Fatalf("IncomingRelationships(%v): %v", nb, err)
		}
		if len(in) != 0 {
			t.Fatalf("neighbor %v still has %d incoming rels after delete", nb, len(in))
		}
	}

	// No dangling cross-shard residue anywhere.
	rep, err := st.VerifyConsistency()
	if err != nil {
		t.Fatalf("VerifyConsistency: %v", err)
	}
	if !rep.OK() {
		t.Fatalf("post-delete store inconsistent: %+v", rep)
	}
}

// TestDeleteNodeWithHistoryForeignIncomingStub covers the third category: a
// Model-A foreign-incoming half-edge stub connected to the node being
// deleted. The stub has no version chain (nothing to tombstone) and must be
// removed via DeleteRelationshipForeignIncoming BEFORE the node's own
// with-history delete, so the node-shard's tombstone validation and cascade
// sweep see stub-free adjacency.
func TestDeleteNodeWithHistoryForeignIncomingStub(t *testing.T) {
	st := newMemStore(t, 0, 2)

	end := putNode(t, st, mkNodeID(0, 1), 10) // local, shard 0

	// Stub: rel ID and START node both on a foreign slot (9).
	stubRelID := mkRelID(9, 1)
	foreignStart := mkNodeID(9, 2)
	stub := types.NewRelationship(stubRelID, 5, foreignStart, end.ID())
	if err := st.RecordForeignIncoming(stub, generatedcreate.FreshGraphID); err != nil {
		t.Fatalf("RecordForeignIncoming: %v", err)
	}
	if in, err := st.IncomingRelationships(end.ID(), 0); err != nil || len(in) != 1 {
		t.Fatalf("test setup: IncomingRelationships(end) = (%d, %v), want (1, nil)", len(in), err)
	}

	endTombstone := types.NewNode(end.ID(), 10, nil)
	// The stub is passed as a RelTombstone with a FOREIGN rel-ID slot — the
	// door must detect this via shardIndexForRel returning ErrSlotNotLocal
	// and route it to DeleteRelationshipForeignIncoming instead of treating
	// it as local/remote.
	err := st.DeleteNodeWithHistory(end.ID(), 0, endTombstone, []RelTombstone{
		{ID: stubRelID, PrevVersion: 0, Tombstone: stub.DeepCopy()},
	})
	if err != nil {
		t.Fatalf("DeleteNodeWithHistory: %v", err)
	}

	if _, err := st.GetNode(end.ID()); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode(end) after delete: want ErrNodeNotFound, got %v", err)
	}
	// The stub's index entry lived on end's own shard, keyed by end's raw
	// snowflake ID — check it directly (a raw adjacency lookup, unlike
	// IncomingRelationships, does not require the node itself to still exist).
	if in := st.shards[0].IncomingRelIDs(end.ID().SnowflakeID(), 0); len(in) != 0 {
		t.Fatalf("stub still present after DeleteNodeWithHistory: %d incoming rel IDs", len(in))
	}
	rep, err := st.VerifyConsistency()
	if err != nil {
		t.Fatalf("VerifyConsistency: %v", err)
	}
	if !rep.OK() {
		t.Fatalf("post-delete store inconsistent: %+v", rep)
	}
}
