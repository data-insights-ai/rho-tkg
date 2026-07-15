package sharded

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Contract-exercise battery: direct coverage for the mandatory-Store doors S1
// implemented as thin per-shard delegations but did not test directly (Testing
// Rule 1 — every public method gets a direct test). Correctness of the underlying
// per-shard behaviour is owned by the badger batteries; here we assert the
// SHARDED routing/fold wrapper round-trips across a multi-slot spread.

func TestBulkReadFolds(t *testing.T) {
	st := newMemStore(t, 0, 4)
	var nodeIDs []types.NodeID
	for slot := uint8(0); slot < 4; slot++ {
		id := mkNodeID(slot, 1)
		putNode(t, st, id, 10)
		nodeIDs = append(nodeIDs, id)
	}
	a, b := nodeIDs[0], nodeIDs[1]
	r1 := mkRelID(2, 1)
	r2 := mkRelID(3, 1)
	putRel(t, st, r1, 5, a, b)
	putRel(t, st, r2, 5, b, a)

	// AllNodes / AllRelationships fold every shard.
	all, err := st.AllNodes(QueryOpts{})
	if err != nil {
		t.Fatalf("AllNodes: %v", err)
	}
	assertNodeSet(t, all, nodeIDs...)
	mustSorted(t, all)

	allR, err := st.AllRelationships(QueryOpts{})
	if err != nil {
		t.Fatalf("AllRelationships: %v", err)
	}
	assertRelSet(t, allR, r1, r2)

	// GetNodesByIDs / GetRelationshipsByIDs route each ID to its shard.
	gotN, err := st.GetNodesByIDs(nodeIDs)
	if err != nil {
		t.Fatalf("GetNodesByIDs: %v", err)
	}
	assertNodeSet(t, gotN, nodeIDs...)
	gotR, err := st.GetRelationshipsByIDs([]types.RelID{r1, r2})
	if err != nil {
		t.Fatalf("GetRelationshipsByIDs: %v", err)
	}
	assertRelSet(t, gotR, r1, r2)

	// IncomingRelationshipsForNodes (the rel.go door left uncovered by S1).
	m, err := st.IncomingRelationshipsForNodes([]types.NodeID{a, b}, 0)
	if err != nil {
		t.Fatalf("IncomingRelationshipsForNodes: %v", err)
	}
	assertRelSet(t, m[a], r2)
	assertRelSet(t, m[b], r1)
}

func TestReplaceAndLabelTokenDoors(t *testing.T) {
	st := newMemStore(t, 0, 4)
	nid := mkNodeID(2, 1)
	putNode(t, st, nid, 10)

	// ReplaceNode (no history): overwrite data in place.
	repl := types.NewNode(nid, 10, nil)
	if err := repl.SetProperty("k", "v"); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := st.ReplaceNode(repl); err != nil {
		t.Fatalf("ReplaceNode: %v", err)
	}
	got, _ := st.GetNode(nid)
	if s, _ := got.PropertiesMap()["k"].(string); s != "v" {
		t.Fatalf("ReplaceNode did not persist: %q", s)
	}

	// AddNodeLabelToken then RemoveNodeLabelToken (no history).
	withLabel := types.NewNode(nid, 10, []uint16{20})
	if err := st.AddNodeLabelToken(nid, 20, withLabel); err != nil {
		t.Fatalf("AddNodeLabelToken: %v", err)
	}
	if n, _ := st.NodeCountByLabel(20); n != 1 {
		t.Fatalf("after AddNodeLabelToken, NodeCountByLabel(20) = %d, want 1", n)
	}
	withoutLabel := types.NewNode(nid, 10, nil)
	if err := st.RemoveNodeLabelToken(nid, 20, withoutLabel); err != nil {
		t.Fatalf("RemoveNodeLabelToken: %v", err)
	}
	if n, _ := st.NodeCountByLabel(20); n != 0 {
		t.Fatalf("after RemoveNodeLabelToken, NodeCountByLabel(20) = %d, want 0", n)
	}

	// ReplaceRelationship (no history) + DeleteRelationship.
	a := mkNodeID(0, 1)
	b := mkNodeID(1, 1)
	putNode(t, st, a, 10)
	putNode(t, st, b, 10)
	rid := mkRelID(3, 1)
	putRel(t, st, rid, 5, a, b)
	rr := types.NewRelationship(rid, 5, a, b)
	if err := rr.SetProperty("k", "v"); err != nil {
		t.Fatalf("rel SetProperty: %v", err)
	}
	if err := st.ReplaceRelationship(rr); err != nil {
		t.Fatalf("ReplaceRelationship: %v", err)
	}
	if err := st.DeleteRelationship(rid); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
}

func TestHistoryDoorsCrossSlot(t *testing.T) {
	st := newMemStore(t, 0, 4)

	// PutNodeVersion writes a history-only row; then label-with-history doors.
	nid := mkNodeID(1, 1)
	putNode(t, st, nid, 10)
	v0 := types.NewNode(nid, 10, nil)
	if err := v0.SetProperty("v", "0"); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := st.PutNodeVersion(nid, 0, v0); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}
	if hist, err := st.GetNodeHistory(nid); err != nil || len(hist) != 1 {
		t.Fatalf("GetNodeHistory after PutNodeVersion: %v (n=%d)", err, len(hist))
	}

	// AddNodeLabelTokenWithHistory: snapshots prev, adds label on new version.
	prev, _ := st.GetNode(nid)
	updated := types.NewNode(nid, 10, []uint16{20})
	updated.SetVersion(1)
	if err := st.AddNodeLabelTokenWithHistory(nid, 20, updated, prev.Version(), prev); err != nil {
		t.Fatalf("AddNodeLabelTokenWithHistory: %v", err)
	}
	prev2, _ := st.GetNode(nid)
	removed := types.NewNode(nid, 10, nil)
	removed.SetVersion(2)
	if err := st.RemoveNodeLabelTokenWithHistory(nid, 20, removed, prev2.Version(), prev2); err != nil {
		t.Fatalf("RemoveNodeLabelTokenWithHistory: %v", err)
	}

	// TruncateNodeHistory keeps the newest N history rows.
	if err := st.TruncateNodeHistory(nid, 1); err != nil {
		t.Fatalf("TruncateNodeHistory: %v", err)
	}

	// Relationship history mirror.
	a := mkNodeID(0, 1)
	b := mkNodeID(2, 1)
	putNode(t, st, a, 10)
	putNode(t, st, b, 10)
	rid := mkRelID(3, 1)
	putRel(t, st, rid, 5, a, b)
	rv0 := types.NewRelationship(rid, 5, a, b)
	if err := rv0.SetProperty("v", "0"); err != nil {
		t.Fatalf("rel SetProperty: %v", err)
	}
	if err := st.PutRelVersion(rid, 0, rv0); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}
	if err := st.TruncateRelHistory(rid, 1); err != nil {
		t.Fatalf("TruncateRelHistory: %v", err)
	}

	// DeleteRelWithHistory archives the current row as its tombstone (the core
	// contract: tombstone == current row, prevVersion == current.Version()), then
	// removes the current row. Use a fresh rel so the tombstone's history key does
	// not collide with the PutRelVersion row above.
	rid2 := mkRelID(1, 7)
	putRel(t, st, rid2, 5, a, b)
	cur, _ := st.GetRelationship(rid2)
	if err := st.DeleteRelWithHistory(rid2, cur.Version(), cur); err != nil {
		t.Fatalf("DeleteRelWithHistory: %v", err)
	}
	if _, err := st.GetRelationship(rid2); err == nil {
		t.Fatalf("DeleteRelWithHistory: current rel row should be gone")
	}
}

func TestIterationAndFlushFolds(t *testing.T) {
	st := newMemStore(t, 0, 4)
	var want []types.NodeID
	for slot := uint8(0); slot < 4; slot++ {
		id := mkNodeID(slot, 1)
		putNode(t, st, id, 10)
		want = append(want, id)
	}
	a, b := want[0], want[1]
	putRel(t, st, mkRelID(2, 1), 5, a, b)

	// AllNodeIDs / AllRelIDs folds.
	if ids, err := st.AllNodeIDs(QueryOpts{}); err != nil || len(ids) != len(want) {
		t.Fatalf("AllNodeIDs: %v (n=%d, want %d)", err, len(ids), len(want))
	}
	if ids, err := st.AllRelIDs(QueryOpts{}); err != nil || len(ids) != 1 {
		t.Fatalf("AllRelIDs: %v (n=%d)", err, len(ids))
	}

	// History-ID folds (a node with history so the fold is non-empty).
	nid := want[3]
	prev, _ := st.GetNode(nid)
	cur := types.NewNode(nid, 10, nil)
	cur.SetVersion(1)
	if err := st.ReplaceNodeWithHistory(cur, prev.Version(), prev); err != nil {
		t.Fatalf("ReplaceNodeWithHistory: %v", err)
	}
	if ids, err := st.AllNodeHistoryIDs(); err != nil || len(ids) == 0 {
		t.Fatalf("AllNodeHistoryIDs: %v (n=%d)", err, len(ids))
	}
	if _, err := st.AllRelHistoryIDs(); err != nil {
		t.Fatalf("AllRelHistoryIDs: %v", err)
	}
	if _, err := st.AllNodeHistoryIDsFrom(types.NodeID(0), 10); err != nil {
		t.Fatalf("AllNodeHistoryIDsFrom: %v", err)
	}
	if _, err := st.AllRelHistoryIDsFrom(types.RelID(0), 10); err != nil {
		t.Fatalf("AllRelHistoryIDsFrom: %v", err)
	}

	// ForEach* iterate every shard sequentially.
	nCount := 0
	if err := st.ForEachNodeID(func(types.NodeID) bool { nCount++; return true }); err != nil {
		t.Fatalf("ForEachNodeID: %v", err)
	}
	if nCount != len(want) {
		t.Fatalf("ForEachNodeID visited %d, want %d", nCount, len(want))
	}
	rCount := 0
	if err := st.ForEachRelID(func(types.RelID) bool { rCount++; return true }); err != nil {
		t.Fatalf("ForEachRelID: %v", err)
	}
	if rCount != 1 {
		t.Fatalf("ForEachRelID visited %d, want 1", rCount)
	}

	// Flush folds over every shard (InMemory: a no-op that must not error).
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}
