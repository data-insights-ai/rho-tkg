package sharded

import (
	"cmp"
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Direct coverage for the mandatory-Store doors the sharded store implements
// as thin per-shard delegations. Correctness of the underlying per-shard
// behaviour is owned by the backend batteries; here we assert the sharded
// routing/fold wrapper returns the exact rows across a multi-slot spread.

func TestBulkReadFolds(t *testing.T) {
	t.Parallel()
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

	// IncomingRelationshipsForNodes groups each node's incoming rels.
	m, err := st.IncomingRelationshipsForNodes([]types.NodeID{a, b}, 0)
	if err != nil {
		t.Fatalf("IncomingRelationshipsForNodes: %v", err)
	}
	assertRelSet(t, m[a], r2)
	assertRelSet(t, m[b], r1)
}

func TestReplaceAndLabelTokenDoors(t *testing.T) {
	t.Parallel()
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
	gotR, err := st.GetRelationship(rid)
	if err != nil {
		t.Fatalf("GetRelationship after ReplaceRelationship: %v", err)
	}
	if s, _ := gotR.PropertiesMap()["k"].(string); s != "v" {
		t.Fatalf("ReplaceRelationship did not persist: k = %q, want v", s)
	}
	if err := st.DeleteRelationship(rid); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	if _, err := st.GetRelationship(rid); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationship after DeleteRelationship = %v, want ErrRelNotFound", err)
	}
}

func TestHistoryDoorsCrossSlot(t *testing.T) {
	t.Parallel()
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
	nodeHist := func(when string) []uint32 {
		t.Helper()
		hist, err := st.GetNodeHistory(nid)
		if err != nil {
			t.Fatalf("GetNodeHistory %s: %v", when, err)
		}
		vs := make([]uint32, len(hist))
		for i, h := range hist {
			vs[i] = h.Version()
		}
		return vs
	}
	labelCount := func() int {
		t.Helper()
		n, err := st.NodeCountByLabel(20)
		if err != nil {
			t.Fatalf("NodeCountByLabel: %v", err)
		}
		return n
	}
	if got := nodeHist("after PutNodeVersion"); !slices.Equal(got, []uint32{0}) {
		t.Fatalf("history after PutNodeVersion = %v, want [0]", got)
	}

	// AddNodeLabelTokenWithHistory: snapshots prev, adds label on new version.
	prev, _ := st.GetNode(nid)
	updated := types.NewNode(nid, 10, []uint16{20})
	updated.SetVersion(1)
	if err := st.AddNodeLabelTokenWithHistory(nid, 20, updated, prev.Version(), prev); err != nil {
		t.Fatalf("AddNodeLabelTokenWithHistory: %v", err)
	}
	if n := labelCount(); n != 1 {
		t.Fatalf("after AddNodeLabelTokenWithHistory, NodeCountByLabel(20) = %d, want 1", n)
	}
	prev2, _ := st.GetNode(nid)
	if prev2.Version() != 1 || !prev2.HasLabelTokenRaw(20) {
		t.Fatalf("current after label add: version %d, label 20 %v; want 1, true", prev2.Version(), prev2.HasLabelTokenRaw(20))
	}
	removed := types.NewNode(nid, 10, nil)
	removed.SetVersion(2)
	if err := st.RemoveNodeLabelTokenWithHistory(nid, 20, removed, prev2.Version(), prev2); err != nil {
		t.Fatalf("RemoveNodeLabelTokenWithHistory: %v", err)
	}
	if n := labelCount(); n != 0 {
		t.Fatalf("after RemoveNodeLabelTokenWithHistory, NodeCountByLabel(20) = %d, want 0", n)
	}
	if got := nodeHist("after the label doors"); !slices.Equal(got, []uint32{0, 1}) {
		t.Fatalf("history after the label doors = %v, want [0 1]", got)
	}
	if h1, err := st.GetNodeVersion(nid, 1); err != nil || !h1.HasLabelTokenRaw(20) {
		t.Fatalf("history version 1 must carry label 20 (the archived prev): %v", err)
	}

	// TruncateNodeHistory keeps the newest N history rows.
	if err := st.TruncateNodeHistory(nid, 1); err != nil {
		t.Fatalf("TruncateNodeHistory: %v", err)
	}
	if got := nodeHist("after TruncateNodeHistory"); !slices.Equal(got, []uint32{1}) {
		t.Fatalf("history after TruncateNodeHistory(1) = %v, want [1]", got)
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
	rv1 := types.NewRelationship(rid, 5, a, b)
	rv1.SetVersion(1)
	if err := st.PutRelVersion(rid, 1, rv1); err != nil {
		t.Fatalf("PutRelVersion(1): %v", err)
	}
	relHist := func(when string) []uint32 {
		t.Helper()
		hist, err := st.GetRelHistory(rid)
		if err != nil {
			t.Fatalf("GetRelHistory %s: %v", when, err)
		}
		vs := make([]uint32, len(hist))
		for i, h := range hist {
			vs[i] = h.Version()
		}
		return vs
	}
	if got := relHist("after PutRelVersion"); !slices.Equal(got, []uint32{0, 1}) {
		t.Fatalf("rel history after PutRelVersion = %v, want [0 1]", got)
	}
	if h0, err := st.GetRelVersion(rid, 0); err != nil || h0.PropertiesMap()["v"] != "0" {
		t.Fatalf("GetRelVersion(0) = %v, %v; want the row with v=0", h0, err)
	}
	if err := st.TruncateRelHistory(rid, 1); err != nil {
		t.Fatalf("TruncateRelHistory: %v", err)
	}
	if got := relHist("after TruncateRelHistory"); !slices.Equal(got, []uint32{1}) {
		t.Fatalf("rel history after TruncateRelHistory(1) = %v, want [1]", got)
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
	t.Parallel()
	st := newMemStore(t, 0, 4)
	var want []types.NodeID
	for slot := uint8(0); slot < 4; slot++ {
		id := mkNodeID(slot, 1)
		putNode(t, st, id, 10)
		want = append(want, id)
	}
	a, b := want[0], want[1]
	rA, rB := mkRelID(2, 1), mkRelID(0, 2)
	putRel(t, st, rA, 5, a, b)
	putRel(t, st, rB, 5, b, a)

	// AllNodeIDs / AllRelIDs folds.
	ids, err := st.AllNodeIDs(QueryOpts{})
	if err != nil {
		t.Fatalf("AllNodeIDs: %v", err)
	}
	if !sameSet(ids, want) {
		t.Fatalf("AllNodeIDs = %v, want the set %v", ids, want)
	}
	rids, err := st.AllRelIDs(QueryOpts{})
	if err != nil {
		t.Fatalf("AllRelIDs: %v", err)
	}
	if !sameSet(rids, []types.RelID{rA, rB}) {
		t.Fatalf("AllRelIDs = %v, want the set [%d %d]", rids, rA, rB)
	}

	// History-ID folds: two nodes and two rels with history, each pair on two
	// slots, the other rows without history.
	histNodes := []types.NodeID{want[1], want[3]}
	for _, nid := range histNodes {
		prev, _ := st.GetNode(nid)
		cur := types.NewNode(nid, 10, nil)
		cur.SetVersion(1)
		if err := st.ReplaceNodeWithHistory(cur, prev.Version(), prev); err != nil {
			t.Fatalf("ReplaceNodeWithHistory(%d): %v", nid, err)
		}
	}
	histRels := []types.RelID{rA, rB}
	slices.Sort(histRels) // the cursor doors answer in ascending ID order
	for _, rid := range histRels {
		prev, _ := st.GetRelationship(rid)
		cur := types.NewRelationship(rid, 5, prev.StartNodeID(), prev.EndNodeID())
		cur.SetVersion(1)
		if err := st.ReplaceRelWithHistory(cur, prev.Version(), prev); err != nil {
			t.Fatalf("ReplaceRelWithHistory(%d): %v", rid, err)
		}
	}
	slices.Sort(histNodes)

	nh, err := st.AllNodeHistoryIDs()
	if err != nil {
		t.Fatalf("AllNodeHistoryIDs: %v", err)
	}
	if !sameSet(nh, histNodes) {
		t.Fatalf("AllNodeHistoryIDs = %v, want the set %v", nh, histNodes)
	}
	rh, err := st.AllRelHistoryIDs()
	if err != nil {
		t.Fatalf("AllRelHistoryIDs: %v", err)
	}
	if !sameSet(rh, histRels) {
		t.Fatalf("AllRelHistoryIDs = %v, want the set %v", rh, histRels)
	}

	// The cursor doors: ascending, exclusive after, limit honoured.
	for _, tc := range []struct {
		after types.NodeID
		limit int
		want  []types.NodeID
	}{
		{0, 10, histNodes},
		{0, 1, histNodes[:1]},
		{histNodes[0], 10, histNodes[1:]},
		{histNodes[1], 10, nil},
	} {
		got, err := st.AllNodeHistoryIDsFrom(tc.after, tc.limit)
		if err != nil {
			t.Fatalf("AllNodeHistoryIDsFrom(%d, %d): %v", tc.after, tc.limit, err)
		}
		if !slices.Equal(got, tc.want) {
			t.Fatalf("AllNodeHistoryIDsFrom(%d, %d) = %v, want %v", tc.after, tc.limit, got, tc.want)
		}
	}
	for _, tc := range []struct {
		after types.RelID
		limit int
		want  []types.RelID
	}{
		{0, 10, histRels},
		{0, 1, histRels[:1]},
		{histRels[0], 10, histRels[1:]},
		{histRels[1], 10, nil},
	} {
		got, err := st.AllRelHistoryIDsFrom(tc.after, tc.limit)
		if err != nil {
			t.Fatalf("AllRelHistoryIDsFrom(%d, %d): %v", tc.after, tc.limit, err)
		}
		if !slices.Equal(got, tc.want) {
			t.Fatalf("AllRelHistoryIDsFrom(%d, %d) = %v, want %v", tc.after, tc.limit, got, tc.want)
		}
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
	if rCount != 2 {
		t.Fatalf("ForEachRelID visited %d, want 2", rCount)
	}

	// Flush folds over every shard; in memory each shard's Flush is a no-op, so
	// this only proves the fold returns no error (the disk path is exercised by
	// TestShardedClearCrashAtEveryStage).
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

// sameSet reports whether got and want hold the same IDs, ignoring order.
func sameSet[T cmp.Ordered](got, want []T) bool {
	g, w := slices.Clone(got), slices.Clone(want)
	slices.Sort(g)
	slices.Sort(w)
	return slices.Equal(g, w)
}
