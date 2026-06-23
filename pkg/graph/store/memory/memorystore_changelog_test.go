package memory

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Compile-time proof that the memory store satisfies the optional capability.
var _ storecontract.ChangeFeedCapability = (*Store)(nil)

func memNode(id int64, primary uint16) *types.Node {
	return types.NewNode(types.NodeID(snowflake.ID(id)), primary, nil)
}

func memTags(recs []storecontract.ChangeRecord) []storecontract.ChangeTag {
	out := make([]storecontract.ChangeTag, len(recs))
	for i, r := range recs {
		out[i] = r.Tag
	}
	return out
}

func TestMemoryChangeLog_DisabledByDefault(t *testing.T) {
	ms := New() // no WithChangeLog
	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	lsn, err := ms.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}
	if lsn != 0 {
		t.Fatalf("LastCommittedLSN = %d, want 0 (log disabled)", lsn)
	}
	recs, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("ChangeFeed = %d records, want 0 (log disabled)", len(recs))
	}
}

func TestMemoryChangeLog_BasicFeed(t *testing.T) {
	ms := New(WithChangeLog())
	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode 1: %v", err)
	}
	if err := ms.PutNode(memNode(2, 10)); err != nil {
		t.Fatalf("PutNode 2: %v", err)
	}
	if err := ms.PutRelationship(types.NewRelationship(types.RelID(100), 5, types.NodeID(1), types.NodeID(2))); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	if err := ms.DeleteRelationship(types.RelID(100)); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	recs, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	want := []storecontract.ChangeTag{
		storecontract.ChangeNodePut, storecontract.ChangeNodePut,
		storecontract.ChangeRelPut, storecontract.ChangeRelDelete,
	}
	got := memTags(recs)
	if len(got) != len(want) {
		t.Fatalf("tags = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("tags = %v, want %v", got, want)
		}
		if recs[i].LSN != uint64(i+1) {
			t.Fatalf("record[%d] LSN = %d, want %d", i, recs[i].LSN, i+1)
		}
	}
	// The first record decodes to node 1.
	nw, err := storepkg.DecodeNodePut(recs[0].Payload)
	if err != nil {
		t.Fatalf("DecodeNodePut: %v", err)
	}
	if nw.Wire.ID != 1 {
		t.Fatalf("node put wire id = %d, want 1", nw.Wire.ID)
	}
}

func TestMemoryChangeLog_ForEachAndNilCallback(t *testing.T) {
	ms := New(WithChangeLog())
	for i := int64(1); i <= 4; i++ {
		if err := ms.PutNode(memNode(i, 10)); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}
	var seen []uint64
	if err := ms.ForEachChange(1, func(r storecontract.ChangeRecord) bool {
		seen = append(seen, r.LSN)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if len(seen) != 3 || seen[0] != 2 {
		t.Fatalf("ForEachChange(after=1) = %v, want [2 3 4]", seen)
	}
	if err := ms.ForEachChange(0, nil); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ForEachChange(nil) = %v, want ErrInvalidStoreMutation", err)
	}
}

func memFeedSince(t *testing.T, ms *Store, after uint64) []storecontract.ChangeRecord {
	t.Helper()
	recs, err := ms.ChangeFeed(after, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	return recs
}

// Direct memory-side assertions for the emission doors NOT covered by the basic
// feed test or the (store_test-package) cross-backend parity test, so every
// memory door's record is pinned (Testing Rules 1 & 2).
func TestMemoryChangeLog_AllDoorsEmit(t *testing.T) {
	ms := New(WithChangeLog())
	mk := func(id int64) { _ = ms.PutNode(memNode(id, 10)) }
	mk(1)
	mk(2)
	mk(3)
	mk(4)
	rel := func(id, s, e int64) *types.Relationship {
		r := types.NewRelationship(types.RelID(snowflake.ID(id)), 5, types.NodeID(snowflake.ID(s)), types.NodeID(snowflake.ID(e)))
		if err := ms.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship(%d): %v", id, err)
		}
		return r
	}
	r := rel(100, 1, 2)

	check := func(name string, after uint64, want storecontract.ChangeTag) storecontract.ChangeRecord {
		recs := memFeedSince(t, ms, after)
		if len(recs) != 1 || recs[0].Tag != want {
			t.Fatalf("%s: tags = %v, want [%v]", name, memTags(recs), want)
		}
		return recs[0]
	}

	// ReplaceRelationship → RelPut.
	at, _ := ms.LastCommittedLSN()
	if err := ms.ReplaceRelationship(r.DeepCopy()); err != nil {
		t.Fatalf("ReplaceRelationship: %v", err)
	}
	check("ReplaceRelationship", at, storecontract.ChangeRelPut)

	// ReplaceRelWithHistory → RelPut (new current).
	cur, _ := ms.GetRelationship(types.RelID(100))
	prev := cur.DeepCopy()
	_ = cur.SetProperty("w", int64(7))
	cur.SetVersion(1)
	at, _ = ms.LastCommittedLSN()
	if err := ms.ReplaceRelWithHistory(cur, prev.Version(), prev); err != nil {
		t.Fatalf("ReplaceRelWithHistory: %v", err)
	}
	check("ReplaceRelWithHistory", at, storecontract.ChangeRelPut)

	// ReplaceNode → NodePut.
	n1, _ := ms.GetNode(types.NodeID(1))
	at, _ = ms.LastCommittedLSN()
	if err := ms.ReplaceNode(n1.DeepCopy()); err != nil {
		t.Fatalf("ReplaceNode: %v", err)
	}
	check("ReplaceNode", at, storecontract.ChangeNodePut)

	// RemoveNodeLabelToken (after adding one) → NodePut.
	n2, _ := ms.GetNode(types.NodeID(2))
	withLabel := n2.DeepCopy()
	withLabel.AddLabelTokenRaw(20)
	if err := ms.AddNodeLabelToken(types.NodeID(2), 20, withLabel); err != nil {
		t.Fatalf("AddNodeLabelToken: %v", err)
	}
	cur2, _ := ms.GetNode(types.NodeID(2))
	rem := cur2.DeepCopy()
	rem.RemoveLabelTokenRaw(20)
	at, _ = ms.LastCommittedLSN()
	if err := ms.RemoveNodeLabelToken(types.NodeID(2), 20, rem); err != nil {
		t.Fatalf("RemoveNodeLabelToken: %v", err)
	}
	check("RemoveNodeLabelToken", at, storecontract.ChangeNodePut)

	// PutRelVersion → RelHistoryVersion.
	rv := types.NewRelationship(types.RelID(100), 5, types.NodeID(1), types.NodeID(2))
	rv.SetVersion(9)
	at, _ = ms.LastCommittedLSN()
	if err := ms.PutRelVersion(types.RelID(100), 9, rv); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}
	rec := check("PutRelVersion", at, storecontract.ChangeRelHistoryVersion)
	if hb, _ := storepkg.DecodeHistoryVersionRel(rec.Payload); hb.Version != 9 {
		t.Fatalf("rel history version = %d, want 9", hb.Version)
	}

	// TruncateRelHistory → RelHistoryTruncate (seed extra versions first).
	for v := uint32(0); v < 3; v++ {
		rr := types.NewRelationship(types.RelID(100), 5, types.NodeID(1), types.NodeID(2))
		rr.SetVersion(v)
		_ = ms.PutRelVersion(types.RelID(100), v, rr)
	}
	at, _ = ms.LastCommittedLSN()
	if err := ms.TruncateRelHistory(types.RelID(100), 1); err != nil {
		t.Fatalf("TruncateRelHistory: %v", err)
	}
	rec = check("TruncateRelHistory", at, storecontract.ChangeRelHistoryTruncate)
	if tb, _ := storepkg.DecodeHistoryTruncate(rec.Payload); tb.IsTrim || tb.Bound != 1 {
		t.Fatalf("truncate body = %+v, want {IsTrim:false Bound:1}", tb)
	}

	// TrimNodeHistoryFrom → NodeHistoryTruncate (IsTrim).
	for v := uint32(0); v < 3; v++ {
		nn := memNode(3, 10)
		nn.SetVersion(v)
		_ = ms.PutNodeVersion(types.NodeID(3), v, nn)
	}
	at, _ = ms.LastCommittedLSN()
	if err := ms.TrimNodeHistoryFrom(types.NodeID(3), 1); err != nil {
		t.Fatalf("TrimNodeHistoryFrom: %v", err)
	}
	rec = check("TrimNodeHistoryFrom", at, storecontract.ChangeNodeHistoryTruncate)
	if tb, _ := storepkg.DecodeHistoryTruncate(rec.Payload); !tb.IsTrim || tb.Bound != 1 {
		t.Fatalf("trim body = %+v, want {IsTrim:true Bound:1}", tb)
	}

	// DeleteRelWithHistory → RelDelete (with-history).
	live, _ := ms.GetRelationship(types.RelID(100))
	at, _ = ms.LastCommittedLSN()
	if err := ms.DeleteRelWithHistory(types.RelID(100), live.Version(), live.DeepCopy()); err != nil {
		t.Fatalf("DeleteRelWithHistory: %v", err)
	}
	rec = check("DeleteRelWithHistory", at, storecontract.ChangeRelDelete)
	if rb, _ := storepkg.DecodeRelDelete(rec.Payload); !rb.WithHistory {
		t.Fatalf("rel delete WithHistory = false, want true")
	}

	// DeleteNodeWithHistory → NodeDelete (with-history): node 4 + its rel 200.
	r200 := rel(200, 4, 1)
	node4, _ := ms.GetNode(types.NodeID(4))
	at, _ = ms.LastCommittedLSN()
	if err := ms.DeleteNodeWithHistory(types.NodeID(4), node4.Version(), node4.DeepCopy(), []RelTombstone{{
		ID:          r200.ID(),
		PrevVersion: r200.Version(),
		Tombstone:   r200.DeepCopy(),
	}}); err != nil {
		t.Fatalf("DeleteNodeWithHistory: %v", err)
	}
	rec = check("DeleteNodeWithHistory", at, storecontract.ChangeNodeDelete)
	if nb, _ := storepkg.DecodeNodeDelete(rec.Payload); !nb.WithHistory || len(nb.RelTombstones) != 1 {
		t.Fatalf("node delete body = %+v, want WithHistory + 1 rel tombstone", nb)
	}

	// DeleteNode (hard, unconnected) → NodeDelete (no history).
	at, _ = ms.LastCommittedLSN()
	if err := ms.DeleteNode(types.NodeID(3)); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	rec = check("DeleteNode", at, storecontract.ChangeNodeDelete)
	if nb, _ := storepkg.DecodeNodeDelete(rec.Payload); nb.WithHistory || len(nb.CascadedRelIDs) != 0 {
		t.Fatalf("hard delete body = %+v, want {WithHistory:false no-cascade}", nb)
	}
}

func TestMemoryChangeLog_ClearReAnchors(t *testing.T) {
	ms := New(WithChangeLog())
	_ = ms.PutNode(memNode(1, 10))
	_ = ms.PutNode(memNode(2, 10))
	before, _ := ms.LastCommittedLSN()

	if err := ms.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	recs, _ := ms.ChangeFeed(0, 0)
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeClear {
		t.Fatalf("post-Clear feed = %v, want [Clear]", memTags(recs))
	}
	if recs[0].LSN <= before {
		t.Fatalf("Clear marker LSN %d <= pre-Clear %d (must stay monotonic)", recs[0].LSN, before)
	}
}
