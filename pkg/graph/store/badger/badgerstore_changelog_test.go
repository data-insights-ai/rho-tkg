package badger

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Compile-time proof that the badger store satisfies the optional capability.
var _ storecontract.ChangeFeedCapability = (*Store)(nil)

func newChangeLogStore(t *testing.T, sync bool) *Store {
	t.Helper()
	bs, err := New(Config{InMemory: true, ChangeLog: true, SyncWrites: sync})
	if err != nil {
		t.Fatalf("New(ChangeLog): %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	return bs
}

func drainFeed(t *testing.T, bs *Store) []storecontract.ChangeRecord {
	t.Helper()
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	recs, err := bs.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	return recs
}

func tagSeq(recs []storecontract.ChangeRecord) []storecontract.ChangeTag {
	out := make([]storecontract.ChangeTag, len(recs))
	for i, r := range recs {
		out[i] = r.Tag
	}
	return out
}

func assertLSNContiguous(t *testing.T, recs []storecontract.ChangeRecord) {
	t.Helper()
	for i, r := range recs {
		if r.LSN != uint64(i+1) {
			t.Fatalf("record[%d] LSN = %d, want %d (LSNs must be 1..N gap-free, ascending)", i, r.LSN, i+1)
		}
	}
}

// ─── Disabled by default (zero overhead) ────────────────────────────────────

func TestChangeLog_DisabledByDefault(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t) // ChangeLog not set
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestRel(t, bs, 100, 5, 1, 2)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	lsn, err := bs.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}
	if lsn != 0 {
		t.Fatalf("LastCommittedLSN = %d, want 0 (log disabled)", lsn)
	}
	recs, err := bs.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("ChangeFeed returned %d records, want 0 (log disabled)", len(recs))
	}
}

// ─── NodePut / RelPut parity (rules 1, 2) ───────────────────────────────────

func TestChangeLog_NodePutRelPutParity(t *testing.T) {
	t.Parallel()
	for _, sync := range []bool{true, false} {
		t.Run(fmt.Sprintf("sync=%v", sync), func(t *testing.T) {
			bs := newChangeLogStore(t, sync)
			putTestNode(t, bs, 1, 10, nil)
			putTestNode(t, bs, 2, 10, nil)
			putTestRel(t, bs, 100, 5, 1, 2)

			recs := drainFeed(t, bs)
			if got := tagSeq(recs); len(got) != 3 ||
				got[0] != storecontract.ChangeNodePut ||
				got[1] != storecontract.ChangeNodePut ||
				got[2] != storecontract.ChangeRelPut {
				t.Fatalf("tags = %v, want [NodePut NodePut RelPut]", got)
			}
			assertLSNContiguous(t, recs)

			// NodePut payload decodes to the right node wire.
			nw, err := storepkg.DecodeNodePut(recs[0].Payload)
			if err != nil {
				t.Fatalf("DecodeNodePut: %v", err)
			}
			if nw.Wire.ID != 1 || nw.Wire.PrimaryLabel != 10 || nw.WithHistory {
				t.Fatalf("node put = {id:%d pl:%d wh:%v}, want {1 10 false}", nw.Wire.ID, nw.Wire.PrimaryLabel, nw.WithHistory)
			}
			// RelPut payload decodes to the right rel wire.
			rw, err := storepkg.DecodeRelPut(recs[2].Payload)
			if err != nil {
				t.Fatalf("DecodeRelPut: %v", err)
			}
			if rw.Wire.ID != 100 || rw.Wire.StartID != 1 || rw.Wire.EndID != 2 || rw.Wire.RelType != 5 {
				t.Fatalf("rel wire = %+v, want {id:100 s:1 e:2 rt:5}", rw)
			}
		})
	}
}

// ─── Replace emits a fresh NodePut carrying the new state (rule 15) ──────────

func TestChangeLog_TwoPhase_PutThenReplaceRemembersBoth(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, true)

	putTestNode(t, bs, 1, 10, nil)
	current, _ := bs.GetNode(types.NodeID(1))
	prevState := current.DeepCopy()
	_ = current.SetProperty("x", int64(42))
	current.SetVersion(1)
	if err := bs.ReplaceNodeWithHistory(current, 0, prevState); err != nil {
		t.Fatalf("ReplaceNodeWithHistory: %v", err)
	}

	recs := drainFeed(t, bs)
	// Phase 1 (create v0) then phase 2 (replace -> v1): both present, in order.
	if got := tagSeq(recs); len(got) != 2 || got[0] != storecontract.ChangeNodePut || got[1] != storecontract.ChangeNodePut {
		t.Fatalf("tags = %v, want [NodePut NodePut]", got)
	}
	v0, _ := storepkg.DecodeNodePut(recs[0].Payload)
	v1, _ := storepkg.DecodeNodePut(recs[1].Payload)
	if v0.Wire.Version != 0 || v0.WithHistory {
		t.Fatalf("record[0] = {v:%d wh:%v}, want {0 false} (create)", v0.Wire.Version, v0.WithHistory)
	}
	if v1.Wire.Version != 1 || !v1.WithHistory {
		t.Fatalf("record[1] = {v:%d wh:%v}, want {1 true} (replace-with-history)", v1.Wire.Version, v1.WithHistory)
	}
}

// ─── Delete kinds ───────────────────────────────────────────────────────────

func TestChangeLog_DeleteNode_Unconnected(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, true)
	putTestNode(t, bs, 1, 10, nil)
	if err := bs.DeleteNode(types.NodeID(1)); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	recs := drainFeed(t, bs)
	if got := tagSeq(recs); len(got) != 2 || got[1] != storecontract.ChangeNodeDelete {
		t.Fatalf("tags = %v, want [NodePut NodeDelete]", got)
	}
	body, err := storepkg.DecodeNodeDelete(recs[1].Payload)
	if err != nil {
		t.Fatalf("DecodeNodeDelete: %v", err)
	}
	if body.ID != 1 || body.WithHistory || len(body.CascadedRelIDs) != 0 {
		t.Fatalf("delete body = %+v, want {ID:1 WithHistory:false no-cascade}", body)
	}
}

func TestChangeLog_DeleteRelationship(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, true)
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestRel(t, bs, 100, 5, 1, 2)
	if err := bs.DeleteRelationship(types.RelID(100)); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	recs := drainFeed(t, bs)
	last := recs[len(recs)-1]
	if last.Tag != storecontract.ChangeRelDelete {
		t.Fatalf("last tag = %v, want RelDelete", last.Tag)
	}
	body, err := storepkg.DecodeRelDelete(last.Payload)
	if err != nil {
		t.Fatalf("DecodeRelDelete: %v", err)
	}
	if body.ID != 100 || body.WithHistory {
		t.Fatalf("rel delete body = %+v, want {ID:100 WithHistory:false}", body)
	}
}

// Cascade emits EXACTLY ONE ChangeNodeDelete (not one per cascaded rel) carrying
// the cascaded relationship IDs (rule 16 — exact-set, not over-reporting).
func TestChangeLog_DeleteNodeCascade_SingleRecordWithCascadedRels(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, true)
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestNode(t, bs, 3, 10, nil)
	putTestRel(t, bs, 100, 5, 1, 2)
	putTestRel(t, bs, 101, 5, 3, 1) // incoming to node 1

	if err := bs.DeleteNodeCascade(types.NodeID(1)); err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}
	recs := drainFeed(t, bs)
	deletes := 0
	var body storepkg.NodeDeleteBody
	for _, r := range recs {
		if r.Tag == storecontract.ChangeNodeDelete {
			deletes++
			b, err := storepkg.DecodeNodeDelete(r.Payload)
			if err != nil {
				t.Fatalf("DecodeNodeDelete: %v", err)
			}
			body = b
		}
		if r.Tag == storecontract.ChangeRelDelete {
			t.Fatalf("cascade must NOT emit standalone RelDelete records; got one")
		}
	}
	if deletes != 1 {
		t.Fatalf("cascade emitted %d NodeDelete records, want exactly 1", deletes)
	}
	if body.ID != 1 || body.WithHistory {
		t.Fatalf("cascade body = %+v, want {ID:1 WithHistory:false}", body)
	}
	gotRels := append([]int64(nil), body.CascadedRelIDs...)
	sort.Slice(gotRels, func(i, j int) bool { return gotRels[i] < gotRels[j] })
	if len(gotRels) != 2 || gotRels[0] != 100 || gotRels[1] != 101 {
		t.Fatalf("CascadedRelIDs = %v, want [100 101]", gotRels)
	}
}

func TestChangeLog_DeleteNodeWithHistory(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, true)
	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 30, 1, nil)
	rel := putTestRel(t, bs, 100, 5, 10, 30)

	node, _ := bs.GetNode(types.NodeID(10))
	nodeTombstone := node.DeepCopy()
	relTombstone := rel.DeepCopy()

	if err := bs.DeleteNodeWithHistory(types.NodeID(10), node.Version(), nodeTombstone, []RelTombstone{{
		ID:          rel.ID(),
		PrevVersion: rel.Version(),
		Tombstone:   relTombstone,
	}}); err != nil {
		t.Fatalf("DeleteNodeWithHistory: %v", err)
	}

	recs := drainFeed(t, bs)
	var del *storecontract.ChangeRecord
	for i := range recs {
		if recs[i].Tag == storecontract.ChangeNodeDelete {
			del = &recs[i]
		}
	}
	if del == nil {
		t.Fatalf("no ChangeNodeDelete in feed: %v", tagSeq(recs))
	}
	body, err := storepkg.DecodeNodeDelete(del.Payload)
	if err != nil {
		t.Fatalf("DecodeNodeDelete: %v", err)
	}
	if !body.WithHistory {
		t.Fatalf("WithHistory = false, want true")
	}
	if body.Tombstone == nil || body.Tombstone.ID != 10 {
		t.Fatalf("node tombstone = %+v, want id 10", body.Tombstone)
	}
	if len(body.RelTombstones) != 1 || body.RelTombstones[0].ID != 100 {
		t.Fatalf("rel tombstones = %+v, want [id 100]", body.RelTombstones)
	}
	if len(body.CascadedRelIDs) != 1 || body.CascadedRelIDs[0] != 100 {
		t.Fatalf("cascaded rel IDs = %v, want [100]", body.CascadedRelIDs)
	}
}

func TestChangeLog_DeleteRelWithHistory(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, true)
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	rel := putTestRel(t, bs, 100, 5, 1, 2)
	tombstone := rel.DeepCopy()

	if err := bs.DeleteRelWithHistory(types.RelID(100), rel.Version(), tombstone); err != nil {
		t.Fatalf("DeleteRelWithHistory: %v", err)
	}
	recs := drainFeed(t, bs)
	last := recs[len(recs)-1]
	if last.Tag != storecontract.ChangeRelDelete {
		t.Fatalf("last tag = %v, want RelDelete", last.Tag)
	}
	body, err := storepkg.DecodeRelDelete(last.Payload)
	if err != nil {
		t.Fatalf("DecodeRelDelete: %v", err)
	}
	if !body.WithHistory || body.Tombstone == nil || body.Tombstone.ID != 100 {
		t.Fatalf("rel delete body = %+v, want {WithHistory:true Tombstone.ID:100}", body)
	}
}

// White-box: requeueLog merges failed records ahead of newer pending ones, and a
// subsequent flush re-sorts by LSN so the persisted order is correct regardless
// of the requeue interleave (the flush-failure recovery contract).
func TestChangeLog_RequeueLogPreservesOrder(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false) // async — we drive flush manually

	// Simulate a newer record already buffered, then a failed-flush requeue of
	// two older records.
	bs.pendingLog = []pendingLogRecord{{lsn: 3, value: storepkg.EncodeChangeValue(storecontract.ChangeClear, nil)}}
	bs.requeueLog([]pendingLogRecord{
		{lsn: 1, value: storepkg.EncodeChangeValue(storecontract.ChangeClear, nil)},
		{lsn: 2, value: storepkg.EncodeChangeValue(storecontract.ChangeClear, nil)},
	})
	if len(bs.pendingLog) != 3 || bs.pendingLog[0].lsn != 1 || bs.pendingLog[2].lsn != 3 {
		t.Fatalf("after requeue pendingLog LSNs = %v, want [1 2 3] (failed prepended)", func() []uint64 {
			out := make([]uint64, len(bs.pendingLog))
			for i, r := range bs.pendingLog {
				out[i] = r.lsn
			}
			return out
		}())
	}
	// Keep logSeq ahead of the hand-set LSNs so no real write collides.
	bs.logSeq.Store(3)
	if err := bs.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	recs, _ := bs.ChangeFeed(0, 0)
	if len(recs) != 3 {
		t.Fatalf("persisted %d records, want 3", len(recs))
	}
	assertLSNContiguous(t, recs) // 1,2,3 ascending after the flush re-sort
}

// ─── History-version records (rule 2 parity) ────────────────────────────────

func TestChangeLog_PutNodeVersion_And_PutRelVersion(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, true)

	n := types.NewNode(types.NodeID(1), 10, nil)
	n.SetVersion(7)
	if err := bs.PutNodeVersion(types.NodeID(1), 7, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}
	r := types.NewRelationship(types.RelID(100), 5, types.NodeID(1), types.NodeID(2))
	r.SetVersion(3)
	if err := bs.PutRelVersion(types.RelID(100), 3, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}

	recs := drainFeed(t, bs)
	if got := tagSeq(recs); len(got) != 2 ||
		got[0] != storecontract.ChangeNodeHistoryVersion ||
		got[1] != storecontract.ChangeRelHistoryVersion {
		t.Fatalf("tags = %v, want [NodeHistoryVersion RelHistoryVersion]", got)
	}
	nb, err := storepkg.DecodeHistoryVersionNode(recs[0].Payload)
	if err != nil {
		t.Fatalf("DecodeHistoryVersionNode: %v", err)
	}
	if nb.Version != 7 || nb.Wire.ID != 1 {
		t.Fatalf("node history body = {v:%d id:%d}, want {7 1}", nb.Version, nb.Wire.ID)
	}
	rb, err := storepkg.DecodeHistoryVersionRel(recs[1].Payload)
	if err != nil {
		t.Fatalf("DecodeHistoryVersionRel: %v", err)
	}
	if rb.Version != 3 || rb.Wire.ID != 100 {
		t.Fatalf("rel history body = {v:%d id:%d}, want {3 100}", rb.Version, rb.Wire.ID)
	}
}

// ─── Truncate / Trim records (rule 2 parity) ────────────────────────────────

func TestChangeLog_TruncateAndTrim(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, true)

	// Seed several node history versions so a truncate actually deletes.
	for v := uint32(0); v < 5; v++ {
		n := types.NewNode(types.NodeID(1), 10, nil)
		n.SetVersion(v)
		if err := bs.PutNodeVersion(types.NodeID(1), v, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", v, err)
		}
	}
	startLSN, _ := bs.LastCommittedLSN()
	if err := bs.TruncateNodeHistory(types.NodeID(1), 2); err != nil {
		t.Fatalf("TruncateNodeHistory: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	recs, err := bs.ChangeFeed(startLSN, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodeHistoryTruncate {
		t.Fatalf("tags after truncate = %v, want [NodeHistoryTruncate]", tagSeq(recs))
	}
	tb, err := storepkg.DecodeHistoryTruncate(recs[0].Payload)
	if err != nil {
		t.Fatalf("DecodeHistoryTruncate: %v", err)
	}
	if tb.ID != 1 || tb.IsTrim || tb.Bound != 2 {
		t.Fatalf("truncate body = %+v, want {ID:1 IsTrim:false Bound:2}", tb)
	}

	// Trim variant (rel side for parity).
	for v := uint32(0); v < 4; v++ {
		r := types.NewRelationship(types.RelID(100), 5, types.NodeID(1), types.NodeID(2))
		r.SetVersion(v)
		if err := bs.PutRelVersion(types.RelID(100), v, r); err != nil {
			t.Fatalf("PutRelVersion(%d): %v", v, err)
		}
	}
	mid, _ := bs.LastCommittedLSN()
	if err := bs.TrimRelHistoryFrom(types.RelID(100), 2); err != nil {
		t.Fatalf("TrimRelHistoryFrom: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	recs, _ = bs.ChangeFeed(mid, 0)
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeRelHistoryTruncate {
		t.Fatalf("tags after trim = %v, want [RelHistoryTruncate]", tagSeq(recs))
	}
	tb, _ = storepkg.DecodeHistoryTruncate(recs[0].Payload)
	if tb.ID != 100 || !tb.IsTrim || tb.Bound != 2 {
		t.Fatalf("trim body = %+v, want {ID:100 IsTrim:true Bound:2}", tb)
	}
}

// A no-op truncate (nothing to delete) emits NO record.
func TestChangeLog_TruncateNoOp_EmitsNothing(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, true)
	putTestNode(t, bs, 1, 10, nil) // one version, no history
	startLSN, _ := bs.LastCommittedLSN()
	if err := bs.TruncateNodeHistory(types.NodeID(1), 10); err != nil {
		t.Fatalf("TruncateNodeHistory: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	recs, _ := bs.ChangeFeed(startLSN, 0)
	if len(recs) != 0 {
		t.Fatalf("no-op truncate emitted %d records, want 0", len(recs))
	}
}

// ─── Label-token mutations emit NodePut ─────────────────────────────────────

func TestChangeLog_LabelTokenMutations(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, true)
	n := types.NewNode(types.NodeID(1), 10, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	updated := n.DeepCopy()
	updated.AddLabelTokenRaw(20)
	if err := bs.AddNodeLabelToken(types.NodeID(1), 20, updated); err != nil {
		t.Fatalf("AddNodeLabelToken: %v", err)
	}
	recs := drainFeed(t, bs)
	if got := tagSeq(recs); len(got) != 2 || got[1] != storecontract.ChangeNodePut {
		t.Fatalf("tags = %v, want [NodePut NodePut]", got)
	}
	nw, _ := storepkg.DecodeNodePut(recs[1].Payload)
	hasExtra := false
	for _, e := range nw.Wire.ExtraLabels {
		if e == 20 {
			hasExtra = true
		}
	}
	if !hasExtra {
		t.Fatalf("updated NodePut wire missing label 20: extras=%v", nw.Wire.ExtraLabels)
	}
}

// ─── Batch doors emit one record per entity ─────────────────────────────────

func TestChangeLog_Batches(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, true)

	nodes := []*types.Node{
		types.NewNode(types.NodeID(1), 10, nil),
		types.NewNode(types.NodeID(2), 10, nil),
		types.NewNode(types.NodeID(3), 10, nil),
	}
	if err := bs.PutNodesBatch(nodes); err != nil {
		t.Fatalf("PutNodesBatch: %v", err)
	}
	rels := []*types.Relationship{
		types.NewRelationship(types.RelID(100), 5, types.NodeID(1), types.NodeID(2)),
		types.NewRelationship(types.RelID(101), 5, types.NodeID(2), types.NodeID(3)),
	}
	if err := bs.PutRelationshipsBatch(rels); err != nil {
		t.Fatalf("PutRelationshipsBatch: %v", err)
	}
	recs := drainFeed(t, bs)
	wantTags := []storecontract.ChangeTag{
		storecontract.ChangeNodePut, storecontract.ChangeNodePut, storecontract.ChangeNodePut,
		storecontract.ChangeRelPut, storecontract.ChangeRelPut,
	}
	got := tagSeq(recs)
	if len(got) != len(wantTags) {
		t.Fatalf("batch tags = %v, want %v", got, wantTags)
	}
	for i := range wantTags {
		if got[i] != wantTags[i] {
			t.Fatalf("batch tags = %v, want %v", got, wantTags)
		}
	}
	assertLSNContiguous(t, recs)

	// DeleteRelationshipsBatch then DeleteNodesBatch (now unconnected).
	startLSN, _ := bs.LastCommittedLSN()
	if err := bs.DeleteRelationshipsBatch([]types.RelID{100, 101}); err != nil {
		t.Fatalf("DeleteRelationshipsBatch: %v", err)
	}
	if err := bs.DeleteNodesBatch([]types.NodeID{1, 2, 3}); err != nil {
		t.Fatalf("DeleteNodesBatch: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	recs, _ = bs.ChangeFeed(startLSN, 0)
	got = tagSeq(recs)
	wantTags = []storecontract.ChangeTag{
		storecontract.ChangeRelDelete, storecontract.ChangeRelDelete,
		storecontract.ChangeNodeDelete, storecontract.ChangeNodeDelete, storecontract.ChangeNodeDelete,
	}
	if len(got) != len(wantTags) {
		t.Fatalf("delete-batch tags = %v, want %v", got, wantTags)
	}
	for i := range wantTags {
		if got[i] != wantTags[i] {
			t.Fatalf("delete-batch tags = %v, want %v", got, wantTags)
		}
	}
}

// ─── Adversarial exact-set: a scripted lifecycle yields an exact ordered feed ─

func TestChangeLog_AdversarialExactSequence(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, true)

	putTestNode(t, bs, 1, 10, nil)                     // NodePut   (1)
	putTestNode(t, bs, 2, 10, nil)                     // NodePut   (2)
	putTestRel(t, bs, 100, 5, 1, 2)                    // RelPut    (3)
	if err := bs.DeleteRelationship(100); err != nil { // RelDelete (4)
		t.Fatalf("DeleteRelationship: %v", err)
	}
	if err := bs.DeleteNode(2); err != nil { // NodeDelete (5) — now unconnected
		t.Fatalf("DeleteNode(2): %v", err)
	}

	recs := drainFeed(t, bs)
	want := []storecontract.ChangeTag{
		storecontract.ChangeNodePut, storecontract.ChangeNodePut, storecontract.ChangeRelPut,
		storecontract.ChangeRelDelete, storecontract.ChangeNodeDelete,
	}
	got := tagSeq(recs)
	if len(got) != len(want) {
		t.Fatalf("tags = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("tags = %v, want %v", got, want)
		}
	}
	assertLSNContiguous(t, recs)
}

// ─── ForEachChange cursor / early-stop / limit ──────────────────────────────

func TestChangeLog_ForEachChange_CursorAndEarlyStop(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, true)
	for i := int64(1); i <= 5; i++ {
		putTestNode(t, bs, i, 10, nil)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// afterLSN=2 skips the first two.
	var seen []uint64
	if err := bs.ForEachChange(2, func(r storecontract.ChangeRecord) bool {
		seen = append(seen, r.LSN)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if len(seen) != 3 || seen[0] != 3 || seen[2] != 5 {
		t.Fatalf("ForEachChange(after=2) LSNs = %v, want [3 4 5]", seen)
	}

	// Early stop after the first.
	count := 0
	if err := bs.ForEachChange(0, func(r storecontract.ChangeRecord) bool {
		count++
		return false
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if count != 1 {
		t.Fatalf("early-stop visited %d, want 1", count)
	}

	// limit via ChangeFeed.
	recs, err := bs.ChangeFeed(0, 2)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("ChangeFeed(limit=2) = %d records, want 2", len(recs))
	}

	// nil callback fails closed.
	if err := bs.ForEachChange(0, nil); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ForEachChange(nil) = %v, want ErrInvalidStoreMutation", err)
	}
}

// ─── Concurrency: LSNs are gap-free and total under parallel writers ─────────

func TestChangeLog_ConcurrentWritersGapFreeLSNs(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, true)
	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			if err := bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(id+1)), 10, nil)); err != nil {
				t.Errorf("PutNode(%d): %v", id, err)
			}
		}(int64(i))
	}
	wg.Wait()

	recs := drainFeed(t, bs)
	if len(recs) != n {
		t.Fatalf("got %d records, want %d", len(recs), n)
	}
	seen := make(map[uint64]struct{}, n)
	for _, r := range recs {
		if _, dup := seen[r.LSN]; dup {
			t.Fatalf("duplicate LSN %d", r.LSN)
		}
		seen[r.LSN] = struct{}{}
	}
	for i := uint64(1); i <= n; i++ {
		if _, ok := seen[i]; !ok {
			t.Fatalf("missing LSN %d (LSNs must be gap-free 1..%d)", i, n)
		}
	}
}

// ─── Restart: the LSN allocator resumes monotonically (crash-consistent seed) ─

func TestChangeLog_RestartSeedsLSN(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	bs, err := New(Config{Dir: dir, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestNode(t, bs, 3, 10, nil)
	beforeLSN, _ := bs.LastCommittedLSN()
	if beforeLSN != 3 {
		t.Fatalf("LastCommittedLSN before close = %d, want 3", beforeLSN)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := New(Config{Dir: dir, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	if lsn, _ := reopened.LastCommittedLSN(); lsn != 3 {
		t.Fatalf("LastCommittedLSN after reopen = %d, want 3 (watermark must persist)", lsn)
	}
	// The next write must NOT reuse an LSN.
	putTestNode(t, reopened, 4, 10, nil)
	if err := reopened.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	recs, _ := reopened.ChangeFeed(3, 0)
	if len(recs) != 1 || recs[0].LSN != 4 {
		t.Fatalf("post-restart record LSN = %v, want [4] (monotonic resume)", func() []uint64 {
			out := make([]uint64, len(recs))
			for i, r := range recs {
				out[i] = r.LSN
			}
			return out
		}())
	}
}

// ─── White-box: a log-only flush (records, zero entity ops) still commits ────
//
// Guards the flush() empty-check fix: `len(ops)==0 && len(logs)==0`. Without it,
// a flush cycle carrying only log records would early-return and silently drop
// them.
func TestChangeLog_LogOnlyFlushPersists(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false) // async so we control the flush
	bs.logChangeRaw(storecontract.ChangeClear, nil)
	if err := bs.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	lsn, _ := bs.LastCommittedLSN()
	if lsn != 1 {
		t.Fatalf("LastCommittedLSN = %d, want 1 (log-only flush must persist)", lsn)
	}
	recs, _ := bs.ChangeFeed(0, 0)
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeClear {
		t.Fatalf("feed = %v, want [Clear]", tagSeq(recs))
	}
}

// ─── White-box: a corrupt 0x09 value fails the read closed (lesson 47) ───────

func TestChangeLog_CorruptRecordFailsClosed(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, true)
	putTestNode(t, bs, 1, 10, nil)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Inject a change-log key carrying an unknown tag byte.
	if err := bs.db.Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storepkg.ChangeLogKey(2), []byte{0xFF, 0x00})
	}); err != nil {
		t.Fatalf("inject corrupt record: %v", err)
	}
	_, err := bs.ChangeFeed(0, 0)
	if !errors.Is(err, storecontract.ErrCorruptWire) {
		t.Fatalf("ChangeFeed over corrupt record = %v, want ErrCorruptWire", err)
	}
}

// ─── Truncate/Trim — all four node/rel × keep-count/trim corners (rule 2/17) ──

func TestChangeLog_TruncateRelHistory_And_TrimNodeHistory(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, true)

	// Rel-side keep-count truncate (mirror of the node-side case elsewhere).
	for v := uint32(0); v < 4; v++ {
		r := types.NewRelationship(types.RelID(100), 5, types.NodeID(1), types.NodeID(2))
		r.SetVersion(v)
		if err := bs.PutRelVersion(types.RelID(100), v, r); err != nil {
			t.Fatalf("PutRelVersion(%d): %v", v, err)
		}
	}
	at, _ := bs.LastCommittedLSN()
	if err := bs.TruncateRelHistory(types.RelID(100), 1); err != nil {
		t.Fatalf("TruncateRelHistory: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	recs, _ := bs.ChangeFeed(at, 0)
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeRelHistoryTruncate {
		t.Fatalf("tags = %v, want [RelHistoryTruncate]", tagSeq(recs))
	}
	tb, _ := storepkg.DecodeHistoryTruncate(recs[0].Payload)
	if tb.ID != 100 || tb.IsTrim || tb.Bound != 1 {
		t.Fatalf("rel truncate body = %+v, want {ID:100 IsTrim:false Bound:1}", tb)
	}

	// Node-side trim-from.
	for v := uint32(0); v < 4; v++ {
		n := types.NewNode(types.NodeID(1), 10, nil)
		n.SetVersion(v)
		if err := bs.PutNodeVersion(types.NodeID(1), v, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", v, err)
		}
	}
	at, _ = bs.LastCommittedLSN()
	if err := bs.TrimNodeHistoryFrom(types.NodeID(1), 2); err != nil {
		t.Fatalf("TrimNodeHistoryFrom: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	recs, _ = bs.ChangeFeed(at, 0)
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodeHistoryTruncate {
		t.Fatalf("tags = %v, want [NodeHistoryTruncate]", tagSeq(recs))
	}
	tb, _ = storepkg.DecodeHistoryTruncate(recs[0].Payload)
	if tb.ID != 1 || !tb.IsTrim || tb.Bound != 2 {
		t.Fatalf("node trim body = %+v, want {ID:1 IsTrim:true Bound:2}", tb)
	}
}

// ─── Replace doors emit a fresh Put with the replaced state ──────────────────

func TestChangeLog_ReplaceDoors(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, true)
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	r := putTestRel(t, bs, 100, 5, 1, 2)

	// ReplaceNode → ChangeNodePut.
	n1, _ := bs.GetNode(types.NodeID(1))
	at, _ := bs.LastCommittedLSN()
	if err := bs.ReplaceNode(n1.DeepCopy()); err != nil {
		t.Fatalf("ReplaceNode: %v", err)
	}
	recs := afterFeed(t, bs, at)
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodePut {
		t.Fatalf("ReplaceNode tags = %v, want [NodePut]", tagSeq(recs))
	}

	// ReplaceRelationship → ChangeRelPut.
	at, _ = bs.LastCommittedLSN()
	if err := bs.ReplaceRelationship(r.DeepCopy()); err != nil {
		t.Fatalf("ReplaceRelationship: %v", err)
	}
	recs = afterFeed(t, bs, at)
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeRelPut {
		t.Fatalf("ReplaceRelationship tags = %v, want [RelPut]", tagSeq(recs))
	}

	// ReplaceRelWithHistory (rel mirror of the node two-phase test): emits the
	// new-current RelPut; the prior version goes to history (not the feed).
	cur, _ := bs.GetRelationship(types.RelID(100))
	prev := cur.DeepCopy()
	_ = cur.SetProperty("w", int64(9))
	cur.SetVersion(1)
	at, _ = bs.LastCommittedLSN()
	if err := bs.ReplaceRelWithHistory(cur, prev.Version(), prev); err != nil {
		t.Fatalf("ReplaceRelWithHistory: %v", err)
	}
	recs = afterFeed(t, bs, at)
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeRelPut {
		t.Fatalf("ReplaceRelWithHistory tags = %v, want [RelPut]", tagSeq(recs))
	}
	rw, _ := storepkg.DecodeRelPut(recs[0].Payload)
	if rw.Wire.Version != 1 || !rw.WithHistory {
		t.Fatalf("replaced rel = {v:%d wh:%v}, want {1 true}", rw.Wire.Version, rw.WithHistory)
	}
}

// ─── Label-token doors (plain + with-history) emit exactly one NodePut ───────

func TestChangeLog_LabelTokenWithHistoryDoors(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, true)
	n := types.NewNode(types.NodeID(1), 10, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	// AddNodeLabelTokenWithHistory: one NodePut carrying the new label, NOT a
	// stray history-version record (lesson 42 hot-spot).
	withAdd := n.DeepCopy()
	withAdd.AddLabelTokenRaw(20)
	withAdd.SetVersion(1)
	at, _ := bs.LastCommittedLSN()
	if err := bs.AddNodeLabelTokenWithHistory(types.NodeID(1), 20, withAdd, 0, n.DeepCopy()); err != nil {
		t.Fatalf("AddNodeLabelTokenWithHistory: %v", err)
	}
	recs := afterFeed(t, bs, at)
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodePut {
		t.Fatalf("AddNodeLabelTokenWithHistory tags = %v, want exactly [NodePut]", tagSeq(recs))
	}
	nw, _ := storepkg.DecodeNodePut(recs[0].Payload)
	has := false
	for _, e := range nw.Wire.ExtraLabels {
		if e == 20 {
			has = true
		}
	}
	if !has {
		t.Fatalf("with-history label-add NodePut missing label 20: %v", nw.Wire.ExtraLabels)
	}

	// RemoveNodeLabelToken (plain): one NodePut without label 20.
	cur, _ := bs.GetNode(types.NodeID(1))
	rem := cur.DeepCopy()
	rem.RemoveLabelTokenRaw(20)
	at, _ = bs.LastCommittedLSN()
	if err := bs.RemoveNodeLabelToken(types.NodeID(1), 20, rem); err != nil {
		t.Fatalf("RemoveNodeLabelToken: %v", err)
	}
	recs = afterFeed(t, bs, at)
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeNodePut {
		t.Fatalf("RemoveNodeLabelToken tags = %v, want exactly [NodePut]", tagSeq(recs))
	}
}

// ─── Cascade CascadedRelIDs are deterministic + sorted (the masked bug) ──────

func TestChangeLog_CascadeRelIDsSortedDeterministic(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, true)
	putTestNode(t, bs, 1, 10, nil)
	// Insert endpoints + rels with deliberately OUT-OF-ORDER rel IDs so a naive
	// (map-iteration) emit order would not happen to be sorted.
	for i, rid := range []int64{500, 100, 900, 300, 700} {
		putTestNode(t, bs, int64(10+i), 10, nil)
		putTestRel(t, bs, rid, 5, 1, int64(10+i))
	}
	at, _ := bs.LastCommittedLSN()
	if err := bs.DeleteNodeCascade(types.NodeID(1)); err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}
	recs := afterFeed(t, bs, at)
	var del *storecontract.ChangeRecord
	for i := range recs {
		if recs[i].Tag == storecontract.ChangeNodeDelete {
			del = &recs[i]
		}
	}
	if del == nil {
		t.Fatalf("no NodeDelete in cascade feed: %v", tagSeq(recs))
	}
	body, _ := storepkg.DecodeNodeDelete(del.Payload)
	want := []int64{100, 300, 500, 700, 900} // ascending, regardless of insert order
	if len(body.CascadedRelIDs) != len(want) {
		t.Fatalf("CascadedRelIDs = %v, want %v", body.CascadedRelIDs, want)
	}
	for i := range want {
		if body.CascadedRelIDs[i] != want[i] {
			t.Fatalf("CascadedRelIDs = %v, want sorted %v", body.CascadedRelIDs, want)
		}
	}
}

// afterFeed flushes and returns the records with LSN > after.
func afterFeed(t *testing.T, bs *Store, after uint64) []storecontract.ChangeRecord {
	t.Helper()
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	recs, err := bs.ChangeFeed(after, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	return recs
}

// ─── Clear wipes the feed and re-anchors with a ChangeClear marker ───────────

func TestChangeLog_ClearReAnchors(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, true)
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	beforeLSN, _ := bs.LastCommittedLSN()

	if err := bs.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	recs := drainFeed(t, bs)
	// Pre-Clear records are gone; a single ChangeClear marker remains, at an LSN
	// strictly greater than the pre-Clear watermark (monotonic across Clear).
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeClear {
		t.Fatalf("post-Clear feed = %v, want [Clear]", tagSeq(recs))
	}
	if recs[0].LSN <= beforeLSN {
		t.Fatalf("Clear marker LSN %d <= pre-Clear watermark %d (must stay monotonic)", recs[0].LSN, beforeLSN)
	}
}

// TestChangeLog_ClearOnDiskWipesAllAndKeepsWatermark exercises the on-disk
// (non-DropAll) Clear path used when the change-log is enabled. It guards two
// things at once: (1) the DropPrefix + selective-meta-delete rewrite must wipe
// data, indexes AND metadata (registries) exactly as DropAll did — no leakage;
// (2) the durable watermark (LastLSNKey) must survive Clear+Close+Reopen so the
// LSN allocator resumes strictly above the pre-Clear watermark (lesson 52).
func TestChangeLog_ClearOnDiskWipesAllAndKeepsWatermark(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestRel(t, bs, 100, 5, 1, 2)
	// Seed a real registry and a custom meta sentinel — both must be wiped.
	labels := registrypkg.NewLabelRegistry()
	if _, err := labels.GetOrCreate("Person"); err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}
	relTypes := registrypkg.NewRelTypeRegistry()
	if err := bs.SaveRegistries(labels, relTypes); err != nil {
		t.Fatalf("SaveRegistries: %v", err)
	}
	if err := bs.MetaSet("custom_sentinel", []byte("present")); err != nil {
		t.Fatalf("MetaSet: %v", err)
	}
	beforeLSN, _ := bs.LastCommittedLSN()
	if beforeLSN == 0 {
		t.Fatal("pre-Clear watermark must be > 0")
	}

	if err := bs.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}

	// Clear marker emitted strictly above the pre-Clear watermark.
	if lsn, _ := bs.LastCommittedLSN(); lsn <= beforeLSN {
		t.Fatalf("post-Clear LSN %d <= pre-Clear %d (must stay monotonic)", lsn, beforeLSN)
	}
	// Data gone.
	if got, gerr := bs.GetNode(types.NodeID(snowflake.ID(1))); gerr == nil && got != nil {
		t.Fatal("node survived Clear")
	}
	// Metadata gone — registry and the custom sentinel.
	if found, lerr := bs.LoadLabelRegistry(registrypkg.NewLabelRegistry()); lerr != nil {
		t.Fatalf("LoadLabelRegistry: %v", lerr)
	} else if found {
		t.Fatal("label registry survived Clear (meta leakage from DropPrefix rewrite)")
	}
	if v, merr := bs.MetaGet("custom_sentinel"); merr != nil {
		t.Fatalf("MetaGet: %v", merr)
	} else if v != nil {
		t.Fatal("custom meta sentinel survived Clear")
	}

	afterClearLSN, _ := bs.LastCommittedLSN()
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: the watermark must persist and the allocator must NOT reuse LSNs.
	reopened, err := New(Config{Dir: dir, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if lsn, _ := reopened.LastCommittedLSN(); lsn != afterClearLSN {
		t.Fatalf("post-reopen watermark = %d, want %d (must persist across Clear+reopen)", lsn, afterClearLSN)
	}
	putTestNode(t, reopened, 3, 10, nil)
	if err := reopened.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if lsn, _ := reopened.LastCommittedLSN(); lsn != afterClearLSN+1 {
		t.Fatalf("post-Clear write LSN = %d, want %d (no reuse)", lsn, afterClearLSN+1)
	}
}

// TestChangeLog_ClearCrashAfterDataDropKeepsWatermark simulates a crash in the
// middle of Clear — after the data/change-log keyspaces are dropped but BEFORE
// the marker WriteBatch (which carries the new watermark) is flushed. The fix
// (lesson 52) drops data via DropPrefix and deliberately leaves LastLSNKey
// intact, so the watermark is never absent. With the old db.DropAll() Clear,
// LastLSNKey would be gone at this point and a reopen would reseed the LSN
// allocator to 0, colliding with a consumer's pre-Clear watermark.
func TestChangeLog_ClearCrashAfterDataDropKeepsWatermark(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestNode(t, bs, 3, 10, nil)
	beforeLSN, _ := bs.LastCommittedLSN()
	if beforeLSN != 3 {
		t.Fatalf("pre-crash watermark = %d, want 3", beforeLSN)
	}

	// Simulate clearAndReanchorChangeLog up to (but not including) the marker
	// batch (step 4): delete the entity-counter meta keys (step 2), then drop the
	// data/history/change-log-record keyspaces (step 3), leaving the meta keyspace
	// — and therefore LastLSNKey — intact. Then "crash" (close before the marker
	// batch). loadIndexes must reopen cleanly (counters absent → trust live rows)
	// AND resume the LSN allocator above the pre-crash watermark.
	if err := bs.db.Update(func(txn *badgerv4.Txn) error {
		if err := txn.Delete(counterNodeCountKey); err != nil {
			return err
		}
		return txn.Delete(counterRelCountKey)
	}); err != nil {
		t.Fatalf("delete counters: %v", err)
	}
	if err := bs.db.DropPrefix(
		[]byte{storepkg.KeyNode}, []byte{storepkg.KeyRel},
		[]byte{storepkg.KeyLabel}, []byte{storepkg.KeyRelType},
		[]byte{storepkg.KeyOut}, []byte{storepkg.KeyIn},
		[]byte{storepkg.KeyHistNode}, []byte{storepkg.KeyHistRel},
		[]byte{storepkg.KeyChangeLog},
	); err != nil {
		t.Fatalf("DropPrefix: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: even though every change-log record was dropped, the watermark must
	// survive so the allocator resumes ABOVE the pre-crash watermark.
	reopened, err := New(Config{Dir: dir, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if lsn, _ := reopened.LastCommittedLSN(); lsn != beforeLSN {
		t.Fatalf("post-crash watermark = %d, want %d (LastLSNKey must survive a mid-Clear crash)", lsn, beforeLSN)
	}
	putTestNode(t, reopened, 4, 10, nil)
	if err := reopened.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	recs, _ := reopened.ChangeFeed(beforeLSN, 0)
	if len(recs) != 1 || recs[0].LSN != beforeLSN+1 {
		t.Fatalf("post-crash write LSN = %v, want [%d] (no collision with pre-crash watermark)", lsnSeq(recs), beforeLSN+1)
	}
}

func lsnSeq(recs []storecontract.ChangeRecord) []uint64 {
	out := make([]uint64, len(recs))
	for i, r := range recs {
		out[i] = r.LSN
	}
	return out
}
