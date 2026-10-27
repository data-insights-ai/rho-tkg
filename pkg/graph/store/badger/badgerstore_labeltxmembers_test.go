package badger

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func labelMembers(t *testing.T, bs *Store, tok uint16) map[types.NodeID]types.Instant {
	t.Helper()
	out := make(map[types.NodeID]types.Instant)
	if err := bs.ForEachLabelTxMember(tok, func(id types.NodeID, firstTx types.Instant) bool {
		out[id] = firstTx
		return true
	}); err != nil {
		t.Fatalf("ForEachLabelTxMember: %v", err)
	}
	return out
}

func bgNode(id int64, tokens []uint16, txFrom types.Instant) *types.Node {
	var primary uint16
	var extra []uint16
	if len(tokens) > 0 {
		primary = tokens[0]
		extra = tokens[1:]
	}
	n := types.NewNode(types.NodeID(id), primary, extra)
	n.SetTemporal(&types.TemporalMetadata{TxFrom: txFrom, ValidFrom: txFrom})
	return n
}

// TestBadgerLabelTxMembership_RebuildOnReopen verifies the sidecar is rebuilt at
// open (like the sibling indexes): a historical-only label carried only by a
// version-history row written before a Close is recovered by a fresh lazy
// build after reopen (proving the build scans the 0x07 keyspace), the current
// members stay out of it (TXMEMBERS: the label index holds them) and the
// point form still answers them.
func TestBadgerLabelTxMembership_RebuildOnReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	const tokL uint16 = 1
	const tokM uint16 = 2

	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// n1 current row carries {L}; a history version carries {L,M} so M is a
	// historical-only member (never in the current label index).
	n1 := bgNode(101, []uint16{tokL}, 10)
	if err := bs.PutNode(n1); err != nil {
		t.Fatalf("put n1: %v", err)
	}
	histVer := bgNode(101, []uint16{tokL, tokM}, 5)
	histVer.SetVersion(0)
	if err := bs.PutNodeVersion(types.NodeID(101), 0, histVer); err != nil {
		t.Fatalf("put n1 history: %v", err)
	}
	n2 := bgNode(102, []uint16{tokL}, 20)
	if err := bs.PutNode(n2); err != nil {
		t.Fatalf("put n2: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen — the sidecar is nil again; first ForEach rebuilds from persistence.
	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer bs2.Close()

	if lm := labelMembers(t, bs2, tokL); len(lm) != 0 {
		t.Fatalf("after reopen L streams %v, want no current member", lm)
	}
	got := 0
	if err := bs2.LabelTxMembersOf(tokL, []types.NodeID{101, 102}, func(types.NodeID, types.Instant) bool {
		got++
		return true
	}); err != nil || got != 2 {
		t.Fatalf("after reopen L point form: %d of nodes 101, 102 (%v), want 2", got, err)
	}
	// Historical-only label M is recovered from the 0x07 history keyspace scan.
	mm := labelMembers(t, bs2, tokM)
	if _, ok := mm[types.NodeID(101)]; !ok {
		t.Fatalf("after reopen historical-only label M missing node 101 (%v)", mm)
	}
}

// TestBadgerLabelTxMembership_PendingOverlay verifies the lazy build folds
// UNFLUSHED (pending) writes: a history row written but not yet flushed makes
// its node a former member of a label its pending current row lacks, and not
// of one the pending current row carries.
func TestBadgerLabelTxMembership_PendingOverlay(t *testing.T) {
	t.Parallel()
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer bs.Close()
	const tokL, tokK uint16 = 1, 2
	for id, cur := range map[int64]uint16{201: tokK, 202: tokL} {
		if err := bs.PutNode(bgNode(id, []uint16{cur}, 10)); err != nil {
			t.Fatalf("put: %v", err)
		}
		hv := bgNode(id, []uint16{tokL}, 5)
		hv.SetVersion(0)
		if err := bs.PutNodeVersion(types.NodeID(id), 0, hv); err != nil {
			t.Fatalf("put version: %v", err)
		}
	}
	// Do NOT flush — build must still see the pending rows.
	lm := labelMembers(t, bs, tokL)
	if tx, ok := lm[types.NodeID(201)]; !ok || tx != 5 {
		t.Fatalf("pending history row not folded into build (%v)", lm)
	}
	if _, ok := lm[types.NodeID(202)]; ok {
		t.Fatalf("node 202's pending current row carries L, yet it is a former member (%v)", lm)
	}
}

// TestBadgerLabelTxMembership_PostBuildHistoryVersion covers the guarded
// incremental hook in PutNodeVersion: once the sidecar is BUILT, a later
// history-version insert carrying a novel label must be captured (the
// import/replica-after-a-pinned-scan window).
func TestBadgerLabelTxMembership_PostBuildHistoryVersion(t *testing.T) {
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer bs.Close()
	const tokL uint16 = 1
	const tokX uint16 = 9
	if err := bs.PutNode(bgNode(501, []uint16{tokL}, 10)); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Force the build (flag now true).
	_ = labelMembers(t, bs, tokL)
	// A history version carrying novel label X, inserted AFTER the build.
	hv := bgNode(501, []uint16{tokL, tokX}, 6)
	hv.SetVersion(0)
	if err := bs.PutNodeVersion(types.NodeID(501), 0, hv); err != nil {
		t.Fatalf("put version: %v", err)
	}
	xm := labelMembers(t, bs, tokX)
	if _, ok := xm[types.NodeID(501)]; !ok {
		t.Fatalf("post-build history-version novel label X not captured (%v)", xm)
	}
}

// TestBadgerRelTypeTxMembership_Basic covers the rel-type mirror: a live rel is
// a current member (the type index), not streamed; deleted, it is a former
// member with an unknown first acquisition.
func TestBadgerRelTypeTxMembership_Basic(t *testing.T) {
	t.Parallel()
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer bs.Close()
	// Endpoints.
	for _, id := range []int64{301, 302} {
		if err := bs.PutNode(bgNode(id, []uint16{1}, 10)); err != nil {
			t.Fatalf("put node %d: %v", id, err)
		}
	}
	const knows uint16 = 7
	r := types.NewRelationship(types.RelID(400), knows, types.NodeID(301), types.NodeID(302))
	r.SetTemporal(&types.TemporalMetadata{TxFrom: 15, ValidFrom: 15})
	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("put rel: %v", err)
	}
	members := func() map[types.RelID]types.Instant {
		out := make(map[types.RelID]types.Instant)
		if err := bs.ForEachRelTypeTxMember(knows, func(id types.RelID, firstTx types.Instant) bool {
			out[id] = firstTx
			return true
		}); err != nil {
			t.Fatalf("ForEachRelTypeTxMember: %v", err)
		}
		return out
	}
	if out := members(); len(out) != 0 {
		t.Fatalf("live rel 400 streamed as a former KNOWS member (%v)", out)
	}
	if err := bs.DeleteRelationship(types.RelID(400)); err != nil {
		t.Fatalf("delete rel: %v", err)
	}
	if tx, ok := members()[types.RelID(400)]; !ok || tx != 0 {
		t.Fatalf("deleted rel 400: KNOWS former members %v, want 400 at 0", members())
	}
}

// TestBadgerTxMembersForgetRetentionPurge: a retention purge removes a node,
// its relationships and every history row of both, so after the purge's
// cascade (a departure) neither stays a former member: the streams hold
// nothing to resolve (TXMEMBERS, the sidecars stay bounded under retention).
func TestBadgerTxMembersForgetRetentionPurge(t *testing.T) {
	t.Parallel()
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer bs.Close()
	const tokL, link uint16 = 1, 7
	for _, id := range []int64{601, 602} {
		if err := bs.PutNode(bgNode(id, []uint16{tokL}, 10)); err != nil {
			t.Fatalf("put node: %v", err)
		}
		hv := bgNode(id, []uint16{tokL}, 5)
		hv.SetVersion(0)
		if err := bs.PutNodeVersion(types.NodeID(id), 0, hv); err != nil {
			t.Fatalf("put node version: %v", err)
		}
	}
	r := types.NewRelationship(700, link, 601, 602)
	r.SetTemporal(&types.TemporalMetadata{TxFrom: 10, ValidFrom: 10})
	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("put rel: %v", err)
	}
	rv := types.NewRelationship(700, link, 601, 602)
	rv.SetTemporal(&types.TemporalMetadata{TxFrom: 5, ValidFrom: 5})
	if err := bs.PutRelVersion(700, 0, rv); err != nil {
		t.Fatalf("put rel version: %v", err)
	}
	rels := func() map[types.RelID]types.Instant {
		out := map[types.RelID]types.Instant{}
		if err := bs.ForEachRelTypeTxMember(link, func(id types.RelID, tx types.Instant) bool {
			out[id] = tx
			return true
		}); err != nil {
			t.Fatalf("ForEachRelTypeTxMember: %v", err)
		}
		return out
	}
	if lm, rm := labelMembers(t, bs, tokL), rels(); len(lm)+len(rm) != 0 {
		t.Fatalf("before the purge: former members %v %v, want none (all current)", lm, rm)
	}
	res, err := bs.PurgeNodesByLabelBefore(tokL, types.Instant(1<<62), 0)
	if err != nil || res.NodesPurged != 2 || res.RelsPurged != 1 {
		t.Fatalf("purge: %+v %v, want 2 nodes and 1 relationship", res, err)
	}
	if lm, rm := labelMembers(t, bs, tokL), rels(); len(lm)+len(rm) != 0 {
		t.Fatalf("after the purge: former members %v %v, want none (no row left)", lm, rm)
	}
}
