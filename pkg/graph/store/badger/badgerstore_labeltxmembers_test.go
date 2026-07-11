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
// open (like the sibling indexes): members written before a Close are recovered
// by a fresh lazy build after reopen — including a historical-only label carried
// only by a version-history row (proving the build scans the 0x07 keyspace).
func TestBadgerLabelTxMembership_RebuildOnReopen(t *testing.T) {
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

	lm := labelMembers(t, bs2, tokL)
	if len(lm) != 2 {
		t.Fatalf("after reopen L members = %d, want 2 (%v)", len(lm), lm)
	}
	for _, id := range []types.NodeID{101, 102} {
		if _, ok := lm[id]; !ok {
			t.Fatalf("after reopen L missing node %d", id)
		}
	}
	// Historical-only label M is recovered from the 0x07 history keyspace scan.
	mm := labelMembers(t, bs2, tokM)
	if _, ok := mm[types.NodeID(101)]; !ok {
		t.Fatalf("after reopen historical-only label M missing node 101 (%v)", mm)
	}
}

// TestBadgerLabelTxMembership_PendingOverlay verifies the lazy build folds
// UNFLUSHED (pending) writes: a node written but not yet flushed is a member.
func TestBadgerLabelTxMembership_PendingOverlay(t *testing.T) {
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer bs.Close()
	const tokL uint16 = 1
	n := bgNode(201, []uint16{tokL}, 10)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Do NOT flush — build must still see the pending row.
	lm := labelMembers(t, bs, tokL)
	if _, ok := lm[types.NodeID(201)]; !ok {
		t.Fatalf("pending node not folded into build (%v)", lm)
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

// TestBadgerRelTypeTxMembership_Basic covers the rel-type mirror.
func TestBadgerRelTypeTxMembership_Basic(t *testing.T) {
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
	out := make(map[types.RelID]types.Instant)
	if err := bs.ForEachRelTypeTxMember(knows, func(id types.RelID, firstTx types.Instant) bool {
		out[id] = firstTx
		return true
	}); err != nil {
		t.Fatalf("ForEachRelTypeTxMember: %v", err)
	}
	if tx, ok := out[types.RelID(400)]; !ok {
		t.Fatalf("rel 400 missing from KNOWS members (%v)", out)
	} else if tx != 15 {
		t.Fatalf("rel 400 firstTxFrom = %d, want 15", tx)
	}
}
