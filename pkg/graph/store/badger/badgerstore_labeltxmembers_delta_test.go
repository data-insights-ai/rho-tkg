package badger

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 18d: ensureLabelTxMembersBuilt / ensureRelTypeTxMembersBuilt (the K1
// label/rel-type transaction-time membership sidecar) decoded every history
// row (0x07/0x08) via a bare SafeUnmarshal into a plain NodeWire/RelWire. Under
// HistoryDeltaEncoding, a non-anchor history row can be a 'D'-tagged DELTA —
// SafeUnmarshal fails on one (its first byte isn't a valid msgpack map
// header) and the failure was silently swallowed ("skip an unreadable row;
// the fold fallback stays correct"). That comment is WRONG once
// labelTxMembersBuilt flips true: from that point on, the sidecar is the ONLY
// candidate source a pinned ByLabel/ByType scan consults (that's the whole
// point of building it — pruning the full-history fold), so a node/rel whose
// ONLY label/type evidence is a delta-encoded history row was silently never
// recorded. The sidecar's documented contract is a SOUND SUPERSET
// (over-inclusion is safe; under-inclusion breaks the contract and can make a
// pinned temporal query miss a real match).
//
// The fix needs no anchor read: DiffNodeHistory/DiffRelHistory build a
// delta's Meta as the target wire with Properties cleared, so every
// non-property field — including PrimaryLabel/ExtraLabels/RelType/TxFrom —
// survives in Meta verbatim. decodeNodeWireForMembership /
// decodeRelWireForMembership route through this instead of a bare
// SafeUnmarshal.

// deltaMembershipTestNode returns version v of a node carrying a large
// unchanging blob (so a delta reliably wins on size — mirrors deltaVersionNode
// in badgerstore_history_delta_test.go) plus the given extra label set.
func deltaMembershipTestNode(id snowflake.ID, v uint32, primary uint16, extra []uint16, txFrom int64) *types.Node {
	n := types.NewNode(types.NodeID(id), primary, extra)
	_ = n.SetProperty("blob", deltaBlob)
	n.SetVersion(v)
	n.SetTemporal(&types.TemporalMetadata{TxFrom: types.Instant(txFrom)})
	return n
}

func deltaMembershipTestRel(id, startID, endID snowflake.ID, v uint32, relType uint16, txFrom int64) *types.Relationship {
	r := types.NewRelationship(types.RelID(id), relType, types.NodeID(startID), types.NodeID(endID))
	_ = r.SetProperty("blob", deltaBlob)
	r.SetVersion(v)
	r.SetTemporal(&types.TemporalMetadata{TxFrom: types.Instant(txFrom)})
	return r
}

func testEnsureLabelTxMembersDeltaRow(t *testing.T, flush bool) {
	t.Helper()
	bs := deltaTestStore(t, true)
	const specialLabel = uint16(30)
	id := snowflake.ID(999001)

	v0 := deltaMembershipTestNode(id, 0, 10, nil, 1000) // no specialLabel — anchor
	if err := bs.PutNodeVersion(types.NodeID(id), 0, v0); err != nil {
		t.Fatalf("PutNodeVersion v0: %v", err)
	}
	v1 := deltaMembershipTestNode(id, 1, 10, []uint16{specialLabel}, 1100) // HAS specialLabel — delta
	if err := bs.PutNodeVersion(types.NodeID(id), 1, v1); err != nil {
		t.Fatalf("PutNodeVersion v1: %v", err)
	}
	// No current row for this node — its ONLY label evidence is v1.

	if flush {
		if err := bs.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}

	// Confirm v1 really is stored as a delta — else this test proves nothing.
	raw, err := bs.readHistoryNodeRaw(id, 1)
	if err != nil {
		t.Fatalf("readHistoryNodeRaw v1: %v", err)
	}
	if storepkg.HistoryValueKindOf(raw) != storepkg.HistoryDelta {
		t.Fatalf("v1 not stored as a delta (kind=%d) — test setup invalid, delta encoding did not engage", storepkg.HistoryValueKindOf(raw))
	}

	found := false
	if err := bs.ForEachLabelTxMember(specialLabel, func(nid types.NodeID, _ types.Instant) bool {
		if nid == types.NodeID(id) {
			found = true
		}
		return true
	}); err != nil {
		t.Fatalf("ForEachLabelTxMember: %v", err)
	}
	if !found {
		t.Fatal("node whose only label evidence is a delta-encoded history row was not recorded as a member — BACKLOG 18d regression")
	}
}

func TestEnsureLabelTxMembersBuilt_DeltaEncodedCommittedRowIsNotDropped(t *testing.T) {
	t.Parallel()
	testEnsureLabelTxMembersDeltaRow(t, true)
}

func TestEnsureLabelTxMembersBuilt_DeltaEncodedPendingRowIsNotDropped(t *testing.T) {
	t.Parallel()
	testEnsureLabelTxMembersDeltaRow(t, false)
}

func testEnsureRelTypeTxMembersDeltaRow(t *testing.T, flush bool) {
	t.Helper()
	bs := deltaTestStore(t, true)
	const specialType = uint16(31)
	const otherType = uint16(10)
	id := snowflake.ID(999101)
	startID := snowflake.ID(999102)
	endID := snowflake.ID(999103)

	v0 := deltaMembershipTestRel(id, startID, endID, 0, otherType, 1000) // anchor, different type
	if err := bs.PutRelVersion(types.RelID(id), 0, v0); err != nil {
		t.Fatalf("PutRelVersion v0: %v", err)
	}
	v1 := deltaMembershipTestRel(id, startID, endID, 1, specialType, 1100) // delta, HAS specialType
	if err := bs.PutRelVersion(types.RelID(id), 1, v1); err != nil {
		t.Fatalf("PutRelVersion v1: %v", err)
	}

	if flush {
		if err := bs.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}

	raw, err := bs.readHistoryRelRaw(id, 1)
	if err != nil {
		t.Fatalf("readHistoryRelRaw v1: %v", err)
	}
	if storepkg.HistoryValueKindOf(raw) != storepkg.HistoryDelta {
		t.Fatalf("v1 not stored as a delta (kind=%d) — test setup invalid", storepkg.HistoryValueKindOf(raw))
	}

	found := false
	if err := bs.ForEachRelTypeTxMember(specialType, func(rid types.RelID, _ types.Instant) bool {
		if rid == types.RelID(id) {
			found = true
		}
		return true
	}); err != nil {
		t.Fatalf("ForEachRelTypeTxMember: %v", err)
	}
	if !found {
		t.Fatal("relationship whose only type evidence is a delta-encoded history row was not recorded as a member — BACKLOG 18d regression")
	}
}

func TestEnsureRelTypeTxMembersBuilt_DeltaEncodedCommittedRowIsNotDropped(t *testing.T) {
	t.Parallel()
	testEnsureRelTypeTxMembersDeltaRow(t, true)
}

func TestEnsureRelTypeTxMembersBuilt_DeltaEncodedPendingRowIsNotDropped(t *testing.T) {
	t.Parallel()
	testEnsureRelTypeTxMembersDeltaRow(t, false)
}
