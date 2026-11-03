package memory

import (
	"testing"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestFrozenHistoryRowReadsAllocateNothing (HEADLEND review): the store's
// per-history-row readers take a frozen row's temporal block by value, so
// they allocate nothing per row (a frozen row's Temporal() copies), and they
// answer what Temporal() answers.
func TestFrozenHistoryRowReadsAllocateNothing(t *testing.T) {
	tm := types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000, TxFrom: 100, TxTo: 200}
	n := types.NewNode(types.NodeID(10), 1, nil)
	ntm := tm
	n.SetTemporal(&ntm)
	n.Freeze()
	r := types.NewRelationship(types.RelID(100), 1, types.NodeID(10), types.NodeID(20))
	rtm := tm
	r.SetTemporal(&rtm)
	r.Freeze()
	a := types.LegacyValidAxis
	id := n.ID().SnowflakeID()
	rid := r.ID().SnowflakeID()

	if f, to := nodeTemporalBoundsOf(a, id, n); f != 1000 || to != 2000 {
		t.Fatalf("nodeTemporalBoundsOf = [%d, %d), want [1000, 2000)", f, to)
	}
	if f, to := relTemporalBoundsOf(a, rid, r); f != 1000 || to != 2000 {
		t.Fatalf("relTemporalBoundsOf = [%d, %d), want [1000, 2000)", f, to)
	}
	bare := types.NewNode(types.NodeID(11), 1, nil)
	wantF, wantTo := indexpkg.NodeTemporalBounds(a, bare.ID().SnowflakeID(), nil)
	if f, to := nodeTemporalBoundsOf(a, bare.ID().SnowflakeID(), bare); f != wantF || to != wantTo {
		t.Fatalf("nodeTemporalBoundsOf without a block = [%d, %d), want [%d, %d)", f, to, wantF, wantTo)
	}
	if nodeTxFrom(n) != 100 || relTxFrom(r) != 100 {
		t.Fatalf("nodeTxFrom / relTxFrom = %d / %d, want 100", nodeTxFrom(n), relTxFrom(r))
	}
	if nodeMatchesTxTime(n, 150) || relMatchesTxTime(r, 150) {
		t.Fatal("a superseded row (TxTo set) matched as current")
	}
	if row := indexpkg.ValidEndRowOfNode(n, true); row.TxFrom != 100 || row.TxTo != 200 || row.ValidTo != 2000 {
		t.Fatalf("ValidEndRowOfNode = %+v", row)
	}
	if row := indexpkg.ValidEndRowOfRel(r, true); row.TxFrom != 100 || row.TxTo != 200 || row.ValidTo != 2000 {
		t.Fatalf("ValidEndRowOfRel = %+v", row)
	}

	for name, f := range map[string]func(){
		"nodeTemporalBoundsOf": func() { _, _ = nodeTemporalBoundsOf(a, id, n) },
		"relTemporalBoundsOf":  func() { _, _ = relTemporalBoundsOf(a, rid, r) },
		"nodeTxFrom":           func() { _ = nodeTxFrom(n) },
		"relTxFrom":            func() { _ = relTxFrom(r) },
		"nodeMatchesTxTime":    func() { _ = nodeMatchesTxTime(n, 150) },
		"relMatchesTxTime":     func() { _ = relMatchesTxTime(r, 150) },
	} {
		if allocs := testing.AllocsPerRun(10, f); allocs != 0 {
			t.Errorf("%s on a frozen row: %.0f allocs, want 0", name, allocs)
		}
	}
	// ValidEndRowOfNode / ValidEndRowOfRel allocate their token slice (one
	// label, one type), nothing more.
	if allocs := testing.AllocsPerRun(10, func() { _ = indexpkg.ValidEndRowOfNode(n, true) }); allocs > 1 {
		t.Errorf("ValidEndRowOfNode on a frozen row: %.0f allocs, want <= 1 (the token slice)", allocs)
	}
	if allocs := testing.AllocsPerRun(10, func() { _ = indexpkg.ValidEndRowOfRel(r, true) }); allocs > 1 {
		t.Errorf("ValidEndRowOfRel on a frozen row: %.0f allocs, want <= 1 (the token slice)", allocs)
	}
}
