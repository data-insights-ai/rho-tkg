package memory

import (
	"testing"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestFrozenHistoryRowReadsAllocateNothing (HEADLEND review): the store's
// per-history-row readers outside the as-of selection read a frozen row's
// Temporal() inline: the copy it returns stays on the stack (it does not
// escape), so they allocate nothing per row now that history rows are
// frozen. The temporal-index bounds (memorystore_index.go), the label
// sidecar's TxFrom, the tx match, the retention predicate's ValidTo and the
// valid-end rows; the as-of selection, generic, needs TemporalByValue
// (TestMemoryAsOfAllocsFlatInHistory).
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

	if f, to := indexpkg.NodeTemporalBounds(a, id, n.Temporal()); f != 1000 || to != 2000 {
		t.Fatalf("NodeTemporalBounds = [%d, %d), want [1000, 2000)", f, to)
	}
	if nodeTxFrom(n) != 100 || relTxFrom(r) != 100 {
		t.Fatalf("nodeTxFrom / relTxFrom = %d / %d, want 100", nodeTxFrom(n), relTxFrom(r))
	}
	if nodeMatchesTxTime(n, 150) || relMatchesTxTime(r, 150) {
		t.Fatal("a superseded row (TxTo set) matched as current")
	}

	for name, f := range map[string]func(){
		"NodeTemporalBounds": func() { _, _ = indexpkg.NodeTemporalBounds(a, id, n.Temporal()) },
		"RelTemporalBounds":  func() { _, _ = indexpkg.RelTemporalBounds(a, rid, r.Temporal()) },
		"nodeTxFrom":         func() { _ = nodeTxFrom(n) },
		"relTxFrom":          func() { _ = relTxFrom(r) },
		"nodeMatchesTxTime":  func() { _ = nodeMatchesTxTime(n, 150) },
		"relMatchesTxTime":   func() { _ = relMatchesTxTime(r, 150) },
		"retention ValidTo": func() {
			tm := n.Temporal()
			_ = tm != nil && tm.ValidTo < 5000
		},
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
