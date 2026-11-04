//go:build measure

// Measurement level (René 4 Nov, `make measure`): testing.AllocsPerRun counts
// every allocation in the process, and this package runs parallel tests, so
// the bounds run by hand, not in a gate (MEMGATE).

package memory

import (
	"testing"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestFrozenHistoryRowReadsAllocateNothing (HEADLEND review): the readers of
// TestFrozenHistoryRowReads read a frozen row's Temporal() inline: the copy it
// returns stays on the stack (it does not escape), so they allocate nothing
// per row now that history rows are frozen. Also the retention predicate's
// ValidTo and the valid-end rows; the as-of selection, generic, needs
// TemporalByValue (TestMemoryAsOfAllocsFlatInHistory).
func TestFrozenHistoryRowReadsAllocateNothing(t *testing.T) {
	n, r := frozenRows()
	a := types.LegacyValidAxis
	id := n.ID().SnowflakeID()
	rid := r.ID().SnowflakeID()
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
