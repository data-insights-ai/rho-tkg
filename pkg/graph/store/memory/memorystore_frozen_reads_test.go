package memory

import (
	"testing"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// frozenRows is a frozen node and relationship with the temporal block
// [1000, 2000) valid, [100, 200) tx.
func frozenRows() (*types.Node, *types.Relationship) {
	tm := types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000, TxFrom: 100, TxTo: 200}
	n := types.NewNode(types.NodeID(10), 1, nil)
	ntm := tm
	n.SetTemporal(&ntm)
	n.Freeze()
	r := types.NewRelationship(types.RelID(100), 1, types.NodeID(10), types.NodeID(20))
	rtm := tm
	r.SetTemporal(&rtm)
	r.Freeze()
	return n, r
}

// TestFrozenHistoryRowReads (HEADLEND review): the store's per-history-row
// readers outside the as-of selection answer from a frozen row's Temporal():
// the temporal-index bounds (memorystore_index.go), the label sidecar's
// TxFrom and the tx match. That they allocate nothing per row is
// TestFrozenHistoryRowReadsAllocateNothing, at the measurement level
// (memorystore_frozen_reads_measure_test.go, MEMGATE).
func TestFrozenHistoryRowReads(t *testing.T) {
	n, r := frozenRows()
	if f, to := indexpkg.NodeTemporalBounds(types.LegacyValidAxis, n.ID().SnowflakeID(), n.Temporal()); f != 1000 || to != 2000 {
		t.Fatalf("NodeTemporalBounds = [%d, %d), want [1000, 2000)", f, to)
	}
	if nodeTxFrom(n) != 100 || relTxFrom(r) != 100 {
		t.Fatalf("nodeTxFrom / relTxFrom = %d / %d, want 100", nodeTxFrom(n), relTxFrom(r))
	}
	if nodeMatchesTxTime(n, 150) || relMatchesTxTime(r, 150) {
		t.Fatal("a superseded row (TxTo set) matched as current")
	}
}
