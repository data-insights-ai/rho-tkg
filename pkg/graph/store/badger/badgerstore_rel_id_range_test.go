package badger

import (
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRelIDRange_RowsHistoryAndPending: the range spans current rows and
// history rows, pending (unflushed) writes included, and is empty for a store
// with neither; a deleted relationship's history keeps its ID in the range.
func TestRelIDRange_RowsHistoryAndPending(t *testing.T) {
	for _, flush := range []time.Duration{0, time.Hour} {
		bs, err := New(Config{Dir: t.TempDir(), FlushInterval: flush})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, ok, err := bs.RelIDRange(); err != nil || ok {
			t.Fatalf("empty store: ok %v, %v", ok, err)
		}
		a, b := types.NodeID(1<<40), types.NodeID(2<<40)
		for _, id := range []types.NodeID{a, b} {
			if err := bs.PutNode(types.NewNode(id, 1, nil)); err != nil {
				t.Fatal(err)
			}
		}
		low, mid, high := types.RelID(5<<40), types.RelID(7<<40), types.RelID(9<<40)
		for _, id := range []types.RelID{mid, high} {
			if err := bs.PutRelationship(types.NewRelationship(id, 1, a, b)); err != nil {
				t.Fatal(err)
			}
		}
		if err := bs.PutRelVersion(low, 0, types.NewRelationship(low, 1, a, b)); err != nil {
			t.Fatal(err)
		}
		if err := bs.DeleteRelationship(high); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutRelVersion(high, 0, types.NewRelationship(high, 1, a, b)); err != nil {
			t.Fatal(err)
		}
		lo, hi, ok, err := bs.RelIDRange()
		if err != nil || !ok || lo != snowflake.ID(low) || hi != snowflake.ID(high) {
			t.Fatalf("flush %v: range %d..%d ok %v err %v, want %d..%d", flush, lo, hi, ok, err, low, high)
		}
		_ = bs.Close()
	}
}
