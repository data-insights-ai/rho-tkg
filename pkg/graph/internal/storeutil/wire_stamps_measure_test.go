//go:build measure

// Measurement level (René 4 Nov, `scripts/measure.sh`): testing.AllocsPerRun reads
// the process-wide malloc count, and this package runs parallel tests, so the
// bound runs by hand, not in a gate.

package storeutil

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestDecodeWireStampsNoAllocs (backlogs 30 and 42): DecodeWireRowStamps and
// DecodeWireTxStamps read a stored row's stamps without allocating.
// TestDecodeWireRowStamps and TestHistoryValueTxStamps pin the values.
func TestDecodeWireStampsNoAllocs(t *testing.T) {
	n := types.NewNode(types.NodeID(snowflake.ID(7)), 1, nil)
	n.SetVersion(4)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: 1, ValidTo: 2, TxFrom: 30, TxTo: 40, DeletedAt: 50, UpdatedAt: 60})
	raw, err := MarshalNodeWire(n)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeWireRowStamps(raw); err != nil || got.TxFrom != 30 {
		t.Fatalf("DecodeWireRowStamps = (%+v, %v)", got, err)
	}
	if a := testing.AllocsPerRun(50, func() { _, _ = DecodeWireRowStamps(raw) }); a != 0 {
		t.Errorf("DecodeWireRowStamps allocates %.0f per row", a)
	}
	if a := testing.AllocsPerRun(50, func() { _, _, _, _ = DecodeWireTxStamps(raw) }); a != 0 {
		t.Errorf("DecodeWireTxStamps allocates %.0f per full row", a)
	}
}
