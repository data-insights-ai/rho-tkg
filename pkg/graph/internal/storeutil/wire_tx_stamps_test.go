package storeutil

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// DecodeWireTxStamps / HistoryValueTxStamps (backlog 30) must read the same
// TxFrom / TxTo / DeletedAt the full decode does, for full node and
// relationship rows, delta rows of both kinds, and rows without a temporal
// block (zeros); a newer format version and a corrupt value fail. Distinct
// values per field catch a decoder that reads the wrong field.
func TestHistoryValueTxStamps(t *testing.T) {
	tm := &types.TemporalMetadata{ValidFrom: 1, ValidTo: 2, TxFrom: 30, TxTo: 40, DeletedAt: 50, UpdatedAt: 60}

	n := types.NewNode(types.NodeID(snowflake.ID(7)), 1, nil)
	n.SetVersion(4)
	n.SetTemporal(tm)
	full, err := MarshalNodeWire(n)
	if err != nil {
		t.Fatal(err)
	}
	r := types.NewRelationship(types.RelID(snowflake.ID(9)), 2, types.NodeID(1), types.NodeID(2))
	r.SetVersion(4)
	r.SetTemporal(tm)
	relFull, err := MarshalRelWire(r)
	if err != nil {
		t.Fatal(err)
	}
	anchorNode := types.NewNode(types.NodeID(snowflake.ID(7)), 1, nil)
	anchorNode.SetTemporal(&types.TemporalMetadata{TxFrom: 1})
	nodeDelta, err := EncodeNodeHistoryDelta(DiffNodeHistory(NodeToWire(anchorNode), NodeToWire(n)))
	if err != nil {
		t.Fatal(err)
	}
	anchorRel := types.NewRelationship(types.RelID(snowflake.ID(9)), 2, types.NodeID(1), types.NodeID(2))
	anchorRel.SetTemporal(&types.TemporalMetadata{TxFrom: 1})
	relDelta, err := EncodeRelHistoryDelta(DiffRelHistory(RelToWire(anchorRel), RelToWire(r)))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		raw  []byte
		node bool
	}{
		{"node full", full, true},
		{"rel full", relFull, false},
		{"node delta", nodeDelta, true},
		{"rel delta", relDelta, false},
	} {
		f, to, da, err := HistoryValueTxStamps(tc.raw, tc.node)
		if err != nil || f != 30 || to != 40 || da != 50 {
			t.Fatalf("%s: HistoryValueTxStamps = (%d, %d, %d, %v), want (30, 40, 50)", tc.name, f, to, da, err)
		}
	}
	if f, to, da, err := DecodeWireTxStamps(full); err != nil || f != 30 || to != 40 || da != 50 {
		t.Fatalf("DecodeWireTxStamps = (%d, %d, %d, %v)", f, to, da, err)
	}
	if a := testing.AllocsPerRun(50, func() { _, _, _, _ = DecodeWireTxStamps(full) }); a != 0 {
		t.Fatalf("DecodeWireTxStamps allocates %.0f per full row", a)
	}

	bare := types.NewNode(types.NodeID(snowflake.ID(8)), 1, nil)
	bareRaw, err := MarshalNodeWire(bare)
	if err != nil {
		t.Fatal(err)
	}
	if f, to, da, err := HistoryValueTxStamps(bareRaw, true); err != nil || f != 0 || to != 0 || da != 0 {
		t.Fatalf("no temporal block = (%d, %d, %d, %v), want zeros", f, to, da, err)
	}

	newer := NodeToWire(n)
	newer.FormatVersion = CurrentWireFormatVersion + 1
	newerRaw, err := MarshalNodeWireStruct(newer)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := HistoryValueTxStamps(newerRaw, true); !errors.Is(err, storepkg.ErrWireFormatVersionUnsupported) {
		t.Fatalf("newer format = %v, want ErrWireFormatVersionUnsupported", err)
	}
	for _, bad := range [][]byte{{0xc1}, {'D', 0xc1}} {
		if _, _, _, err := HistoryValueTxStamps(bad, true); err == nil {
			t.Fatalf("corrupt value %x decoded", bad)
		}
		if _, _, _, err := HistoryValueTxStamps(bad, false); err == nil {
			t.Fatalf("corrupt rel value %x decoded", bad)
		}
	}
}
