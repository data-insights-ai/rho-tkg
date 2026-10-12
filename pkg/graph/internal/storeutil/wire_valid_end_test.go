package storeutil

import (
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestDecodeValidEndValue — backlog 51's write-path decode returns the row's
// temporal block on every value shape a store holds: a full node row, a full
// relationship row, a node and a relationship history delta, a row without a
// temporal block (nil); a corrupt value and a row of a newer format version
// fail (the latter with ErrWireFormatVersionUnsupported, full and delta).
//
// Not parallel: it lowers the package's readable format version.
func TestDecodeValidEndValue(t *testing.T) {
	tm := &types.TemporalMetadata{ValidFrom: 5, ValidTo: 40, TxFrom: 10, TxTo: 30, DeletedAt: 30}
	n := types.NewNode(types.NodeID(8), 3, []uint16{7})
	if err := n.SetProperty("k", "v"); err != nil {
		t.Fatal(err)
	}
	n.SetTemporal(tm)
	r := types.NewRelationship(types.RelID(9), 4, types.NodeID(2), types.NodeID(6))
	if err := r.SetProperty("k", int64(1)); err != nil {
		t.Fatal(err)
	}
	r.SetTemporal(tm)
	full := func(b []byte, err error) []byte {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	nodeFull := full(MarshalNodeWire(n))
	relFull := full(MarshalRelWire(r))
	nw, rw := NodeToWire(n), RelToWire(r)
	anchorN, anchorR := nw, rw
	anchorN.Properties, anchorR.Properties = nil, nil
	nodeDelta := full(EncodeNodeHistoryDelta(DiffNodeHistory(anchorN, nw)))
	relDelta := full(EncodeRelHistoryDelta(DiffRelHistory(anchorR, rw)))
	bare := types.NewNode(types.NodeID(8), 3, nil)
	bareFull := full(MarshalNodeWire(bare))
	for _, c := range []struct {
		name string
		raw  []byte
		node bool
		tm   bool
	}{
		{"node full", nodeFull, true, true},
		{"rel full", relFull, false, true},
		{"node delta", nodeDelta, true, true},
		{"rel delta", relDelta, false, true},
		{"no temporal block", bareFull, true, false},
	} {
		if HistoryValueKindOf(c.raw) == HistoryDelta != (c.name == "node delta" || c.name == "rel delta") {
			t.Fatalf("%s: value kind not as built", c.name)
		}
		got, err := DecodeValidEndValue(c.raw, c.node)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if !c.tm {
			if got != nil {
				t.Fatalf("%s: %+v, want no temporal block", c.name, got)
			}
			continue
		}
		if got == nil || got.ValidTo != tm.ValidTo || got.TxFrom != tm.TxFrom || got.TxTo != tm.TxTo || got.DeletedAt != tm.DeletedAt {
			t.Fatalf("%s: %+v, want %+v", c.name, got, tm)
		}
	}
	for _, node := range []bool{true, false} {
		if _, err := DecodeValidEndValue([]byte{0xc1, 0x00}, node); err == nil {
			t.Fatalf("node=%v: corrupt value decoded", node)
		}
		if _, err := DecodeValidEndValue([]byte{historyDeltaTag, 0xc1}, node); err == nil {
			t.Fatalf("node=%v: corrupt delta decoded", node)
		}
	}
	prev := maxReadableWireFormatVersion
	maxReadableWireFormatVersion = 0
	defer func() { maxReadableWireFormatVersion = prev }()
	for name, c := range map[string]struct {
		raw  []byte
		node bool
	}{"full": {nodeFull, true}, "node delta": {nodeDelta, true}, "rel delta": {relDelta, false}} {
		if _, err := DecodeValidEndValue(c.raw, c.node); !errors.Is(err, storepkg.ErrWireFormatVersionUnsupported) {
			t.Fatalf("newer %s row: %v, want ErrWireFormatVersionUnsupported", name, err)
		}
	}
}
