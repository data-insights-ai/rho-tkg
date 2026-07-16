package storeutil

import (
	"bytes"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// TestMarshalWirePooledByteIdentical is the load-bearing invariant: the pooled
// encoder must produce EXACTLY the same bytes as msgpack.Marshal for every wire
// shape, or the content hash / replica byte-exactness / v1-v2 wire format would
// silently diverge. Covers empty, property-bearing, temporal-tail (v1 and v2),
// and integrity-bearing rows for both Node and Rel wires.
func TestMarshalWirePooledByteIdentical(t *testing.T) {
	nodeWires := []NodeWire{
		{ID: 1, PrimaryLabel: 2, Version: 0},
		{ID: 3, PrimaryLabel: 2, Version: 1, ExtraLabels: []int{5, 6}},
		{ID: 7, PrimaryLabel: 2, FormatVersion: 2, TxFrom: 111, TxTo: 0},
		{ID: 8, PrimaryLabel: 2, FormatVersion: 1, TxFrom: 111},
		{ID: 9, PrimaryLabel: 2, HasTemporal: true, ValidFrom: 10, ValidTo: 20, CreatedAt: 5},
		{ID: 10, PrimaryLabel: 2, Hash: "abc", PrevHash: "def", CreatedBy: "u", Version: 4},
		{ID: 11, PrimaryLabel: 2, Properties: []PropertyWire{{Key: "k", Value: int64(1)}, {Key: "s", Value: "v"}}},
	}
	for i, w := range nodeWires {
		want, err := msgpack.Marshal(w)
		if err != nil {
			t.Fatalf("node[%d] Marshal: %v", i, err)
		}
		got, err := marshalWirePooled(w)
		if err != nil {
			t.Fatalf("node[%d] pooled: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("node[%d] pooled bytes differ from Marshal:\n got=%x\nwant=%x", i, got, want)
		}
	}

	relWires := []RelWire{
		{ID: 1, RelType: 2, StartID: 3, EndID: 4, Version: 0},
		{ID: 5, RelType: 2, StartID: 3, EndID: 4, FormatVersion: 2, TxFrom: 9, TxTo: 0},
		{ID: 6, RelType: 2, StartID: 3, EndID: 4, Properties: []PropertyWire{{Key: "w", Value: int64(2026)}}},
		{ID: 7, RelType: 2, StartID: 3, EndID: 4, Hash: "h", FromNodeHash: "f", ToNodeHash: "t"},
	}
	for i, w := range relWires {
		want, err := msgpack.Marshal(w)
		if err != nil {
			t.Fatalf("rel[%d] Marshal: %v", i, err)
		}
		got, err := marshalWirePooled(w)
		if err != nil {
			t.Fatalf("rel[%d] pooled: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("rel[%d] pooled bytes differ from Marshal:\n got=%x\nwant=%x", i, got, want)
		}
	}

	// Reuse: the same pooled buffer serves many encodes; a prior larger encode
	// must not leak trailing bytes into a later smaller one.
	big, _ := marshalWirePooled(NodeWire{ID: 99, PrimaryLabel: 1, Properties: []PropertyWire{{Key: "long-key-name", Value: "a fairly long string value to grow the buffer"}}})
	small, _ := marshalWirePooled(NodeWire{ID: 1, PrimaryLabel: 1})
	wantSmall, _ := msgpack.Marshal(NodeWire{ID: 1, PrimaryLabel: 1})
	if !bytes.Equal(small, wantSmall) {
		t.Fatalf("pooled reuse leaked bytes: got=%x want=%x", small, wantSmall)
	}
	_ = big
}
