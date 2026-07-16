package storeutil

import (
	"encoding/hex"
	"testing"
)

// The golden vectors below were captured from the reflective property-slice
// encode (enc.Encode([]PropertyWire{...})) BEFORE encodePropertyArray replaced
// it. They lock the on-wire byte format of property-bearing rows across the
// change, which the marshalWirePooled-vs-msgpack.Marshal test CANNOT do (both
// sides route through the same EncodeMsgpack, so that test would compare
// new-against-new and pass vacuously). If a future edit to EncodeMsgpack or
// PropertyWire.EncodeMsgpack changes these bytes, the content hash, replica
// byte-exactness, and v1/v2 wire compatibility all break — this test fails first.
var wireGolden = map[string]string{
	// v2 node, tokenized key, one int64 property + hash + trailing temporal tail.
	"node_v2_1prop": "88a26676cc02a26964d30000000000003039a2706c01a1709183a26b74cd0003a176d30000000000000063a174cc02a17603a168ac616263313233646566343536a27466d30000000000000000a27474d30000000000000000",
	// v1 node, string keys, int64 + string properties.
	"node_v1_2prop": "84a26964d3000000000000000ba2706c02a1709283a16ba16ba176d30000000000000001a174cc0283a16ba173a176a176a174cc03a17600",
	// v2 node, extra labels + float/bool/blob(custom-pointer) properties — the
	// full PropertyWire optional-field matrix.
	"node_v2_mixed": "88a26676cc02a26964d30000000000000007a2706c02a2656c920506a1709383a26b74cd0009a176cb40091eb851eb851fa174cc0483a16ba162a176c3a174cc0185a26b74cd0002a176c4027879a174cc05a26374a4626c6f62a26370c3a17600a27466d30000000000000000a27474d30000000000000000",
	// v2 rel, int64 + string properties, hash tail.
	"rel_v2_2prop": "8aa26676cc02a26964d30000000000000005a2727402a173d30000000000000003a165d30000000000000004a1709283a26b74cd0001a176d300000000000007eaa174cc0283a16ba16ea176a178a174cc03a17600a168a168a27466d30000000000000000a27474d30000000000000000",
}

func TestWireEncodePropertyGolden(t *testing.T) {
	nodeCases := map[string]NodeWire{
		"node_v2_1prop": {FormatVersion: 2, ID: 12345, PrimaryLabel: 1, Version: 3, Hash: "abc123def456",
			Properties: []PropertyWire{{KeyToken: 3, Value: int64(99), Type: 2}}},
		"node_v1_2prop": {ID: 11, PrimaryLabel: 2,
			Properties: []PropertyWire{{Key: "k", Value: int64(1), Type: 2}, {Key: "s", Value: "v", Type: 3}}},
		"node_v2_mixed": {FormatVersion: 2, ID: 7, PrimaryLabel: 2, ExtraLabels: []int{5, 6},
			Properties: []PropertyWire{
				{KeyToken: 9, Value: 3.14, Type: 4},
				{Key: "b", Value: true, Type: 1},
				{KeyToken: 2, Value: []byte("xy"), Type: 5, CustomType: "blob", CustomPointer: true},
			}},
	}
	for name, w := range nodeCases {
		got, err := marshalWirePooled(w)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if h := hex.EncodeToString(got); h != wireGolden[name] {
			t.Fatalf("%s wire bytes drifted from golden:\n got=%s\nwant=%s", name, h, wireGolden[name])
		}
	}

	rel := RelWire{FormatVersion: 2, ID: 5, RelType: 2, StartID: 3, EndID: 4, Hash: "h",
		Properties: []PropertyWire{{KeyToken: 1, Value: int64(2026), Type: 2}, {Key: "n", Value: "x", Type: 3}}}
	got, err := marshalWirePooled(rel)
	if err != nil {
		t.Fatal(err)
	}
	if h := hex.EncodeToString(got); h != wireGolden["rel_v2_2prop"] {
		t.Fatalf("rel_v2_2prop wire bytes drifted from golden:\n got=%s\nwant=%s", h, wireGolden["rel_v2_2prop"])
	}
}
