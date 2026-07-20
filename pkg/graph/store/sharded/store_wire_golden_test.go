package sharded

import (
	"encoding/hex"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/vmihailenco/msgpack/v5"
)

// storeWireGolden was captured via msgpack.Marshal on vectorDefBlob/
// slotCatalog BEFORE they had custom EncodeMsgpack methods (pure
// reflection-based struct encoding) — see BACKLOG 15v. Locks the on-wire
// byte format across the switch to hand-written encoders.
//
// slotCatalog's SlotShard map field is deliberately EXCLUDED here for the
// non-empty-map case: Go map iteration order is randomized, so the
// reflective baseline for a populated map is itself non-deterministic byte-
// for-byte across runs — a fixed hex golden for that case would be flaky,
// not load-bearing. slotCatalog_nilmap (SlotShard == nil) has no map-
// ordering ambiguity (msgpack nil either way) and is safe to golden-pin; the
// non-empty-map case is instead verified via round-trip correctness in
// store_wire_roundtrip_test.go, which is order-independent.
var storeWireGolden = map[string]string{
	"vectorDefBlob":       "84a16ccd0003a170a3766563a164cc80a16dcc01",
	"vectorDefBlob_slice": "9184a16ccd0001a170a161a16402a16dcc01",
	"slotCatalog_nilmap":  "85a176cc01a162cc00a16ecc00a164cc01a16dc0",
}

// TestStoreWireEncodeGolden proves the hand-written vectorDefBlob/slotCatalog
// EncodeMsgpack methods (BACKLOG 15v) produce byte-identical output to the
// previous pure-reflection msgpack.Marshal encoding, for the deterministic
// cases (see storeWireGolden's doc comment for why the non-empty-map
// slotCatalog case is excluded from byte-golden comparison).
func TestStoreWireEncodeGolden(t *testing.T) {
	cases := map[string]any{
		"vectorDefBlob":       vectorDefBlob{LabelToken: 3, PropertyKey: "vec", Dims: 128, Metric: storecontract.DistanceCosine},
		"vectorDefBlob_slice": []vectorDefBlob{{LabelToken: 1, PropertyKey: "a", Dims: 2, Metric: storecontract.DistanceCosine}},
		"slotCatalog_nilmap":  &slotCatalog{FormatVersion: 1, BaseSlot: 0, SlotCount: 0, Discipline: disciplineUnified},
	}
	for name, v := range cases {
		got, err := msgpack.Marshal(v)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		want, ok := storeWireGolden[name]
		if !ok {
			t.Fatalf("%s: no golden vector", name)
		}
		if h := hex.EncodeToString(got); h != want {
			t.Fatalf("%s wire bytes drifted from golden:\n got=%s\nwant=%s", name, h, want)
		}
	}
}
