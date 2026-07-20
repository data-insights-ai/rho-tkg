package storeutil

import (
	"encoding/hex"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// historyDeltaGolden was captured via msgpack.Marshal on NodeHistoryDelta /
// RelHistoryDelta / propKeyRef BEFORE they had custom EncodeMsgpack methods
// (pure reflection-based struct encoding, matching struct tags in
// wire_history_delta.go) — see BACKLOG 15t. Locks the on-wire byte format of
// HistoryDeltaEncoding rows across the switch to hand-written encoders.
var historyDeltaGolden = map[string]string{
	"NodeHistoryDelta_min":  "81a16d86a26676cc02a26964d30000000000000064a2706c01a17602a27466d30000000000000000a27474d30000000000000000",
	"NodeHistoryDelta_full": "83a16d86a26676cc02a26964d30000000000000064a2706c01a17602a27466d30000000000000000a27474d30000000000000000a270739183a26b74cd0003a176d30000000000000009a174cc02a270729281a26b74cd000581a16ba36f6c64",
	"RelHistoryDelta_min":   "81a16d88a26676cc02a26964d300000000000000c8a2727402a173d3000000000000000aa165d30000000000000014a17602a27466d30000000000000000a27474d30000000000000000",
	"RelHistoryDelta_full":  "83a16d88a26676cc02a26964d300000000000000c8a2727402a173d3000000000000000aa165d30000000000000014a17602a27466d30000000000000000a27474d30000000000000000a270739183a26b74cd0003a176d30000000000000009a174cc02a270729281a26b74cd000581a16ba36f6c64",
	"propKeyRef_token":      "81a26b74cd0005",
	"propKeyRef_key":        "81a16ba36f6c64",
	"propKeyRef_both_zero":  "80",
}

// TestHistoryDeltaEncodeGolden proves the hand-written EncodeMsgpack methods
// for NodeHistoryDelta/RelHistoryDelta/propKeyRef (BACKLOG 15t) produce
// byte-identical output to the previous pure-reflection msgpack.Marshal
// encoding, for the all-omitted-optional-fields case and the
// every-optional-field-present case.
func TestHistoryDeltaEncodeGolden(t *testing.T) {
	nw := NodeWire{FormatVersion: 2, ID: 100, PrimaryLabel: 1, Version: 2}
	rw := RelWire{FormatVersion: 2, ID: 200, RelType: 2, StartID: 10, EndID: 20, Version: 2}
	ps := []PropertyWire{{KeyToken: 3, Value: int64(9), Type: 2}}
	pr := []propKeyRef{{Token: 5}, {Key: "old"}}

	cases := map[string]any{
		"NodeHistoryDelta_min":  NodeHistoryDelta{Meta: nw},
		"NodeHistoryDelta_full": NodeHistoryDelta{Meta: nw, PS: ps, PR: pr},
		"RelHistoryDelta_min":   RelHistoryDelta{Meta: rw},
		"RelHistoryDelta_full":  RelHistoryDelta{Meta: rw, PS: ps, PR: pr},
		"propKeyRef_token":      propKeyRef{Token: 5},
		"propKeyRef_key":        propKeyRef{Key: "old"},
		"propKeyRef_both_zero":  propKeyRef{},
	}
	for name, v := range cases {
		got, err := msgpack.Marshal(v)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		want, ok := historyDeltaGolden[name]
		if !ok {
			t.Fatalf("%s: no golden vector", name)
		}
		if h := hex.EncodeToString(got); h != want {
			t.Fatalf("%s wire bytes drifted from golden:\n got=%s\nwant=%s", name, h, want)
		}
	}
}
