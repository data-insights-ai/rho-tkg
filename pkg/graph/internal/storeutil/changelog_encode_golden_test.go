package storeutil

import (
	"encoding/hex"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// changeBodyGolden was captured via msgpack.Marshal on the change-log body
// types BEFORE they had custom EncodeMsgpack methods (pure reflection-based
// struct encoding, matching struct tags in changelog.go) — see BACKLOG 15s.
// Locks the on-wire byte format across the switch to hand-written encoders:
// if a future edit to any body type's EncodeMsgpack changes these bytes, a
// change-log/replica-apply wire-format regression breaks cross-version
// compatibility and this test fails first.
var changeBodyGolden = map[string]string{
	"NodePutBody_min":          "81a17786a26676cc02a26964d30000000000000064a2706c01a17601a27466d30000000000000000a27474d30000000000000000",
	"NodePutBody_wh":           "82a17786a26676cc02a26964d30000000000000064a2706c01a17601a27466d30000000000000000a27474d30000000000000000a27768c3",
	"RelPutBody_min":           "81a17788a26676cc02a26964d300000000000000c8a2727402a173d3000000000000000aa165d30000000000000014a17601a27466d30000000000000000a27474d30000000000000000",
	"RelPutBody_wh":            "82a17788a26676cc02a26964d300000000000000c8a2727402a173d3000000000000000aa165d30000000000000014a17601a27466d30000000000000000a27474d30000000000000000a27768c3",
	"NodeDeleteBody_min":       "81a26964d30000000000000005",
	"NodeDeleteBody_full":      "85a26964d30000000000000005a27768c3a2746e86a26676cc02a26964d30000000000000064a2706c01a17601a27466d30000000000000000a27474d30000000000000000a272749188a26676cc02a26964d300000000000000c8a2727402a173d3000000000000000aa165d30000000000000014a17601a27466d30000000000000000a27474d30000000000000000a2637292d300000000000000c8d300000000000000c9",
	"RelDeleteBody_min":        "81a26964d30000000000000007",
	"RelDeleteBody_full":       "83a26964d30000000000000007a27768c3a2747288a26676cc02a26964d300000000000000c8a2727402a173d3000000000000000aa165d30000000000000014a17601a27466d30000000000000000a27474d30000000000000000",
	"ForeignIncomingDeleteBody": "82a26964d30000000000000009a165d3000000000000000b",
	"RangePurgeBody_min":       "82a16ccd0003a162d300000000000003e8",
	"RangePurgeBody_mode":      "83a16ccd0003a162d300000000000003e8a16dcc01",
	"HistoryVersionNodeBody":   "82a176cf0000000000000004a17786a26676cc02a26964d30000000000000064a2706c01a17601a27466d30000000000000000a27474d30000000000000000",
	"HistoryVersionRelBody":    "82a176cf0000000000000004a17788a26676cc02a26964d300000000000000c8a2727402a173d3000000000000000aa165d30000000000000014a17601a27466d30000000000000000a27474d30000000000000000",
	"HistoryTruncateBody_min":  "82a26964d3000000000000000da162d30000000000000003",
	"HistoryTruncateBody_trim": "83a26964d3000000000000000da27472c3a162d30000000000000003",
	"MetaBody_min":             "81a16ba16b",
	"MetaBody_val":             "82a16ba16ba176c40176",
}

// TestChangeBodyEncodeGolden proves every hand-written change-log body
// EncodeMsgpack (BACKLOG 15s) produces byte-identical output to the previous
// pure-reflection msgpack.Marshal encoding, for both the all-omitted-optional-
// fields case and the every-optional-field-present case of each of the 10
// body types.
func TestChangeBodyEncodeGolden(t *testing.T) {
	nw := NodeWire{FormatVersion: 2, ID: 100, PrimaryLabel: 1, Version: 1}
	rw := RelWire{FormatVersion: 2, ID: 200, RelType: 2, StartID: 10, EndID: 20, Version: 1}

	cases := map[string]any{
		"NodePutBody_min":           NodePutBody{Wire: nw},
		"NodePutBody_wh":            NodePutBody{Wire: nw, WithHistory: true},
		"RelPutBody_min":            RelPutBody{Wire: rw},
		"RelPutBody_wh":             RelPutBody{Wire: rw, WithHistory: true},
		"NodeDeleteBody_min":        NodeDeleteBody{ID: 5},
		"NodeDeleteBody_full":       NodeDeleteBody{ID: 5, WithHistory: true, Tombstone: &nw, RelTombstones: []RelWire{rw}, CascadedRelIDs: []int64{200, 201}},
		"RelDeleteBody_min":         RelDeleteBody{ID: 7},
		"RelDeleteBody_full":        RelDeleteBody{ID: 7, WithHistory: true, Tombstone: &rw},
		"ForeignIncomingDeleteBody": ForeignIncomingDeleteBody{RelID: 9, EndID: 11},
		"RangePurgeBody_min":        RangePurgeBody{LabelToken: 3, Before: 1000},
		"RangePurgeBody_mode":       RangePurgeBody{LabelToken: 3, Before: 1000, Mode: 1},
		"HistoryVersionNodeBody":    HistoryVersionNodeBody{Version: 4, Wire: nw},
		"HistoryVersionRelBody":     HistoryVersionRelBody{Version: 4, Wire: rw},
		"HistoryTruncateBody_min":   HistoryTruncateBody{ID: 13, Bound: 3},
		"HistoryTruncateBody_trim":  HistoryTruncateBody{ID: 13, IsTrim: true, Bound: 3},
		"MetaBody_min":              MetaBody{Key: "k"},
		"MetaBody_val":              MetaBody{Key: "k", Value: []byte("v")},
	}
	for name, v := range cases {
		got, err := msgpack.Marshal(v)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		want, ok := changeBodyGolden[name]
		if !ok {
			t.Fatalf("%s: no golden vector", name)
		}
		if h := hex.EncodeToString(got); h != want {
			t.Fatalf("%s wire bytes drifted from golden:\n got=%s\nwant=%s", name, h, want)
		}
	}
}
