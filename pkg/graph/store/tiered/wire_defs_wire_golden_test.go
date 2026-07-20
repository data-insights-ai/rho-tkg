package tiered

import (
	"encoding/hex"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// tieredWireGolden was captured via msgpack.Marshal on RegistryFileData/
// vectorIdxDef/tieredHFIdef/temporalIndexFileData BEFORE they had custom
// EncodeMsgpack methods (pure reflection-based struct encoding) — see
// BACKLOG 15v. Locks the on-wire byte format across the switch to
// hand-written encoders. temporalIndexFileData_min deliberately exercises a
// nil (never-populated) HighFrequency field to lock the nil-vs-empty-array
// distinction reflection's non-omitempty slice encoding preserves.
var tieredWireGolden = map[string]string{
	"RegistryFileData":           "82a66c6162656c7392a141a142a872656c747970657391a352454c",
	"vectorIdxDef_min":           "84a16ccd0003a170a3766563a164cc80a16dcc01",
	"vectorIdxDef_full":          "88a16ccd0003a170a3766563a164cc80a16dcc01a26266c3a2686d10a3656663ccc8a365667340",
	"vectorIdxDef_slice":         "9184a16ccd0001a170a161a16402a16dcc01",
	"tieredHFIdef":               "82a16ccd0003a162d3000000000000ea60",
	"temporalIndexFileData_min":  "82a17492cd0001cd0002a168c0",
	"temporalIndexFileData_full": "82a17492cd0001cd0002a1689282a16ccd0003a162d3000000000000ea6082a16ccd0004a162d300000000000003e8",
}

// TestTieredWireEncodeGolden proves every hand-written store-level
// persistence-type EncodeMsgpack (BACKLOG 15v) produces byte-identical output
// to the previous pure-reflection msgpack.Marshal encoding.
func TestTieredWireEncodeGolden(t *testing.T) {
	cases := map[string]any{
		"RegistryFileData":          &RegistryFileData{Labels: []string{"A", "B"}, RelTypes: []string{"REL"}},
		"vectorIdxDef_min":          vectorIdxDef{LabelToken: 3, PropertyKey: "vec", Dims: 128, Metric: DistanceCosine},
		"vectorIdxDef_full":         vectorIdxDef{LabelToken: 3, PropertyKey: "vec", Dims: 128, Metric: DistanceCosine, UseBruteForce: true, M: 16, EfConstruction: 200, EfSearch: 64},
		"vectorIdxDef_slice":        []vectorIdxDef{{LabelToken: 1, PropertyKey: "a", Dims: 2, Metric: DistanceCosine}},
		"tieredHFIdef":              tieredHFIdef{LabelToken: 3, BucketSizeMillis: 60000},
		"temporalIndexFileData_min": &temporalIndexFileData{TemporalLabels: []uint16{1, 2}},
		"temporalIndexFileData_full": &temporalIndexFileData{
			TemporalLabels: []uint16{1, 2},
			HighFrequency:  []tieredHFIdef{{LabelToken: 3, BucketSizeMillis: 60000}, {LabelToken: 4, BucketSizeMillis: 1000}},
		},
	}
	for name, v := range cases {
		got, err := msgpack.Marshal(v)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		want, ok := tieredWireGolden[name]
		if !ok {
			t.Fatalf("%s: no golden vector", name)
		}
		if h := hex.EncodeToString(got); h != want {
			t.Fatalf("%s wire bytes drifted from golden:\n got=%s\nwant=%s", name, h, want)
		}
	}
}
