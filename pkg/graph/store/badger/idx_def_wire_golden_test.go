package badger

import (
	"encoding/hex"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// idxDefGolden was captured via msgpack.Marshal on the index-definition
// persistence types BEFORE they had custom EncodeMsgpack methods (pure
// reflection-based struct encoding, matching struct tags) — see BACKLOG 15v.
// Locks the on-wire byte format across the switch to hand-written encoders,
// both for a single value and for the top-level slice shape the actual
// persist* call sites use.
var idxDefGolden = map[string]string{
	"hfIdxDef":              "82a16ccd0003a162d3000000000000ea60",
	"propIdxDef":            "82a16ccd0003a170a46e616d65",
	"vectorIdxDef_min":      "84a16ccd0003a170a3766563a164cc80a16dcc01",
	"vectorIdxDef_full":     "88a16ccd0003a170a3766563a164cc80a16dcc01a26266c3a2686d10a3656663ccc8a365667340",
	"compositeIdxDef":       "82a16ccd0003a16b92a161a162",
	"relPropIdxDef":         "82a174cd0005a170a6776569676874",
	"hfIdxDef_slice":        "9282a16ccd0003a162d3000000000000ea6082a16ccd0007a162d300000000000003e8",
	"propIdxDef_slice":      "9182a16ccd0003a170a46e616d65",
	"vectorIdxDef_slice":    "9184a16ccd0003a170a3766563a164cc80a16dcc01",
	"compositeIdxDef_slice": "9182a16ccd0003a16b92a161a162",
	"relPropIdxDef_slice":   "9182a174cd0005a170a6776569676874",
}

// TestIdxDefEncodeGolden proves every hand-written index-definition
// EncodeMsgpack (BACKLOG 15v) produces byte-identical output to the previous
// pure-reflection msgpack.Marshal encoding, both for a single value and for
// the top-level-slice shape actually used at the persist* call sites.
func TestIdxDefEncodeGolden(t *testing.T) {
	cases := map[string]any{
		"hfIdxDef":              hfIdxDef{LabelToken: 3, BucketSizeMillis: 60000},
		"propIdxDef":            propIdxDef{LabelToken: 3, PropertyKey: "name"},
		"vectorIdxDef_min":      vectorIdxDef{LabelToken: 3, PropertyKey: "vec", Dims: 128, Metric: DistanceCosine},
		"vectorIdxDef_full":     vectorIdxDef{LabelToken: 3, PropertyKey: "vec", Dims: 128, Metric: DistanceCosine, UseBruteForce: true, M: 16, EfConstruction: 200, EfSearch: 64},
		"compositeIdxDef":       compositeIdxDef{LabelToken: 3, Keys: []string{"a", "b"}},
		"relPropIdxDef":         relPropIdxDef{RelTypeToken: 5, PropertyKey: "weight"},
		"hfIdxDef_slice":        []hfIdxDef{{LabelToken: 3, BucketSizeMillis: 60000}, {LabelToken: 7, BucketSizeMillis: 1000}},
		"propIdxDef_slice":      []propIdxDef{{LabelToken: 3, PropertyKey: "name"}},
		"vectorIdxDef_slice":    []vectorIdxDef{{LabelToken: 3, PropertyKey: "vec", Dims: 128, Metric: DistanceCosine}},
		"compositeIdxDef_slice": []compositeIdxDef{{LabelToken: 3, Keys: []string{"a", "b"}}},
		"relPropIdxDef_slice":   []relPropIdxDef{{RelTypeToken: 5, PropertyKey: "weight"}},
	}
	for name, v := range cases {
		got, err := msgpack.Marshal(v)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		want, ok := idxDefGolden[name]
		if !ok {
			t.Fatalf("%s: no golden vector", name)
		}
		if h := hex.EncodeToString(got); h != want {
			t.Fatalf("%s wire bytes drifted from golden:\n got=%s\nwant=%s", name, h, want)
		}
	}
}
