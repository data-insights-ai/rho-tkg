package badger

import (
	"encoding/hex"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// Round 4 R2: a definition with RangeCounts carries one more key, "rc"; a
// plain definition keeps its golden bytes (TestIdxDefEncodeGolden), both
// round-trip, and a reader that does not know "rc" skips it (the index then
// loads plain).
func TestIdxDefRangeCountsWire(t *testing.T) {
	for name, c := range map[string]struct {
		in   any
		out  any
		want string
	}{
		"prop": {propIdxDef{LabelToken: 3, PropertyKey: "name", RangeCounts: true}, &propIdxDef{}, "83a16ccd0003a170a46e616d65a27263c3"},
		"rel":  {relPropIdxDef{RelTypeToken: 3, PropertyKey: "name", RangeCounts: true}, &relPropIdxDef{}, "83a174cd0003a170a46e616d65a27263c3"},
	} {
		b, err := msgpack.Marshal(c.in)
		if err != nil {
			t.Fatal(err)
		}
		if got := hex.EncodeToString(b); got != c.want {
			t.Fatalf("%s: %s, want %s", name, got, c.want)
		}
		if err := msgpack.Unmarshal(b, c.out); err != nil {
			t.Fatal(err)
		}
		switch o := c.out.(type) {
		case *propIdxDef:
			if *o != c.in.(propIdxDef) {
				t.Fatalf("%s round trip: %+v", name, *o)
			}
		case *relPropIdxDef:
			if *o != c.in.(relPropIdxDef) {
				t.Fatalf("%s round trip: %+v", name, *o)
			}
		}
	}
	// A reader without the field: the "rc" key is skipped.
	b, _ := msgpack.Marshal(propIdxDef{LabelToken: 3, PropertyKey: "name", RangeCounts: true})
	var old struct {
		LabelToken  uint16 `msgpack:"l"`
		PropertyKey string `msgpack:"p"`
	}
	if err := msgpack.Unmarshal(b, &old); err != nil || old.LabelToken != 3 || old.PropertyKey != "name" {
		t.Fatalf("an older reader: %+v %v", old, err)
	}
}
