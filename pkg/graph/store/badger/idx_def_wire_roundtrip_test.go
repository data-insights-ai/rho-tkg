package badger

import (
	"reflect"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// TestIdxDefRoundTrip proves every hand-written index-definition
// EncodeMsgpack/DecodeMsgpack pair reproduces the original
// value exactly, both for a single value and for a slice (the shape the
// persist*/load* call sites actually use).
func TestIdxDefRoundTrip(t *testing.T) {
	t.Parallel()
	t.Run("single", func(t *testing.T) {
		cases := []struct {
			name string
			in   any
			out  any
		}{
			{"hfIdxDef", hfIdxDef{LabelToken: 3, BucketSizeMillis: 60000}, &hfIdxDef{}},
			{"propIdxDef", propIdxDef{LabelToken: 3, PropertyKey: "name"}, &propIdxDef{}},
			{"vectorIdxDef_min", vectorIdxDef{LabelToken: 3, PropertyKey: "vec", Dims: 128, Metric: DistanceCosine}, &vectorIdxDef{}},
			{"vectorIdxDef_full", vectorIdxDef{LabelToken: 3, PropertyKey: "vec", Dims: 128, Metric: DistanceCosine, UseBruteForce: true, M: 16, EfConstruction: 200, EfSearch: 64}, &vectorIdxDef{}},
			{"compositeIdxDef", compositeIdxDef{LabelToken: 3, Keys: []string{"a", "b"}}, &compositeIdxDef{}},
			{"relPropIdxDef", relPropIdxDef{RelTypeToken: 5, PropertyKey: "weight"}, &relPropIdxDef{}},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				b, err := msgpack.Marshal(c.in)
				if err != nil {
					t.Fatalf("Marshal: %v", err)
				}
				if err := msgpack.Unmarshal(b, c.out); err != nil {
					t.Fatalf("Unmarshal: %v", err)
				}
				got := reflect.ValueOf(c.out).Elem().Interface()
				if !reflect.DeepEqual(got, c.in) {
					t.Fatalf("round-trip mismatch:\n got=%+v\nwant=%+v", got, c.in)
				}
			})
		}
	})

	t.Run("slice", func(t *testing.T) {
		hf := []hfIdxDef{{LabelToken: 3, BucketSizeMillis: 60000}, {LabelToken: 7, BucketSizeMillis: 1000}}
		b, err := msgpack.Marshal(hf)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		var got []hfIdxDef
		if err := msgpack.Unmarshal(b, &got); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if !reflect.DeepEqual(got, hf) {
			t.Fatalf("slice round-trip mismatch:\n got=%+v\nwant=%+v", got, hf)
		}

		vd := []vectorIdxDef{
			{LabelToken: 1, PropertyKey: "a", Dims: 2, Metric: DistanceCosine},
			{LabelToken: 2, PropertyKey: "b", Dims: 4, Metric: DistanceEuclidean, UseBruteForce: true, M: 32},
		}
		vb, err := msgpack.Marshal(vd)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		var vgot []vectorIdxDef
		if err := msgpack.Unmarshal(vb, &vgot); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if !reflect.DeepEqual(vgot, vd) {
			t.Fatalf("vector slice round-trip mismatch:\n got=%+v\nwant=%+v", vgot, vd)
		}
	})
}
