package tiered

import (
	"reflect"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// TestTieredWireRoundTrip proves every hand-written store-level
// persistence-type EncodeMsgpack/DecodeMsgpack pair (BACKLOG 15v) reproduces
// the original value exactly, including the nil-vs-empty-slice case for
// temporalIndexFileData's non-omitempty fields.
func TestTieredWireRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		in   any
		out  any
	}{
		{"RegistryFileData", &RegistryFileData{Labels: []string{"A", "B"}, RelTypes: []string{"REL"}}, &RegistryFileData{}},
		{"vectorIdxDef_min", vectorIdxDef{LabelToken: 3, PropertyKey: "vec", Dims: 128, Metric: DistanceCosine}, &vectorIdxDef{}},
		{"vectorIdxDef_full", vectorIdxDef{LabelToken: 3, PropertyKey: "vec", Dims: 128, Metric: DistanceCosine, UseBruteForce: true, M: 16, EfConstruction: 200, EfSearch: 64}, &vectorIdxDef{}},
		{"tieredHFIdef", tieredHFIdef{LabelToken: 3, BucketSizeMillis: 60000}, &tieredHFIdef{}},
		{"temporalIndexFileData_min", &temporalIndexFileData{TemporalLabels: []uint16{1, 2}}, &temporalIndexFileData{}},
		{"temporalIndexFileData_full", &temporalIndexFileData{
			TemporalLabels: []uint16{1, 2},
			HighFrequency:  []tieredHFIdef{{LabelToken: 3, BucketSizeMillis: 60000}, {LabelToken: 4, BucketSizeMillis: 1000}},
		}, &temporalIndexFileData{}},
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
			var got, want any
			if rv := reflect.ValueOf(c.in); rv.Kind() == reflect.Pointer {
				got = reflect.ValueOf(c.out).Elem().Interface()
				want = rv.Elem().Interface()
			} else {
				got = reflect.ValueOf(c.out).Elem().Interface()
				want = c.in
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("round-trip mismatch:\n got=%+v\nwant=%+v", got, want)
			}
		})
	}

	// Slice-of-struct shape used at the actual persist call site.
	t.Run("vectorIdxDef_slice", func(t *testing.T) {
		defs := []vectorIdxDef{{LabelToken: 1, PropertyKey: "a", Dims: 2, Metric: DistanceCosine}}
		b, err := msgpack.Marshal(defs)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		var got []vectorIdxDef
		if err := msgpack.Unmarshal(b, &got); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if !reflect.DeepEqual(got, defs) {
			t.Fatalf("slice round-trip mismatch:\n got=%+v\nwant=%+v", got, defs)
		}
	})
}

// TestTieredWireEncodeByValue locks in a real regression this backlog item's
// implementation hit: msgpack.Marshal(v) on a NON-ADDRESSABLE value (a plain
// struct literal boxed into an `any`, e.g. `reg := RegistryFileData{...};
// msgpack.Marshal(reg)` rather than `msgpack.Marshal(&reg)`) requires
// EncodeMsgpack to have a VALUE receiver — a pointer-receiver-only
// EncodeMsgpack fails with "Encode(non-addressable ...)" for exactly this
// call shape, which internal/core's export/import test helpers use for
// RegistryFileData. Every EncodeMsgpack in this file must use a value
// receiver (DecodeMsgpack correctly stays pointer-receiver, since decode
// always targets an addressable &out).
func TestTieredWireEncodeByValue(t *testing.T) {
	if _, err := msgpack.Marshal(RegistryFileData{Labels: []string{"A"}, RelTypes: []string{"B"}}); err != nil {
		t.Fatalf("msgpack.Marshal(RegistryFileData{...}) (by value) = %v, want nil (EncodeMsgpack must use a value receiver)", err)
	}
	if _, err := msgpack.Marshal(temporalIndexFileData{TemporalLabels: []uint16{1}}); err != nil {
		t.Fatalf("msgpack.Marshal(temporalIndexFileData{...}) (by value) = %v, want nil (EncodeMsgpack must use a value receiver)", err)
	}
	if _, err := msgpack.Marshal(vectorIdxDef{LabelToken: 1, PropertyKey: "a", Dims: 2}); err != nil {
		t.Fatalf("msgpack.Marshal(vectorIdxDef{...}) (by value) = %v, want nil", err)
	}
	if _, err := msgpack.Marshal(tieredHFIdef{LabelToken: 1, BucketSizeMillis: 1000}); err != nil {
		t.Fatalf("msgpack.Marshal(tieredHFIdef{...}) (by value) = %v, want nil", err)
	}
}
