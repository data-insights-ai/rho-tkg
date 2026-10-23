package index

import (
	"fmt"
	"reflect"
	"testing"
)

func TestPropertyStatsAccumulatorCodecRoundTrip(t *testing.T) {
	t.Parallel()
	sparse := NewPropertyStatsAccumulator()
	sparse.Observe("a", "apple")
	sparse.Observe("b", "pear")
	sparse.Forget("apple") // dirty

	dense := NewPropertyStatsAccumulator()
	for i := 0; i < 20_000; i++ {
		dense.Observe(fmt.Sprint(i), int32(i-5))
	}

	empty := NewPropertyStatsAccumulator()
	unordered := NewPropertyStatsAccumulator()
	unordered.Observe("true", true) // NDV only, no family

	typed := []any{int(-3), int8(-4), int16(5), int32(-6), int64(7), uint(8), uint8(9), uint16(10),
		uint32(11), uint64(1 << 60), float32(1.5), float64(-2.25), "z"}
	cases := map[string]*PropertyStatsAccumulator{"sparse": sparse, "dense": dense, "empty": empty, "unordered": unordered}
	for _, v := range typed {
		a := NewPropertyStatsAccumulator()
		a.Observe(fmt.Sprintf("%T", v), v)
		cases[fmt.Sprintf("%T", v)] = a
	}
	for name, a := range cases {
		t.Run(name, func(t *testing.T) {
			blob := a.AppendBinary([]byte("prefix"))[len("prefix"):]
			got, n, err := DecodePropertyStatsAccumulator(append(blob, 0xAA))
			if err != nil || n != len(blob) {
				t.Fatalf("decode: n=%d of %d, err %v", n, len(blob), err)
			}
			wNDV, wMin, wMax := a.Snapshot()
			gNDV, gMin, gMax := got.Snapshot()
			if gNDV != wNDV || !reflect.DeepEqual(gMin, wMin) || !reflect.DeepEqual(gMax, wMax) ||
				got.Dirty() != a.Dirty() || got.family != a.family {
				t.Fatalf("got ndv=%d min=%#v max=%#v dirty=%v, want ndv=%d min=%#v max=%#v dirty=%v",
					gNDV, gMin, gMax, got.Dirty(), wNDV, wMin, wMax, a.Dirty())
			}
			if (got.sketch.dense != nil) != (a.sketch.dense != nil) {
				t.Fatal("sketch representation changed")
			}
			// Further observations behave the same on both.
			a.Observe("new", int64(1))
			got.Observe("new", int64(1))
			if x, y := a.Sketch().Estimate(), got.Sketch().Estimate(); x != y {
				t.Fatalf("after one more value: %d vs %d", y, x)
			}
		})
	}
}

func TestPropertyStatsAccumulatorCodecRejectsDamage(t *testing.T) {
	t.Parallel()
	a := NewPropertyStatsAccumulator()
	a.Observe("1", int64(1))
	a.Observe("9", int64(9))
	blob := a.AppendBinary(nil)
	for i := 0; i < len(blob); i++ {
		if _, _, err := DecodePropertyStatsAccumulator(blob[:i]); err == nil {
			t.Fatalf("truncated at %d of %d decoded", i, len(blob))
		}
	}
	bad := map[string][]byte{
		"version":             append([]byte{9}, blob[1:]...),
		"family":              append([]byte{1, 1, 'x'}, blob[3:]...),
		"min of other family": append(append([]byte{}, blob[:4]...), append([]byte{statsKindString, 1, 'q'}, blob[6:]...)...),
	}
	for name, b := range bad {
		if _, _, err := DecodePropertyStatsAccumulator(b); err == nil {
			t.Errorf("%s: damaged encoding decoded", name)
		}
	}
}
