package storeutil

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The segment fallback column stores property slices outside a declared
// schema with the entity wire's type tags; these pin that the standalone
// codec keeps every value's exact Go type and bits. (Nested values follow the
// entity wire's rules exactly: a nested small integer is widened there, so it
// is left out here — see tasks/backlog.md.)
func TestPropertySliceCodec_RoundTripKeepsKindsAndBits(t *testing.T) {
	ps, err := types.NewPropertySlice(map[string]any{
		"a": int8(-3), "b": uint64(math.MaxUint64), "c": math.Float64frombits(0x7ff8000000000123),
		"d": float32(math.Copysign(0, -1)), "e": []any{"x", int64(2), map[string]any{"k": true}},
		"f": map[string]string{"z": "y"}, "g": []float32{1.5}, "h": []byte{0, 1},
		"i": types.TemporalValue{Kind: types.TemporalDate, Value: "2026-09-24"},
		"j": []int{1, -2}, "k": "", "l": []bool{true},
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := MarshalPropertySlice(ps)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalPropertySlice(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(ps) {
		t.Fatalf("%d properties, want %d", len(got), len(ps))
	}
	for i := range ps {
		if got[i].Key != ps[i].Key || reflect.TypeOf(got[i].Value) != reflect.TypeOf(ps[i].Value) ||
			!bytes.Equal(types.AppendPropertyValueHashBytes(nil, got[i].Value), types.AppendPropertyValueHashBytes(nil, ps[i].Value)) {
			t.Fatalf("property %q: got %T(%v), want %T(%v)", ps[i].Key, got[i].Value, got[i].Value, ps[i].Value, ps[i].Value)
		}
	}
}

func TestPropertySliceCodec_EmptySliceIsNil(t *testing.T) {
	b, err := MarshalPropertySlice(nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalPropertySlice(b)
	if err != nil || got != nil {
		t.Fatalf("empty round trip: %v %v", got, err)
	}
}

func TestPropertySliceCodec_CorruptInputIsCorruptWire(t *testing.T) {
	good, err := MarshalPropertySlice(types.PropertySlice{{Key: "a", Value: "x"}, {Key: "b", Value: int64(1)}})
	if err != nil {
		t.Fatal(err)
	}
	bad := [][]byte{
		{},                 // no value at all
		{0xc1},             // reserved msgpack byte
		good[:len(good)-1], // truncated
		append([]byte{0x92}, bytes.Repeat([]byte{0x91}, 200)...), // too deep
	}
	for i, b := range bad {
		if _, err := UnmarshalPropertySlice(b); !errors.Is(err, storepkg.ErrCorruptWire) {
			t.Fatalf("case %d: %v, want ErrCorruptWire", i, err)
		}
	}
	// Well-formed msgpack that violates the property invariants: unsorted
	// keys and a reserved tkg_ key.
	for _, ps := range []types.PropertySlice{{{Key: "b", Value: "x"}, {Key: "a", Value: "y"}}, {{Key: "tkg_x", Value: "x"}}} {
		pw := make([]PropertyWire, len(ps))
		for i, p := range ps {
			if pw[i], err = propertyToWire(p); err != nil {
				t.Fatal(err)
			}
		}
		b, err := marshalWirePooled(pw)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := UnmarshalPropertySlice(b); !errors.Is(err, storepkg.ErrCorruptWire) {
			t.Fatalf("invalid slice %v: %v, want ErrCorruptWire", ps, err)
		}
	}
}

func TestPropertySliceCodec_MarshalRejectsUnsupportedValue(t *testing.T) {
	if _, err := MarshalPropertySlice(types.PropertySlice{{Key: "a", Value: struct{}{}}}); err == nil {
		t.Fatal("unsupported value marshalled")
	}
}
