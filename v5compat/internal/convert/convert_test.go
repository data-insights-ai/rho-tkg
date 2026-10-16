package convert

import (
	"errors"
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
)

// TestAnyToValueEdge pins the one place where `any` becomes graph.Value.
func TestAnyToValueEdge(t *testing.T) {
	ok := []struct {
		name string
		in   any
		want any // read-back through ValueToAny: int64, float64, string, bool or nil
	}{
		{"int", 3, int64(3)},
		{"int8 min", int8(math.MinInt8), int64(math.MinInt8)},
		{"int16", int16(-7), int64(-7)},
		{"int32", int32(-5), int64(-5)},
		{"int64 max", int64(math.MaxInt64), int64(math.MaxInt64)},
		{"int64 min", int64(math.MinInt64), int64(math.MinInt64)},
		{"uint", uint(9), int64(9)},
		{"uint8", uint8(255), int64(255)},
		{"uint16", uint16(65535), int64(65535)},
		{"uint32", uint32(math.MaxUint32), int64(math.MaxUint32)},
		{"uint64 at MaxInt64", uint64(math.MaxInt64), int64(math.MaxInt64)},
		{"float32", float32(1.5), float64(1.5)},
		{"float64", 2.25, float64(2.25)},
		{"negative zero", math.Copysign(0, -1), float64(0)},
		{"float past 2^53", float64(1 << 60), float64(1 << 60)},
		{"empty string", "", ""},
		{"string", "x", "x"},
		{"true", true, true},
		{"false", false, false},
		{"nil is Null", nil, nil},
	}
	for _, c := range ok {
		v, err := AnyToValue(c.in)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		got := ValueToAny(v)
		if got != c.want {
			t.Fatalf("%s: read back %T(%v), want %T(%v)", c.name, got, got, c.want, c.want)
		}
	}
	if v, _ := AnyToValue(nil); !v.IsNull() {
		t.Fatalf("nil must be Null, got kind %v", v.Kind())
	}
	if v, _ := AnyToValue(uint8(7)); v.Kind() != graph.KindInt {
		t.Fatalf("uint8 must be an Int, got kind %v", v.Kind())
	}
	if v, _ := AnyToValue(float32(1)); v.Kind() != graph.KindFloat {
		t.Fatalf("an integral float32 stays a Float, got kind %v", v.Kind())
	}

	refused := []struct {
		name string
		in   any
		why  error
	}{
		{"uint64 above MaxInt64", uint64(math.MaxInt64) + 1, ErrOutOfRange},
		{"uint64 max", uint64(math.MaxUint64), ErrOutOfRange},
		{"uint above MaxInt64", uint(math.MaxUint64), ErrOutOfRange},
		{"NaN", math.NaN(), ErrNotFinite},
		{"+Inf", math.Inf(1), ErrNotFinite},
		{"-Inf", math.Inf(-1), ErrNotFinite},
		{"float32 NaN", float32(math.NaN()), ErrNotFinite},
		{"[]any", []any{1, 2}, ErrListNotYetSupported},
		{"empty []any", []any{}, ErrListNotYetSupported},
		{"[]int64", []int64{1}, ErrListNotYetSupported},
		{"[]string", []string{"a"}, ErrListNotYetSupported},
		{"[]byte", []byte("x"), ErrListNotYetSupported},
		{"map", map[string]any{"a": 1}, ErrUnsupportedType},
		{"func", func() {}, ErrUnsupportedType},
		{"struct", struct{}{}, ErrUnsupportedType},
		{"pointer", new(int), ErrUnsupportedType},
	}
	for _, c := range refused {
		v, err := AnyToValue(c.in)
		if err == nil {
			t.Fatalf("%s: accepted as %v", c.name, v)
		}
		if !errors.Is(err, c.why) {
			t.Fatalf("%s: err = %v, want errors.Is %v", c.name, err, c.why)
		}
		if !errors.Is(err, ErrUnsupportedValue) {
			t.Fatalf("%s: err = %v, want the common class ErrUnsupportedValue", c.name, err)
		}
	}
}
