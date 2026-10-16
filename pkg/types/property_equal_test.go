package types

import (
	"math"
	"slices"
	"testing"
)

// TestEqualIndexKeys pins the forms of the property equality at the width
// boundaries: every width holding the exact value, nothing for NaN, nil and
// unindexable values, no rounding past 2^53, 2^63 and 2^64.
func TestEqualIndexKeys(t *testing.T) {
	for _, tc := range []struct {
		name string
		v    any
		want []string
	}{
		{"int 3", int(3), []string{"i64:3", "i:3", "i32:3", "i16:3", "i8:3", "u64:3", "u:3", "u32:3", "u16:3", "u8:3", "f64:3", "f32:3"}},
		{"float32 3 = int 3", float32(3), []string{"f64:3", "f32:3", "i64:3", "i:3", "i32:3", "i16:3", "i8:3", "u64:3", "u:3", "u32:3", "u16:3", "u8:3"}},
		{"-3", int8(-3), []string{"i64:-3", "i:-3", "i32:-3", "i16:-3", "i8:-3", "f64:-3", "f32:-3"}},
		{"300", uint16(300), []string{"i64:300", "i:300", "i32:300", "i16:300", "u64:300", "u:300", "u32:300", "u16:300", "f64:300", "f32:300"}},
		{"-0.0", math.Copysign(0, -1), []string{"f64:0", "f32:0", "i64:0", "i:0", "i32:0", "i16:0", "i8:0", "u64:0", "u:0", "u32:0", "u16:0", "u8:0"}},
		{"3.5", 3.5, []string{"f64:3.5", "f32:3.5"}},
		{"float32 0.1 widened", float32(0.1), []string{"f64:0.10000000149011612", "f32:0.1"}},
		{"0.1 has no float32", 0.1, []string{"f64:0.1"}},
		{"+Inf", math.Inf(1), []string{"f64:+Inf", "f32:+Inf"}},
		{"2^53+1 no float", int64(1<<53 + 1), []string{"i64:9007199254740993", "i:9007199254740993", "u64:9007199254740993", "u:9007199254740993"}},
		{"MinInt64", int64(math.MinInt64), []string{"i64:-9223372036854775808", "i:-9223372036854775808", "f64:-9.223372036854776e+18", "f32:-9.223372e+18"}},
		{"float 2^63 unsigned only", float64(1 << 63), []string{"f64:9.223372036854776e+18", "f32:9.223372e+18", "u64:9223372036854775808", "u:9223372036854775808"}},
		{"MaxUint64 no float", uint64(math.MaxUint64), []string{"u64:18446744073709551615", "u:18446744073709551615"}},
		{"float 2^64 no int", float64(math.MaxUint64), []string{"f64:1.8446744073709552e+19", "f32:1.8446744e+19"}},
		{"-1e300 integral, no int", -1e300, []string{"f64:-1e+300"}},
		{"NaN", math.NaN(), nil},
		{"float32 NaN", float32(math.NaN()), nil},
		{"nil", nil, nil},
		{"list", []any{int64(3)}, nil},
		{"string", "3", []string{"s:3"}},
		{"bool", true, []string{"b:true"}},
		{"temporal", TemporalValue{Kind: 1, Value: "2026-01-01"}, []string{IndexablePropertyValueKey(TemporalValue{Kind: 1, Value: "2026-01-01"})}},
	} {
		if tc.name == "int 3" {
			for _, w := range []any{int16(3), int32(3), int64(3), uint(3), uint32(3), uint64(3)} {
				if got := EqualIndexKeys(w); !slices.Equal(got, tc.want) {
					t.Errorf("EqualIndexKeys(%T 3) = %v, want %v", w, got, tc.want)
				}
			}
		}
		if got := EqualIndexKeys(tc.v); !slices.Equal(got, tc.want) {
			t.Errorf("%s: EqualIndexKeys(%T %v) = %v, want %v", tc.name, tc.v, tc.v, got, tc.want)
		}
		if got := EqualIndexValues(tc.v); len(got) != len(tc.want) {
			t.Errorf("%s: EqualIndexValues has %d forms, want %d", tc.name, len(got), len(tc.want))
		}
	}
}

func TestNumericEqualAndPropertyEqual(t *testing.T) {
	for _, tc := range []struct {
		a, b any
		want bool
	}{
		{int64(3), 3.0, true},
		{uint8(3), float32(3), true},
		{int64(1<<53 + 1), float64(1 << 53), false},
		{float64(1 << 63), int64(math.MaxInt64), false},
		{uint64(math.MaxUint64), float64(math.MaxUint64), false},
		{math.Copysign(0, -1), int(0), true},
		{math.NaN(), math.NaN(), false},
		{float32(0.1), 0.1, false},
		{3, "3", false},
		{"3", 3, false},
	} {
		if got := NumericEqual(tc.a, tc.b); got != tc.want {
			t.Errorf("NumericEqual(%T %v, %T %v) = %v, want %v", tc.a, tc.a, tc.b, tc.b, got, tc.want)
		}
		if got := PropertyEqual(tc.a, tc.b); got != tc.want {
			t.Errorf("PropertyEqual(%T %v, %T %v) = %v, want %v", tc.a, tc.a, tc.b, tc.b, got, tc.want)
		}
	}
	if !PropertyEqual("a", "a") || PropertyEqual("a", "b") || !PropertyEqual([]float64{math.NaN()}, []float64{math.NaN()}) {
		t.Error("PropertyEqual: non-numbers compare as PropertyValueEqual")
	}
}

func TestCompareNumber(t *testing.T) {
	for _, tc := range []struct {
		v      any
		f      float64
		want   int
		wantOK bool
	}{
		{int64(3), 3, 0, true},
		{int64(1<<53 + 1), 1 << 53, 1, true},
		{int64(1 << 53), 1<<53 + 2, -1, true},
		{int64(3), 3.5, -1, true},
		{int64(-3), -3.5, 1, true},
		{int64(math.MaxInt64), 1 << 63, -1, true},
		{int64(math.MinInt64), -1e300, 1, true},
		{uint64(math.MaxUint64), 1 << 63, 1, true},
		{uint64(math.MaxUint64), float64(math.MaxUint64), -1, true},
		{uint64(3), -1, 1, true},
		{uint64(3), 3.5, -1, true},
		{uint8(3), 3, 0, true},
		{int8(-3), -3, 0, true},
		{int16(4), 3, 1, true},
		{int32(4), 3, 1, true},
		{int(4), 3, 1, true},
		{uint(2), 3, -1, true},
		{uint16(3), 3, 0, true},
		{uint32(3), 3, 0, true},
		{float32(0.5), 0.5, 0, true},
		{2.5, 3, -1, true},
		{math.Inf(1), math.MaxFloat64, 1, true},
		{math.NaN(), 3, 0, false},
		{int64(3), math.NaN(), 0, false},
		{"3", 3, 0, false},
	} {
		got, ok := CompareNumber(tc.v, tc.f)
		if got != tc.want || ok != tc.wantOK {
			t.Errorf("CompareNumber(%T %v, %v) = %d, %v; want %d, %v", tc.v, tc.v, tc.f, got, ok, tc.want, tc.wantOK)
		}
	}
}

func TestCanonicalIndexKey(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{IndexablePropertyValueKey(int(3)), "i64:3"},
		{IndexablePropertyValueKey(int8(-3)), "i64:-3"},
		{IndexablePropertyValueKey(uint8(3)), "i64:3"},
		{IndexablePropertyValueKey(uint64(math.MaxUint64)), "u64:18446744073709551615"},
		{IndexablePropertyValueKey(int64(7)), "i64:7"},
		{IndexablePropertyValueKey(3.0), "f64:3"},
		{IndexablePropertyValueKey("i:3"), "s:i:3"},
		{"", ""},
		{"i:x", "i:x"},
		{"u:-1", "u:-1"},
	} {
		if got := CanonicalIndexKey(tc.in); got != tc.want {
			t.Errorf("CanonicalIndexKey(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestNumericFloat64(t *testing.T) {
	for _, v := range []any{int(2), int8(2), int16(2), int32(2), int64(2), uint(2), uint8(2), uint16(2), uint32(2), uint64(2), float32(2), 2.0} {
		if f, ok := NumericFloat64(v); !ok || f != 2 {
			t.Errorf("NumericFloat64(%T) = %v, %v", v, f, ok)
		}
	}
	if _, ok := NumericFloat64("2"); ok {
		t.Error("NumericFloat64(string): ok")
	}
}
