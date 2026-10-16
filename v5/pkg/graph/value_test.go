package graph

import (
	"errors"
	"math"
	"testing"
)

func mustFloat(t testing.TB, f float64) Value {
	t.Helper()
	v, err := Float(f)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestValueKindsAndAccessors(t *testing.T) {
	if (Value{}).Kind() != KindInvalid || (Value{}).Validate() == nil {
		t.Fatal("zero Value must be invalid")
	}
	if !errors.Is((Value{}).Validate(), ErrInvalidValue) {
		t.Fatal("zero Value must be ErrInvalidValue")
	}
	if Null().Kind() != KindNull || Null().Validate() != nil || !Null().IsNull() {
		t.Fatal("null")
	}
	if s, ok := String("").AsString(); !ok || s != "" {
		t.Fatal("empty string is a value")
	}
	if i, ok := Int(0).AsInt(); !ok || i != 0 {
		t.Fatal("zero int is a value")
	}
	if b, ok := Bool(false).AsBool(); !ok || b {
		t.Fatal("false is a value")
	}
	if f, ok := mustFloat(t, 0).AsFloat(); !ok || f != 0 {
		t.Fatal("zero float is a value")
	}
	// accessors never coerce
	if _, ok := Int(1).AsFloat(); ok {
		t.Fatal("Int must not read as float")
	}
	if _, ok := mustFloat(t, 1).AsInt(); ok {
		t.Fatal("Float must not read as int")
	}
	if _, ok := Bool(true).AsInt(); ok {
		t.Fatal("Bool must not read as int")
	}
	if _, ok := String("1").AsInt(); ok {
		t.Fatal("String must not read as int")
	}
}

func TestFloatRefusesNonFinite(t *testing.T) {
	for _, f := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := Float(f); !errors.Is(err, ErrInvalidValue) {
			t.Errorf("Float(%v): %v", f, err)
		}
	}
	z := mustFloat(t, math.Copysign(0, -1))
	if f, _ := z.AsFloat(); math.Signbit(f) {
		t.Fatal("signed zero must canonicalize to +0")
	}
}

// One rule: Cypher equality across numeric widths, 3 == 3.0.
func TestValueEqualIsCypherEquality(t *testing.T) {
	type c struct {
		name string
		a, b Value
		want bool
	}
	big := int64(1) << 53
	cases := []c{
		{"3 == 3.0", Int(3), mustFloat(t, 3), true},
		{"3.0 == 3", mustFloat(t, 3), Int(3), true},
		{"0 == 0.0", Int(0), mustFloat(t, 0), true},
		{"0 == -0.0", Int(0), mustFloat(t, math.Copysign(0, -1)), true},
		{"3 != 3.5", Int(3), mustFloat(t, 3.5), false},
		{"int == int", Int(7), Int(7), true},
		{"int != int", Int(7), Int(8), false},
		{"float == float", mustFloat(t, 1.5), mustFloat(t, 1.5), true},
		{"2^53+1 != 2^53 float", Int(big + 1), mustFloat(t, float64(big)), false},
		{"2^53 == 2^53 float", Int(big), mustFloat(t, float64(big)), true},
		{"MaxInt64 != 2^63 float", Int(math.MaxInt64), mustFloat(t, 9223372036854775808), false},
		{"MinInt64 == -2^63 float", Int(math.MinInt64), mustFloat(t, -9223372036854775808), true},
		{"string == string", String("a"), String("a"), true},
		{"string != string", String("a"), String("b"), false},
		{"1 != true", Int(1), Bool(true), false},
		{"0 != false", Int(0), Bool(false), false},
		{"1 != \"1\"", Int(1), String("1"), false},
		{"true == true", Bool(true), Bool(true), true},
		{"null != null", Null(), Null(), false},
		{"null != 0", Null(), Int(0), false},
		{"zero Value != zero Value", Value{}, Value{}, false},
		{"zero Value != 0", Value{}, Int(0), false},
	}
	for _, x := range cases {
		if got := x.a.Equal(x.b); got != x.want {
			t.Errorf("%s: Equal = %v, want %v", x.name, got, x.want)
		}
		if got := x.b.Equal(x.a); got != x.want {
			t.Errorf("%s (reversed): Equal = %v, want %v", x.name, got, x.want)
		}
	}
}
