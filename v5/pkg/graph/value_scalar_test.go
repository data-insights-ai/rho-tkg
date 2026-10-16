package graph

import (
	"errors"
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

// TestValueScalarRoundTrip: every kind survives Value -> Scalar -> Value with
// its kind (3 stays Int, 3.0 stays Float); the zero Value and unknown kinds are
// ErrInvalidValue; Value.Equal is the engine's Scalar.Equal, not a third rule.
func TestValueScalarRoundTrip(t *testing.T) {
	vals := []Value{
		Null(), String(""), String("x\x00y"), Bool(true), Bool(false),
		Int(0), Int(3), Int(math.MaxInt64), Int(math.MinInt64),
		mustFloat(t, 0), mustFloat(t, 3), mustFloat(t, -2.5), mustFloat(t, math.MaxFloat64), mustFloat(t, math.SmallestNonzeroFloat64),
	}
	for _, v := range vals {
		s, err := valueToScalar(v)
		if err != nil {
			t.Fatalf("%v: %v", v, err)
		}
		back, err := scalarToValue(s)
		if err != nil || back != v {
			t.Fatalf("round trip %v -> %v (%v)", v, back, err)
		}
	}
	if s, _ := valueToScalar(Int(3)); s.Kind() != graphstate.ScalarI64 {
		t.Fatalf("Int(3) became %v", s.Kind())
	}
	if s, _ := valueToScalar(mustFloat(t, 3)); s.Kind() != graphstate.ScalarF64 {
		t.Fatalf("Float(3) became %v", s.Kind())
	}
	// -0 is normalised before it reaches the engine.
	if s, _ := valueToScalar(mustFloat(t, math.Copysign(0, -1))); s.Render() != "0" {
		t.Fatalf("-0 rendered %q", s.Render())
	}
	for _, bad := range []Value{{}, {kind: ValueKind(99)}} {
		if _, err := valueToScalar(bad); !errors.Is(err, ErrInvalidValue) {
			t.Fatalf("invalid %v: %v", bad, err)
		}
	}
	if _, err := scalarToValue(graphstate.Scalar{}); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("invalid scalar: %v", err)
	}
	// One equality rule: Value.Equal agrees with Scalar.Equal on every pair.
	for _, a := range vals {
		for _, b := range vals {
			sa, _ := valueToScalar(a)
			sb, _ := valueToScalar(b)
			want, err := sa.Equal(sb, graphstate.Limits{})
			if err != nil || a.Equal(b) != want {
				t.Fatalf("Equal(%v,%v) = %v, engine %v (%v)", a, b, a.Equal(b), want, err)
			}
		}
	}
}
