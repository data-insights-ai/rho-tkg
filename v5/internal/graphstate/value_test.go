package graphstate

import (
	"errors"
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func mustF64(t *testing.T, v float64) Scalar {
	t.Helper()
	s, err := F64(v)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestScalarNumericEquality(t *testing.T) {
	two53 := float64(1 << 53)
	for _, tc := range []struct {
		name string
		a, b Scalar
		want bool
	}{
		{"I64(3)=F64(3.0)", I64(3), mustF64(t, 3.0), true},
		{"F64(3.0)=I64(3) reversed", mustF64(t, 3.0), I64(3), true},
		{"F64(2.5)!=I64(2)", mustF64(t, 2.5), I64(2), false},
		{"F64(2.5)!=I64(3)", mustF64(t, 2.5), I64(3), false},
		{"I64(2^53+1)!=F64(2^53)", I64(1<<53 + 1), mustF64(t, two53), false},
		{"I64(2^53)=F64(2^53)", I64(1 << 53), mustF64(t, two53), true},
		{"F64(9.3e18)!=I64(MaxInt64)", mustF64(t, 9.3e18), I64(math.MaxInt64), false},
		{"F64(2^63)!=I64(MaxInt64)", mustF64(t, 9223372036854775808), I64(math.MaxInt64), false},
		{"F64(-9.3e18)!=I64(MinInt64)", mustF64(t, -9.3e18), I64(math.MinInt64), false},
		{"F64(-0.0)=I64(0)", mustF64(t, math.Copysign(0, -1)), I64(0), true},
		{"F64(-2^63)=I64(MinInt64)", mustF64(t, -9223372036854775808), I64(math.MinInt64), true},
		{"F64=F64", mustF64(t, 2.5), mustF64(t, 2.5), true},
		{"I64!=I64", I64(1), I64(2), false},
		{"Null!=I64(0)", Null(), I64(0), false},
		{"Bool(true)!=I64(1)", Bool(true), I64(1), false},
		{"Bool(false)!=F64(0)", Bool(false), mustF64(t, 0), false},
		{"String(3)!=I64(3)", String("3"), I64(3), false},
	} {
		got, err := tc.a.Equal(tc.b, Limits{})
		if err != nil || got != tc.want {
			t.Errorf("%s: got %v err %v want %v", tc.name, got, err, tc.want)
		}
	}
	// Cypher: null = null is not true.
	if got, err := Null().Equal(Null(), Limits{}); err != nil || got {
		t.Errorf("Null=Null: got %v err %v, want false", got, err)
	}
	// F64(9.3e18) differs from every I64 key.
	k, _ := mustF64(t, 9.3e18).EqualityKey(Limits{})
	for _, v := range []int64{math.MaxInt64, math.MinInt64, 0, 9300000000000000000 % math.MaxInt64} {
		ik, _ := I64(v).EqualityKey(Limits{})
		if k == ik {
			t.Errorf("F64(9.3e18) keyed equal to I64(%d)", v)
		}
	}
}

func TestScalarWireRoundTripKeepsKind(t *testing.T) {
	axis := temporal.Axis{}
	for _, v := range []Scalar{Null(), String("x"), String(""), Bool(true), Bool(false), I64(3), I64(math.MinInt64), mustF64(t, 3.0), mustF64(t, 2.5), mustF64(t, -9223372036854775808)} {
		wire, err := v.AppendWire(nil, Limits{})
		if err != nil {
			t.Fatal(v, err)
		}
		got, err := DecodeWire(wire, axis, false, temporal.Limits{})
		if err != nil || got.Kind() != v.Kind() {
			t.Fatalf("%v: kind %v err %v", v.Render(), got.Kind(), err)
		}
		if eq, _ := got.Identical(v, Limits{}); !eq || got.Render() != v.Render() {
			t.Fatalf("%v round trip -> %v", v.Render(), got.Render())
		}
	}
	// appends to a prefix
	if w, _ := I64(1).AppendWire([]byte{7}, Limits{}); w[0] != 7 || len(w) != 10 {
		t.Fatal(w)
	}
	f3, _ := F64(3.0)
	w, _ := f3.AppendWire(nil, Limits{})
	if got, _ := DecodeWire(w, axis, false, temporal.Limits{}); got.Kind() != ScalarF64 {
		t.Fatal("F64(3.0) became", got.Kind())
	}
	nan := append([]byte{byte(ScalarF64)}, 0x7f, 0xf8, 0, 0, 0, 0, 0, 1)
	inf := append([]byte{byte(ScalarF64)}, 0x7f, 0xf0, 0, 0, 0, 0, 0, 0)
	negZero := append([]byte{byte(ScalarF64)}, 0x80, 0, 0, 0, 0, 0, 0, 0)
	for name, bad := range map[string][]byte{
		"empty":         {},
		"unknown kind":  {200, 0},
		"invalid kind":  {byte(ScalarInvalid)},
		"kind past max": {byte(ScalarDescriptor) + 1},
		"truncated i64": {byte(ScalarI64), 0, 0, 0},
		"oversize i64":  append([]byte{byte(ScalarI64)}, make([]byte, 9)...),
		"truncated f64": {byte(ScalarF64), 1},
		"oversize f64":  append([]byte{byte(ScalarF64)}, make([]byte, 9)...),
		"nan":           nan,
		"inf":           inf,
		"negative zero": negZero,
		"null extra":    {byte(ScalarNull), 0},
		"bool 2":        {byte(ScalarBool), 2},
		"bool short":    {byte(ScalarBool)},
		"bool long":     {byte(ScalarBool), 1, 1},
		"scope no axis": {byte(ScalarScope), 1},
	} {
		if _, err := DecodeWire(bad, axis, false, temporal.Limits{}); !errors.Is(err, ErrCorruptWire) {
			t.Errorf("%s: err %v, want ErrCorruptWire", name, err)
		}
	}
	// an axis supplied for a non-scope kind is corrupt too
	if _, err := DecodeWire(w, axis, true, temporal.Limits{}); !errors.Is(err, ErrCorruptWire) {
		t.Errorf("axis on F64: %v", err)
	}
}

// A value dictionary keyed by the numeric-canonical key would hand the F64 ValueID
// to an I64 property and every projection of it would be contradictory.
func TestIntegerPropertyAfterFloatPropertyProjectsAsInteger(t *testing.T) {
	v := newFixtureView(t)
	all, _ := temporal.All(v.axis)
	v.defs[ownerSchemaKey{Node, "f"}] = PropertyDefinition{Name: "f", Owner: Node, Type: ScalarF64, Cardinality: ScalarCardinality}
	v.defs[ownerSchemaKey{Node, "i"}] = PropertyDefinition{Name: "i", Owner: Node, Type: ScalarI64, Cardinality: ScalarCardinality}
	rev, _ := state.NewRevision(1, 1)
	f3 := mustF64(t, 3.0)
	d, err := Plan(t.Context(), v, []Operation{
		{Kind: CreateNode, Owner: 1, Life: 1, Scope: all},
		{Kind: Set, Owner: 1, Life: 1, Scope: all, Name: "f", Value: f3, ValueID: 1},
	}, rev, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	v.install(t, d)
	rev2, _ := state.NewRevision(2, 1)
	d, err = Plan(t.Context(), v, []Operation{
		{Kind: Set, Owner: 1, Life: 1, Scope: all, Name: "i", Value: I64(3), ValueID: 2},
	}, rev2, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	v.install(t, d)
	got, err := Project(t.Context(), v, 1, testPosition(t, v.axis, 1), Effective, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	prop := func(name string) Scalar {
		for _, p := range got.Properties {
			if p.Name == name {
				return p.Scalar
			}
		}
		t.Fatal("missing property", name)
		return Scalar{}
	}
	if k := prop("i").Kind(); k != ScalarI64 {
		t.Fatal("i projected as", k)
	}
	if k := prop("f").Kind(); k != ScalarF64 {
		t.Fatal("f projected as", k)
	}
	if eq, _ := prop("i").Equal(prop("f"), Limits{}); !eq {
		t.Fatal("I64(3) and F64(3.0) must still be Equal")
	}
}
