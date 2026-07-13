package types

import (
	"math"
	"testing"
)

// The classification rule is a total function over everything the allowlist
// admits; each class is pinned here so a change is a deliberate, reviewed act
// (planners build ordering-soundness proofs on it).
func TestClassifyPropertyValueRule(t *testing.T) {
	cases := []struct {
		name string
		v    any
		want PropertyTypeClass
	}{
		{"int", int(1), ClassNumeric},
		{"int8", int8(-1), ClassNumeric},
		{"int16", int16(2), ClassNumeric},
		{"int32", int32(-2), ClassNumeric},
		{"int64", int64(3), ClassNumeric},
		{"uint", uint(1), ClassNumeric},
		{"uint8", uint8(1), ClassNumeric},
		{"uint16", uint16(1), ClassNumeric},
		{"uint32", uint32(1), ClassNumeric},
		{"uint64", uint64(1), ClassNumeric},
		{"float64", 1.5, ClassNumeric},
		{"float32", float32(1.5), ClassNumeric},
		{"posInf", math.Inf(1), ClassNumeric},  // ±Inf is orderable
		{"negInf", math.Inf(-1), ClassNumeric}, // ±Inf is orderable
		{"nan64", math.NaN(), ClassNaN},
		{"nan32", float32(math.NaN()), ClassNaN},
		{"string", "s", ClassString},
		{"emptyString", "", ClassString},
		{"bool", true, ClassBool},
		{"intSlice", []int64{1}, ClassOther},
		{"float32Slice", []float32{1}, ClassOther},
		{"anySlice", []any{"a"}, ClassOther},
		{"map", map[string]any{"a": 1}, ClassOther},
		{"bytes", []byte{1}, ClassOther},
		{"nil", nil, ClassOther},
	}
	for _, tc := range cases {
		if got := classifyPropertyValue(tc.v); got != tc.want {
			t.Errorf("%s: class = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// Node and Relationship iterate identically (structural mirrors) and never
// expose values — only key + class; early stop honored; nil receivers no-op.
func TestForEachPropertyTypeClassNodeRelParity(t *testing.T) {
	props := map[string]any{
		"n":   int64(1),
		"s":   "x",
		"b":   true,
		"sl":  []int64{1, 2},
		"nan": math.NaN(),
	}
	want := map[string]PropertyTypeClass{
		"n": ClassNumeric, "s": ClassString, "b": ClassBool,
		"sl": ClassOther, "nan": ClassNaN,
	}

	n := NewNode(1, 1, nil)
	ps, err := NewOwnedPropertySlice(props)
	if err != nil {
		t.Fatalf("NewPropertySlice: %v", err)
	}
	if err := n.SetOwnedProperties(ps); err != nil {
		t.Fatalf("SetOwnedProperties: %v", err)
	}
	r := NewRelationship(2, 1, 1, 3)
	ps2, err := NewOwnedPropertySlice(props)
	if err != nil {
		t.Fatalf("NewPropertySlice: %v", err)
	}
	if err := r.SetOwnedProperties(ps2); err != nil {
		t.Fatalf("rel SetOwnedProperties: %v", err)
	}

	collect := func(iter func(func(string, PropertyTypeClass) bool)) map[string]PropertyTypeClass {
		got := map[string]PropertyTypeClass{}
		iter(func(k string, c PropertyTypeClass) bool {
			got[k] = c
			return true
		})
		return got
	}
	gotN := collect(n.ForEachPropertyTypeClass)
	gotR := collect(r.ForEachPropertyTypeClass)
	for k, w := range want {
		if gotN[k] != w {
			t.Errorf("node %q: class %d, want %d", k, gotN[k], w)
		}
		if gotR[k] != w {
			t.Errorf("rel %q: class %d, want %d", k, gotR[k], w)
		}
	}
	if len(gotN) != len(want) || len(gotR) != len(want) {
		t.Fatalf("iteration count: node %d, rel %d, want %d", len(gotN), len(gotR), len(want))
	}

	// Early stop after the first callback.
	calls := 0
	n.ForEachPropertyTypeClass(func(string, PropertyTypeClass) bool {
		calls++
		return false
	})
	if calls != 1 {
		t.Fatalf("early stop: %d calls, want 1", calls)
	}

	// Nil receivers no-op.
	var nilN *Node
	var nilR *Relationship
	nilN.ForEachPropertyTypeClass(func(string, PropertyTypeClass) bool { t.Fatal("nil node iterated"); return false })
	nilR.ForEachPropertyTypeClass(func(string, PropertyTypeClass) bool { t.Fatal("nil rel iterated"); return false })
}
