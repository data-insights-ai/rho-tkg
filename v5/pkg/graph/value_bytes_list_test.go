package graph

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

func mustVList(t *testing.T, vs ...Value) Value {
	t.Helper()
	v, err := List(vs)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// TestValueBytesEqualIsBytewise: Bytes compare bytewise, never to a String;
// nil and empty are one value; the accessor and constructor copy.
func TestValueBytesEqualIsBytewise(t *testing.T) {
	cases := []struct {
		name string
		a, b Value
		want bool
	}{
		{"same", Bytes([]byte{0, 1, 255}), Bytes([]byte{0, 1, 255}), true},
		{"nil empty", Bytes(nil), Bytes([]byte{}), true},
		{"differ", Bytes([]byte{1}), Bytes([]byte{2}), false},
		{"prefix", Bytes([]byte{1}), Bytes([]byte{1, 0}), false},
		{"reversed", Bytes([]byte{1, 2}), Bytes([]byte{2, 1}), false},
		{"string with same bytes", Bytes([]byte("a")), String("a"), false},
		{"zero value", Bytes([]byte{1}), Value{}, false},
		{"int", Bytes([]byte{1}), Int(1), false},
	}
	for _, c := range cases {
		if got := c.a.Equal(c.b); got != c.want {
			t.Errorf("%s: %t want %t", c.name, got, c.want)
		}
		if got := c.b.Equal(c.a); got != c.want {
			t.Errorf("%s reversed: %t want %t", c.name, got, c.want)
		}
	}
	src := []byte{1, 2}
	v := Bytes(src)
	src[0] = 9
	got, ok := v.AsBytes()
	got[1] = 9
	again, _ := v.AsBytes()
	if !ok || again[0] != 1 || again[1] != 2 {
		t.Fatalf("aliased %v", again)
	}
	if _, ok := String("a").AsBytes(); ok {
		t.Fatal("string read as bytes")
	}
	if Bytes(nil).Validate() != nil || Bytes(nil).Kind() != KindBytes {
		t.Fatal("empty bytes invalid")
	}
}

// TestValueListEquality: elementwise by the numeric rule, in order; nested
// lists and null-holding lists.
func TestValueListEquality(t *testing.T) {
	f3 := mustFloat(t, 3)
	cases := []struct {
		name string
		a, b Value
		want bool
	}{
		{"same ints", mustVList(t, Int(1), Int(2)), mustVList(t, Int(1), Int(2)), true},
		{"int float element", mustVList(t, Int(3)), mustVList(t, f3), true},
		{"order", mustVList(t, Int(1), Int(2)), mustVList(t, Int(2), Int(1)), false},
		{"length", mustVList(t, Int(1)), mustVList(t, Int(1), Int(1)), false},
		{"empty", mustVList(t), mustVList(t), true},
		{"empty vs bytes", mustVList(t), Bytes(nil), false},
		{"bool vs int", mustVList(t, Bool(true)), mustVList(t, Int(1)), false},
		{"null inside", mustVList(t, Null()), mustVList(t, Null()), false},
		{"bytes inside", mustVList(t, Bytes([]byte{1})), mustVList(t, Bytes([]byte{1})), true},
		{"list vs int", mustVList(t, Int(3)), Int(3), false},
	}
	for _, c := range cases {
		if got := c.a.Equal(c.b); got != c.want {
			t.Errorf("%s: %t want %t", c.name, got, c.want)
		}
	}
	if _, err := List([]Value{mustVList(t)}); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("nested: %v", err)
	}
	if _, err := List([]Value{{}}); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("zero element: %v", err)
	}
	l := mustVList(t, Int(3), f3, String("x"))
	els, ok := l.AsList()
	if !ok || len(els) != 3 || els[0].Kind() != KindInt || els[1].Kind() != KindFloat {
		t.Fatalf("elements %v", els)
	}
	els[0] = Int(9)
	again, _ := l.AsList()
	if n, _ := again[0].AsInt(); n != 3 {
		t.Fatal("AsList aliased")
	}
	if _, ok := Int(1).AsList(); ok {
		t.Fatal("int read as list")
	}
}

// TestValueBytesListScalarRoundTrip: the one conversion file keeps both kinds.
func TestValueBytesListScalarRoundTrip(t *testing.T) {
	for _, v := range []Value{Bytes(nil), Bytes([]byte{0, 9}), mustVList(t), mustVList(t, Int(1), mustFloat(t, 1), Null(), String("s"), Bool(true), Bytes([]byte{7}))} {
		s, err := valueToScalar(v)
		if err != nil {
			t.Fatal(err)
		}
		if v.Kind() == KindBytes && s.Kind() != graphstate.ScalarBytes || v.Kind() == KindList && s.Kind() != graphstate.ScalarList {
			t.Fatalf("scalar kind %d", s.Kind())
		}
		back, err := scalarToValue(s)
		if err != nil {
			t.Fatal(err)
		}
		a, _ := valueToScalar(back)
		ok, err := a.Identical(s, graphstate.Limits{})
		if err != nil || !ok || back.Kind() != v.Kind() {
			t.Fatalf("round trip %v -> %v (%v)", v, back, err)
		}
	}
	for _, k := range []ValueKind{KindBytes, KindList} {
		if _, err := kindToScalar(k); err != nil {
			t.Errorf("declared kind %d: %v", k, err)
		}
	}
}
