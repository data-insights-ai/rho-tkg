package graphstate

import (
	"bytes"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func mustList(t *testing.T, elems ...Scalar) Scalar {
	t.Helper()
	v, err := List(elems)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func decode(t *testing.T, wire []byte) (Scalar, error) {
	t.Helper()
	return DecodeWire(wire, temporal.Axis{}, false, temporal.Limits{})
}

// TestScalarBytesAndListWireRoundTrip: Bytes and List keep their kind through
// AppendWire/DecodeWire; empty, nil, nested, oversize and truncated input.
func TestScalarBytesAndListWireRoundTrip(t *testing.T) {
	l := Limits{}
	good := []Scalar{
		Bytes(nil), Bytes([]byte{}), Bytes([]byte{0}), Bytes([]byte("\x00\xff\x00")),
		mustList(t), mustList(t, I64(1), I64(2)), mustList(t, mustF64(t, 2.5), String("x"), Bool(true), Null(), Bytes([]byte{1})),
	}
	for i, v := range good {
		w, err := v.AppendWire(nil, l)
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		back, err := decode(t, w)
		if err != nil || back.Kind() != v.Kind() {
			t.Fatalf("%d: back %v %v", i, back.Kind(), err)
		}
		w2, _ := back.AppendWire(nil, l)
		if !bytes.Equal(w, w2) {
			t.Fatalf("%d: wire not stable", i)
		}
	}
	// nil and empty bytes are one value; empty list and Bytes are different kinds.
	a, _ := Bytes(nil).AppendWire(nil, l)
	b, _ := Bytes([]byte{}).AppendWire(nil, l)
	if !bytes.Equal(a, b) {
		t.Fatal("nil bytes and empty bytes differ on the wire")
	}
	if bs, ok := Bytes(nil).BytesValue(); !ok || len(bs) != 0 {
		t.Fatal("empty bytes accessor")
	}
	if _, ok := String("").BytesValue(); ok {
		t.Fatal("string read as bytes")
	}
	// accessor and constructor copy: the scalar is immutable.
	src := []byte{1, 2, 3}
	v := Bytes(src)
	src[0] = 9
	got, _ := v.BytesValue()
	got[1] = 9
	again, _ := v.BytesValue()
	if !bytes.Equal(again, []byte{1, 2, 3}) {
		t.Fatalf("aliased: %v", again)
	}
	// nested list, scope/descriptor/invalid elements are refused.
	if _, err := List([]Scalar{mustList(t)}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("nested list: %v", err)
	}
	if _, err := List([]Scalar{{}}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("zero element: %v", err)
	}
	// truncated list element, trailing junk, nested list on the wire, bad F64 element.
	w, _ := mustList(t, String("abc"), I64(1)).AppendWire(nil, l)
	for _, bad := range [][]byte{
		w[:len(w)-1],                      // cut inside the last element
		w[:3],                             // cut inside the first length
		append(append([]byte{}, w...), 7), // 1 stray byte: a truncated length
		{byte(ScalarList), 0, 0, 0, 1},    // length without element
		{byte(ScalarList), 0, 0, 0, 0},    // zero-length element (no kind byte)
		{byte(ScalarList), 0, 0, 0, 1, byte(ScalarList)},                              // nested list
		{byte(ScalarList), 0, 0, 0, 1, 0},                                             // invalid element kind
		{byte(ScalarList), 0, 0, 0, 9, byte(ScalarF64), 0x7f, 0xf8, 0, 0, 0, 0, 0, 0}, // NaN
		{byte(ScalarList), 0xff, 0xff, 0xff, 0xff},                                    // huge length
	} {
		if _, err := decode(t, bad); !errors.Is(err, ErrCorruptWire) {
			t.Fatalf("% x: %v", bad, err)
		}
	}
	// oversize: bytes and lists past MaxReadBytes are ErrResourceLimit, at the boundary they fit.
	small := Limits{MaxReadBytes: 16}
	if _, err := Bytes(make([]byte, 15)).AppendWire(nil, small); err != nil {
		t.Fatalf("boundary: %v", err)
	}
	if _, err := Bytes(make([]byte, 16)).AppendWire(nil, small); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("oversize bytes: %v", err)
	}
	if _, err := mustList(t, Bytes(make([]byte, 20))).AppendWire(nil, small); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("oversize list: %v", err)
	}
	// a bytes wire and a string wire with the same payload are different values.
	sw, _ := String("ab").AppendWire(nil, l)
	bw, _ := Bytes([]byte("ab")).AppendWire(nil, l)
	if bytes.Equal(sw, bw) {
		t.Fatal("bytes and string share a wire")
	}
	// the elements come back in order with their kinds.
	lv := mustList(t, I64(3), mustF64(t, 3), Null())
	els, ok := lv.ListValue()
	if !ok || len(els) != 3 || els[0].Kind() != ScalarI64 || els[1].Kind() != ScalarF64 || els[2].Kind() != ScalarNull {
		t.Fatalf("elements %v", els)
	}
}

// TestScalarBytesAndListKeys: IdentityKey is exact (bytes, element kinds, order);
// EqualityKey/Equal is bytewise for Bytes and elementwise numeric for List.
func TestScalarBytesAndListKeys(t *testing.T) {
	l := Limits{}
	id := func(a, b Scalar) bool {
		ok, err := a.Identical(b, l)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	eq := func(a, b Scalar) bool {
		ok, err := a.Equal(b, l)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	f3 := mustF64(t, 3)
	cases := []struct {
		name         string
		a, b         Scalar
		ident, equal bool
	}{
		{"bytes same", Bytes([]byte{1, 2}), Bytes([]byte{1, 2}), true, true},
		{"bytes nil empty", Bytes(nil), Bytes([]byte{}), true, true},
		{"bytes differ", Bytes([]byte{1, 2}), Bytes([]byte{2, 1}), false, false},
		{"bytes prefix", Bytes([]byte{1}), Bytes([]byte{1, 0}), false, false},
		{"bytes vs string", Bytes([]byte("a")), String("a"), false, false},
		{"bytes vs empty list", Bytes(nil), mustList(t), false, false},
		{"list same", mustList(t, I64(1), I64(2)), mustList(t, I64(1), I64(2)), true, true},
		{"list order", mustList(t, I64(1), I64(2)), mustList(t, I64(2), I64(1)), false, false},
		{"list int vs float element", mustList(t, I64(3)), mustList(t, f3), false, true},
		{"list length", mustList(t, I64(3)), mustList(t, I64(3), I64(3)), false, false},
		{"list empty", mustList(t), mustList(t), true, true},
		{"list frame ambiguity", mustList(t, String("ab"), String("c")), mustList(t, String("a"), String("bc")), false, false},
		{"list bool vs int", mustList(t, Bool(true)), mustList(t, I64(1)), false, false},
		{"list bytes elements", mustList(t, Bytes([]byte{1})), mustList(t, Bytes([]byte{1})), true, true},
		{"list null never equal", mustList(t, Null()), mustList(t, Null()), true, false},
		{"list null among values", mustList(t, I64(1), Null()), mustList(t, I64(1), Null()), true, false},
		{"list vs scalar", mustList(t, I64(3)), I64(3), false, false},
	}
	for _, c := range cases {
		if got := id(c.a, c.b); got != c.ident {
			t.Errorf("%s: identical %t want %t", c.name, got, c.ident)
		}
		if got := eq(c.a, c.b); got != c.equal {
			t.Errorf("%s: equal %t want %t", c.name, got, c.equal)
		}
		ka, _ := c.a.EqualityKey(l)
		kb, _ := c.b.EqualityKey(l)
		if c.equal && ka != kb {
			t.Errorf("%s: equal values, different EqualityKey", c.name)
		}
		if !c.equal && !c.ident && ka == kb {
			t.Errorf("%s: unequal values share an EqualityKey", c.name)
		}
	}
}
