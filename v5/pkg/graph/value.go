package graph

import (
	"fmt"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

// ValueKind is the type of a Value.
type ValueKind uint8

// Value kinds. The set mirrors the engine's typed scalar (null, string, bool,
// int64, finite float64) and is extended by new typed kinds, never by a boxed
// universal payload.
const (
	// KindInvalid is the kind of the zero Value.
	KindInvalid ValueKind = iota
	KindNull
	KindString
	KindBool
	KindInt
	KindFloat
	// KindBytes is an exact byte string, distinct from a string; nil and empty are one value.
	KindBytes
	// KindList is a flat ordered list of string, bool, int, float or bytes
	// elements; lists do not nest and hold no null.
	KindList
)

// Value is an immutable, typed property value. The zero Value is invalid;
// absence is an Unset op, a present null is Null().
//
// Numeric equality is one rule: Cypher equality across numeric widths, so
// Int(3).Equal(Float(3)) is true. The comparison is exact (an int64 above 2^53
// equals a float only if the float holds that exact integer). Kinds never
// coerce otherwise: Int(1) does not equal Bool(true) or String("1"), and the
// accessors AsInt and AsFloat read only their own kind. Null equals nothing,
// itself included (Cypher: null = null is unknown); test IsNull.
//
// Bytes compare bytewise and never equal a String. A List compares in order,
// element by element, by the same numeric rule (List{Int(3)} equals
// List{Float(3)}); a list holds no null, so list equality is never unknown.
// Constructors and accessors copy, so a Value is immutable.
//
// Float values are finite; signed zero is normalised to +0. Callers that hold
// an `any` convert once at their edge (compat layer), which is where the v4
// 0 versus 0.0 question is settled.
type Value struct {
	kind ValueKind
	s    string
	i    int64
	f    float64
	b    bool
	l    *[]Value // list elements; a pointer keeps Value comparable
}

// Null is a present null.
func Null() Value { return Value{kind: KindNull} }

// String is an exact string.
func String(v string) Value { return Value{kind: KindString, s: v} }

// Bool is a typed boolean, distinct from integer 1 and 0.
func Bool(v bool) Value { return Value{kind: KindBool, b: v} }

// Int is an exact int64.
func Int(v int64) Value { return Value{kind: KindInt, i: v} }

// Float is a finite float64; NaN and the infinities are ErrInvalidValue.
func Float(v float64) (Value, error) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return Value{}, fmt.Errorf("%w: float must be finite", ErrInvalidValue)
	}
	if v == 0 {
		v = 0 // -0 becomes +0
	}
	return Value{kind: KindFloat, f: v}, nil
}

// Bytes is an exact byte string; the input is copied.
func Bytes(b []byte) Value { return Value{kind: KindBytes, s: string(b)} }

// List is a flat list. A null, a nested list, the zero Value or an unknown
// kind as an element is ErrInvalidValue.
func List(vs []Value) (Value, error) {
	out := make([]Value, len(vs))
	for i, e := range vs {
		if e.kind <= KindNull || e.kind > KindBytes {
			return Value{}, fmt.Errorf("%w: list element %d has kind %d", ErrInvalidValue, i, e.kind)
		}
		out[i] = e
	}
	return Value{kind: KindList, l: &out}, nil
}

// Kind returns the value's type (KindInvalid for the zero Value).
func (v Value) Kind() ValueKind { return v.kind }

// IsNull reports a present null.
func (v Value) IsNull() bool { return v.kind == KindNull }

// AsString returns the string of a String value only.
func (v Value) AsString() (string, bool) { return v.s, v.kind == KindString }

// AsBool returns the boolean of a Bool value only.
func (v Value) AsBool() (bool, bool) { return v.b, v.kind == KindBool }

// AsInt returns the integer of an Int value only.
func (v Value) AsInt() (int64, bool) { return v.i, v.kind == KindInt }

// AsFloat returns the float of a Float value only.
func (v Value) AsFloat() (float64, bool) { return v.f, v.kind == KindFloat }

// AsBytes returns a copy of the bytes of a Bytes value only.
func (v Value) AsBytes() ([]byte, bool) {
	if v.kind != KindBytes {
		return nil, false
	}
	return []byte(v.s), true
}

// AsList returns a copy of the elements of a List value only.
func (v Value) AsList() ([]Value, bool) {
	if v.kind != KindList || v.l == nil {
		return nil, false
	}
	return append([]Value{}, *v.l...), true
}

// Validate reports ErrInvalidValue for the zero Value or an unknown kind.
func (v Value) Validate() error {
	if v.kind < KindNull || v.kind > KindList {
		return fmt.Errorf("%w: zero or unknown kind", ErrInvalidValue)
	}
	return nil
}

// Equal is Cypher equality (see Value): the engine's Scalar.Equal, so there is
// one numeric rule. Invalid values and null equal nothing.
func (v Value) Equal(o Value) bool {
	a, err := valueToScalar(v)
	if err != nil {
		return false
	}
	b, err := valueToScalar(o)
	if err != nil {
		return false
	}
	eq, err := a.Equal(b, graphstate.Limits{})
	return err == nil && eq
}

func checkProps(m map[string]Value) error {
	for k, v := range m {
		if err := nonEmpty("property key", k); err != nil {
			return err
		}
		if err := v.Validate(); err != nil {
			return err
		}
	}
	return nil
}
