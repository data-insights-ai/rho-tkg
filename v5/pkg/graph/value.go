package graph

import (
	"fmt"
	"math"
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
// Float values are finite; signed zero is normalised to +0. Callers that hold
// an `any` convert once at their edge (compat layer), which is where the v4
// 0 versus 0.0 question is settled.
type Value struct {
	kind ValueKind
	s    string
	i    int64
	f    float64
	b    bool
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

// Validate reports ErrInvalidValue for the zero Value or an unknown kind.
func (v Value) Validate() error {
	if v.kind < KindNull || v.kind > KindFloat {
		return fmt.Errorf("%w: zero or unknown kind", ErrInvalidValue)
	}
	return nil
}

// Equal is Cypher equality (see Value). Invalid values and null equal nothing.
func (v Value) Equal(o Value) bool {
	if v.Validate() != nil || o.Validate() != nil || v.kind == KindNull || o.kind == KindNull {
		return false
	}
	switch {
	case v.kind == KindString && o.kind == KindString:
		return v.s == o.s
	case v.kind == KindBool && o.kind == KindBool:
		return v.b == o.b
	case v.kind == KindInt && o.kind == KindInt:
		return v.i == o.i
	case v.kind == KindFloat && o.kind == KindFloat:
		return v.f == o.f
	case v.kind == KindInt && o.kind == KindFloat:
		return intEqualsFloat(v.i, o.f)
	case v.kind == KindFloat && o.kind == KindInt:
		return intEqualsFloat(o.i, v.f)
	}
	return false
}

// intEqualsFloat is exact: f must be integral and inside the int64 range.
func intEqualsFloat(i int64, f float64) bool {
	if f != math.Trunc(f) || f < -(1<<63) || f >= 1<<63 {
		return false
	}
	return int64(f) == i
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
