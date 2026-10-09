package graphstate

import (
	"encoding/binary"
	"math"
	"strconv"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// ScalarKind is an explicit narrow property subset, extendable by new typed kinds.
type ScalarKind uint8

// Scalar kinds never coerce I64 to F64 or interpret ordinary strings as time.
const (
	ScalarInvalid ScalarKind = iota
	ScalarNull
	ScalarString
	ScalarBool
	ScalarI64
	ScalarF64
	ScalarScope
)

// Scalar is immutable typed data, not a boxed universal row payload. Finite F64
// is numeric: signed zeros canonicalize to +0. Raw source bits require separate
// provenance/opaque preservation; no IEEE source-bit guarantee is made here.
type Scalar struct {
	kind    ScalarKind
	text    string
	i64     int64
	f64     float64
	boolean bool
	scope   temporal.Scope
}

// Null constructs a present-null scalar; absence is an Unset operation.
func Null() Scalar { return Scalar{kind: ScalarNull} }

// String constructs an exact string, bounded when used by the planner.
func String(v string) Scalar { return Scalar{kind: ScalarString, text: v} }

// Bool constructs a typed boolean, distinct from integer one/zero.
func Bool(v bool) Scalar { return Scalar{kind: ScalarBool, boolean: v} }

// I64 constructs a typed exact int64 property value.
func I64(v int64) Scalar { return Scalar{kind: ScalarI64, i64: v} }

// F64 constructs a finite numeric property, normalizing signed zero explicitly.
func F64(v float64) (Scalar, error) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return Scalar{}, ErrInvalidInput
	}
	if v == 0 {
		v = 0
	}
	return Scalar{kind: ScalarF64, f64: v}, nil
}

// ScopeValue preserves a temporal value without changing its owner's scope.
func ScopeValue(v temporal.Scope) Scalar { return Scalar{kind: ScalarScope, scope: v} }

// Kind returns its exact type, or invalid for the zero value.
func (v Scalar) Kind() ScalarKind { return v.kind }

// StringValue returns only a typed string.
func (v Scalar) StringValue() (string, bool) { return v.text, v.kind == ScalarString }

// BoolValue returns only a typed boolean.
func (v Scalar) BoolValue() (bool, bool) { return v.boolean, v.kind == ScalarBool }

// Int64Value returns only a typed int64.
func (v Scalar) Int64Value() (int64, bool) { return v.i64, v.kind == ScalarI64 }

// Float64Value returns only a finite numeric float64.
func (v Scalar) Float64Value() (float64, bool) { return v.f64, v.kind == ScalarF64 }

// Scope returns only a preserved immutable temporal property value.
func (v Scalar) Scope() (temporal.Scope, bool) { return v.scope, v.kind == ScalarScope }

// EqualityKey is a deterministic typed key, not an ordered numeric index key.
func (v Scalar) EqualityKey(l Limits) (string, error) {
	l, err := l.resolve()
	if err != nil {
		return "", err
	}
	b, err := v.bytes(l)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
func (v Scalar) bytes(l Limits) ([]byte, error) {
	if v.kind < ScalarNull || v.kind > ScalarScope {
		return nil, ErrInvalidInput
	}
	b := []byte{byte(v.kind)}
	switch v.kind {
	case ScalarString:
		if len(v.text) > l.MaxReadBytes-1 {
			return nil, ErrResourceLimit
		}
		// This complete typed key needs no nested framing; string bytes retain
		// lexical enumeration order. Container codecs supply their own lengths.
		b = append(b, v.text...)
	case ScalarBool:
		if v.boolean {
			b = append(b, 1)
		} else {
			b = append(b, 0)
		}
	case ScalarI64:
		b = binary.BigEndian.AppendUint64(b, uint64(v.i64)) // #nosec G115 -- exact two's-complement value encoding.
	case ScalarF64:
		if math.IsNaN(v.f64) || math.IsInf(v.f64, 0) {
			return nil, ErrInvalidInput
		}
		bits := math.Float64bits(v.f64)
		if v.f64 == 0 {
			bits = 0
		}
		b = binary.BigEndian.AppendUint64(b, bits)
	case ScalarScope:
		wire, err := temporal.AppendScope(nil, v.scope, l.Component.Temporal)
		if err != nil {
			return nil, err
		}
		b = append(b, wire...)
	}
	if len(b) > l.MaxReadBytes {
		return nil, ErrResourceLimit
	}
	return b, nil
}

// Equal compares declared typed values with finite-F64 numeric zero semantics.
func (v Scalar) Equal(other Scalar, l Limits) (bool, error) {
	l, err := l.resolve()
	if err != nil {
		return false, err
	}
	a, err := v.EqualityKey(l)
	if err != nil {
		return false, err
	}
	b, err := other.EqualityKey(l)
	if err != nil {
		return false, err
	}
	return a == b, nil
}

// Render is diagnostic text, not a wire codec or temporal interpretation.
func (v Scalar) Render() string {
	switch v.kind {
	case ScalarNull:
		return "null"
	case ScalarString:
		return v.text
	case ScalarBool:
		return strconv.FormatBool(v.boolean)
	case ScalarI64:
		return strconv.FormatInt(v.i64, 10)
	case ScalarF64:
		return strconv.FormatFloat(v.f64, 'g', -1, 64)
	default:
		return ""
	}
}
