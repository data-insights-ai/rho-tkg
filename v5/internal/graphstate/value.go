package graphstate

import (
	"cmp"
	"encoding/binary"
	"errors"
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
	ScalarDescriptor
	// ScalarBytes is an exact byte string, distinct from a string; nil and empty are one value.
	ScalarBytes
	// ScalarList is a flat ordered list of string, bool, i64, f64 or bytes
	// elements. Lists do not nest; kinds are kept per element.
	ScalarList
)

// Bytes and List keep their payload in text (raw bytes; or u32 length | element
// wire per element), so a Scalar stays comparable and immutable.
//
// Scalar is immutable typed data, not a boxed universal row payload. Finite F64
// is numeric: signed zeros canonicalize to +0. Raw source bits require separate
// provenance/opaque preservation; no IEEE source-bit guarantee is made here.
type Scalar struct {
	kind       ScalarKind
	text       string
	i64        int64
	f64        float64
	boolean    bool
	scope      temporal.Scope
	descriptor *temporal.OpaqueDescriptor
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

// Bytes constructs an exact byte string. The input is copied; nil and empty are
// the same value, and Bytes never equals a String with the same bytes.
func Bytes(b []byte) Scalar { return Scalar{kind: ScalarBytes, text: string(b)} }

// List constructs a flat list. Elements keep their kinds and order; a null,
// nested list, scope, descriptor or zero element is ErrInvalidInput. Without
// null elements, Equal and EqualityKey are one rule for every non-null value.
func List(elems []Scalar) (Scalar, error) {
	var body []byte
	for _, e := range elems {
		if !listElementKind(e.kind) {
			return Scalar{}, ErrInvalidInput
		}
		w, err := e.AppendWire(nil, Limits{MaxReadBytes: maxWire})
		if err != nil {
			return Scalar{}, err
		}
		body = binary.BigEndian.AppendUint32(body, uint32(len(w))) // #nosec G115 -- bounded by maxWire.
		body = append(body, w...)
		if len(body) > maxWire-1 {
			return Scalar{}, ErrResourceLimit
		}
	}
	return Scalar{kind: ScalarList, text: string(body)}, nil
}

// UniqueForm returns the member of v's equality class that a unique claim
// stores: a list with every integral float element exact in int64 as I64, any
// other value unchanged. Unique claims key by identity; within this form
// IdentityKey and EqualityKey agree, so a claim follows Equal ([3.0,4] is the
// claim [3,4]), as a typed scalar key does through its declared kind.
func (v Scalar) UniqueForm() Scalar {
	els, ok := v.ListValue()
	if !ok {
		return v
	}
	changed := false
	for i, e := range els {
		if e.kind == ScalarF64 && e.f64 == math.Trunc(e.f64) && e.f64 >= -(1<<63) && e.f64 < 1<<63 {
			els[i], changed = I64(int64(e.f64)), true
		}
	}
	if !changed {
		return v
	}
	out, err := List(els)
	if err != nil {
		return v // unreachable: same elements, I64 for F64, same or smaller wire
	}
	return out
}

// maxWire is the largest wire Limits accept (Limits.resolve).
const maxWire = 64 << 20

func listElementKind(k ScalarKind) bool {
	switch k {
	case ScalarString, ScalarBool, ScalarI64, ScalarF64, ScalarBytes:
		return true
	}
	return false
}

// BytesValue returns a copy of a typed byte string only.
func (v Scalar) BytesValue() ([]byte, bool) {
	if v.kind != ScalarBytes {
		return nil, false
	}
	return []byte(v.text), true
}

// ListValue returns the elements of a typed list only, in order, each with its kind.
func (v Scalar) ListValue() ([]Scalar, bool) {
	if v.kind != ScalarList {
		return nil, false
	}
	els, err := parseList(v.text)
	return els, err == nil
}

// parseList decodes a list body: repeated u32 length | element wire. Every
// length is checked against what is left; any non-list element kind and any
// non-canonical element is ErrCorruptWire.
func parseList(body string) ([]Scalar, error) {
	els := []Scalar{}
	for len(body) > 0 {
		if len(body) < 4 {
			return nil, ErrCorruptWire
		}
		n := int(binary.BigEndian.Uint32([]byte(body[:4])))
		body = body[4:]
		if n < 1 || n > len(body) {
			return nil, ErrCorruptWire
		}
		if !listElementKind(ScalarKind(body[0])) {
			return nil, ErrCorruptWire
		}
		e, err := DecodeWire([]byte(body[:n]), temporal.Axis{}, false, temporal.Limits{})
		if err != nil {
			return nil, ErrCorruptWire
		}
		els = append(els, e)
		body = body[n:]
	}
	return els, nil
}

// ScopeValue preserves a temporal value without changing its owner's scope.
func ScopeValue(v temporal.Scope) Scalar { return Scalar{kind: ScalarScope, scope: v} }

// DescriptorValue preserves a previously constructed immutable envelope. It
// validates no payload semantics and does not reapply default child limits;
// the caller's operation limits still bound emission, planning and storage.
func DescriptorValue(d temporal.OpaqueDescriptor) (Scalar, error) {
	if d.SupportLevel() != temporal.DescriptorPreservationOnly {
		return Scalar{}, ErrInvalidInput
	}
	return Scalar{kind: ScalarDescriptor, descriptor: new(d)}, nil
}

// Descriptor returns the immutable child only for a typed descriptor scalar.
// Its Spec accessor owns mutable copies. Zero/other scalars return zero,false.
func (v Scalar) Descriptor() (temporal.OpaqueDescriptor, bool) {
	if v.kind != ScalarDescriptor || v.descriptor == nil {
		return temporal.OpaqueDescriptor{}, false
	}
	return *v.descriptor, true
}

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

// ErrCorruptWire reports stored scalar bytes that are not a canonical wire value.
var ErrCorruptWire = errors.New("graphstate: corrupt scalar wire")

// EqualityKey is a deterministic typed key, not an ordered index key. Numbers
// key by value: an integral F64 inside the int64 range keys as the same I64, so
// I64(3) and F64(3.0) share a key (Cypher exact equality) while 2^53+1 never
// collides with 2^53. Descriptor keys identify exact preserved envelopes, never
// payload semantic equivalence. Bytes key bytewise; a List keys elementwise by
// this same numeric rule, in order. Use AppendWire for the kind-preserving stored form.
func (v Scalar) EqualityKey(l Limits) (string, error) {
	if v.kind == ScalarF64 && !math.IsNaN(v.f64) && !math.IsInf(v.f64, 0) &&
		v.f64 == math.Trunc(v.f64) && v.f64 >= -(1<<63) && v.f64 < 1<<63 {
		v = I64(int64(v.f64))
	}
	if v.kind == ScalarList {
		return v.listEqualityKey(l)
	}
	b, err := v.AppendWire(nil, l)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// listEqualityKey frames each element's EqualityKey, so elements compare by the
// numeric rule (I64(3) and F64(3.0) alike) and by position, never by splitting.
func (v Scalar) listEqualityKey(l Limits) (string, error) {
	l, err := l.resolve()
	if err != nil {
		return "", err
	}
	els, ok := v.ListValue()
	if !ok {
		return "", ErrInvalidInput
	}
	b := []byte{byte(ScalarList)}
	for _, e := range els {
		k, err := e.EqualityKey(l)
		if err != nil {
			return "", err
		}
		b = binary.BigEndian.AppendUint32(b, uint32(len(k))) // #nosec G115 -- element wire is bounded by Limits.
		b = append(b, k...)
	}
	if len(b) > l.MaxReadBytes {
		return "", ErrResourceLimit
	}
	return string(b), nil
}

// IdentityKey is the kind-preserving wire as a string: the key of the value
// dictionary (interning, ValueID lookup, set order). I64(3) and F64(3.0) are
// distinct identities there; they meet only through Equal and EqualityKey.
func (v Scalar) IdentityKey(l Limits) (string, error) {
	b, err := v.AppendWire(nil, l)
	return string(b), err
}

// AppendWire appends the kind-preserving stored encoding: kind byte plus payload.
// Unlike EqualityKey, F64(3.0) stays F64. DecodeWire is its inverse.
func (v Scalar) AppendWire(dst []byte, l Limits) ([]byte, error) {
	l, err := l.resolve()
	if err != nil {
		return nil, err
	}
	b, err := v.bytes(l)
	if err != nil {
		return nil, err
	}
	return append(dst, b...), nil
}

// DecodeWire is the one decoder of AppendWire output. A scope needs its decoded
// axis and every other kind forbids one; any other mismatch is ErrCorruptWire,
// and temporal resource refusals also wrap ErrResourceLimit.
func DecodeWire(wire []byte, axis temporal.Axis, hasAxis bool, tl temporal.Limits) (Scalar, error) {
	if len(wire) == 0 {
		return Scalar{}, ErrCorruptWire
	}
	kind := ScalarKind(wire[0])
	if hasAxis != (kind == ScalarScope) {
		return Scalar{}, ErrCorruptWire
	}
	body := wire[1:]
	switch kind {
	case ScalarNull:
		if len(body) == 0 {
			return Null(), nil
		}
	case ScalarString:
		return String(string(body)), nil
	case ScalarBytes:
		return Scalar{kind: ScalarBytes, text: string(body)}, nil
	case ScalarList:
		if _, err := parseList(string(body)); err == nil {
			return Scalar{kind: ScalarList, text: string(body)}, nil
		}
	case ScalarBool:
		if len(body) == 1 && body[0] <= 1 {
			return Bool(body[0] == 1), nil
		}
	case ScalarI64:
		if len(body) == 8 {
			return I64(int64(binary.BigEndian.Uint64(body))), nil // #nosec G115 -- exact two's-complement payload.
		}
	case ScalarF64:
		if len(body) == 8 {
			if bits := binary.BigEndian.Uint64(body); bits != 1<<63 {
				if v, err := F64(math.Float64frombits(bits)); err == nil {
					return v, nil
				}
			}
		}
	case ScalarDescriptor:
		d, err := temporal.DecodeOpaqueDescriptor(body, tl)
		if err == nil {
			if v, err := DescriptorValue(d); err == nil {
				return v, nil
			}
		} else if errors.Is(err, temporal.ErrResourceLimit) {
			return Scalar{}, errors.Join(ErrResourceLimit, err)
		}
	case ScalarScope:
		s, err := temporal.DecodeScope(body, axis, tl)
		if err == nil {
			return ScopeValue(s), nil
		}
		if errors.Is(err, temporal.ErrResourceLimit) {
			return Scalar{}, errors.Join(ErrResourceLimit, err)
		}
	}
	return Scalar{}, ErrCorruptWire
}
func (v Scalar) bytes(l Limits) ([]byte, error) {
	if v.kind < ScalarNull || v.kind > ScalarList {
		return nil, ErrInvalidInput
	}
	b := []byte{byte(v.kind)}
	switch v.kind {
	case ScalarBytes, ScalarList:
		if len(v.text) > l.MaxReadBytes-1 {
			return nil, ErrResourceLimit
		}
		b = append(b, v.text...)
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
	case ScalarDescriptor:
		if v.descriptor == nil {
			return nil, ErrInvalidInput
		}
		tl := l.Component.Temporal
		td := temporal.DefaultLimits()
		tl.MaxValueBytes = min(cmp.Or(tl.MaxValueBytes, td.MaxValueBytes), max(1, l.MaxReadBytes-1))
		tl.MaxDescriptorBytes = min(cmp.Or(tl.MaxDescriptorBytes, td.MaxDescriptorBytes), max(1, l.MaxReadBytes-1))
		wire, err := temporal.AppendOpaqueDescriptor(nil, *v.descriptor, tl)
		if err != nil {
			if errors.Is(err, temporal.ErrResourceLimit) {
				return nil, errors.Join(ErrResourceLimit, err)
			}
			return nil, err
		}
		b = append(b, wire...)
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

// retainedBytes charges canonical typed payload plus definitions retained by the
// immutable value. EqualityKey intentionally uses axis identity/hash only; its
// byte length is not the retained-value ledger and is not a Go heap measurement.
func (v Scalar) retainedBytes(canonicalBytes int) int {
	n := canonicalBytes
	if v.kind == ScalarScope {
		n += axisBytes(v.scope.Axis())
	}
	return n
}

// Equal is Cypher exact equality, consumed by query evaluation (pkg/graph, WP-08)
// and never by the value dictionary: numbers compare by value (I64(3) = F64(3.0)),
// kinds otherwise never coerce, and null equals nothing, itself included.
// Descriptor equality means exact envelope identity only, not solver equality.
// Value identity uses Identical.
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
	return a == b && v.kind != ScalarNull, nil
}

// Identical reports equal IdentityKeys (kind-preserving wire): same kind, same
// value, and null identical to null. It is the relation for value identity,
// interning and dependency checks, where I64(3) and F64(3.0) are different values.
func (v Scalar) Identical(other Scalar, l Limits) (bool, error) {
	l, err := l.resolve()
	if err != nil {
		return false, err
	}
	a, err := v.IdentityKey(l)
	if err != nil {
		return false, err
	}
	b, err := other.IdentityKey(l)
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
	case ScalarDescriptor:
		return "" // No payload interpretation or implicit JSON rendering.
	default:
		return ""
	}
}
