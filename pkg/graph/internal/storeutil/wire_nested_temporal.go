package storeutil

import (
	"fmt"
	"strings"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Nested value wire envelopes.
//
// A TOP-LEVEL property value is carried with its own property type tag, which
// reconstructs its exact Go kind after msgpack decoding. Values NESTED inside
// an []any / map[string]any have no tag of their own: msgpack encodes them
// structurally and decoding yields only generic values — a nested int16 comes
// back as some integer width (normalized to int64), an int or uint as int64, a
// []string as []any, a map[string]string as map[string]any, a typed nil
// container as nil, a registered struct as a map, and a temporal as something
// other than a TemporalValue. The content hash covers the exact kinds, so any
// such loss makes a stored entity fail its hash chain after a persisted round
// trip (backlog item 3). Writing a temporal's ISO rendering as a plain string
// is moreover exactly the type erasure TemporalValue exists to prevent.
//
// So a nested value whose kind msgpack would lose is encoded as a marker-led
// array:
//
//	["\x00tkg.tv", kind, iso]              a types.TemporalValue
//	["\x00tkg.k", tag, payload]            a scalar or typed container carried
//	                                       under its property type tag; the
//	                                       payload is what a top-level property
//	                                       of that tag carries
//	["\x00tkg.k", tag]                     a typed nil container of that tag
//	["\x00tkg.c", type, pointer, msgpack]  a registered custom struct, with the
//	                                       same per-value round-trip proof as a
//	                                       top-level one
//
// Kinds msgpack preserves on its own — nil, bool, string, int64, uint64,
// float32, float64, a non-nil []byte and non-nil []any / map[string]any (whose
// elements are encoded recursively) — are written raw, so a value made only of
// those keeps its exact previous wire bytes. Rows written before an envelope
// existed carry the raw value and decode as they always did.
//
// The markers are byte sequences no legal property key or realistic string
// payload starts with, but they are not impossible to forge: a caller may
// store an []any whose FIRST element is such a string. Such a list is
// therefore escaped on the way out
//
//	["\x00tkg.esc", <original elements…>]
//
// and unwrapped on the way in, which makes the mapping total and injective —
// every original value has exactly one encoding and every valid encoding
// exactly one original (the decoder accepts an envelope only in the form the
// encoder writes). Only a list's first element can collide (a marker string
// anywhere else is just a string), so the check is one comparison per nested
// list.
const (
	nestedWireMarkerPrefix   = "\x00tkg."
	nestedWireTemporalMarker = nestedWireMarkerPrefix + "tv"
	nestedWireEscapeMarker   = nestedWireMarkerPrefix + "esc"
	nestedWireKindMarker     = nestedWireMarkerPrefix + "k"
	nestedWireCustomMarker   = nestedWireMarkerPrefix + "c"
)

// Element counts of the envelopes.
const (
	nestedTemporalWireLen = 3
	nestedKindWireLen     = 3
	nestedKindNilWireLen  = 2
	nestedCustomWireLen   = 4
)

// nestedKindTag returns the property type tag a nested value is carried under
// in a kind envelope, and false for a value whose kind survives the msgpack
// hop on its own (or that another envelope carries). The decoder accepts a
// kind envelope only when its reconstructed value maps back to the same tag
// here, so this one switch defines both directions.
func nestedKindTag(v any) (byte, bool) {
	switch x := v.(type) {
	case int, int8, int16, int32, uint, uint8, uint16, uint32,
		[]string, []int, []int64, []float32, []float64, []bool, map[string]string:
		return PropertyTypeTag(v), true
	case []byte:
		return ptSliceByte, x == nil
	case []any:
		return ptSliceAny, x == nil
	case map[string]any:
		return ptMapStrAny, x == nil
	}
	return 0, false
}

// isNativeNestedScalar reports the kinds msgpack preserves on its own, so the
// write path settles the common elements without a custom-type registry
// lookup.
func isNativeNestedScalar(v any) bool {
	switch v.(type) {
	case nil, bool, string, int64, uint64, float32, float64:
		return true
	}
	return false
}

// isNestedCustomValue reports whether v is a registered custom struct value,
// carried in the custom envelope.
func isNestedCustomValue(v any) bool {
	if _, isTemporal := v.(types.TemporalValue); isTemporal || v == nil {
		return false
	}
	_, _, ok := types.RegisteredPropertyStructWireType(v)
	return ok
}

// isNestedWireMarker reports whether v is a string in the reserved marker
// namespace — the only values whose appearance as a list's first element is
// ambiguous with an envelope.
func isNestedWireMarker(v any) bool {
	s, ok := v.(string)
	return ok && strings.HasPrefix(s, nestedWireMarkerPrefix)
}

// nestedWireNeedsEncoding reports whether v contains anything an envelope has
// to carry. It allocates nothing, so property values made only of kinds
// msgpack preserves pay one traversal and keep their original wire bytes.
func nestedWireNeedsEncoding(v any, depth int) bool {
	if depth > maxWireDecodeDepth {
		return false
	}
	switch val := v.(type) {
	case types.TemporalValue:
		return true
	case []any:
		if val == nil || (len(val) > 0 && isNestedWireMarker(val[0])) {
			return true
		}
		for _, e := range val {
			if nestedWireNeedsEncoding(e, depth+1) {
				return true
			}
		}
		return false
	case map[string]any:
		if val == nil {
			return true
		}
		for _, e := range val {
			if nestedWireNeedsEncoding(e, depth+1) {
				return true
			}
		}
		return false
	}
	if isNativeNestedScalar(v) {
		return false
	}
	if _, ok := nestedKindTag(v); ok {
		return true
	}
	return isNestedCustomValue(v)
}

// encodeNestedWireValue rewrites nested values whose kind msgpack would lose
// into their envelope and escapes marker-shaped lists. Callers gate it on
// nestedWireNeedsEncoding. It fails only for a custom struct whose msgpack
// round trip would change its hash bytes.
func encodeNestedWireValue(v any, depth int) (any, error) {
	if depth > maxWireDecodeDepth {
		return v, nil
	}
	switch val := v.(type) {
	case types.TemporalValue:
		return []any{nestedWireTemporalMarker, int(val.Kind), val.Value}, nil
	case []any:
		if val == nil {
			return []any{nestedWireKindMarker, int(ptSliceAny)}, nil
		}
		escape := len(val) > 0 && isNestedWireMarker(val[0])
		out := make([]any, 0, len(val)+1)
		if escape {
			out = append(out, nestedWireEscapeMarker)
		}
		for _, e := range val {
			enc, err := encodeNestedWireValue(e, depth+1)
			if err != nil {
				return nil, err
			}
			out = append(out, enc)
		}
		return out, nil
	case map[string]any:
		if val == nil {
			return []any{nestedWireKindMarker, int(ptMapStrAny)}, nil
		}
		out := make(map[string]any, len(val))
		for k, e := range val {
			enc, err := encodeNestedWireValue(e, depth+1)
			if err != nil {
				return nil, err
			}
			out[k] = enc
		}
		return out, nil
	}
	if isNativeNestedScalar(v) {
		return v, nil
	}
	if tag, ok := nestedKindTag(v); ok {
		if isTypedNilPropertyValue(v) {
			return []any{nestedWireKindMarker, int(tag)}, nil
		}
		return []any{nestedWireKindMarker, int(tag), v}, nil
	}
	if isNestedCustomValue(v) {
		data, typeName, pointer, err := encodeCustomWireValue(v)
		if err != nil {
			return nil, err
		}
		return []any{nestedWireCustomMarker, typeName, pointer, data}, nil
	}
	return v, nil
}

// decodeNestedWireValue is the exact inverse of encodeNestedWireValue. A
// malformed envelope is an error rather than a best-effort guess: a corrupt or
// hostile blob must not decode into a silently different property value.
func decodeNestedWireValue(v any, depth int) (any, error) {
	if depth > maxWireDecodeDepth {
		return nil, fmt.Errorf("nested temporal envelope exceeds %d levels", maxWireDecodeDepth)
	}
	switch val := v.(type) {
	case []any:
		if val == nil {
			return val, nil
		}
		if len(val) > 0 && isNestedWireMarker(val[0]) {
			return decodeNestedWireEnvelope(val, depth)
		}
		out := make([]any, len(val))
		for i, e := range val {
			decoded, err := decodeNestedWireValue(e, depth+1)
			if err != nil {
				return nil, err
			}
			out[i] = decoded
		}
		return out, nil
	case map[string]any:
		if val == nil {
			return val, nil
		}
		out := make(map[string]any, len(val))
		for k, e := range val {
			decoded, err := decodeNestedWireValue(e, depth+1)
			if err != nil {
				return nil, err
			}
			out[k] = decoded
		}
		return out, nil
	default:
		return v, nil
	}
}

// decodeNestedWireEnvelope handles a list whose first element is a reserved
// marker. The escaped case decodes the payload ELEMENTS, never re-testing the
// unwrapped list for a marker — the original list's own first element is a
// plain string and must stay one.
func decodeNestedWireEnvelope(val []any, depth int) (any, error) {
	marker := val[0].(string) //nolint:errcheck // isNestedWireMarker proved the type
	switch marker {
	case nestedWireTemporalMarker:
		if len(val) != nestedTemporalWireLen {
			return nil, fmt.Errorf("nested temporal envelope has %d elements, want %d", len(val), nestedTemporalWireLen)
		}
		kind, ok := wireUint64(val[1])
		if !ok {
			return nil, fmt.Errorf("nested temporal kind has type %T, want integer", val[1])
		}
		if kind > 255 {
			return nil, fmt.Errorf("nested temporal kind %d out of range", kind)
		}
		iso, ok := val[2].(string)
		if !ok {
			return nil, fmt.Errorf("nested temporal rendering has type %T, want string", val[2])
		}
		tv := types.TemporalValue{Kind: types.TemporalKind(kind), Value: iso} // #nosec G115 -- bounded above
		if err := tv.Validate(); err != nil {
			return nil, err
		}
		return tv, nil
	case nestedWireKindMarker:
		return decodeNestedKindEnvelope(val)
	case nestedWireCustomMarker:
		return decodeNestedCustomEnvelope(val)
	case nestedWireEscapeMarker:
		out := make([]any, len(val)-1)
		for i, e := range val[1:] {
			decoded, err := decodeNestedWireValue(e, depth+1)
			if err != nil {
				return nil, err
			}
			out[i] = decoded
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unknown nested wire marker %q", marker)
	}
}

// decodeNestedKindEnvelope reconstructs a value carried under its property
// type tag. The payload passes the same tag validation as a top-level
// property (range, element kinds, float32 exactness), and the result must be
// a value the encoder wraps under that very tag — anything else (an int64 or
// string in an envelope, a tag of another envelope, a nil payload) is a
// non-canonical or corrupt encoding and an error.
func decodeNestedKindEnvelope(val []any) (any, error) {
	if len(val) != nestedKindWireLen && len(val) != nestedKindNilWireLen {
		return nil, fmt.Errorf("nested kind envelope has %d elements, want %d or %d", len(val), nestedKindNilWireLen, nestedKindWireLen)
	}
	rawTag, ok := wireUint64(val[1])
	if !ok {
		return nil, fmt.Errorf("nested kind envelope tag has type %T, want integer", val[1])
	}
	if rawTag == uint64(ptUnknown) || rawTag > uint64(ptTemporal) {
		return nil, fmt.Errorf("nested kind envelope has unknown type tag %d", rawTag)
	}
	tag := byte(rawTag) // #nosec G115 -- bounded above
	if len(val) == nestedKindNilWireLen {
		if !isNillablePropertyWireType(tag) {
			return nil, fmt.Errorf("nested kind envelope marks non-nillable type tag %d as typed nil", tag)
		}
		return typedNilPropertyWireValue(tag)
	}
	if err := validatePropertyWireValue(val[2], tag); err != nil {
		return nil, fmt.Errorf("nested kind envelope: %w", err)
	}
	out := reconstructTypedValue(val[2], tag)
	if got, ok := nestedKindTag(out); !ok || got != tag || isTypedNilPropertyValue(out) {
		return nil, fmt.Errorf("nested kind envelope carries type tag %d, which the encoder never wraps in one", tag)
	}
	return out, nil
}

// decodeNestedCustomEnvelope reconstructs a registered custom struct through
// the registry, exactly as a top-level custom property.
func decodeNestedCustomEnvelope(val []any) (any, error) {
	if len(val) != nestedCustomWireLen {
		return nil, fmt.Errorf("nested custom envelope has %d elements, want %d", len(val), nestedCustomWireLen)
	}
	typeName, ok := val[1].(string)
	if !ok {
		return nil, fmt.Errorf("nested custom envelope type name has type %T, want string", val[1])
	}
	pointer, ok := val[2].(bool)
	if !ok {
		return nil, fmt.Errorf("nested custom envelope pointer flag has type %T, want bool", val[2])
	}
	data, ok := val[3].([]byte)
	if !ok {
		return nil, fmt.Errorf("nested custom envelope payload has type %T, want []byte", val[3])
	}
	return reconstructCustomPropertyValue(data, typeName, pointer)
}
