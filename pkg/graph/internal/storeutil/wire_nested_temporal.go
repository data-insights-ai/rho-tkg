package storeutil

import (
	"fmt"
	"strings"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Nested temporal wire envelope.
//
// A TOP-LEVEL types.TemporalValue is carried by its own property type tag
// (ptTemporal). Values NESTED inside an []any / map[string]any have no tag of
// their own: msgpack encodes them structurally and decoding yields only
// generic containers, so a nested temporal would come back as something other
// than a TemporalValue and its kind would be lost. Writing the ISO rendering
// as a plain string instead is exactly the type erasure TemporalValue exists
// to prevent: it makes a genuine string property that happens to look like a
// date indistinguishable from a date.
//
// So a nested temporal is encoded as a marker-led array
//
//	["\x00tkg.tv", kind, iso]
//
// The marker is a byte sequence no legal property key or realistic string
// payload starts with, but it is not impossible to forge: a caller may store
// an []any whose FIRST element is that very string. Such a list is therefore
// escaped on the way out
//
//	["\x00tkg.esc", <original elements…>]
//
// and unwrapped on the way in, which makes the mapping total and injective —
// every original value has exactly one encoding and every valid encoding
// exactly one original. Only a list's first element can collide (a marker
// string anywhere else is just a string), so the check is one comparison per
// nested list and nothing is rewritten unless the value actually contains a
// temporal or a marker-shaped list.
const (
	nestedWireMarkerPrefix   = "\x00tkg."
	nestedWireTemporalMarker = nestedWireMarkerPrefix + "tv"
	nestedWireEscapeMarker   = nestedWireMarkerPrefix + "esc"
	// Stub for the red run of backlog item 3; not yet written or read.
	nestedWireKindMarker   = nestedWireMarkerPrefix + "k"
	nestedWireCustomMarker = nestedWireMarkerPrefix + "c"
)

// nestedTemporalWireLen is the element count of a temporal envelope.
const nestedTemporalWireLen = 3

// isNestedWireMarker reports whether v is a string in the reserved marker
// namespace — the only values whose appearance as a list's first element is
// ambiguous with an envelope.
func isNestedWireMarker(v any) bool {
	s, ok := v.(string)
	return ok && strings.HasPrefix(s, nestedWireMarkerPrefix)
}

// nestedWireNeedsEncoding reports whether v contains anything the envelope
// encoding has to rewrite. It allocates nothing, so property values without
// nested temporals pay one traversal and keep their original wire bytes.
func nestedWireNeedsEncoding(v any, depth int) bool {
	if depth > maxWireDecodeDepth {
		return false
	}
	switch val := v.(type) {
	case types.TemporalValue:
		return true
	case []any:
		if len(val) > 0 && isNestedWireMarker(val[0]) {
			return true
		}
		for _, e := range val {
			if nestedWireNeedsEncoding(e, depth+1) {
				return true
			}
		}
	case map[string]any:
		for _, e := range val {
			if nestedWireNeedsEncoding(e, depth+1) {
				return true
			}
		}
	}
	return false
}

// encodeNestedWireValue rewrites nested temporals into their envelope and
// escapes marker-shaped lists. Callers gate it on nestedWireNeedsEncoding.
func encodeNestedWireValue(v any, depth int) any {
	if depth > maxWireDecodeDepth {
		return v
	}
	switch val := v.(type) {
	case types.TemporalValue:
		return []any{nestedWireTemporalMarker, int(val.Kind), val.Value}
	case []any:
		if val == nil {
			return val
		}
		escape := len(val) > 0 && isNestedWireMarker(val[0])
		out := make([]any, 0, len(val)+1)
		if escape {
			out = append(out, nestedWireEscapeMarker)
		}
		for _, e := range val {
			out = append(out, encodeNestedWireValue(e, depth+1))
		}
		return out
	case map[string]any:
		if val == nil {
			return val
		}
		out := make(map[string]any, len(val))
		for k, e := range val {
			out[k] = encodeNestedWireValue(e, depth+1)
		}
		return out
	default:
		return v
	}
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
