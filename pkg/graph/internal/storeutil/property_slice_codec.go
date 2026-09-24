package storeutil

import (
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// MarshalPropertySlice encodes a property slice on its own, as the msgpack
// array of PropertyWire the entity wire uses for its "p" field (untokenized
// keys, the same type tags). The column-segment fallback column (ADR-0011)
// stores the properties outside a declared schema this way, so every value
// keeps its exact Go type through a round trip. A nil or empty slice encodes
// as an empty array.
func MarshalPropertySlice(ps types.PropertySlice) ([]byte, error) {
	pw, err := propertiesToWireChecked(ps)
	if err != nil {
		return nil, fmt.Errorf("property slice: %w", err)
	}
	if pw == nil {
		pw = []PropertyWire{}
	}
	return marshalWirePooled(pw)
}

// UnmarshalPropertySlice decodes MarshalPropertySlice output. The bytes are
// untrusted: decoding goes through SafeUnmarshal (depth guard + panic
// recovery, lesson 47) and the same property-wire validation as a stored
// entity (sorted unique keys, no tkg_ key, value shape per type tag). Every
// failure wraps store.ErrCorruptWire. An empty array decodes to nil.
func UnmarshalPropertySlice(b []byte) (types.PropertySlice, error) {
	var pw []PropertyWire
	if err := SafeUnmarshal(b, &pw); err != nil {
		return nil, fmt.Errorf("%w: property slice: %v", storepkg.ErrCorruptWire, err)
	}
	ps, err := wireToPropertiesChecked(pw)
	if err != nil {
		return nil, fmt.Errorf("%w: property slice: %v", storepkg.ErrCorruptWire, err)
	}
	return ps, nil
}
