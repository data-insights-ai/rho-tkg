package storeutil

import (
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/registry"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// ApplyPropertyKeyTokens replaces each PropertyWire.Key with its registry-
// allocated KeyToken when the registry has (or can allocate) one. The Key
// field is cleared in that case so msgpack's `omitempty` drops it from the
// wire. When the registry is full and returns token 0, Key is left intact
// (graceful fallback to raw-key wire format).
//
// Mutates pw in place. Pass nil registry to skip tokenization.
func ApplyPropertyKeyTokens(pw []PropertyWire, reg *registrypkg.PropertyKeyRegistry) {
	if reg == nil {
		return
	}
	for i := range pw {
		if pw[i].Key == "" {
			continue
		}
		tok, _ := reg.GetOrCreate(pw[i].Key)
		if tok != 0 {
			pw[i].KeyToken = tok
			pw[i].Key = ""
		}
	}
}

// ResolvePropertyKeyTokens populates PropertyWire.Key from KeyToken via the
// registry. Reads of pre-tokenization (V1) data are no-ops because Key is
// already set. Returns an error if a token cannot be resolved — that
// indicates the registry is desynchronized from the persisted data and
// further decoding would silently produce wrong keys.
//
// Mutates pw in place. Pass nil registry to skip resolution (rows with
// KeyToken-only and no Key will be rejected by the property validator).
func ResolvePropertyKeyTokens(pw []PropertyWire, reg *registrypkg.PropertyKeyRegistry) error {
	if reg == nil {
		return nil
	}
	for i := range pw {
		if pw[i].KeyToken == 0 {
			continue
		}
		if pw[i].Key != "" {
			continue // wire carries both; trust Key
		}
		resolved := reg.Resolve(pw[i].KeyToken)
		if resolved == "" {
			return fmt.Errorf("storeutil: property-key token %d not in registry", pw[i].KeyToken)
		}
		pw[i].Key = resolved
	}
	return nil
}

// MarshalNodeWireWithKeys is the registry-aware variant of MarshalNodeWire.
// Pass a non-nil registry to dictionary-encode property keys on the wire.
func MarshalNodeWireWithKeys(n *types.Node, reg *registrypkg.PropertyKeyRegistry) ([]byte, error) {
	w, err := NodeToWireChecked(n)
	if err != nil {
		return nil, err
	}
	ApplyPropertyKeyTokens(w.Properties, reg)
	return msgpack.Marshal(w)
}

// MarshalRelWireWithKeys is the registry-aware variant of MarshalRelWire.
func MarshalRelWireWithKeys(r *types.Relationship, reg *registrypkg.PropertyKeyRegistry) ([]byte, error) {
	w, err := RelToWireChecked(r)
	if err != nil {
		return nil, err
	}
	ApplyPropertyKeyTokens(w.Properties, reg)
	return msgpack.Marshal(w)
}

// UnmarshalNodeWireWithKeys decodes a NodeWire from msgpack bytes and
// resolves any tokenized property keys via the registry. Pre-tokenization
// (V1) rows pass through unchanged.
func UnmarshalNodeWireWithKeys(data []byte, reg *registrypkg.PropertyKeyRegistry) (NodeWire, error) {
	var w NodeWire
	if err := msgpack.Unmarshal(data, &w); err != nil {
		return NodeWire{}, err
	}
	if err := ResolvePropertyKeyTokens(w.Properties, reg); err != nil {
		return NodeWire{}, err
	}
	return w, nil
}

// UnmarshalRelWireWithKeys decodes a RelWire and resolves tokenized keys.
func UnmarshalRelWireWithKeys(data []byte, reg *registrypkg.PropertyKeyRegistry) (RelWire, error) {
	var w RelWire
	if err := msgpack.Unmarshal(data, &w); err != nil {
		return RelWire{}, err
	}
	if err := ResolvePropertyKeyTokens(w.Properties, reg); err != nil {
		return RelWire{}, err
	}
	return w, nil
}
