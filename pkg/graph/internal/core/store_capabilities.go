package core

import (
	"fmt"

	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Capability accessors — type-assert the store field against an optional
// capability and surface ErrCapabilityNotSupported (with a diagnostic
// message) when the underlying backend does not implement it.
//
// Core's `store` field is typed as MandatoryStore so out-of-tree backends
// that satisfy only the mandatory capabilities can still be plugged in;
// their consumers must accept ErrCapabilityNotSupported on the optional
// surfaces. In-tree backends (memory.Store, badger.Store, tiered.Store)
// satisfy every capability, so these assertions always succeed in the
// reference configuration.

func (c *Core) propertyIndexCap() (storepkg.PropertyIndexCapability, error) {
	cap, ok := c.store.(storepkg.PropertyIndexCapability)
	if !ok {
		return nil, fmt.Errorf("%w: PropertyIndexCapability", storepkg.ErrCapabilityNotSupported)
	}
	return cap, nil
}

func (c *Core) temporalIndexCap() (storepkg.TemporalIndexCapability, error) {
	cap, ok := c.store.(storepkg.TemporalIndexCapability)
	if !ok {
		return nil, fmt.Errorf("%w: TemporalIndexCapability", storepkg.ErrCapabilityNotSupported)
	}
	return cap, nil
}

func (c *Core) vectorIndexCap() (storepkg.VectorIndexCapability, error) {
	cap, ok := c.store.(storepkg.VectorIndexCapability)
	if !ok {
		return nil, fmt.Errorf("%w: VectorIndexCapability", storepkg.ErrCapabilityNotSupported)
	}
	return cap, nil
}

func (c *Core) highFrequencyIndexCap() (storepkg.HighFrequencyIndexCapability, error) {
	cap, ok := c.store.(storepkg.HighFrequencyIndexCapability)
	if !ok {
		return nil, fmt.Errorf("%w: HighFrequencyIndexCapability", storepkg.ErrCapabilityNotSupported)
	}
	return cap, nil
}

// nodesByLabelAndProperty answers the (label-token, property-key, value)
// query whether or not the underlying store implements
// PropertyIndexCapability. When the capability is present, the call
// delegates to the store's optimised NodesByLabelAndProperty path. When
// absent, the graph layer falls back to a label scan + property filter
// over the mandatory NodesByLabel + property comparison. Every in-tree
// backend already applies the same scan-and-filter internally when no
// property index covers (label, key); replicating it here ensures
// out-of-tree mandatory-only backends still get the correct semantics —
// the optional capability is index management (acceleration), not query
// correctness.
//
// Empty-key contract (R4-F9): `indexpkg.PropertyValueKey` returns "" for
// values it cannot canonicalise (slices, maps, nested structs without a
// custom `HashableValue` etc.). The in-tree backends (and the property
// index itself) treat such queries as "no matches"; the graph-layer
// fallback must do the same — otherwise every candidate whose stored
// value is also unindexable canonicalises to "" and matches falsely.
func (c *Core) nodesByLabelAndProperty(tok uint16, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	if cap, ok := c.store.(storepkg.PropertyIndexCapability); ok {
		return cap.NodesByLabelAndProperty(tok, key, value, opts)
	}
	wantKey := indexpkg.PropertyValueKey(value)
	if wantKey == "" {
		// The query value itself is not canonicalisable — by contract,
		// no matches. Mirrors memory.Store / badger.Store internal
		// guards before their fallback scans.
		return nil, nil
	}
	// Fallback: label scan + property filter. Pagination is applied
	// after filtering since the property predicate can drop arbitrary
	// elements; pre-filter pagination would over-count toward Limit.
	pageOpts := opts
	pageOpts.After = 0
	pageOpts.Limit = 0
	candidates, err := c.store.NodesByLabel(tok, pageOpts)
	if err != nil {
		return nil, err
	}
	out := make([]*types.Node, 0, len(candidates))
	for _, n := range candidates {
		v, found := n.GetProperty(key)
		if !found {
			continue
		}
		gotKey := indexpkg.PropertyValueKey(v)
		if gotKey == "" {
			// Stored value is also unindexable; refuse to claim
			// equality through the empty sentinel.
			continue
		}
		if gotKey != wantKey {
			continue
		}
		out = append(out, n)
	}
	return storeutil.PaginateNodes(out, opts.After, opts.Limit), nil
}
