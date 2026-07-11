package core

import (
	"fmt"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
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

func (c *Core) relPropertyIndexCap() (storepkg.RelPropertyIndexCapability, error) {
	cap, ok := c.store.(storepkg.RelPropertyIndexCapability)
	if !ok {
		return nil, fmt.Errorf("%w: RelPropertyIndexCapability", storepkg.ErrCapabilityNotSupported)
	}
	return cap, nil
}

func (c *Core) compositeIndexCap() (storepkg.CompositePropertyIndexCapability, error) {
	cap, ok := c.store.(storepkg.CompositePropertyIndexCapability)
	if !ok {
		return nil, fmt.Errorf("%w: CompositePropertyIndexCapability", storepkg.ErrCapabilityNotSupported)
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
// query whether or not the underlying store implements the accelerated
// property-query capability. Exact in-tree stores and direct external
// implementations use NodesByLabelAndProperty. Concrete wrappers that merely
// inherit an in-tree method intentionally fall back to a label scan so wrapper
// NodesByLabel overrides remain visible. Every in-tree backend already applies
// the same scan-and-filter internally when no property index covers
// (label, key); replicating it here ensures out-of-tree mandatory-only
// backends still get the correct semantics — the optional capability is index
// management/acceleration, not query correctness.
//
// Empty-key contract (R4-F9): `indexpkg.PropertyValueKey` returns "" for
// values it cannot canonicalise (slices, maps, nested structs without a
// custom `HashableValue` etc.). The in-tree backends (and the property
// index itself) treat such queries as "no matches"; the graph-layer
// fallback must do the same — otherwise every candidate whose stored
// value is also unindexable canonicalises to "" and matches falsely.
func (c *Core) nodesByLabelAndProperty(tok uint16, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	wantKey := indexpkg.PropertyValueKey(value)
	if wantKey == "" {
		// The query value itself is not canonicalisable — by contract,
		// no matches. Mirrors memory.Store / badger.Store internal
		// guards before their fallback scans.
		return nil, nil
	}
	if c.propertyQuery != nil {
		nodes, err := c.propertyQuery.NodesByLabelAndProperty(tok, key, value, opts)
		if err != nil {
			return nil, err
		}
		if !c.propertyQueryTrust {
			if err := validateNodesByLabelAndProperty(tok, key, wantKey, opts, nodes); err != nil {
				return nil, err
			}
			nodes = copyNodeRows(nodes)
		}
		return nodes, nil
	}
	// Fallback: label scan + property filter. Pagination is applied
	// after filtering since the property predicate can drop arbitrary
	// elements; pre-filter Limit would over-count. The cursor can still
	// be pushed into the label scan because filtering preserves ID order.
	pageOpts := opts
	pageOpts.Limit = 0
	candidates, err := c.store.NodesByLabel(tok, pageOpts)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := c.validateNodesByLabelPage(tok, pageOpts, candidates); err != nil {
			return nil, err
		}
	}
	out := make([]*types.Node, 0, len(candidates))
	for _, n := range candidates {
		gotKey, found := n.IndexablePropertyValueKey(key)
		if !found || gotKey == "" {
			// Stored value is also unindexable; refuse to claim
			// equality through the empty sentinel.
			continue
		}
		if gotKey != wantKey {
			continue
		}
		out = append(out, n)
	}
	out = storeutil.PaginateNodes(out, opts.After, opts.Limit)
	if !c.storeRowsTrust {
		out = copyNodeRows(out)
	}
	return out, nil
}

// nodesByLabelAndProperties answers the (label-token, values map) composite
// equality query whether or not the underlying store implements the
// accelerated CompositePropertyIndexCapability. Mirrors
// nodesByLabelAndProperty's structure exactly (same trust-boundary shape,
// same "index acceleration, never the sole source of correctness"
// contract) — see its doc comment for the empty-key / R4-F9 rationale,
// which applies here per-component via indexpkg.NodeMatchesAllProperties.
func (c *Core) nodesByLabelAndProperties(tok uint16, values map[string]any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	if c.compositeQuery != nil {
		nodes, err := c.compositeQuery.NodesByLabelAndProperties(tok, values, opts)
		if err != nil {
			return nil, err
		}
		if !c.compositeQueryTrust {
			if err := validateNodesByLabelAndProperties(tok, values, opts, nodes); err != nil {
				return nil, err
			}
			nodes = copyNodeRows(nodes)
		}
		return nodes, nil
	}
	// Mandatory fallback: label scan + post-filter over every declared pair.
	// Every in-tree backend already applies the same scan-and-filter
	// internally when no composite index covers the requested key set;
	// replicating it here ensures a MandatoryStore-only backend (or one that
	// simply omits the optional capability, e.g. tiered in v1 — see
	// docs/query-planners.md) still gets correct, if unaccelerated, results.
	pageOpts := opts
	pageOpts.Limit = 0
	candidates, err := c.store.NodesByLabel(tok, pageOpts)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := c.validateNodesByLabelPage(tok, pageOpts, candidates); err != nil {
			return nil, err
		}
	}
	out := make([]*types.Node, 0, len(candidates))
	for _, n := range candidates {
		if !indexpkg.NodeMatchesAllProperties(n, values) {
			continue
		}
		out = append(out, n)
	}
	out = storeutil.PaginateNodes(out, opts.After, opts.Limit)
	if !c.storeRowsTrust {
		out = copyNodeRows(out)
	}
	return out, nil
}
