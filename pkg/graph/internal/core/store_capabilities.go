package core

import (
	"fmt"
	"reflect"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Capability accessors type-assert the store against an optional capability and
// return ErrCapabilityNotSupported (with a diagnostic) when the backend lacks it.
// The store field is a MandatoryStore so out-of-tree backends can plug in;
// in-tree backends satisfy every capability.

// propertyIndexCap resolves PropertyIndexCapability (DDL and query on one
// interface) with the same wrapper-promotion guard as propertyQueryCapability
// (core.go; backlog 14c): a wrapper that only inherits the interface by embedding
// could create an index its queries never consult, so it gets
// ErrCapabilityNotSupported for DDL too.
func (c *Core) propertyIndexCap() (storepkg.PropertyIndexCapability, error) {
	cap, ok := c.store.(storepkg.PropertyIndexCapability)
	if !ok {
		return nil, fmt.Errorf("%w: PropertyIndexCapability", storepkg.ErrCapabilityNotSupported)
	}
	if !isExactNativeStore(c.store) && embedsNativeCapability(c.store,
		reflect.TypeOf((*storepkg.PropertyIndexCapability)(nil)).Elem(), "NodesByLabelAndProperty") {
		return nil, fmt.Errorf("%w: PropertyIndexCapability", storepkg.ErrCapabilityNotSupported)
	}
	return cap, nil
}

// relPropertyIndexCap mirrors propertyIndexCap for RelPropertyIndexCapability
// (backlog 14c), paired with relPropertyQueryCapability.
func (c *Core) relPropertyIndexCap() (storepkg.RelPropertyIndexCapability, error) {
	cap, ok := c.store.(storepkg.RelPropertyIndexCapability)
	if !ok {
		return nil, fmt.Errorf("%w: RelPropertyIndexCapability", storepkg.ErrCapabilityNotSupported)
	}
	if !isExactNativeStore(c.store) && embedsNativeCapability(c.store,
		reflect.TypeOf((*storepkg.RelPropertyIndexCapability)(nil)).Elem(), "RelationshipsByTypeAndProperty") {
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

func (c *Core) relTypeTemporalIndexCap() (storepkg.RelTypeTemporalIndexCapability, error) {
	cap, ok := c.store.(storepkg.RelTypeTemporalIndexCapability)
	if !ok {
		return nil, fmt.Errorf("%w: RelTypeTemporalIndexCapability", storepkg.ErrCapabilityNotSupported)
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

// nodesByLabelAndProperty answers the (label-token, property-key, value) query
// with or without the accelerated capability. Exact in-tree stores and direct
// external implementations use NodesByLabelAndProperty; concrete wrappers that
// inherit it fall back to a label scan so their NodesByLabel overrides stay
// visible. The capability is acceleration, not query correctness.
//
// Empty-key contract: indexpkg.PropertyValueKey returns "" for values it cannot
// canonicalise (slices, maps, structs without HashableValue). Such queries match
// nothing, as in the in-tree backends; otherwise every unindexable stored value
// would also canonicalise to "" and match falsely.
func (c *Core) nodesByLabelAndProperty(tok uint16, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	wantKey := indexpkg.PropertyValueKey(value)
	if wantKey == "" {
		// The query value is not canonicalisable: no matches, as in memory/badger.
		return nil, nil
	}
	if c.propertyQuery != nil {
		nodes, err := c.propertyQuery.NodesByLabelAndProperty(tok, key, value, opts)
		if err != nil {
			return nil, err
		}
		if !c.propertyQueryTrust {
			elements, err := c.nodeIndexElements(tok, key)
			if err != nil {
				return nil, err
			}
			if err := validateNodesByLabelAndProperty(tok, key, wantKey, elements, opts, nodes); err != nil {
				return nil, err
			}
			nodes = copyNodeRows(nodes)
		}
		return nodes, nil
	}
	// Fallback: label scan + filter. Limit is applied after filtering; the cursor can
	// still go into the scan because filtering preserves ID order.
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
	elements, err := c.nodeIndexElements(tok, key)
	if err != nil {
		return nil, err
	}
	out := make([]*types.Node, 0, len(candidates))
	for _, n := range candidates {
		// HasPropertyIndexKey never matches through the empty key of an unindexable value.
		if n.HasPropertyIndexKey(key, elements, wantKey) {
			out = append(out, n)
		}
	}
	out = storeutil.PaginateNodes(out, opts.After, opts.Limit)
	if !c.storeRowsTrust {
		out = copyNodeRows(out)
	}
	return out, nil
}

// nodesByLabelAndProperties answers the (label-token, values map) composite
// equality query with or without CompositePropertyIndexCapability; same shape as
// nodesByLabelAndProperty (see its empty-key note, which applies per component
// via indexpkg.NodeMatchesAllProperties).
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
	// Fallback: label scan + post-filter over every declared pair.
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
