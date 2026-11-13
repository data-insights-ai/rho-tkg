package core

import (
	"fmt"
	"reflect"

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
// with or without the accelerated capability, under the property equality
// (property_equal.go): the rows holding one of the value's forms. A store with
// PropertyKeysLookupCapability reads every form in one lookup (one index
// walk, round 11); else exact in-tree stores and direct external
// implementations use NodesByLabelAndProperty once per form and unite (per
// form only where a property index serves it, so a store's own scan runs
// once); concrete wrappers that inherit it fall back to a label scan so their
// NodesByLabel overrides stay visible. The capability is acceleration, not
// query correctness. A value without forms (nil, NaN, a list, a map) matches
// nothing.
func (c *Core) nodesByLabelAndProperty(tok uint16, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	return c.nodesByLabelAndPropertyForms(tok, key, types.EqualIndexValues(value), opts)
}

// nodesByLabelAndPropertyForms is nodesByLabelAndProperty over the equal forms
// of one value or several (unionEqualForms).
func (c *Core) nodesByLabelAndPropertyForms(tok uint16, key string, forms []any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	return c.nodesByLabelAndPropertyFormKeys(tok, key, forms, nil, opts)
}

// nodesByLabelAndPropertyFormKeys is nodesByLabelAndPropertyForms with the
// forms' keys (keysOf) the caller built already; nil builds them.
func (c *Core) nodesByLabelAndPropertyFormKeys(tok uint16, key string, forms []any, keys equalKeys, opts storepkg.QueryOpts) ([]*types.Node, error) {
	if len(forms) == 0 {
		return nil, nil
	}
	// A unique key: the owner index reads one claim per value form instead
	// of every shard or the property index (UNIQIDX, UNIQUELOOK).
	if oi, ok := c.ownerIndexAnswers(tok, key, opts); ok {
		nodes, err := c.nodesByOwnerIndex(oi, tok, key, forms, opts)
		if err == nil {
			err = opts.AddCandidates(len(nodes))
		}
		if err != nil {
			return nil, err
		}
		return nodes, nil
	}
	if keys == nil {
		keys = keysOf(forms)
	}
	lookup := c.propertyKeysQuery != nil
	perForm := !lookup && c.propertyQuery != nil && len(forms) == 1
	if !lookup && c.propertyQuery != nil && !perForm {
		indexed, err := c.nodePropertyIndexed(tok, key)
		if err != nil {
			return nil, err
		}
		perForm = indexed
	}
	if lookup || perForm {
		// Under a read budget a key without an index is the store's own label
		// scan: the label counts before it, the index's matches after.
		scanned := false
		if m, _ := opts.ReadMeter(); m != nil {
			indexed, err := c.nodePropertyIndexed(tok, key)
			if err != nil {
				return nil, err
			}
			if !indexed {
				if err := c.meterLabelScan(tok, opts); err != nil {
					return nil, err
				}
				scanned = true
			}
		}
		var nodes []*types.Node
		if lookup {
			found, err := c.propertyKeysQuery.NodesByLabelAndPropertyKeys(tok, key, keys, opts)
			if err != nil {
				return nil, err
			}
			nodes = found
		} else {
			for _, form := range forms {
				found, err := c.propertyQuery.NodesByLabelAndProperty(tok, key, form, opts)
				if err != nil {
					return nil, err
				}
				nodes = append(nodes, found...)
			}
			if len(forms) > 1 {
				nodes = uniteNodes(nodes, opts)
			}
		}
		if !c.propertyQueryTrust {
			elements, err := c.nodeIndexElements(tok, key)
			if err != nil {
				return nil, err
			}
			if err := validateNodesByLabelAndProperty(tok, key, keys, elements, opts, nodes); err != nil {
				return nil, err
			}
			nodes = copyNodeRows(nodes)
		}
		if !scanned {
			if err := opts.AddCandidates(len(nodes)); err != nil {
				return nil, err
			}
		}
		return nodes, nil
	}
	// Fallback: label scan + filter. Limit is applied after filtering; the cursor can
	// still go into the scan because filtering preserves ID order. Under a read
	// budget the label's every node counts before the scan reads one.
	if err := c.meterLabelScan(tok, opts); err != nil {
		return nil, err
	}
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
	match := keys.match()
	for _, n := range candidates {
		if match.Node(n, key, elements) {
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
// nodesByLabelAndProperty. A pair whose value has several forms (a number) is
// never combined with the others' forms: the single-key lookup of the pair
// with the fewest forms reads the candidates and every pair filters them.
func (c *Core) nodesByLabelAndProperties(tok uint16, values map[string]any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	forms, multi := equalForms(values)
	if forms == nil {
		return nil, nil
	}
	want := equalKeysOf(values)
	if multi {
		// Never combine forms (up to 12 per pair): read through the pair with
		// the fewest forms and keep the rows carrying every other pair.
		key := fewestForms(forms)
		pageOpts := opts
		pageOpts.Limit = 0
		candidates, err := c.nodesByLabelAndProperty(tok, key, values[key], pageOpts)
		if err != nil {
			return nil, err
		}
		out := make([]*types.Node, 0, len(candidates))
		for _, n := range candidates {
			if nodeMatchesAllEqual(n, want) {
				out = append(out, n)
			}
		}
		return storeutil.PaginateNodes(out, opts.After, opts.Limit), nil
	}
	if c.compositeQuery != nil {
		indexed := c.compositeDeclared(tok, values)
		if !indexed {
			// The store scans the label without a definition (its fallback).
			if err := c.meterLabelScan(tok, opts); err != nil {
				return nil, err
			}
		}
		nodes, err := c.compositeQuery.NodesByLabelAndProperties(tok, values, opts)
		if err != nil {
			return nil, err
		}
		if indexed {
			if err := opts.AddCandidates(len(nodes)); err != nil {
				return nil, err
			}
		}
		if !c.compositeQueryTrust {
			if err := validateNodesByLabelAndProperties(tok, want, opts, nodes); err != nil {
				return nil, err
			}
			nodes = copyNodeRows(nodes)
		}
		return nodes, nil
	}
	// Fallback: label scan + post-filter over every declared pair.
	if err := c.meterLabelScan(tok, opts); err != nil {
		return nil, err
	}
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
		if nodeMatchesAllEqual(n, want) {
			out = append(out, n)
		}
	}
	out = storeutil.PaginateNodes(out, opts.After, opts.Limit)
	if !c.storeRowsTrust {
		out = copyNodeRows(out)
	}
	return out, nil
}

// meterLabelScan counts, under opts' read budget, the label's every node a
// fallback scan examines, before the scan reads one (StatsCapability's count;
// a store without it counts 0 here, so the scan is not refused in advance).
func (c *Core) meterLabelScan(tok uint16, opts storepkg.QueryOpts) error {
	if m, _ := opts.ReadMeter(); m == nil {
		return nil
	}
	stats, ok := c.store.(storepkg.StatsCapability)
	if !ok {
		return nil
	}
	n, err := stats.NodeCountByLabel(tok)
	if err != nil {
		return err
	}
	return opts.AddCandidates(n)
}

// compositeDeclared reports whether a composite definition on tok has exactly
// values' keys (the store then answers from it, not a label scan).
func (c *Core) compositeDeclared(tok uint16, values map[string]any) bool {
	intro, ok := c.store.(storepkg.CompositeIndexIntrospectionCapability)
	if !ok {
		return true // unknown: counted after the read, as an index answer
	}
	defs, err := intro.ListCompositePropertyIndexes(tok)
	if err != nil {
		return true
	}
	for _, def := range defs {
		if len(def) != len(values) {
			continue
		}
		all := true
		for _, k := range def {
			if _, has := values[k]; !has {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}
