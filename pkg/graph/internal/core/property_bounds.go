package core

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// PropertyBounds returns the smallest and largest numeric value of propKey
// over label's current nodes with the exact type-class partition of the same
// nodes (Missing included), read at one moment under the store's index lock:
// O(1) from the property index's ordered view (its first and last key) and
// maintained counters, no node and no index entry walked. See
// store.PropertyBounds for the value rules (NaN excluded, a float32 by its
// shortest decimal rendering) and NumericOnly / NullFree.
//
// ok=false with no error where the bounds cannot be stated exactly (never
// answered by a walk): no property index on (label, propKey), an index being
// built, an Elements index, an index holding an integer of magnitude past
// 2^53, badger PropertyIndexOnDisk, planner statistics disabled, a store
// without store.NodePropertyBoundsCapability (tiered, sharded, external
// stores), an unknown label, or a temporal QueryOpts (the index holds current
// values). Errors: a malformed label or key, ErrGraphClosed.
func (s *StatOps) PropertyBounds(label, propKey string, opts storepkg.QueryOpts) (storepkg.PropertyBounds, bool, error) {
	c := s.c
	var zero storepkg.PropertyBounds
	if err := c.validateIndexLabel(label); err != nil {
		return zero, false, err
	}
	if err := storepkg.ValidateIndexPropertyKey(propKey); err != nil {
		return zero, false, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return zero, false, ErrGraphClosed
	}
	bounds, native := c.store.(storepkg.NodePropertyBoundsCapability)
	if !native || !c.storeRowsTrust || opts.HasTimeCoordinate() {
		return zero, false, nil
	}
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return zero, false, nil
	}
	return bounds.NodePropertyBounds(tok, propKey)
}

// RelPropertyBounds is the relationship mirror of PropertyBounds over a
// relationship type's property index, type-class counts and type count
// (store.RelPropertyBoundsCapability: memory and badger).
func (s *StatOps) RelPropertyBounds(typeName, propKey string, opts storepkg.QueryOpts) (storepkg.PropertyBounds, bool, error) {
	c := s.c
	var zero storepkg.PropertyBounds
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return zero, false, err
	}
	if err := storepkg.ValidateIndexPropertyKey(propKey); err != nil {
		return zero, false, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return zero, false, ErrGraphClosed
	}
	bounds, native := c.store.(storepkg.RelPropertyBoundsCapability)
	if !native || !c.storeRowsTrust || opts.HasTimeCoordinate() {
		return zero, false, nil
	}
	tok, ok := c.lookupRelTypeQueryToken(typeName)
	if !ok {
		return zero, false, nil
	}
	return bounds.RelPropertyBounds(tok, propKey)
}
