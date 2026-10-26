package core

import (
	"errors"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ByLabelAndProperty returns nodes matching the label and property value, with
// optional pagination; nil if the label is not registered.
//
// Without a temporal filter it uses the store property index (O(matches)) when
// the backend implements PropertyIndexCapability, else a label scan + filter.
// With a temporal filter the candidates are the current matches plus the nodes
// whose rows ever carried label and value (the property membership sidecar of a
// declared index, backlog 8; else every known history ID); each is resolved to
// its version at the requested time and the predicate re-checked there.
func (n *NodeOps) ByLabelAndProperty(label, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := c.validateTemporalQueryOptsScan(opts, scopeNodes); err != nil {
		return nil, err
	}
	if err := c.validateIndexLabel(label); err != nil {
		return nil, err
	}
	if err := c.validateIndexPropertyKey(key); err != nil {
		return nil, err
	}
	if err := types.ValidatePropertyValue(value); err != nil {
		return nil, fmt.Errorf("graph: nodes by label and property value: %w", err)
	}
	var result []*types.Node
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.nodesByLabelAndPropertyLocked(label, key, value, opts)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ByLabelAndPropertyValues returns nodes carrying the label whose property
// matches ANY of values, each under the property equality of
// ByLabelAndProperty: the union of ByLabelAndProperty per value, each node
// once, in ID order, paged by opts.After / opts.Limit over the union (VALUES).
// Equal values and repeats count once; no values, or values no node has,
// match nothing. One index read for all values where the store has
// PropertyKeysLookupCapability (every in-tree backend): its work grows with
// the distinct values (one posting list each, the union held as one cursor
// per value's form).
func (n *NodeOps) ByLabelAndPropertyValues(label, key string, values []any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := c.validateTemporalQueryOptsScan(opts, scopeNodes); err != nil {
		return nil, err
	}
	if err := c.validateIndexLabel(label); err != nil {
		return nil, err
	}
	if err := c.validateIndexPropertyKey(key); err != nil {
		return nil, err
	}
	for _, value := range values {
		if err := types.ValidatePropertyValue(value); err != nil {
			return nil, fmt.Errorf("graph: nodes by label and property values: %w", err)
		}
	}
	forms := unionEqualForms(values)
	var result []*types.Node
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.nodesByLabelAndPropertyFormsLocked(label, key, forms, opts)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// nodesByLabelAndPropertyLocked is the lock-free body of NodeOps.ByLabelAndProperty.
// Callers must hold c.mu (R or W).
func (c *Core) nodesByLabelAndPropertyLocked(label, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	return c.nodesByLabelAndPropertyFormsLocked(label, key, types.EqualIndexValues(value), opts)
}

// nodesByLabelAndPropertyFormsLocked is nodesByLabelAndPropertyLocked over
// the equal forms of one value or several (unionEqualForms). Callers must hold
// c.mu (R or W).
func (c *Core) nodesByLabelAndPropertyFormsLocked(label, key string, forms []any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	temporal := hasTemporalFilter(opts)
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	if !temporal {
		return c.nodesByLabelAndPropertyForms(tok, key, forms, opts)
	}
	if len(forms) == 0 {
		return nil, nil
	}
	keys := keysOf(forms)
	currentIDs := func() ([]types.NodeID, error) {
		currentMatching, err := c.nodesByLabelAndPropertyForms(tok, key, forms, storepkg.QueryOpts{Depth: opts.Depth})
		if err != nil {
			return nil, err
		}
		return c.nodeIDsFromLabelRows(tok, currentMatching)
	}

	elements, err := c.nodeIndexElements(tok, key)
	if err != nil {
		return nil, err
	}
	var result []*types.Node
	valueKeys := map[string]equalKeys{key: keys}
	match := keys.match()
	pred := func(n *types.Node) bool {
		return n.HasLabelTokenRaw(tok) && match.Node(n, key, elements)
	}
	resolveOpts := c.normalizeTxAtOnlyOpts(opts)
	if err := c.forEachNodePropertyCandidateID(tok, valueKeys, currentIDs, opts, func(id types.NodeID) error {
		n, err := c.findNodeVersionForOpts(id, resolveOpts, pred)
		if err != nil {
			if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrNodeNotFound) {
				return nil
			}
			return err
		}
		result = append(result, n)
		return nil
	}); err != nil {
		return nil, err
	}
	storeutil.SortNodesByID(result)
	result = storeutil.PaginateNodes(result, opts.After, opts.Limit)
	return result, nil
}

// ByLabelAndProperties returns nodes matching the label AND every (key, value)
// pair in values (EQUALITY-only; docs/query-planners.md "Composite property
// indexes"). Exactly the 2..4 keys of a composite index are accelerated; other
// key sets answer correctly via label scan + post-filter. Nil if the label is not
// registered.
//
// Same structure as ByLabelAndProperty: composite index when the backend
// implements CompositePropertyIndexCapability and a definition matches, else
// scan + filter. With a temporal filter the candidates are the current matches
// plus the intersection of the indexed keys' membership sidecars (backlog 8; else
// every known history ID), each re-checked at its version at the requested time.
func (n *NodeOps) ByLabelAndProperties(label string, values map[string]any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := c.validateTemporalQueryOptsScan(opts, scopeNodes); err != nil {
		return nil, err
	}
	if err := c.validateLabelAndProperties(label, values, "nodes by label and properties value"); err != nil {
		return nil, err
	}
	var result []*types.Node
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.nodesByLabelAndPropertiesLocked(label, values, opts)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// validateLabelAndProperties checks a composite equality lookup's label,
// keys (2..4 valid index keys) and values; what names the value check.
func (c *Core) validateLabelAndProperties(label string, values map[string]any, what string) error {
	if err := c.validateIndexLabel(label); err != nil {
		return err
	}
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	if err := storepkg.ValidateCompositeIndexKeys(keys); err != nil {
		return err
	}
	for _, k := range keys {
		if err := c.validateIndexPropertyKey(k); err != nil {
			return err
		}
	}
	for _, v := range values {
		if err := types.ValidatePropertyValue(v); err != nil {
			return fmt.Errorf("graph: %s: %w", what, err)
		}
	}
	return nil
}

// nodesByLabelAndPropertiesLocked is the lock-free body of
// NodeOps.ByLabelAndProperties. Callers must hold c.mu (R or W).
func (c *Core) nodesByLabelAndPropertiesLocked(label string, values map[string]any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	temporal := hasTemporalFilter(opts)
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	// Multi-key lookup is equality on every key; an Elements key (backlog 63) means
	// "contains" on the single-key door, so the conjunction is refused.
	for k := range values {
		if err := c.refuseNodeElementsIndex(tok, k, "ByLabelAndProperties"); err != nil {
			return nil, err
		}
	}
	if !temporal {
		return c.nodesByLabelAndProperties(tok, values, opts)
	}
	currentIDs := func() ([]types.NodeID, error) {
		currentMatching, err := c.nodesByLabelAndProperties(tok, values, storepkg.QueryOpts{Depth: opts.Depth})
		if err != nil {
			return nil, err
		}
		return c.nodeIDsFromLabelRows(tok, currentMatching)
	}

	var result []*types.Node
	valueKeys := equalKeysOf(values)
	pred := func(n *types.Node) bool {
		return n.HasLabelTokenRaw(tok) && nodeMatchesAllEqual(n, valueKeys)
	}
	resolveOpts := c.normalizeTxAtOnlyOpts(opts)
	if err := c.forEachNodePropertyCandidateID(tok, valueKeys, currentIDs, opts, func(id types.NodeID) error {
		n, err := c.findNodeVersionForOpts(id, resolveOpts, pred)
		if err != nil {
			if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrNodeNotFound) {
				return nil
			}
			return err
		}
		result = append(result, n)
		return nil
	}); err != nil {
		return nil, err
	}
	storeutil.SortNodesByID(result)
	result = storeutil.PaginateNodes(result, opts.After, opts.Limit)
	return result, nil
}
