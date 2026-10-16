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

// nodesByLabelAndPropertyLocked is the lock-free body of NodeOps.ByLabelAndProperty.
// Callers must hold c.mu (R or W).
func (c *Core) nodesByLabelAndPropertyLocked(label, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	temporal := hasTemporalFilter(opts)
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	if !temporal {
		return c.nodesByLabelAndProperty(tok, key, value, opts)
	}
	keys := equalKeys(types.EqualIndexKeys(value))
	if keys == nil {
		return nil, nil
	}
	currentMatching, err := c.nodesByLabelAndProperty(tok, key, value, storepkg.QueryOpts{Depth: opts.Depth})
	if err != nil {
		return nil, err
	}
	currentIDs, err := c.nodeIDsFromLabelRows(tok, currentMatching)
	if err != nil {
		return nil, err
	}

	elements, err := c.nodeIndexElements(tok, key)
	if err != nil {
		return nil, err
	}
	var result []*types.Node
	valueKeys := map[string]equalKeys{key: keys}
	pred := func(n *types.Node) bool {
		return n.HasLabelTokenRaw(tok) && keys.node(n, key, elements)
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
	if err := c.validateIndexLabel(label); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	if err := storepkg.ValidateCompositeIndexKeys(keys); err != nil {
		return nil, err
	}
	for _, k := range keys {
		if err := c.validateIndexPropertyKey(k); err != nil {
			return nil, err
		}
	}
	for _, v := range values {
		if err := types.ValidatePropertyValue(v); err != nil {
			return nil, fmt.Errorf("graph: nodes by label and properties value: %w", err)
		}
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
	currentMatching, err := c.nodesByLabelAndProperties(tok, values, storepkg.QueryOpts{Depth: opts.Depth})
	if err != nil {
		return nil, err
	}
	currentIDs, err := c.nodeIDsFromLabelRows(tok, currentMatching)
	if err != nil {
		return nil, err
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
