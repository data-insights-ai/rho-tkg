package core

import (
	"errors"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ByLabelAndProperty returns nodes matching the label and property value,
// with optional pagination. Resolves the label name to a token.
// Returns nil if the label is not registered.
//
// Without a temporal filter, the call falls through to the store-level
// property index for O(matches) lookup when the backend implements
// PropertyIndexCapability; otherwise the graph layer falls back to a
// label-scan + property filter using the mandatory NodesByLabel surface.
// The fallback is correctness-preserving (every in-tree backend already
// applies the same scan-and-filter internally when no property index
// covers the (label, key) pair). When opts carries a temporal filter,
// the candidate set is the union of (nodes currently matching
// label+property — seeded via the same path) and (every known history
// ID). Each candidate is then resolved to its version overlapping the
// requested time and the predicate re-checked against that historical
// version, so a node whose label and property held at the requested
// time is included even if a later version no longer matches.
func (n *NodeOps) ByLabelAndProperty(label, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := c.validateTemporalQueryOptsScan(opts); err != nil {
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
	var targetKey string
	if temporal {
		targetKey = indexpkg.PropertyValueKey(value)
	}
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	if !temporal {
		return c.nodesByLabelAndProperty(tok, key, value, opts)
	}
	if targetKey == "" {
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

	var result []*types.Node
	pred := func(n *types.Node) bool {
		if !n.HasLabelTokenRaw(tok) {
			return false
		}
		gotKey, found := n.IndexablePropertyValueKey(key)
		return found && gotKey == targetKey
	}
	if err := c.forEachNodeCandidateIDByDepth(currentIDs, opts.Depth, func(id types.NodeID) error {
		n, err := c.findNodeVersionForOpts(id, opts, pred)
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

// ByLabelAndProperties returns nodes matching the label AND every (key,
// value) pair in values (AND-conjunction, EQUALITY-only in v1 — no
// partial-prefix or range semantics; see docs/query-planners.md "Composite
// property indexes"). values must supply exactly the 2..4 keys a composite
// index declares to be eligible for acceleration — a superset or subset of
// keys still answers correctly via the label-scan + post-filter fallback,
// just without the O(matches) speedup. Resolves the label name to a token.
// Returns nil if the label is not registered.
//
// Mirrors NodeOps.ByLabelAndProperty's structure exactly: without a temporal
// filter, falls through to the store-level composite index for O(matches)
// lookup when the backend implements CompositePropertyIndexCapability and a
// matching definition exists; otherwise falls back to a label-scan +
// property filter using the mandatory NodesByLabel surface. When opts
// carries a temporal filter, the candidate set is the union of (nodes
// currently matching label+properties) and (every known history ID); each
// candidate is resolved to its version overlapping the requested time and
// the predicate re-checked against that historical version.
func (n *NodeOps) ByLabelAndProperties(label string, values map[string]any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := c.validateTemporalQueryOptsScan(opts); err != nil {
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
	pred := func(n *types.Node) bool {
		if !n.HasLabelTokenRaw(tok) {
			return false
		}
		return indexpkg.NodeMatchesAllProperties(n, values)
	}
	if err := c.forEachNodeCandidateIDByDepth(currentIDs, opts.Depth, func(id types.NodeID) error {
		n, err := c.findNodeVersionForOpts(id, opts, pred)
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
