package core

import (
	"errors"
	"fmt"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"

	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/index"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
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
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
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
	temporal := hasTemporalFilter(opts)
	var targetKey string
	if temporal {
		targetKey = indexpkg.PropertyValueKey(value)
	}
	var result []*types.Node
	err := c.readUnderRLock(func() error {
		tok, ok := c.labels.Lookup(label)
		if !ok {
			return nil
		}
		if !temporal {
			nodes, err := c.nodesByLabelAndProperty(tok, key, value, opts)
			result = nodes
			return err
		}
		if targetKey == "" {
			return nil
		}
		// Indexed (or fallback-scanned) candidate set: nodes that currently
		// match label+property, merged with all history IDs to cover
		// deleted/changed nodes whose historical version matched.
		currentMatching, err := c.nodesByLabelAndProperty(tok, key, value, storepkg.QueryOpts{Depth: opts.Depth})
		if err != nil {
			return err
		}
		currentIDs, err := c.nodeIDsFromLabelRows(tok, currentMatching)
		if err != nil {
			return err
		}

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
			return err
		}
		storeutil.SortNodesByID(result)
		result = storeutil.PaginateNodes(result, opts.After, opts.Limit)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
