package core

import (
	"errors"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	if !hasTemporalFilter(opts) {
		return c.nodesByLabelAndProperty(tok, key, value, opts)
	}
	if opts.Depth != storepkg.DepthAll {
		return nil, ErrDepthTemporalUnsupported
	}
	targetKey := indexpkg.PropertyValueKey(value)
	if targetKey == "" {
		return nil, nil
	}
	// Indexed (or fallback-scanned) candidate set: nodes that currently
	// match label+property, merged with all history IDs to cover
	// deleted/changed nodes whose historical version matched.
	currentMatching, err := c.nodesByLabelAndProperty(tok, key, value, storepkg.QueryOpts{})
	if err != nil {
		return nil, err
	}
	currentIDs := make([]types.NodeID, 0, len(currentMatching))
	for _, n := range currentMatching {
		currentIDs = append(currentIDs, n.ID())
	}

	pred := func(n *types.Node) bool {
		if !n.HasLabelTokenRaw(tok) {
			return false
		}
		v, found := n.GetProperty(key)
		return found && indexpkg.PropertyValueKey(v) == targetKey
	}
	var result []*types.Node
	err = c.forEachNodeCandidateID(currentIDs, func(id types.NodeID) error {
		n, err := c.findNodeVersionForOpts(id, opts, pred)
		if err != nil {
			if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrNodeNotFound) {
				return nil
			}
			return err
		}
		result = append(result, n)
		return nil
	})
	if err != nil {
		return nil, err
	}
	storeutil.SortNodesByID(result)
	return storeutil.PaginateNodes(result, opts.After, opts.Limit), nil
}
