package core

import (
	"errors"
	"sort"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// forEachAdjacentRelVersionLocked is the version-aware body shared by
// RelOps.ForEachAdjacentRelAt and RelOps.ForEachAdjacentEndpointAt when opts
// carries a valid-time filter (ValidAt, or ValidStart+ValidEnd).
//
// v4.35.0: before this, both doors tested the CURRENT row only (native
// inline-stamp arm and decode fallback alike), so a relationship whose latest
// version carries a later valid_from disappeared from every adjacency query
// at an earlier instant while the store still held the older version. Every
// node door resolves the version chain (nodesByLabelLocked →
// findNodeVersionForOpts); this is the relationship mirror, built from the
// same parts Temporal().OutgoingRelsAt uses (directionalRelsAtLocked):
//
//   - candidates = current adjacency ids ∪ deleted rel ids (endpoints are
//     immutable, so a rel that ever touched nodeID still does — the deleted
//     fold is what keeps a since-deleted edge visible inside its window, in
//     parity with the node path's forEachNodeCandidateIDByDepth);
//   - each candidate resolves through findRelVersionForOpts with a predicate
//     on direction and type, so the yielded row is the version valid under
//     opts, not the live row;
//   - a live row that provably answers a point query (relCurrentAnswersAt,
//     the same belief-watermark shortcut the node path uses) is yielded
//     without a history read.
//
// Rows are yielded in ascending rel-id order, like Outgoing/Incoming. The
// caller holds the read lock.
func (c *Core) forEachAdjacentRelVersionLocked(nodeID types.NodeID, typeName string, incoming bool, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	var tok uint16
	if typeName != "" {
		var ok bool
		if tok, ok = c.lookupRelTypeQueryToken(typeName); !ok {
			// Unregistered type: no rows, but a missing node still errors.
			return c.validateRequestedNodesExist([]types.NodeID{nodeID})
		}
	}

	var (
		rows []*types.Relationship
		err  error
	)
	if incoming {
		rows, err = c.store.IncomingRelationships(nodeID, tok)
	} else {
		rows, err = c.store.OutgoingRelationships(nodeID, tok)
	}
	if err != nil {
		if !errors.Is(err, storepkg.ErrNodeNotFound) {
			return err
		}
		// No live row: the node is either unknown (error, as the no-filter
		// path reports) or deleted but valid under opts (its since-deleted
		// adjacency is reachable through the deleted fold below).
		if _, nerr := c.findNodeVersionForOpts(nodeID, normalizeTxAtOnlyOpts(opts), nil); nerr != nil {
			return storepkg.ErrNodeNotFound
		}
		rows = nil
	}
	var currentIDs []types.RelID
	if incoming {
		currentIDs, err = c.incomingRelIDsFromRows(nodeID, tok, rows)
	} else {
		currentIDs, err = c.outgoingRelIDsFromRows(nodeID, tok, rows)
	}
	if err != nil {
		return err
	}
	live := make(map[types.RelID]*types.Relationship, len(rows))
	for _, r := range rows {
		live[r.ID()] = r
	}

	// Deterministic order: the candidate fold walks a set.
	var ids []types.RelID
	if err := c.forEachRelAdjacencyCandidateIDByDepth(currentIDs, opts.Depth, func(id types.RelID) error {
		ids = append(ids, id)
		return nil
	}); err != nil {
		return err
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	pred := func(r *types.Relationship) bool {
		if tok != 0 && !r.HasTypeTokenRaw(tok) {
			return false
		}
		if incoming {
			return r.EndNodeID() == nodeID
		}
		return r.StartNodeID() == nodeID
	}
	resolveOpts := normalizeTxAtOnlyOpts(opts)
	for _, id := range ids {
		var r *types.Relationship
		if cur := live[id]; cur != nil && opts.ValidAt != 0 && c.relCurrentAnswersAt(cur, opts.ValidAt, opts.TxAt) && pred(cur) {
			r = cur
		} else {
			var rerr error
			r, rerr = c.findRelVersionForOpts(id, resolveOpts, pred)
			if rerr != nil {
				if errors.Is(rerr, storepkg.ErrNoVersionValidAt) || errors.Is(rerr, storepkg.ErrRelNotFound) {
					continue
				}
				return rerr
			}
		}
		if !fn(r) {
			return nil
		}
	}
	return nil
}
