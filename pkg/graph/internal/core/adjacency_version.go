package core

import (
	"cmp"
	"errors"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// forEachAdjacentRelVersionLocked is the version-aware body shared by
// RelOps.ForEachAdjacentRelAt and RelOps.ForEachAdjacentEndpointAt when opts
// carries a temporal filter (ValidAt, ValidStart+ValidEnd, TxAt or TxPin —
// hasTemporalFilter).
//
// v4.35.0: before this, both doors tested the CURRENT row only (native
// inline-stamp arm and decode fallback alike), so a relationship whose latest
// version carries a later valid_from disappeared from every adjacency query
// at an earlier instant while the store still held the older version. Every
// node door resolves the version chain (nodesByLabelLocked →
// findNodeVersionForOpts); this is the relationship mirror, built from the
// same parts Temporal().OutgoingRelsAt uses (directionalRelsAtLocked):
//
//   - candidates = current adjacency ids ∪ nodeID's ex-adjacency — the
//     relationships that named nodeID in a row its live adjacency no longer
//     holds: deleted ones and earlier lives of ones re-imported with other
//     endpoints (forEachRelExAdjacentID); it is what keeps a since-deleted
//     edge visible inside its window;
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
		if _, nerr := c.findNodeVersionForOpts(nodeID, c.normalizeTxAtOnlyOpts(opts), nil); nerr != nil {
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
	// Rows and their IDs in ascending ID order (stores sort by ID; an
	// untrusted store's order was validated above), so the live row of a
	// candidate is found by one forward walk instead of a map.
	byID := func(a, b *types.Relationship) int { return cmp.Compare(a.ID(), b.ID()) }
	if !slices.IsSortedFunc(rows, byID) {
		rows = slices.Clone(rows)
		slices.SortFunc(rows, byID)
		currentIDs = slices.Clone(currentIDs)
		slices.Sort(currentIDs)
	}
	ids, err := c.relAdjacencyCandidateIDsSorted(nodeID, adjDirsOf(!incoming), currentIDs, opts.Depth)
	if err != nil {
		return err
	}

	pred := func(r *types.Relationship) bool {
		if tok != 0 && !r.HasTypeTokenRaw(tok) {
			return false
		}
		if incoming {
			return r.EndNodeID() == nodeID
		}
		return r.StartNodeID() == nodeID
	}
	resolveOpts := c.normalizeTxAtOnlyOpts(opts)
	single := c.singleVersionAdjacency(opts)
	next := 0
	for _, id := range ids {
		var r *types.Relationship
		for next < len(rows) && rows[next].ID() < id {
			next++
		}
		var cur *types.Relationship
		if next < len(rows) && rows[next].ID() == id {
			cur = rows[next]
		}
		if cur != nil && opts.HasValidAt() && c.relCurrentAnswersAt(cur, opts.ValidAt, txAtOf(opts)) && pred(cur) {
			r = cur
		} else if r1, decided, serr := single.resolve(cur, pred); serr != nil {
			return serr
		} else if decided {
			if r1 == nil {
				continue
			}
			r = r1
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

// singleVersionAdjacency answers, inside forEachAdjacentRelVersionLocked, a
// live adjacent relationship that has no history rows from that live row
// alone (round 5 request 6, sigma-tkgd C4s: a window search over
// relationships without history was 11 times the static one, a point read
// and a history read per edge). Such a relationship has exactly one version,
// so the canonical chain resolver run over the one-row chain gives what
// findRelVersionForOpts would give after loading it.
//
// It applies only under a pure valid-time filter (ValidAt, or ValidStart and
// ValidEnd): a transaction-time pin (TxAt, TxPin) also consults compaction
// and retention watermarks on the full path, so it keeps that path. It needs
// the store's HistoryPresenceCapability; without it every relationship takes
// the full path. The presence check runs after the live row was read: a write
// that replaced the row in between left a history row, so the check sends it
// to the full path.
type singleVersionAdjacencyResolver struct {
	c        *Core
	presence storepkg.HistoryPresenceCapability
	probe    chainProbe
}

func (c *Core) singleVersionAdjacency(opts storepkg.QueryOpts) singleVersionAdjacencyResolver {
	if opts.HasTxAt() || opts.HasTxPin() {
		return singleVersionAdjacencyResolver{}
	}
	var probe chainProbe
	switch {
	case opts.HasValidAt():
		probe = chainProbe{kind: probePoint, validAt: opts.ValidAt}
	case opts.HasValidInterval():
		probe = chainProbe{kind: probeInterval, validStart: opts.ValidStart, validEnd: opts.ValidEnd}
	default:
		return singleVersionAdjacencyResolver{}
	}
	presence, ok := c.store.(storepkg.HistoryPresenceCapability)
	if !ok {
		return singleVersionAdjacencyResolver{}
	}
	return singleVersionAdjacencyResolver{c: c, presence: presence, probe: probe}
}

// resolve reports decided=false when cur must take the full path (no live
// row, no capability, or a relationship with history); decided=true with
// r == nil when the one version does not answer opts or fails pred.
func (s singleVersionAdjacencyResolver) resolve(cur *types.Relationship, pred func(*types.Relationship) bool) (r *types.Relationship, decided bool, err error) {
	if s.presence == nil || cur == nil {
		return nil, false, nil
	}
	has, err := s.presence.HasRelHistory(cur.ID())
	if err != nil {
		return nil, false, err
	}
	if has {
		return nil, false, nil
	}
	if covered, ok := s.firstVersionCovers(cur); ok {
		if !covered || !pred(cur) {
			return nil, true, nil
		}
		if !s.c.storeRowsTrust {
			// An untrusted store's rows leave the core as copies
			// (store_fetch.go), as the full path's point read does.
			return cur.DeepCopy(), true, nil
		}
		return cur, true, nil
	}
	r, err = s.c.resolveRelChain([]*types.Relationship{cur}, s.probe, pred)
	if err != nil {
		if errors.Is(err, storepkg.ErrNoVersionValidAt) {
			return nil, true, nil
		}
		return nil, false, err
	}
	if !s.c.storeRowsTrust && r == cur {
		r = cur.DeepCopy()
	}
	return r, true, nil
}

// relAdjacencyCandidateIDsSorted is forEachRelAdjacencyCandidateID for one
// node collected in ascending order: currentIDs (ascending) united with the
// node's ex-adjacency in dirs. With an empty ex-adjacency it returns
// currentIDs itself, building no set.
func (c *Core) relAdjacencyCandidateIDsSorted(nodeID types.NodeID, dirs adjDirs, currentIDs []types.RelID, depth storepkg.ShardDepth) ([]types.RelID, error) {
	var off []types.RelID
	if err := c.forEachRelExAdjacentID([]types.NodeID{nodeID}, dirs, depth, func(id types.RelID) bool {
		off = append(off, id)
		return true
	}); err != nil {
		return nil, err
	}
	if len(off) == 0 {
		return currentIDs, nil
	}
	ids := make([]types.RelID, 0, len(currentIDs)+len(off))
	ids = append(append(ids, currentIDs...), off...)
	slices.Sort(ids)
	return slices.Compact(ids), nil
}

// firstVersionCovers decides, without the allocations of the chain resolver
// (Temporal() copies a frozen row's metadata on every call), whether the
// one-row chain of a first version (version 0) without a delete stamp answers
// the probe. For such a row resolveRelChain reduces to: no row dropped by the
// transaction-time filter (tx is 0) or as a retracted life, no life cap, the
// bounds [effective valid-from (storeutil.RelValidFrom), valid-to or open)
// (relVersionBounds of a chain's only row), then the point or overlap test of
// resolveRelVersionAtCapped / resolveRelChainDuring. ok=false for any other
// row: the caller runs the resolver. TestSingleVersionAdjacencyMatchesResolver
// holds the two equal.
func (s singleVersionAdjacencyResolver) firstVersionCovers(r *types.Relationship) (covered, ok bool) {
	if r.Version() != 0 {
		return false, false
	}
	if _, _, deletedAt := r.TxStamps(); deletedAt != 0 {
		return false, false
	}
	_, vEnd, _ := r.ValidRange()
	vStart := storeutil.RelValidFrom(r)
	switch s.probe.kind {
	case probePoint:
		t := s.probe.validAt
		return storeutil.IntervalCovers(vStart, vEnd, t), true
	case probeInterval:
		return storeutil.IntervalMeets(vStart, vEnd, s.probe.validStart, s.probe.validEnd), true
	}
	return false, false
}
