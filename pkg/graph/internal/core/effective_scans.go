package core

import (
	"errors"
	"fmt"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Broader effective scans (backlog 35, requested by sigma-tkgd 2026-10-09)
//
// Every effective scan is a candidate gather (under the read lock) followed by
// streamNodeTimelines / streamRelTimelines: each candidate's timeline is read
// by the per-entity body (nodeEffectiveTimelineFrom, under the read lock and
// its entity lock — the snapshot rule of the per-entity door — through one
// store read snapshot per scan, effective_scan_reads.go), filtered by the
// door's row predicate, and handed to fn without graph locks. The doors differ
// only in their candidate set and predicate; the resolution is the per-entity
// door's, so a scan's segments of an entity equal that entity's timeline
// segments the predicate keeps, by construction.
// =============================================================================

// ErrInvalidDirection is returned by ForEachRelEffectiveAtNodes for a
// direction other than temporal.Outgoing / temporal.Incoming.
var ErrInvalidDirection = errors.New("graph: invalid relationship direction")

// streamNodeTimelines streams the effective timeline at pin of each id (in
// order; an unknown id is skipped) as the segments whose row keep accepts
// (keep nil: every segment); fn returning false stops. The timelines are read
// through streamEntityTimelines (one store snapshot per scan, backlog 55).
func (c *Core) streamNodeTimelines(ids []types.NodeID, pin types.Instant, keep func(*types.Node) bool, fn func(temporal.NodeSegment) bool) error {
	return streamEntityTimelines(c, ids, func(rd entityReads, id types.NodeID) ([]temporal.NodeSegment, error) {
		segs, err := c.nodeEffectiveTimelineFrom(rd, id, pin)
		return skipUnknown(segs, err, storepkg.ErrNodeNotFound)
	}, func(_ types.NodeID, segs []temporal.NodeSegment) bool {
		for _, s := range segs {
			if (keep == nil || keep(s.Node)) && !fn(s) {
				return false
			}
		}
		return true
	})
}

// streamRelTimelines is streamNodeTimelines for relationships.
func (c *Core) streamRelTimelines(ids []types.RelID, pin types.Instant, keep func(*types.Relationship) bool, fn func(temporal.RelSegment) bool) error {
	return streamEntityTimelines(c, ids, func(rd entityReads, id types.RelID) ([]temporal.RelSegment, error) {
		segs, err := c.relEffectiveTimelineFrom(rd, id, pin)
		return skipUnknown(segs, err, storepkg.ErrRelNotFound)
	}, func(_ types.RelID, segs []temporal.RelSegment) bool {
		for _, s := range segs {
			if (keep == nil || keep(s.Rel)) && !fn(s) {
				return false
			}
		}
		return true
	})
}

// firstOccurrences returns ids without repeats, in the order of their first
// occurrence (a new slice; the caller's is not touched).
func firstOccurrences[T comparable](ids []T) []T {
	seen := make(map[T]struct{}, len(ids))
	out := make([]T, 0, len(ids))
	for _, id := range ids {
		if _, dup := seen[id]; !dup {
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out
}

// ForEachNodeEffective streams the effective timeline at pin of every node
// recorded by the pin — live, closed, and deleted before or after the pin
// alike; none created after it — as NodeSegments: each node's segments are
// its NodeEffectiveTimeline(id, pin) (the row NodeAtTx(id, t, pin) at every
// t), contiguous and ascending; node order is unspecified; fn
// returning false stops the scan.
//
// Candidates are every current node and every node with history (one ID scan
// each), so the cost is one per-entity timeline read per node ever stored.
// Gates, isolation and errors are ForEachNodeEffectiveByLabel's (graph
// compaction / retention watermarks fail the whole scan).
func (t *TempOps) ForEachNodeEffective(pin types.Instant, fn func(temporal.NodeSegment) bool) error {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateEffectiveScanPin(pin, scopeNodes); err != nil {
		return err
	}
	var ids []types.NodeID
	if err := c.readUnderRLock(func() error {
		var current []types.NodeID
		if err := c.forEachNodeID(func(id types.NodeID) bool {
			current = append(current, id)
			return true
		}); err != nil {
			return err
		}
		return c.forEachNodeCandidateIDByDepth(current, storepkg.DepthAll, func(id types.NodeID) error {
			ids = append(ids, id)
			return nil
		})
	}); err != nil {
		return err
	}
	return c.streamNodeTimelines(ascending(ids), pin, nil, fn)
}

// ForEachRelEffective is ForEachNodeEffective for relationships (each
// relationship's RelEffectiveTimeline). DECLARED view: not masked by endpoint
// validity.
func (t *TempOps) ForEachRelEffective(pin types.Instant, fn func(temporal.RelSegment) bool) error {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateEffectiveScanPin(pin, scopeAllRels); err != nil {
		return err
	}
	var ids []types.RelID
	if err := c.readUnderRLock(func() error {
		var current []types.RelID
		if err := c.forEachRelID(func(id types.RelID) bool {
			current = append(current, id)
			return true
		}); err != nil {
			return err
		}
		return c.forEachRelCandidateIDByDepth(current, storepkg.DepthAll, func(id types.RelID) error {
			ids = append(ids, id)
			return nil
		})
	}); err != nil {
		return err
	}
	return c.streamRelTimelines(ascending(ids), pin, nil, fn)
}

// NodesEffectiveByIDs streams NodeEffectiveTimeline(id, pin) for each id, in
// the order of ids: a repeated id streams once (at its first occurrence), an
// unknown id or one created after the pin streams nothing. fn returning false
// stops. It is the per-entity door batched: the same per-entity compaction
// and retention gates (a refused member fails the call), an invalid id
// (ErrInvalidStoreMutation) refused before any segment, ErrInvalidTimeRange,
// ErrTxPinTooNew, ErrNilCallback, ErrGraphClosed. fn runs without graph locks.
func (t *TempOps) NodesEffectiveByIDs(ids []types.NodeID, pin types.Instant, fn func(temporal.NodeSegment) bool) error {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	for _, id := range ids {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return err
		}
	}
	if err := c.validateEffectivePin(pin); err != nil {
		return err
	}
	return c.streamNodeTimelines(firstOccurrences(ids), pin, nil, fn)
}

// RelsEffectiveByIDs is NodesEffectiveByIDs for relationships
// (RelEffectiveTimeline per id).
func (t *TempOps) RelsEffectiveByIDs(ids []types.RelID, pin types.Instant, fn func(temporal.RelSegment) bool) error {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	for _, id := range ids {
		if err := storepkg.ValidateRelID(id); err != nil {
			return err
		}
	}
	if err := c.validateEffectivePin(pin); err != nil {
		return err
	}
	for _, id := range ids {
		if err := c.checkRelTypePointRetention(id, pin); err != nil {
			return err
		}
	}
	return c.streamRelTimelines(firstOccurrences(ids), pin, nil, fn)
}

// ForEachRelEffectiveAtNodes streams, for the relationships adjacent to the
// seed nodes in one direction (temporal.Outgoing: the seed is the start node;
// temporal.Incoming: the end node), their effective timelines at pin limited
// to the segments whose row has a seed at that endpoint and, when relTypes is
// not empty, one of those types: for every valid instant t the segments
// containing t are exactly the rows RelAtTx(id, t, pin) of the relationships
// whose row at t has a seed endpoint in that direction. Relationships deleted
// before the pin are included (a seed deleted since is still a seed); each
// relationship streams once, its segments contiguous and ascending, even when
// several seeds (or both endpoints) match; relationship order is unspecified;
// repeated seeds count once; an unknown seed or type contributes nothing (all
// types unknown: nothing). fn returning false stops. DECLARED view: not masked
// by endpoint validity.
//
// Candidates are the seeds' current adjacency (one batched store read) and
// their ex-adjacency (deleted relationships, earlier lives of re-imported
// ones) — the adjacency doors' gather (forEachRelAdjacencyCandidateID), so
// one call reads the seeds' ever-degree, not every deleted relationship nor
// every relationship with history. Gates,
// isolation and errors are ForEachRelEffectiveByType's plus an invalid seed
// (ErrInvalidStoreMutation) and an invalid direction (ErrInvalidDirection).
func (t *TempOps) ForEachRelEffectiveAtNodes(nodeIDs []types.NodeID, dir temporal.Direction, relTypes []string, pin types.Instant, fn func(temporal.RelSegment) bool) error {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.relEffectiveAtNodesGates(nodeIDs, dir, relTypes, pin); err != nil {
		return err
	}
	outgoing := dir == temporal.Outgoing
	seeds := firstOccurrences(nodeIDs)
	if len(seeds) == 0 {
		return nil // no seed: no relationship, and no history fold
	}
	var (
		toks []uint16
		ids  []types.RelID
	)
	if err := c.readUnderRLock(func() error {
		for _, name := range relTypes {
			if tok, ok := c.lookupRelTypeQueryToken(name); ok && !slices.Contains(toks, tok) {
				toks = append(toks, tok)
			}
		}
		if len(relTypes) > 0 && len(toks) == 0 {
			return nil // no requested type exists
		}
		var err error
		ids, err = c.relEffectiveAdjacencyCandidates(seeds, toks, outgoing)
		return err
	}); err != nil {
		return err
	}
	seedSet := make(map[types.NodeID]struct{}, len(seeds))
	for _, id := range seeds {
		seedSet[id] = struct{}{}
	}
	keep := func(r *types.Relationship) bool {
		end := r.EndNodeID()
		if outgoing {
			end = r.StartNodeID()
		}
		if _, ok := seedSet[end]; !ok {
			return false
		}
		return len(toks) == 0 || slices.ContainsFunc(toks, r.HasTypeTokenRaw)
	}
	return c.streamRelTimelines(ascending(ids), pin, keep, fn)
}

// relEffectiveAtNodesGates are ForEachRelEffectiveAtNodes' argument and pin
// checks (its windowed form checks them for the empty window too).
func (c *Core) relEffectiveAtNodesGates(nodeIDs []types.NodeID, dir temporal.Direction, relTypes []string, pin types.Instant) error {
	if dir != temporal.Outgoing && dir != temporal.Incoming {
		return fmt.Errorf("%w: %d", ErrInvalidDirection, dir)
	}
	for _, id := range nodeIDs {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return err
		}
	}
	for _, name := range relTypes {
		if err := c.validateRelTypeQueryName(name); err != nil {
			return err
		}
	}
	if err := c.validateEffectiveScanPin(pin, scopeNodes); err != nil {
		return err
	}
	return c.checkRelTypesRetention(relTypes, pin)
}

// nodeLabelsCandidatesAt is nodeLabelCandidatesAt (TxPin: pin) for several
// distinct label tokens, each candidate once (c.mu held). One label: its
// candidates as they are. Several on a store without the label-membership
// sidecar: the labels' current members united with ONE fold of the node
// history (not one fold per label). With the sidecar: the labels' candidate
// sets united.
func (c *Core) nodeLabelsCandidatesAt(toks []uint16, pin types.Instant) ([]types.NodeID, error) {
	opts := storepkg.QueryOpts{TxPin: pin}
	switch {
	case len(toks) == 0:
		return nil, nil
	case len(toks) == 1:
		return c.nodeLabelCandidateIDsAt(toks[0], opts)
	}
	var ids []types.NodeID
	gather := func(id types.NodeID) error {
		ids = append(ids, id)
		return nil
	}
	if c.labelTxMembers == nil {
		var current []types.NodeID
		for _, tok := range toks {
			cur, err := c.nodeLabelCurrentIDs(tok)
			if err != nil {
				return nil, err
			}
			current = append(current, cur...)
		}
		err := c.forEachNodeCandidateIDByDepth(current, storepkg.DepthAll, gather)
		return ids, err
	}
	seen := map[types.NodeID]struct{}{}
	for _, tok := range toks {
		cand, err := c.nodeLabelCandidateIDsAt(tok, opts)
		if err != nil {
			return nil, err
		}
		for _, id := range cand {
			if _, dup := seen[id]; !dup {
				seen[id] = struct{}{}
				ids = append(ids, id)
			}
		}
	}
	return ids, nil
}

// nodeLabelCurrentIDs returns the label's current members: from the store's
// index without reading rows where it offers store.MemberIDsCapability, else
// from the label's rows (c.mu held).
func (c *Core) nodeLabelCurrentIDs(tok uint16) ([]types.NodeID, error) {
	if c.memberIDs != nil {
		return c.memberIDs.NodeIDsByLabel(tok)
	}
	rows, err := c.store.NodesByLabel(tok, storepkg.QueryOpts{})
	if err != nil {
		return nil, err
	}
	return c.nodeIDsFromLabelRows(tok, rows)
}

// relEffectiveAdjacencyCandidates gathers the candidates of
// ForEachRelEffectiveAtNodes (c.mu held): the current adjacency of the seeds
// that exist now in the direction (of the one type when toks has one, else of
// every type) united with every relationship with history.
func (c *Core) relEffectiveAdjacencyCandidates(seeds []types.NodeID, toks []uint16, outgoing bool) ([]types.RelID, error) {
	live := make([]types.NodeID, 0, len(seeds))
	for _, id := range seeds {
		if _, err := c.getCurrentNode(id); err != nil {
			if errors.Is(err, storepkg.ErrNodeNotFound) {
				continue // deleted (or never stored): its edges are history
			}
			return nil, err
		}
		live = append(live, id)
	}
	var tok uint16
	if len(toks) == 1 {
		tok = toks[0]
	}
	var current []types.RelID
	if len(live) > 0 {
		var (
			rows map[types.NodeID][]*types.Relationship
			err  error
		)
		if outgoing {
			rows, err = c.store.OutgoingRelationshipsForNodes(live, tok)
		} else {
			rows, err = c.store.IncomingRelationshipsForNodes(live, tok)
		}
		if err != nil {
			return nil, err
		}
		if current, err = c.relIDsFromNodeMapRows(live, tok, rows, outgoing); err != nil {
			return nil, err
		}
	}
	var ids []types.RelID
	err := c.forEachRelAdjacencyCandidateID(seeds, adjDirsOf(outgoing), current, func(id types.RelID) error {
		ids = append(ids, id)
		return nil
	})
	return ids, err
}

// ForEachNodeEffectiveByLabels is ForEachNodeEffectiveByLabel for several
// labels at once: each node that carried one of them in a row recorded by the
// pin streams once, limited to the segments whose row carries at least one
// (for every valid instant t, the union of ByLabel(label, {ValidAt: t, TxAt:
// pin}) over the labels). Repeated labels count once, unknown labels
// contribute nothing; an invalid label fails the call before the scan. Gates,
// isolation and errors are ForEachNodeEffectiveByLabel's.
func (t *TempOps) ForEachNodeEffectiveByLabels(labels []string, pin types.Instant, fn func(temporal.NodeSegment) bool) error {
	return t.c.forEachNodeEffectiveByLabels(labels, pin, fn)
}

// forEachNodeEffectiveByLabels is the body of ForEachNodeEffectiveByLabel(s).
func (c *Core) forEachNodeEffectiveByLabels(labels []string, pin types.Instant, fn func(temporal.NodeSegment) bool) error {
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	for _, label := range labels {
		if err := c.validateIndexLabel(label); err != nil {
			return err
		}
	}
	if err := c.validateEffectiveScanPin(pin, scopeNodes); err != nil {
		return err
	}
	var (
		toks []uint16
		ids  []types.NodeID
	)
	if err := c.readUnderRLock(func() error {
		for _, label := range labels {
			if tok, known := c.labels.Lookup(label); known && !slices.Contains(toks, tok) {
				toks = append(toks, tok)
			}
		}
		var err error
		ids, err = c.nodeLabelsCandidatesAt(toks, pin)
		return err
	}); err != nil {
		return err
	}
	return c.streamNodeTimelines(ascending(ids), pin, func(n *types.Node) bool {
		return slices.ContainsFunc(toks, n.HasLabelTokenRaw)
	}, fn)
}
