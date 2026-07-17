package core

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// nodeDocValuesScanner is the OPTIONAL store capability behind the X5 columnar
// DocValues aggregation fast path (NodeOps.ForEachDocValues). Implemented by the
// in-tree memory and badger stores; stores without it cause the consumer to fall
// back to the per-node aggregation path.
type nodeDocValuesScanner interface {
	ForEachDocValues(labelToken uint16, propKeys []string, fn func(types.NodeID, []any, []bool) bool) (gen uint64, ok bool, err error)
	ForEachDocValuesMulti(labelTokens []uint16, propKeys []string, fn func(types.NodeID, []any, []bool) bool) (gen uint64, ok bool, err error)
	DocValuesSnapshot(labelToken uint16, propKeys []string) (snap types.NodeColumnReader, gen uint64, ok bool, err error)
	NodeMutationEpoch() uint64
}

// nodeMutationEpochScanner exposes ONLY the node-mutation epoch, DECOUPLED from
// the columnar DocValues path. A consumer keys a read cache on this epoch to
// invalidate it on writes; the value need not be the DocValues cache's — only
// monotonically advancing on every node mutation. Splitting it out lets a
// partitioned backend (sharded) that declines DocValues STILL expose a correct
// advancing epoch (folded across shards) — without it the epoch defaulted to a
// constant 0 there, so an epoch-keyed consumer cache never invalidated and
// served stale reads after writes on a multi-lane deployment.
type nodeMutationEpochScanner interface {
	NodeMutationEpoch() uint64
}

// ForEachDocValues streams the requested property columns for a label's nodes in
// ordinal order, serving each value from a cached columnar snapshot instead of a
// per-node fetch+decode. Membership is the full label index (the unfiltered scan
// set), so a non-temporal aggregation counts every label member.
//
// Returns ok=false (not an error) when the column path is unusable — the store
// lacks the capability, the label is unknown/empty/over-cap, or a requested
// property is not a uniformly numeric/string column — and the caller falls back.
// The returned gen is the node-mutation epoch the snapshot was built at; the
// caller re-checks NodeMutationEpoch()==gen after consuming the rows (Gate 2:
// a concurrent writer during the lock-free scan advances the epoch).
func (n *NodeOps) ForEachDocValues(label string, propKeys []string,
	fn func(types.NodeID, []any, []bool) bool) (gen uint64, ok bool, err error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return 0, false, err
	}
	scanner, native := c.store.(nodeDocValuesScanner)
	if !native {
		return 0, false, nil
	}
	var tok uint16
	var found bool
	if err := c.readUnderRLock(func() error {
		tok, found = c.lookupLabelLocked(label)
		return nil
	}); err != nil {
		return 0, false, err
	}
	if !found {
		return 0, false, nil // unknown label — caller falls back (finds zero rows)
	}
	return scanner.ForEachDocValues(tok, propKeys, fn)
}

// ForEachDocValuesMulti streams the requested property columns for a LABEL
// INTERSECTION (a multi-label pattern like (p:A:B)) in ordinal order, serving each
// value from a cached columnar snapshot over the intersection membership instead of
// a per-node fetch+decode. Same contract as ForEachDocValues: ok=false (not an
// error) when unusable — the store lacks the capability, any label is unknown, the
// intersection is empty/over-cap, or a requested property is not a uniformly
// numeric/string column — and the caller falls back to the general path.
func (n *NodeOps) ForEachDocValuesMulti(labels []string, propKeys []string,
	fn func(types.NodeID, []any, []bool) bool) (gen uint64, ok bool, err error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return 0, false, err
	}
	scanner, native := c.store.(nodeDocValuesScanner)
	if !native {
		return 0, false, nil
	}
	if len(labels) == 0 {
		return 0, false, nil
	}
	toks := make([]uint16, 0, len(labels))
	if err := c.readUnderRLock(func() error {
		for _, label := range labels {
			tok, found := c.lookupLabelLocked(label)
			if !found {
				toks = nil // an unknown label → empty intersection → fall back
				return nil
			}
			toks = append(toks, tok)
		}
		return nil
	}); err != nil {
		return 0, false, err
	}
	if len(toks) != len(labels) {
		return 0, false, nil // an unknown label — caller falls back (finds zero rows)
	}
	return scanner.ForEachDocValuesMulti(toks, propKeys, fn)
}

// DocValuesSnapshot returns a random-access point-lookup handle over a single
// label's cached column snapshot — the X5 expand-aggregation target side, fetching
// a node's properties by ID without materializing it. ok=false (not an error) when
// unusable: no capability, unknown/empty/over-cap label, or any requested property
// not a uniformly numeric/string column (consumer declines the
// whole query). gen is the snapshot's node-mutation epoch (pair with
// RelMutationEpoch for the adjacency Gate-2).
func (n *NodeOps) DocValuesSnapshot(label string, propKeys []string) (snap types.NodeColumnReader, gen uint64, ok bool, err error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, 0, false, err
	}
	scanner, native := c.store.(nodeDocValuesScanner)
	if !native {
		return nil, 0, false, nil
	}
	var tok uint16
	var found bool
	if err := c.readUnderRLock(func() error {
		tok, found = c.lookupLabelLocked(label)
		return nil
	}); err != nil {
		return nil, 0, false, err
	}
	if !found {
		return nil, 0, false, nil // unknown label → fall back (finds zero rows)
	}
	return scanner.DocValuesSnapshot(tok, propKeys)
}

// DocValuesSnapshotAsOf is the transaction-time (AS OF) analogue of
// DocValuesSnapshot: a random-access columnar handle over a label's members AS
// BELIEVED at txAt (a knowledge-time / TxPin belief-state pin) — the time-travel
// aggregation target for `AS OF SYSTEM TIME … RETURN count(*)…`.
//
// Unlike DocValuesSnapshot it is NOT a store capability and NOT cached: it reuses
// the pinned ByLabel resolver (the SAME one g.Nodes().ByLabel{TxPin} uses — K1
// ever-member scoped, history/deleted-aware, chain-resolver-correct so a node
// whose CURRENT label differs from its label-at-txAt is handled), then builds a
// throwaway column set from the resolved as-of nodes. Consequently it works on
// EVERY backend, INCLUDING tiered/sharded which decline the current-state column
// scanner. A specific past pin is immutable and typically one-shot, so caching
// (the current-state epoch-invalidation model) does not apply.
//
// ok=false (not an error) when a requested property is not a uniform
// numeric/string column at txAt (the consumer declines the whole query and falls
// back to the row path). A pin before a relevant label's retention watermark
// returns ErrRetentionExpired (the answer would be silently incomplete) —
// inherited from the pinned scan's retention guard. txAt must be positive.
//
// gen is DELIBERATELY 0 (unlike the current-state door): the returned snapshot is
// a FROZEN materialization of the belief at txAt, built under c.mu.RLock, so the
// consumer's Gate-2 epoch re-check does NOT apply. The current-state node-mutation
// epoch would be wrong in BOTH directions here — it bumps on current writes that
// do not change a past belief (false stale), and it does NOT bump on
// compaction/truncate, which rewrite history and DO change a past belief (false
// fresh). A past pin is immutable except under explicit history-rewriting ADMIN
// ops (compaction / retention purge / bitemporal PutNodeVersion correction);
// those all take c.mu.Lock, so they cannot tear this RLock-held build, and a
// caching consumer should treat them as out-of-band cache-busting events rather
// than gen-recheck. So: use the snapshot as a point-in-time read; do not re-check
// gen.
func (n *NodeOps) DocValuesSnapshotAsOf(label string, propKeys []string, txAt types.Instant) (snap types.NodeColumnReader, gen uint64, ok bool, err error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, 0, false, err
	}
	if txAt <= 0 {
		return nil, 0, false, ErrInvalidTimeRange
	}
	var col *indexpkg.LabelDocValues
	if rerr := c.readUnderRLock(func() error {
		nodes, e := c.nodesByLabelLocked(label, storepkg.QueryOpts{TxPin: txAt})
		if e != nil {
			return e
		}
		ids := make([]types.NodeID, len(nodes))
		byID := make(map[types.NodeID]*types.Node, len(nodes))
		for i, nd := range nodes {
			ids[i] = nd.ID()
			byID[nd.ID()] = nd
		}
		getProp := func(id types.NodeID, key string) (any, bool) {
			nd, present := byID[id]
			if !present {
				return nil, false
			}
			return nd.GetProperty(key)
		}
		// epoch 0 — an as-of snapshot carries no current-state staleness signal
		// (see the doc block). It is a frozen point-in-time materialization.
		col = indexpkg.BuildLabelDocValues(0, ids, propKeys, getProp)
		return nil
	}); rerr != nil {
		return nil, 0, false, rerr
	}
	ps, colOK := col.NewPointSnapshot(propKeys)
	if !colOK {
		return nil, 0, false, nil // a requested key is not a uniform column at txAt
	}
	return ps, 0, true, nil
}

// NodeMutationEpoch returns the store's current node-mutation epoch, or 0 if the
// store lacks the DocValues capability. The consumer's Gate-2 staleness check.
func (n *NodeOps) NodeMutationEpoch() uint64 {
	scanner, native := n.c.store.(nodeMutationEpochScanner)
	if !native {
		return 0
	}
	return scanner.NodeMutationEpoch()
}

// relMutationEpochScanner is the OPTIONAL store capability exposing the global
// relationship-mutation epoch (memory + badger). The X5 expand-aggregation column
// path reads adjacency, so its Gate-2 samples this to discard a torn aggregate
// produced by a concurrent edge insert/delete during the scan.
type relMutationEpochScanner interface {
	RelMutationEpoch() uint64
}

// RelMutationEpoch returns the store's current relationship-mutation epoch, or 0 if
// the store lacks the capability. The expand column path's Gate-2 adjacency check.
func (r *RelOps) RelMutationEpoch() uint64 {
	scanner, native := r.c.store.(relMutationEpochScanner)
	if !native {
		return 0
	}
	return scanner.RelMutationEpoch()
}
