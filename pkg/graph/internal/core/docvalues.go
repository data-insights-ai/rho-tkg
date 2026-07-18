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
// Unlike DocValuesSnapshot it is not a STORE capability (as-of resolution is a
// graph-layer concern — version-chain selection): it reuses the pinned ByLabel
// resolver (the SAME one g.Nodes().ByLabel{TxPin} uses — K1 ever-member scoped,
// history/deleted-aware, chain-resolver-correct so a node whose CURRENT label
// differs from its label-at-txAt is handled), so it works on EVERY backend,
// INCLUDING tiered/sharded which decline the current-state column scanner.
//
// It IS cached (buildAsOfColumns → Core.asOfColumns), keyed by (label, txAt). The
// first build materializes the as-of members — the version-chain resolution is
// unavoidable — but the compact columns are then cached, so REPEATED same-txAt
// aggregations (a dashboard "AS OF SYSTEM TIME $t RETURN count/sum/…") scan the
// compact column instead of re-materializing. Crucially the cache SURVIVES
// write-active ingest: a past belief is immutable under forward writes (a new
// version has TxFrom = now > txAt, invisible at txAt), so ONLY a history rewrite
// below txAt (compaction / retention purge / truncate / past-dated backfill or
// replica apply) invalidates it — see Core.asOfColumns / asOfColumnCache.
//
// ok=false (not an error) when a requested property is not a uniform
// numeric/string column at txAt (the consumer declines the whole query and falls
// back to the row path). A pin before a relevant label's retention watermark
// returns ErrRetentionExpired — inherited from the pinned scan's retention guard.
// txAt must be positive.
//
// gen is the as-of history-rewrite epoch (NOT the current-state node-mutation
// epoch): it is STABLE for a fixed past belief and advances only when a history
// rewrite could have changed the belief at some txAt. A caching consumer keys its
// own result on (txAt, gen) and re-fetches only when gen advances — correct in
// both directions, unlike the current-state epoch which would falsely invalidate on
// every forward write and falsely hold across a compaction.
func (n *NodeOps) DocValuesSnapshotAsOf(label string, propKeys []string, txAt types.Instant) (snap types.NodeColumnReader, gen uint64, ok bool, err error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, 0, false, err
	}
	if txAt <= 0 {
		return nil, 0, false, ErrInvalidTimeRange
	}
	col, rerr := c.buildAsOfColumns(label, propKeys, txAt)
	if rerr != nil {
		return nil, 0, false, rerr
	}
	ps, colOK := col.NewPointSnapshot(propKeys)
	if !colOK {
		return nil, 0, false, nil // a requested key is not a uniform column at txAt
	}
	return ps, col.Epoch(), true, nil
}

// ForEachDocValuesAsOf is the STREAMING transaction-time (AS OF) analogue of
// ForEachDocValues: it enumerates the label's members AS BELIEVED at txAt and
// invokes fn(id, vals, present) for each, in ordinal order, WITHOUT materializing
// a node — the time-travel target for `AS OF SYSTEM TIME … RETURN <agg>(n.prop)…`.
// fn returning false stops the scan. Same design as DocValuesSnapshotAsOf: reuses
// the pinned ByLabel{TxPin} resolver + the cached indexpkg.BuildLabelDocValues
// (Core.asOfColumns, keyed by (label, txAt), invalidated only by history rewrite —
// so repeated same-txAt streams ride the cache even under write-active ingest), so
// it works on EVERY backend (incl. tiered/sharded) and is retention-guarded. gen is
// the as-of history-rewrite epoch (stable for a fixed past belief — see
// DocValuesSnapshotAsOf). ok=false (not an error) when a requested property is not a
// uniform numeric/string column at txAt — the consumer falls back to the row path.
// txAt must be positive.
func (n *NodeOps) ForEachDocValuesAsOf(label string, propKeys []string, txAt types.Instant, fn func(types.NodeID, []any, []bool) bool) (gen uint64, ok bool, err error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return 0, false, err
	}
	if txAt <= 0 {
		return 0, false, ErrInvalidTimeRange
	}
	col, rerr := c.buildAsOfColumns(label, propKeys, txAt)
	if rerr != nil {
		return 0, false, rerr
	}
	// ForEachRow returns false if any requested key is not a uniform column — the
	// same "unusable, caller falls back" signal as NewPointSnapshot. Because it
	// checks the columns before emitting any row, an ok=false stream emits nothing.
	streamed := col.ForEachRow(propKeys, fn)
	return col.Epoch(), streamed, nil
}

// buildAsOfColumns resolves the label's members AS BELIEVED at txAt (the pinned
// ByLabel{TxPin} resolver — K1-scoped, history/deleted-aware, chain-resolver
// correct) and builds a throwaway column set over their as-of property values.
// Shared by DocValuesSnapshotAsOf + ForEachDocValuesAsOf. Epoch 0 — an as-of
// column set carries no current-state staleness signal (see DocValuesSnapshotAsOf).
func (c *Core) buildAsOfColumns(label string, propKeys []string, txAt types.Instant) (*indexpkg.LabelDocValues, error) {
	epoch := c.asOfColumns.currentEpoch()

	// Cache hit: a column built for this (label, txAt) under the current epoch that
	// already holds every requested key. The past belief is immutable under forward
	// ingest, so this hits repeatedly even while writes stream in — the whole point.
	tok, known := c.labels.Lookup(label)
	var key asOfCacheKey
	if known {
		key = asOfCacheKey{label: tok, txAt: int64(txAt)}
		if col, ok := c.asOfColumns.get(key, propKeys, epoch); ok {
			return col, nil
		}
	}

	// Miss: build a superset of any prior keys for this pin (mirrors the current-state
	// cache), materializing the as-of members once. Cache the compact result unless
	// the label is over the column cap (huge labels are one-shot, never cached).
	buildKeys := propKeys
	if known {
		buildKeys = c.asOfColumns.unionKeysFor(key, propKeys)
	}
	var col *indexpkg.LabelDocValues
	cacheable := false
	err := c.readUnderRLock(func() error {
		nodes, e := c.nodesByLabelLocked(label, storepkg.QueryOpts{TxPin: txAt})
		if e != nil {
			return e
		}
		cacheable = known && len(nodes) <= indexpkg.MaxDocValuesNodes
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
		col = indexpkg.BuildLabelDocValues(epoch, ids, buildKeys, getProp)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if cacheable {
		c.asOfColumns.put(key, col, epoch)
	}
	return col, nil
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

// nodeLabelMutationEpochScanner exposes the PER-LABEL node-mutation epoch (BACKLOG 4b,
// badger only).
type nodeLabelMutationEpochScanner interface {
	NodeLabelMutationEpoch(labelToken uint16) uint64
}

// NodeLabelMutationEpoch returns the per-label node-mutation epoch for label — the
// value a single-label ForEachDocValues/DocValuesSnapshot returns as gen (BACKLOG 4b).
// It advances ONLY when a node carrying THIS label is written (or a label-less
// invalidation event fires), NOT on unrelated-label writes, unlike NodeMutationEpoch.
// A Gate-2 re-check on a single-label aggregate should use this to avoid discarding a
// still-valid result after an unrelated-label write. Returns 0 for an unknown label or
// a store without the capability (tiered/sharded — they decline the column scanner).
func (n *NodeOps) NodeLabelMutationEpoch(label string) uint64 {
	c := n.c
	scanner, native := c.store.(nodeLabelMutationEpochScanner)
	if !native {
		return 0
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.lookupLabelLocked(label)
		return nil
	}); err != nil || !ok {
		return 0
	}
	return scanner.NodeLabelMutationEpoch(tok)
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
