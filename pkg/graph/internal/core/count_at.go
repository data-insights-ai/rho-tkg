package core

import (
	"errors"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// CountByLabelAt returns len(ByLabel(label, opts)) without building the
// result: the exact number of nodes carrying label at the read coordinate in
// opts (ValidAt, ValidStart/ValidEnd, TxAt, TxPin, Depth, After, Limit — the
// same options and validation as ByLabel).
//
// Without a temporal filter and without pagination or Depth it is the store's
// label counter (O(1), CountByLabel). With TxPin alone it answers from the
// as-of column set DocValuesSnapshotAsOf / ForEachDocValuesAsOf cached for the
// label and pin, when one is (no pass). Otherwise it resolves the same
// candidates ByLabel resolves, with two savings: a candidate whose current row
// alone decides the answer is decided on the row the label scan already read
// (no copy, no history read), and nothing is materialized, sorted or
// paginated (the count of matches above After, capped at Limit).
//
// Errors are ByLabel's (a malformed label, invalid or conflicting options,
// ErrRetentionExpired for a pin before a retention watermark, ErrGraphClosed).
// An unknown label counts 0.
func (n *NodeOps) CountByLabelAt(label string, opts storepkg.QueryOpts) (int, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return 0, err
	}
	if err := c.validateIndexLabel(label); err != nil {
		return 0, err
	}
	if err := c.validateTemporalQueryOptsScan(opts); err != nil {
		return 0, err
	}
	var count int
	err := c.readUnderRLock(func() error {
		var err error
		count, err = c.countNodesByLabelLocked(label, opts)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

// CountByTypeAt is CountByLabelAt for relationships: len(ByType(typeName,
// opts)) without building the result. The type's counter without a temporal
// filter (CountByType); otherwise ByType's candidates, a candidate whose
// current row alone decides decided on the type scan's row.
func (r *RelOps) CountByTypeAt(typeName string, opts storepkg.QueryOpts) (int, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return 0, err
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return 0, err
	}
	if err := c.validateTemporalQueryOptsScan(opts); err != nil {
		return 0, err
	}
	var count int
	err := c.readUnderRLock(func() error {
		var err error
		count, err = c.countRelsByTypeLocked(typeName, opts)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

// unpaged reports whether opts reads the whole label or type: no cursor, no
// limit, every tier.
func unpaged(opts storepkg.QueryOpts) bool {
	return opts.After == 0 && opts.Limit == 0 && opts.Depth == storepkg.DepthAll
}

// pagedCount is the number of matches above after, capped at limit (0 = no
// limit): the length ByLabel's / ByType's pagination leaves.
type pagedCount struct {
	after types.EntityID
	limit int
	n     int
}

func (p *pagedCount) add(id types.EntityID) {
	if p.after.SnowflakeID() > 0 && id.SnowflakeID() <= p.after.SnowflakeID() {
		return
	}
	p.n++
}

func (p *pagedCount) total() int {
	if p.limit > 0 && p.n > p.limit {
		return p.limit
	}
	return p.n
}

// countNodesByLabelLocked is the body of CountByLabelAt. Callers hold c.mu.
func (c *Core) countNodesByLabelLocked(label string, opts storepkg.QueryOpts) (int, error) {
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return 0, nil
	}
	if !hasTemporalFilter(opts) {
		if unpaged(opts) {
			return c.nodeCountByLabel(tok)
		}
		nodes, err := c.nodesByLabelLocked(label, opts)
		return len(nodes), err
	}
	if opts.TxPin != 0 && unpaged(opts) {
		key := asOfCacheKey{label: tok, txAt: int64(opts.TxPin)}
		if n, hit := c.asOfColumns.memberCount(key, c.asOfColumns.currentEpoch()); hit {
			return n, nil
		}
	}
	current, candIDs, err := c.nodeLabelCandidatesAt(tok, opts)
	if err != nil {
		return 0, err
	}
	rows := make(map[types.NodeID]*types.Node, len(current))
	for _, nd := range current {
		rows[nd.ID()] = nd
	}
	pred := func(n *types.Node) bool { return n.HasLabelTokenRaw(tok) }
	resolveOpts := c.normalizeTxAtOnlyOpts(opts)
	count := pagedCount{after: opts.After, limit: opts.Limit}
	for _, id := range candIDs {
		matched, err := c.nodeMatchesAt(id, rows[id], resolveOpts, pred)
		if err != nil {
			return 0, err
		}
		if matched {
			count.add(types.EntityID(id))
		}
	}
	return count.total(), nil
}

// nodeMatchesAt reports whether findNodeVersionForOpts(id, opts, pred) finds a
// version. current is the node's current row when the caller read it carrying
// the label (nil otherwise); where that row alone decides — under the same
// conditions findNodeVersionForOpts' own current-row shortcuts use — the
// answer is pred(current) with no further read. Every other case runs
// findNodeVersionForOpts itself.
func (c *Core) nodeMatchesAt(id types.NodeID, current *types.Node, opts storepkg.QueryOpts, pred func(*types.Node) bool) (bool, error) {
	if current != nil {
		decided, matched, err := c.nodeCurrentDecides(id, current, opts, pred)
		if err != nil || decided {
			return matched, err
		}
	}
	_, err := c.findNodeVersionForOpts(id, opts, pred)
	if err != nil {
		if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrNodeNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// nodeCurrentDecides is nodeMatchesAt's current-row arm, in
// findNodeVersionForOpts' precedence (TxPin, ValidAt, interval):
//   - TxPin: nodeAsOfLocked's current-row answer (no store as-of door, the
//     row recorded by the pin and not superseded), after its compaction and
//     retention checks;
//   - ValidAt: nodeAtLockedTx's (nodeCurrentAnswersAt), after its checks;
//   - ValidStart/ValidEnd: a match exists when the current row satisfies pred
//     and alone answers the point query at the first instant of the interval
//     it covers, since an interval read finds any version valid somewhere in
//     the interval. A current row that does not satisfy pred decides nothing
//     (an older version may).
func (c *Core) nodeCurrentDecides(id types.NodeID, current *types.Node, opts storepkg.QueryOpts, pred func(*types.Node) bool) (decided, matched bool, err error) {
	switch {
	case opts.TxPin != 0:
		if c.txTimeQuery != nil {
			return false, false, nil
		}
		if err := c.checkNodePointCompaction(id, opts.TxPin); err != nil {
			return true, false, err
		}
		if err := c.checkNodePointRetention(id, opts.TxPin); err != nil {
			return true, false, err
		}
		if tm := current.Temporal(); tm != nil && tm.TxFrom > 0 && tm.TxFrom <= opts.TxPin && tm.TxTo == 0 {
			return true, pred(current), nil
		}
	case opts.ValidAt != 0:
		if err := c.checkNodePointCompaction(id, opts.TxAt); err != nil {
			return true, false, err
		}
		if err := c.checkNodePointRetention(id, opts.TxAt); err != nil {
			return true, false, err
		}
		if c.nodeCurrentAnswersAt(current, opts.ValidAt, opts.TxAt) {
			return true, pred(current), nil
		}
	case opts.ValidStart > 0 && opts.ValidEnd > 0:
		at := max(opts.ValidStart, c.nodeSortValidFrom(current))
		if at < opts.ValidEnd && pred(current) && c.nodeCurrentAnswersAt(current, at, opts.TxAt) {
			return true, true, nil
		}
	}
	return false, false, nil
}

// countRelsByTypeLocked is the body of CountByTypeAt. Callers hold c.mu.
func (c *Core) countRelsByTypeLocked(typeName string, opts storepkg.QueryOpts) (int, error) {
	tok, ok := c.lookupRelTypeQueryToken(typeName)
	if !ok {
		return 0, nil
	}
	if !hasTemporalFilter(opts) {
		if unpaged(opts) {
			return c.relCountByType(tok)
		}
		rels, err := c.relsByTypeLocked(typeName, opts)
		return len(rels), err
	}
	current, candIDs, err := c.relTypeCandidatesAt(tok, opts)
	if err != nil {
		return 0, err
	}
	rows := make(map[types.RelID]*types.Relationship, len(current))
	for _, rel := range current {
		rows[rel.ID()] = rel
	}
	pred := func(r *types.Relationship) bool { return r.HasTypeTokenRaw(tok) }
	resolveOpts := c.normalizeTxAtOnlyOpts(opts)
	count := pagedCount{after: opts.After, limit: opts.Limit}
	for _, id := range candIDs {
		matched, err := c.relMatchesAt(id, rows[id], resolveOpts, pred)
		if err != nil {
			return 0, err
		}
		if matched {
			count.add(types.EntityID(id))
		}
	}
	return count.total(), nil
}

// relMatchesAt is nodeMatchesAt for relationships.
func (c *Core) relMatchesAt(id types.RelID, current *types.Relationship, opts storepkg.QueryOpts, pred func(*types.Relationship) bool) (bool, error) {
	if current != nil {
		decided, matched, err := c.relCurrentDecides(id, current, opts, pred)
		if err != nil || decided {
			return matched, err
		}
	}
	_, err := c.findRelVersionForOpts(id, opts, pred)
	if err != nil {
		if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrRelNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// relCurrentDecides is nodeCurrentDecides for relationships (relAsOfLocked's
// and relAtLockedTx's current-row answers).
func (c *Core) relCurrentDecides(id types.RelID, current *types.Relationship, opts storepkg.QueryOpts, pred func(*types.Relationship) bool) (decided, matched bool, err error) {
	switch {
	case opts.TxPin != 0:
		if c.txTimeQuery != nil {
			return false, false, nil
		}
		if err := c.checkRelPointCompaction(id, opts.TxPin); err != nil {
			return true, false, err
		}
		if err := c.checkRelPointRetention(opts.TxPin); err != nil {
			return true, false, err
		}
		if tm := current.Temporal(); tm != nil && tm.TxFrom > 0 && tm.TxFrom <= opts.TxPin && tm.TxTo == 0 {
			return true, pred(current), nil
		}
	case opts.ValidAt != 0:
		if err := c.checkRelPointCompaction(id, opts.TxAt); err != nil {
			return true, false, err
		}
		if err := c.checkRelPointRetention(opts.TxAt); err != nil {
			return true, false, err
		}
		if c.relCurrentAnswersAt(current, opts.ValidAt, opts.TxAt) {
			return true, pred(current), nil
		}
	case opts.ValidStart > 0 && opts.ValidEnd > 0:
		at := max(opts.ValidStart, c.relSortValidFrom(current))
		if at < opts.ValidEnd && pred(current) && c.relCurrentAnswersAt(current, at, opts.TxAt) {
			return true, true, nil
		}
	}
	return false, false, nil
}
