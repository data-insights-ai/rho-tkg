package core

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// CountByLabelAt returns len(ByLabel(label, opts)) without building the result:
// the exact number of nodes carrying label at the read coordinate in opts (same
// options and validation as ByLabel). Errors are ByLabel's; an unknown label counts 0.
//
// Paths: no temporal filter/pagination/Depth is the store's O(1) label counter;
// TxPin alone uses the cached as-of column set when present; TxAt (and ValidAt)
// counts segments of the label's state cut (backlog 28, state_columns.go);
// otherwise ByLabel's candidates are resolved, deciding a candidate on the row the
// label scan already read when its current row alone decides, with nothing
// materialized, sorted or paginated.
func (n *NodeOps) CountByLabelAt(label string, opts storepkg.QueryOpts) (int, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return 0, err
	}
	if err := c.validateIndexLabel(label); err != nil {
		return 0, err
	}
	if err := c.validateTemporalQueryOptsScan(opts, scopeNodes); err != nil {
		return 0, err
	}
	if n, ok := c.countStateCut(false, label, opts); ok {
		return n, nil
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

// CountByTypeAt is CountByLabelAt for relationships: len(ByType(typeName, opts))
// without building the result, with the same paths using the type counter and state cut.
func (r *RelOps) CountByTypeAt(typeName string, opts storepkg.QueryOpts) (int, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return 0, err
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return 0, err
	}
	if err := c.validateTemporalQueryOptsScan(opts, c.relTypeRetentionScope(typeName)); err != nil {
		return 0, err
	}
	if n, ok := c.countStateCut(true, typeName, opts); ok {
		return n, nil
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

// unpaged reports whether opts reads the whole label or type: no cursor, no limit, every tier.
func unpaged(opts storepkg.QueryOpts) bool {
	return opts.After == 0 && opts.Limit == 0 && opts.Depth == storepkg.DepthAll
}

// pagedCount is the number of matches above after, capped at limit (0 = no limit).
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
	if opts.HasTxPin() && unpaged(opts) {
		key := asOfCacheKey{label: tok, txAt: int64(txPinOf(opts))}
		if n, hit := c.asOfColumns.memberCount(key, c.asOfColumns.currentEpoch()); hit {
			return n, nil
		}
	}
	current, candIDs, err := c.nodeLabelCandidatesAt(tok, opts)
	if err != nil {
		return 0, err
	}
	// Candidates in ascending ID order with their current rows (label_scan_at.go).
	scan := c.newNodeLabelScanAt(tok, current, opts)
	count := pagedCount{after: opts.After, limit: opts.Limit}
	for _, id := range skipAfter(candIDs, opts.After) {
		n, err := scan.resolve(id)
		if err != nil {
			return 0, err
		}
		if n != nil {
			count.add(types.EntityID(id))
			if opts.Limit > 0 && count.n >= opts.Limit {
				break
			}
		}
	}
	return count.total(), nil
}

// nodeCurrentDecides reports whether a label read's current row alone decides what
// findNodeVersionForOpts(id, opts, pred) finds (decided) and whether that is the
// current row (matched), in its precedence: TxPin uses nodeAsOfLocked's current-row
// answer (after compaction and retention checks); ValidAt uses nodeAtLockedTx's
// (nodeCurrentAnswersAt). An interval read decides nothing here: treating the
// current row as answering the interval's first instant over-counted against the
// bitemporal oracle. A node without history rows is decided by singleVersionNodes.
func (c *Core) nodeCurrentDecides(id types.NodeID, current *types.Node, opts storepkg.QueryOpts, pred func(*types.Node) bool) (decided, matched bool, err error) {
	switch {
	case opts.HasTxPin():
		pin := txPinOf(opts)
		if c.txTimeQuery != nil {
			return false, false, nil
		}
		if err := c.checkNodePointCompaction(id, pin); err != nil {
			return true, false, err
		}
		if err := c.checkNodePointRetention(id, pin); err != nil {
			return true, false, err
		}
		if tm := current.Temporal(); tm != nil && tm.TxFrom > 0 && tm.TxFrom <= pin && tm.TxTo == 0 {
			if above, err := c.nodeHasHistoryAbove(id, current.Version()); err != nil || above {
				return err != nil, false, err // a row above the current version decides: resolve the chain
			}
			return true, pred(current), nil
		}
	case opts.HasValidAt():
		txAt := txAtOf(opts)
		if err := c.checkNodePointCompaction(id, txAt); err != nil {
			return true, false, err
		}
		if err := c.checkNodePointRetention(id, txAt); err != nil {
			return true, false, err
		}
		if c.nodeCurrentAnswersAt(current, opts.ValidAt, txAt) {
			return true, pred(current), nil
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
	scan := c.newRelTypeScanAt(tok, current, opts)
	count := pagedCount{after: opts.After, limit: opts.Limit}
	for _, id := range skipAfter(candIDs, opts.After) {
		r, err := scan.resolve(id)
		if err != nil {
			return 0, err
		}
		if r != nil {
			count.add(types.EntityID(id))
			if opts.Limit > 0 && count.n >= opts.Limit {
				break
			}
		}
	}
	return count.total(), nil
}

// relCurrentDecides is nodeCurrentDecides for relationships.
func (c *Core) relCurrentDecides(id types.RelID, current *types.Relationship, opts storepkg.QueryOpts, pred func(*types.Relationship) bool) (decided, matched bool, err error) {
	switch {
	case opts.HasTxPin():
		pin := txPinOf(opts)
		if c.txTimeQuery != nil {
			return false, false, nil
		}
		if err := c.checkRelPointCompaction(id, pin); err != nil {
			return true, false, err
		}
		if err := c.checkRelPointRetention(pin); err != nil {
			return true, false, err
		}
		if tm := current.Temporal(); tm != nil && tm.TxFrom > 0 && tm.TxFrom <= pin && tm.TxTo == 0 {
			if above, err := c.relHasHistoryAbove(id, current.Version()); err != nil || above {
				return err != nil, false, err // a row above the current version decides: resolve the chain
			}
			return true, pred(current), nil
		}
	case opts.HasValidAt():
		txAt := txAtOf(opts)
		if err := c.checkRelPointCompaction(id, txAt); err != nil {
			return true, false, err
		}
		if err := c.checkRelPointRetention(txAt); err != nil {
			return true, false, err
		}
		if c.relCurrentAnswersAt(current, opts.ValidAt, txAt) {
			return true, pred(current), nil
		}
	}
	return false, false, nil
}
