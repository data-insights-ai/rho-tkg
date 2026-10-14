package core

import (
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
// label and pin, when one is (no pass). With TxAt (and ValidAt) it counts the
// segments containing ValidAt of the label's state cut at TxAt (backlog 28,
// state_columns.go: built on the second read at the pin, then no row read).
// Otherwise it resolves the same candidates ByLabel resolves, with two
// savings: a candidate whose current row alone decides the answer is decided
// on the row the label scan already read (no copy, no history read), and
// nothing is materialized, sorted or paginated (the count of matches above
// After, capped at Limit).
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

// CountByTypeAt is CountByLabelAt for relationships: len(ByType(typeName,
// opts)) without building the result. The type's counter without a temporal
// filter (CountByType); the type's state cut with TxAt (and ValidAt);
// otherwise ByType's candidates, a candidate whose current row alone decides
// decided on the type scan's row.
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
	// The candidates in ascending ID order with their current rows
	// (label_scan_at.go): a node without history rows is decided on its row.
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

// nodeCurrentDecides reports whether a label read's current row alone
// decides what findNodeVersionForOpts(id, opts, pred) finds (decided) and
// whether that is the current row (matched), in findNodeVersionForOpts'
// precedence:
//   - TxPin: nodeAsOfLocked's current-row answer (no store as-of door, the
//     row recorded by the pin and not superseded), after its compaction and
//     retention checks;
//   - ValidAt: nodeAtLockedTx's (nodeCurrentAnswersAt), after its checks.
//
// An interval read decides nothing here: the arm that called a current row
// answering the point query at the first instant of the interval it covers a
// match over-counted CountByLabelAt / CountByTypeAt against the bitemporal
// oracle (round 6 request 12). A node without history rows is decided from
// its row by singleVersionNodes instead.
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
			if above, err := c.nodeHasHistoryAbove(id, current.Version()); err != nil || above {
				return err != nil, false, err // a row above the current version decides: resolve the chain
			}
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
			if above, err := c.relHasHistoryAbove(id, current.Version()); err != nil || above {
				return err != nil, false, err // a row above the current version decides: resolve the chain
			}
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
	}
	return false, false, nil
}
