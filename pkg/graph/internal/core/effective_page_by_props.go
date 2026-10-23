package core

import (
	"context"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// The listing page door (row 121, sigma R37 for ai-soc's cold /bars listing):
// ForEachNodeEffectiveByLabelAndProperties is Nodes().ByLabelAndProperties
// paged by ID, each match's NodeEffectiveTimelines at one pin, in one call.
//
// Candidates come from the composite index's posting list, IDs only
// (store.CompositeIndexIDPageCapability), so no row is read to list one; a
// store without it (or values the index cannot answer) lists through
// NodesByLabelAndProperties. Each candidate is then read once, under its
// entity lock: the current row, lent frozen, decides the match (label and
// every pair, the equality of ByLabelAndProperties) and is the same row the
// timeline resolves, and only the winning history rows are decoded. A chunk
// of candidates reads through one store read snapshot where the store offers
// one (badger, sharded, tiered), so a page opens about one read transaction
// per chunk, not one per row.
// =============================================================================

// ForEachNodeEffectiveByLabelAndProperties is the listing page door; its
// contract is temporal.API.ForEachNodeEffectiveByLabelAndProperties.
func (t *TempOps) ForEachNodeEffectiveByLabelAndProperties(ctx context.Context, label string, props map[string]any, pin types.Instant, opts temporal.EffectivePageOpts, fn func(types.NodeID, []temporal.NodeSegment) bool) (types.NodeID, error) {
	c := t.c
	if err := checkCtx(ctx); err != nil {
		return 0, err
	}
	if err := c.checkOpen(); err != nil {
		return 0, err
	}
	if fn == nil {
		return 0, grapherr.ErrNilCallback
	}
	if opts.Limit < 0 {
		return 0, fmt.Errorf("%w: %d (want >= 0; 0 = all)", ErrInvalidQueryLimit, opts.Limit)
	}
	if opts.After < 0 {
		return 0, fmt.Errorf("%w: %d", ErrInvalidQueryCursor, opts.After)
	}
	if err := c.validateTemporalQueryOptsScan(storepkg.QueryOpts{Depth: opts.Depth}, scopeNodes); err != nil {
		return 0, err
	}
	if err := c.validateLabelAndProperties(label, props, "nodes by label and properties value"); err != nil {
		return 0, err
	}
	pins, err := c.validateEffectivePins([]types.Instant{pin})
	if err != nil {
		return 0, err
	}
	var (
		tok   uint16
		known bool
	)
	if err := c.readUnderRLock(func() error {
		if tok, known = c.labels.Lookup(label); !known {
			return nil
		}
		for k := range props {
			if err := c.refuseNodeElementsIndex(tok, k, "ForEachNodeEffectiveByLabelAndProperties"); err != nil {
				return err
			}
		}
		return nil
	}); err != nil || !known {
		return 0, err
	}
	match := nodeEqualMatcher(tok, props)
	rd := entityReads{c: c, scan: true}
	if c.readSnapshots != nil {
		rd.snap = c.readSnapshots.NewReadSnapshot()
		defer rd.snap.Close()
	}
	type entry struct {
		id   types.NodeID
		segs []temporal.NodeSegment
	}
	var (
		after   = opts.After
		matched int
		buf     = make([]entry, 0, effScanChunk)
	)
	for {
		if err := checkCtx(ctx); err != nil {
			return 0, err
		}
		n := effScanChunk
		if opts.Limit > 0 {
			n = min(n, opts.Limit-matched)
		}
		buf = buf[:0]
		var (
			more    bool
			readErr error
		)
		if err := c.readUnderRLock(func() error {
			ids, m, err := c.nodeIDsByLabelAndProperties(tok, props, after, n, opts.Depth)
			if err != nil {
				return err
			}
			more = m
			if rd.snap != nil {
				defer rd.snap.Release() // no iterator (table pins) or read mark while fn runs
			}
			for _, id := range ids {
				after = id
				segs, ok, err := c.nodeEffectiveTimelinesFrom(rd, id, pins, match)
				if err != nil {
					readErr = err
					return nil
				}
				if ok {
					buf = append(buf, entry{id, segs[0]})
				}
			}
			return nil
		}); err != nil {
			return 0, err
		}
		for _, e := range buf {
			if err := checkCtx(ctx); err != nil {
				return 0, err
			}
			matched++
			if !fn(e.id, e.segs) || matched == opts.Limit {
				return e.id, nil
			}
		}
		if readErr != nil {
			return 0, readErr
		}
		if !more {
			return 0, nil
		}
	}
}

// nodeIDsByLabelAndProperties lists up to n candidate IDs above after, in
// ascending order, and whether more may follow (c.mu held): the composite
// index's posting list where the store offers it, else the IDs of
// NodesByLabelAndProperties' page.
func (c *Core) nodeIDsByLabelAndProperties(tok uint16, values map[string]any, after types.NodeID, n int, depth storepkg.ShardDepth) ([]types.NodeID, bool, error) {
	// A numeric value has several index forms (3 and 3.0): only the
	// read path that combines them answers it.
	if _, multi := equalForms(values); !multi && c.compositeIDPage != nil {
		ids, more, handled, err := c.compositeIDPage.NodeIDsByLabelAndPropertiesPage(tok, values, after, n, depth)
		if err != nil || handled {
			return ids, more, err
		}
	}
	rows, err := c.nodesByLabelAndProperties(tok, values, storepkg.QueryOpts{Depth: depth, After: types.EntityID(after), Limit: n})
	if err != nil {
		return nil, false, err
	}
	ids := make([]types.NodeID, len(rows))
	for i, r := range rows {
		ids[i] = r.ID()
	}
	return ids, len(rows) == n, nil
}

// nodeEqualMatcher is the composite lookup's predicate (label and
// nodeMatchesAllEqual on every pair) with string values compared as strings:
// a string has one index form, so equality is its index key's equality,
// without building the row's key per read.
func nodeEqualMatcher(tok uint16, values map[string]any) func(*types.Node) bool {
	strs := make(map[string]string, len(values))
	rest := make(map[string]any, len(values))
	for k, v := range values {
		if s, ok := v.(string); ok {
			strs[k] = s
		} else {
			rest[k] = v
		}
	}
	want := equalKeysOf(rest)
	return func(n *types.Node) bool {
		if !n.HasLabelTokenRaw(tok) {
			return false
		}
		for k, s := range strs {
			if v, ok := n.GetProperty(k); !ok || v != any(s) {
				return false
			}
		}
		return nodeMatchesAllEqual(n, want)
	}
}
