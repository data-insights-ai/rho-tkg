package core

import (
	"context"
	"fmt"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// The value-list page door (ROWPROBE, sigma's paged-row probes):
// ForEachNodeEffectiveByLabelAndPropertyValues is the listing page door
// (effective_page_by_props.go) with one key IN a value list beside fixed
// equality pairs: one call answers a join level's probes instead of a lookup
// and a timeline read per probe.
//
// Candidates are the varying key's matches (Nodes().ByLabelAndPropertyValues:
// one index walk over every value, on disk one iterator), listed once; the
// fixed pairs (in sigma's shape a namespace and a relation, which hold every
// row of a relation) filter while each candidate is read. Each candidate is
// then read as the props door reads it: under its entity lock, the current
// row deciding the match, a chunk through one store read snapshot.
// =============================================================================

// ForEachNodeEffectiveByLabelAndPropertyValues is the value-list page door;
// its contract is temporal.API.ForEachNodeEffectiveByLabelAndPropertyValues.
func (t *TempOps) ForEachNodeEffectiveByLabelAndPropertyValues(ctx context.Context, label string, props map[string]any, key string, values []any, pin types.Instant, opts temporal.EffectivePageOpts, fn func(types.NodeID, []temporal.NodeSegment) bool) (types.NodeID, error) {
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
	if err := c.validateIndexLabel(label); err != nil {
		return 0, err
	}
	if err := c.validateIndexPropertyKey(key); err != nil {
		return 0, err
	}
	for k, v := range props {
		if err := c.validateIndexPropertyKey(k); err != nil {
			return 0, err
		}
		if k == key {
			return 0, fmt.Errorf("%w: duplicate key %q (a fixed pair and the value-list key)", storepkg.ErrInvalidStoreMutation, key)
		}
		if err := types.ValidatePropertyValue(v); err != nil {
			return 0, fmt.Errorf("graph: nodes by label and property values: %w", err)
		}
	}
	for _, v := range values {
		if err := types.ValidatePropertyValue(v); err != nil {
			return 0, fmt.Errorf("graph: nodes by label and property values: %w", err)
		}
	}
	pins, err := c.validateEffectivePins([]types.Instant{pin})
	if err != nil {
		return 0, err
	}
	forms := unionEqualForms(values)
	keys := keysOf(forms)
	var ids []types.NodeID
	var tok uint16
	if err := c.readUnderRLock(func() error {
		var known bool
		if tok, known = c.labels.Lookup(label); !known || len(forms) == 0 {
			return nil
		}
		for k := range props {
			if err := c.refuseNodeElementsIndex(tok, k, "ForEachNodeEffectiveByLabelAndPropertyValues"); err != nil {
				return err
			}
		}
		if err := c.refuseNodeElementsIndex(tok, key, "ForEachNodeEffectiveByLabelAndPropertyValues"); err != nil {
			return err
		}
		rows, err := c.nodesByLabelAndPropertyFormKeys(tok, key, forms, keys, storepkg.QueryOpts{Depth: opts.Depth, After: types.EntityID(opts.After)})
		if err != nil {
			return err
		}
		ids = make([]types.NodeID, len(rows))
		for i, r := range rows {
			ids[i] = r.ID()
		}
		return nil
	}); err != nil || len(ids) == 0 {
		return 0, err
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)

	fixed := nodeEqualMatcher(tok, props)
	in := keys.match()
	match := func(n *types.Node) bool { return fixed(n) && in.Node(n, key, false) }
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
		matched int
		buf     = make([]entry, 0, min(effScanChunk, len(ids)))
	)
	for len(ids) > 0 {
		if err := checkCtx(ctx); err != nil {
			return 0, err
		}
		n := min(effScanChunk, len(ids))
		if opts.Limit > 0 {
			n = min(n, opts.Limit-matched)
		}
		chunk := ids[:n]
		ids = ids[len(chunk):]
		buf = buf[:0]
		var readErr error
		if err := c.readUnderRLock(func() error {
			if rd.snap != nil {
				defer rd.snap.Release() // no iterator (table pins) or read mark while fn runs
			}
			for _, id := range chunk {
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
	}
	return 0, nil
}
