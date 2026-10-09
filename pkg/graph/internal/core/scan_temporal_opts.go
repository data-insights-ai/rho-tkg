package core

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Temporal QueryOpts on the column scans and the unordered numeric range scans.
//
// These doors have the shape of ByLabel / ByType (rule 17): a label or type plus
// QueryOpts. Their fast paths hand opts to a store, and a store answers from
// CURRENT rows only — it filters the live row by valid time and has no notion of
// TxAt / TxPin (storeutil.HasTemporalFilter checks valid time only). So whenever
// hasTemporalFilter(opts) holds (ValidAt, ValidStart+ValidEnd, TxAt or TxPin) the
// doors below take the history-aware path instead: the SAME candidate fold and
// chain resolver ByLabel / ByType use (nodesByLabelLocked / relsByTypeLocked), so
// each entity contributes the version valid under opts, deleted and relabelled
// entities included, and the doors agree with ByLabel / ByType by construction.
// Pinned by TestScanDoorsAgreeWithByLabelOpts_* (pkg/graph).
//
// Cost: that of ByLabel / ByType under the same opts (O(the label's / type's
// temporal membership)). The non-temporal fast paths are unchanged.

// scanNodeColumnsTemporalLocked serves ScanNodeColumns under a temporal opt:
// ByLabel's resolved versions projected into column batches by the shared row
// path. Callers hold c.mu (R or W) and have validated opts.
func (c *Core) scanNodeColumnsTemporalLocked(label string, props []string, opts storepkg.QueryOpts,
	fn func(*storepkg.ColumnBatch) bool) error {

	nodes, err := c.nodesByLabelLocked(label, opts)
	if err != nil {
		return err
	}
	return storepkg.ScanColumnsFromNodes(nodes, props, fn)
}

// scanRelColumnsTemporalLocked serves ScanRelColumns under a temporal opt for
// the given type names (one, or every registered type in token order): ByType's
// resolved versions projected into column batches. Each batch names its type;
// pagination applies per type, as on the current-row path. Callers hold c.mu
// (R or W) and have validated opts.
func (c *Core) scanRelColumnsTemporalLocked(names []string, props []string, opts storepkg.QueryOpts,
	fn func(*storepkg.RelColumnBatch) bool) error {

	for _, name := range names {
		if name == "" {
			continue // token 0 is reserved
		}
		rels, err := c.relsByTypeLocked(name, opts)
		if err != nil {
			return err
		}
		stopped := false
		err = storepkg.ScanColumnsFromRels(rels, props, func(b *storepkg.RelColumnBatch) bool {
			b.RelType = name
			if !fn(b) {
				stopped = true
				return false
			}
			return true
		})
		if err != nil || stopped {
			return err
		}
	}
	return nil
}

// forEachNodeInRangeTemporal serves ForEachByLabelPropertyRange under a
// temporal opt: ByLabel's resolved versions, in ID order, whose numeric propKey
// value AT THE PIN lies in [min, max] — the value-at-t the ordered sibling
// sorts on. Needs no property index. The bounds apply inclusively whatever
// inclMin / inclMax say: the door's contract is over-selection with fn
// re-checking, and an exclusive float64 comparison could drop an int64 past
// 2^53 that rounds onto the bound. After applies by ID (ID order); Limit bounds
// the rows emitted, as on the index path.
func forEachNodeInRangeTemporal(c *Core, label, propKey string, min, max float64,
	opts storepkg.QueryOpts, fn func(*types.Node) bool) error {

	gopts := opts
	gopts.Limit = 0
	var nodes []*types.Node
	if err := c.readUnderRLock(func() error {
		var e error
		nodes, e = c.nodesByLabelLocked(label, gopts)
		return e
	}); err != nil {
		return err
	}
	emitted := 0
	for _, n := range nodes {
		v, ok := n.GetProperty(propKey)
		if !ok {
			continue
		}
		if f, ok := coerceFloat64(v); !ok || f < min || f > max {
			continue
		}
		if !fn(n) {
			return nil
		}
		emitted++
		if opts.Limit > 0 && emitted >= opts.Limit {
			return nil
		}
	}
	return nil
}

// forEachRelInRangeTemporal is the relationship mirror of
// forEachNodeInRangeTemporal, behind ForEachByTypePropertyRange.
func forEachRelInRangeTemporal(c *Core, typeName, propKey string, min, max float64,
	opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error {

	gopts := opts
	gopts.Limit = 0
	var rels []*types.Relationship
	if err := c.readUnderRLock(func() error {
		var e error
		rels, e = c.relsByTypeLocked(typeName, gopts)
		return e
	}); err != nil {
		return err
	}
	emitted := 0
	for _, r := range rels {
		v, ok := r.GetProperty(propKey)
		if !ok {
			continue
		}
		if f, ok := coerceFloat64(v); !ok || f < min || f > max {
			continue
		}
		if !fn(r) {
			return nil
		}
		emitted++
		if opts.Limit > 0 && emitted >= opts.Limit {
			return nil
		}
	}
	return nil
}
