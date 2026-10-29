package core

import (
	"errors"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Valid-end pruning (VALIDENDPRUNE, sigma's paged-row probes).
//
// A current ByLabelAndProperty / ByLabelAndPropertyValues with
// QueryOpts.ValidEndAfter keeps the matches whose effective timeline, at a pin
// read after the lookup, has a segment ending at or after the bound, or an open
// one (segmentsEnd). Every store answers by reading each match's timeline; a
// store with store.PropertyIndexValidEndCapability (badger, tiered's reference
// shard) leaves out the matches a listed definition's entries bound below the
// bound, and takes back the exact end of each match whose entry did not hold it.
//
// The pin is read after the lookup: a write takes its stamp before it buffers
// its rows, so a write the store's sequence (ValidEndProbe.Seq) saw before the
// lookup is stamped at or below the pin, and one it did not see makes the store
// skip the refine.

// takeValidEndAfter clears opts' ValidEndAfter bound and returns it: the doors
// that take it validate the rest without it (validateTemporalQueryOpts refuses
// it everywhere).
func takeValidEndAfter(opts *storepkg.QueryOpts) (types.Instant, bool) {
	bound, ok := opts.ValidEndAfter, opts.HasValidEndAfter()
	opts.ValidEndAfter, opts.ValidEndAfterSet = 0, false
	return bound, ok
}

// checkValidEndAfterCurrent refuses a bound together with a time coordinate:
// the bound is decided at the current state only.
func checkValidEndAfterCurrent(opts storepkg.QueryOpts) error {
	if opts.HasTimeCoordinate() {
		return fmt.Errorf("%w: with a time filter or pin", storepkg.ErrValidEndAfterUnsupported)
	}
	return nil
}

// nodesByLabelAndPropertyFormsValidEnd is nodesByLabelAndPropertyFormsLocked
// for a current read bounded by valid end: each page of matches is filtered,
// and the next page read, until opts.Limit nodes are kept or the matches end.
// Callers must hold c.mu (R or W).
func (c *Core) nodesByLabelAndPropertyFormsValidEnd(label, key string, forms []any, opts storepkg.QueryOpts, bound types.Instant) ([]*types.Node, error) {
	tok, ok := c.labels.Lookup(label)
	if !ok || len(forms) == 0 {
		return nil, nil
	}
	keys := keysOf(forms)
	var out []*types.Node
	page := opts
	for {
		nodes, probes, err := c.validEndCandidates(tok, key, forms, keys, page, bound)
		if err != nil {
			return nil, err
		}
		pin := c.peekNow()
		for i, n := range nodes {
			segs, err := c.nodeEffectiveTimelineLocked(n.ID(), pin)
			if errors.Is(err, storepkg.ErrNodeNotFound) { // purged since the lookup
				continue
			}
			if err != nil {
				return nil, err
			}
			end := segmentsEnd(c.axis(), segs, nodeSegTo)
			if probes != nil && !probes[i].Known { // a write leaves a bound unknown: a known one is the end
				c.validEndQuery.RefineNodePropertyValidEnd(n, probes[i], end)
			}
			if end >= bound { // ValidEndOpen is the largest instant
				out = append(out, n)
				if opts.Limit > 0 && len(out) == opts.Limit {
					return out, nil
				}
			}
		}
		if opts.Limit == 0 || len(nodes) < page.Limit {
			return out, nil
		}
		page.After = types.EntityID(nodes[len(nodes)-1].ID())
	}
}

// validEndCandidates is one page of the current matches, with a probe per node
// where the store prunes (nil probes: every match read).
func (c *Core) validEndCandidates(tok uint16, key string, forms []any, keys equalKeys, opts storepkg.QueryOpts, bound types.Instant) ([]*types.Node, []storepkg.ValidEndProbe, error) {
	if c.validEndQuery != nil {
		opts.ValidEndAfter, opts.ValidEndAfterSet = bound, true
		return c.validEndQuery.NodesByLabelAndPropertyKeysValidEnd(tok, key, keys, opts)
	}
	nodes, err := c.nodesByLabelAndPropertyFormKeys(tok, key, forms, keys, opts)
	return nodes, nil, err
}
