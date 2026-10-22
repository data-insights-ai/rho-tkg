package core

import (
	"context"
	"errors"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Label and type scans restricted to an ordinal bitset (sigma-tkgd round 8
// request 4, a dynamic filter's ID set): the rows ForEachByLabel /
// ForEachByType hand out, kept only for the entities whose current ordinal is
// in the set. The set filters before rows are read where the store can tell
// ordinals without a row (store.OrdinalLookupCapability) and lists a label's
// members (store.MemberIDsCapability): current rows are then lent for the
// selected IDs only, and at a temporal coordinate only the selected
// candidates are resolved. Otherwise the scan reads every row and drops the
// ones outside the set.

// ordinalScanChunk is how many selected entities are resolved per hold of
// the graph read lock at a temporal coordinate.
const ordinalScanChunk = labelScanAtChunk

// ordinalsReady reports ErrCapabilityNotSupported for a store that assigns
// no ordinals (tiered, badger with DisableOrdinals).
func (c *Core) ordinalsReady() error {
	if o, ok := c.store.(storepkg.OrdinalCapability); ok && o.HasOrdinals() {
		return nil
	}
	return fmt.Errorf("%w: ordinals", storepkg.ErrCapabilityNotSupported)
}

// ForEachByLabelOrdinals streams the nodes ForEachByLabel(label, opts) would
// stream whose current ordinal is in set (bit k of set[k/64] selects ordinal
// k; ordinal 0 never), in ascending ID order; fn returning false stops; After
// skips IDs at or below it, Limit bounds the rows handed out. A node without
// a current row has no ordinal and is never selected, at a pin too. Same
// isolation and frozen-row contract as ForEachByLabel. Errors: those of
// ForEachByLabel, ErrCapabilityNotSupported on a store without ordinals.
func (n *NodeOps) ForEachByLabelOrdinals(label string, set []uint64, opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateIndexLabel(label); err != nil {
		return err
	}
	if err := c.validateTemporalQueryOptsScan(opts, scopeNodes); err != nil {
		return err
	}
	if err := c.ordinalsReady(); err != nil {
		return err
	}
	lookup, hasLookup := c.store.(storepkg.OrdinalLookupCapability)
	hasLookup = hasLookup && c.storeRowsTrust
	limit := opts.Limit
	emitted := 0
	emit := func(nd *types.Node) bool {
		if !fn(nd) {
			return false
		}
		emitted++
		return limit <= 0 || emitted < limit
	}
	if hasTemporalFilter(opts) {
		return c.forEachNodeOrdinalsAt(label, set, opts, lookup, hasLookup, emit)
	}
	members := c.memberIDsAt(opts.Depth)
	if !hasLookup || members == nil {
		scan := opts
		scan.Limit = 0
		return n.ForEachByLabel(label, scan, func(nd *types.Node) bool {
			if !storepkg.OrdinalSetHas(set, nd.Ordinal()) {
				return true
			}
			return emit(nd)
		})
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.labels.Lookup(label)
		return nil
	}); err != nil || !ok {
		return err
	}
	ids, err := members.NodeIDsByLabel(tok)
	if err != nil {
		return err
	}
	ids = skipAfter(sortedCandidates(ids, nil), opts.After)
	ords := make([]uint32, len(ids))
	if err := lookup.NodeOrdinalsOf(ids, ords); err != nil {
		return err
	}
	keep := ids[:0:0]
	for i, id := range ids {
		if storepkg.OrdinalSetHas(set, ords[i]) {
			keep = append(keep, id)
		}
	}
	return n.LendByIDs(context.Background(), keep, func(_ int, nd *types.Node) bool {
		// The row is read after the lookup: keep it only if it is still a
		// member with a selected ordinal.
		if !nd.HasLabelTokenRaw(tok) || !storepkg.OrdinalSetHas(set, nd.Ordinal()) {
			return true
		}
		return emit(nd)
	})
}

// forEachNodeOrdinalsAt is ForEachByLabelOrdinals at a temporal coordinate:
// the label read's candidates kept by their current ordinal, then resolved
// as the label read resolves them, ordinalScanChunk per hold of the read
// lock, handed to emit with no lock held.
func (c *Core) forEachNodeOrdinalsAt(label string, set []uint64, opts storepkg.QueryOpts,
	lookup storepkg.OrdinalLookupCapability, hasLookup bool, emit func(*types.Node) bool) error {
	var scan *nodeLabelScanAt
	var keep []types.NodeID
	if err := c.readUnderRLock(func() error {
		tok, ok := c.labels.Lookup(label)
		if !ok {
			return nil
		}
		ids, err := c.nodeLabelCandidateIDsAt(tok, opts)
		if err != nil {
			return err
		}
		ids = skipAfter(ids, opts.After)
		ords := make([]uint32, len(ids))
		if hasLookup {
			if err := lookup.NodeOrdinalsOf(ids, ords); err != nil {
				return err
			}
		} else {
			for i, id := range ids {
				cur, err := c.getCurrentNode(id)
				if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
					return err
				}
				if cur != nil {
					ords[i] = cur.Ordinal()
				}
			}
		}
		for i, id := range ids {
			if storepkg.OrdinalSetHas(set, ords[i]) {
				keep = append(keep, id)
			}
		}
		scan = c.newNodeLabelScanAt(tok, nil, opts)
		return nil
	}); err != nil || scan == nil {
		return err
	}
	buf := make([]*types.Node, 0, min(ordinalScanChunk, len(keep)))
	for start := 0; start < len(keep); start += ordinalScanChunk {
		chunk := keep[start:min(start+ordinalScanChunk, len(keep))]
		buf = buf[:0]
		if err := c.readUnderRLock(func() error {
			for _, id := range chunk {
				nd, err := scan.resolve(id)
				if err != nil {
					return err
				}
				if nd != nil {
					buf = append(buf, nd)
				}
			}
			return nil
		}); err != nil {
			return err
		}
		for _, nd := range buf {
			if !emit(nd) {
				return nil
			}
		}
	}
	return nil
}

// ForEachByTypeOrdinals is ForEachByLabelOrdinals for a relationship type
// (ForEachByType's rows; a relationship of a declared segment type on memory
// has no ordinal and is never selected).
func (r *RelOps) ForEachByTypeOrdinals(typeName string, set []uint64, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return err
	}
	if err := c.validateTemporalQueryOptsScan(opts, c.relTypeRetentionScope(typeName)); err != nil {
		return err
	}
	if err := c.ordinalsReady(); err != nil {
		return err
	}
	lookup, hasLookup := c.store.(storepkg.OrdinalLookupCapability)
	hasLookup = hasLookup && c.storeRowsTrust
	limit := opts.Limit
	emitted := 0
	emit := func(rl *types.Relationship) bool {
		if !fn(rl) {
			return false
		}
		emitted++
		return limit <= 0 || emitted < limit
	}
	if hasTemporalFilter(opts) {
		return c.forEachRelOrdinalsAt(typeName, set, opts, lookup, hasLookup, emit)
	}
	members := c.memberIDsAt(opts.Depth)
	if !hasLookup || members == nil {
		scan := opts
		scan.Limit = 0
		return r.ForEachByType(typeName, scan, func(rl *types.Relationship) bool {
			if !storepkg.OrdinalSetHas(set, rl.Ordinal()) {
				return true
			}
			return emit(rl)
		})
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.lookupRelTypeQueryToken(typeName)
		return nil
	}); err != nil || !ok {
		return err
	}
	ids, err := members.RelIDsByType(tok)
	if err != nil {
		return err
	}
	ids = skipAfter(sortedCandidates(ids, nil), opts.After)
	ords := make([]uint32, len(ids))
	if err := lookup.RelOrdinalsOf(ids, ords); err != nil {
		return err
	}
	keep := ids[:0:0]
	for i, id := range ids {
		if storepkg.OrdinalSetHas(set, ords[i]) {
			keep = append(keep, id)
		}
	}
	return r.LendByIDs(context.Background(), keep, func(_ int, rl *types.Relationship) bool {
		if !rl.HasTypeTokenRaw(tok) || !storepkg.OrdinalSetHas(set, rl.Ordinal()) {
			return true
		}
		return emit(rl)
	})
}

// forEachRelOrdinalsAt is forEachNodeOrdinalsAt for a relationship type.
func (c *Core) forEachRelOrdinalsAt(typeName string, set []uint64, opts storepkg.QueryOpts,
	lookup storepkg.OrdinalLookupCapability, hasLookup bool, emit func(*types.Relationship) bool) error {
	var scan *relTypeScanAt
	var keep []types.RelID
	if err := c.readUnderRLock(func() error {
		tok, ok := c.lookupRelTypeQueryToken(typeName)
		if !ok {
			return nil
		}
		ids, err := c.relTypeCandidateIDsAt(tok, opts)
		if err != nil {
			return err
		}
		ids = skipAfter(ids, opts.After)
		ords := make([]uint32, len(ids))
		if hasLookup {
			if err := lookup.RelOrdinalsOf(ids, ords); err != nil {
				return err
			}
		} else {
			for i, id := range ids {
				cur, err := c.getCurrentRelationship(id)
				if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
					return err
				}
				if cur != nil {
					ords[i] = cur.Ordinal()
				}
			}
		}
		for i, id := range ids {
			if storepkg.OrdinalSetHas(set, ords[i]) {
				keep = append(keep, id)
			}
		}
		scan = c.newRelTypeScanAt(tok, nil, opts)
		return nil
	}); err != nil || scan == nil {
		return err
	}
	buf := make([]*types.Relationship, 0, min(ordinalScanChunk, len(keep)))
	for start := 0; start < len(keep); start += ordinalScanChunk {
		chunk := keep[start:min(start+ordinalScanChunk, len(keep))]
		buf = buf[:0]
		if err := c.readUnderRLock(func() error {
			for _, id := range chunk {
				rl, err := scan.resolve(id)
				if err != nil {
					return err
				}
				if rl != nil {
					buf = append(buf, rl)
				}
			}
			return nil
		}); err != nil {
			return err
		}
		for _, rl := range buf {
			if !emit(rl) {
				return nil
			}
		}
	}
	return nil
}
