package core

import (
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
// not a uniformly numeric/string column (critique Trap B → consumer declines the
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

// NodeMutationEpoch returns the store's current node-mutation epoch, or 0 if the
// store lacks the DocValues capability. The consumer's Gate-2 staleness check.
func (n *NodeOps) NodeMutationEpoch() uint64 {
	scanner, native := n.c.store.(nodeDocValuesScanner)
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
