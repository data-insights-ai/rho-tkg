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

// NodeMutationEpoch returns the store's current node-mutation epoch, or 0 if the
// store lacks the DocValues capability. The consumer's Gate-2 staleness check.
func (n *NodeOps) NodeMutationEpoch() uint64 {
	scanner, native := n.c.store.(nodeDocValuesScanner)
	if !native {
		return 0
	}
	return scanner.NodeMutationEpoch()
}
