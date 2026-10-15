package core

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// nodeEqualCounter is the OPTIONAL store capability behind
// NodeOps.CountByLabelPropertyValue (memory, badger, sharded).
type nodeEqualCounter interface {
	NodeEqualCount(labelToken uint16, propKey string, value any) (int64, bool, error)
}

// relEqualCounter is the relationship mirror of nodeEqualCounter.
type relEqualCounter interface {
	RelEqualCount(relTypeToken uint16, propKey string, value any) (int64, bool, error)
}

// CountByLabelPropertyValue counts the label's nodes whose propKey value
// equals value as Cypher equality keeps it (3 matches 3 and 3.0 of every
// stored width, no string/number crossing, nil and NaN count 0), from the
// property index's value sets: the size of a set, no node read. exact=false
// with no error is the decline (the caller counts otherwise): no property
// index on (label, propKey), the store lacks the capability (tiered and
// external stores), badger with PropertyIndexOnDisk, an index still being
// built, an Elements index, a number of magnitude 2^53 or more, a temporal
// value, a list or map, an unknown label, or a temporal QueryOpts (the index
// holds current values only). A planner learns per (label, key) whether
// exact counts are available from exact.
func (n *NodeOps) CountByLabelPropertyValue(label, propKey string, value any, opts storepkg.QueryOpts) (int64, bool, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return 0, false, err
	}
	if temporalOpts(opts) {
		return 0, false, nil
	}
	counter, native := c.store.(nodeEqualCounter)
	if !native {
		return 0, false, nil
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.lookupLabelLocked(label)
		return nil
	}); err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, nil
	}
	return counter.NodeEqualCount(tok, propKey, value)
}

// CountByTypePropertyValue is NodeOps.CountByLabelPropertyValue for a
// relationship type's property index.
func (r *RelOps) CountByTypePropertyValue(typeName, propKey string, value any, opts storepkg.QueryOpts) (int64, bool, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return 0, false, err
	}
	if temporalOpts(opts) {
		return 0, false, nil
	}
	counter, native := c.store.(relEqualCounter)
	if !native {
		return 0, false, nil
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.lookupRelTypeQueryToken(typeName)
		return nil
	}); err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, nil
	}
	return counter.RelEqualCount(tok, propKey, value)
}

// temporalOpts reports whether opts carries any temporal coordinate (a valid
// time, a valid-time window or a transaction pin), the set the index-backed
// counts decline, as RangeCardinality does.
func temporalOpts(opts storepkg.QueryOpts) bool {
	return opts.ValidAt != 0 || opts.ValidStart != 0 || opts.ValidEnd != 0 || opts.TxAt != 0 || opts.TxPin != 0
}
