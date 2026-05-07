package core

import "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"

// NodeInterval returns the effective [start, end) for a node.
// start is derived from the snowflake ID timestamp if not explicitly set
// via TemporalMetadata.ValidFrom.
//
// Returns types.ErrOpenInterval if ValidTo == 0 (open-ended / still valid),
// because Allen's algebra requires finite intervals.
func (c *Core) NodeInterval(n *types.Node) (start, end types.Instant, err error) {
	start = c.nodeValidFrom(n)
	tm := n.Temporal()
	if tm == nil || tm.ValidTo == 0 {
		return 0, 0, types.ErrOpenInterval
	}
	end = tm.ValidTo
	return start, end, nil
}

// RelInterval returns the effective [start, end) for a relationship.
// start is derived from the snowflake ID timestamp if not explicitly set
// via TemporalMetadata.ValidFrom.
//
// Returns types.ErrOpenInterval if ValidTo == 0 (open-ended / still valid).
func (c *Core) RelInterval(r *types.Relationship) (start, end types.Instant, err error) {
	start = c.relValidFrom(r)
	tm := r.Temporal()
	if tm == nil || tm.ValidTo == 0 {
		return 0, 0, types.ErrOpenInterval
	}
	end = tm.ValidTo
	return start, end, nil
}

// RelateNodes classifies the Allen relation of node A's interval to node B's.
// Both nodes must have finite intervals (ValidTo != 0); returns
// types.ErrOpenInterval otherwise.
func (c *Core) RelateNodes(a, b *types.Node) (types.AllenRelation, error) {
	aStart, aEnd, err := c.NodeInterval(a)
	if err != nil {
		return 0, err
	}
	bStart, bEnd, err := c.NodeInterval(b)
	if err != nil {
		return 0, err
	}
	return types.Relate(aStart, aEnd, bStart, bEnd)
}

// RelateRels classifies the Allen relation of relationship A's interval
// to relationship B's. Both must have finite intervals.
func (c *Core) RelateRels(a, b *types.Relationship) (types.AllenRelation, error) {
	aStart, aEnd, err := c.RelInterval(a)
	if err != nil {
		return 0, err
	}
	bStart, bEnd, err := c.RelInterval(b)
	if err != nil {
		return 0, err
	}
	return types.Relate(aStart, aEnd, bStart, bEnd)
}
