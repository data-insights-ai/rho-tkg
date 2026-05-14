package core

import (
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// NodeInterval returns the effective [start, end) for a node.
// start is derived from the snowflake ID timestamp if not explicitly set
// via TemporalMetadata.ValidFrom.
//
// NodeInterval types.ErrOpenInterval if ValidTo == 0 (open-ended / still valid),
// because Allen's algebra requires finite intervals.
func (t *TempOps) NodeInterval(n *types.Node) (start, end types.Instant, err error) {
	c := t.c
	err = c.readUnderRLock(func() error {
		if n == nil {
			return ErrNilNode
		}
		if err := storepkg.ValidateNodeID(n.ID()); err != nil {
			return err
		}
		start = c.nodeValidFrom(n)
		tm := n.Temporal()
		if tm == nil || tm.ValidTo == 0 {
			return types.ErrOpenInterval
		}
		end = tm.ValidTo
		if start >= end {
			return types.ErrInvalidInterval
		}
		return nil
	})
	return start, end, err
}

// RelInterval returns the effective [start, end) for a relationship.
// start is derived from the snowflake ID timestamp if not explicitly set
// via TemporalMetadata.ValidFrom.
//
// RelInterval types.ErrOpenInterval if ValidTo == 0 (open-ended / still valid).
func (t *TempOps) RelInterval(r *types.Relationship) (start, end types.Instant, err error) {
	c := t.c
	err = c.readUnderRLock(func() error {
		if r == nil {
			return ErrNilRelationship
		}
		if err := storepkg.ValidateRelID(r.ID()); err != nil {
			return err
		}
		start = c.relValidFrom(r)
		tm := r.Temporal()
		if tm == nil || tm.ValidTo == 0 {
			return types.ErrOpenInterval
		}
		end = tm.ValidTo
		if start >= end {
			return types.ErrInvalidInterval
		}
		return nil
	})
	return start, end, err
}

// RelateNodes classifies the Allen relation of node A's interval to node B's.
// Both nodes must have finite intervals (ValidTo != 0); returns
// types.ErrOpenInterval otherwise.
func (t *TempOps) RelateNodes(a, b *types.Node) (types.AllenRelation, error) {
	aStart, aEnd, err := t.NodeInterval(a)
	if err != nil {
		return 0, err
	}
	bStart, bEnd, err := t.NodeInterval(b)
	if err != nil {
		return 0, err
	}
	return types.Relate(aStart, aEnd, bStart, bEnd)
}

// RelateRels classifies the Allen relation of relationship A's interval
// to relationship B's. Both must have finite intervals.
func (t *TempOps) RelateRels(a, b *types.Relationship) (types.AllenRelation, error) {
	aStart, aEnd, err := t.RelInterval(a)
	if err != nil {
		return 0, err
	}
	bStart, bEnd, err := t.RelInterval(b)
	if err != nil {
		return 0, err
	}
	return types.Relate(aStart, aEnd, bStart, bEnd)
}
