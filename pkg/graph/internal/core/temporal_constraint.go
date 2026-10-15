package core

import (
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	temporalpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Public temporal-constraint vocabulary lives in `pkg/graph/temporal`.
// The Graph-coupled enforcement methods (checkTemporalConstraints and
// helpers) stay in this file because they need access to *Core state
// (c.constraints, c.relValidFrom, c.nodeValidFrom).

// ConstraintSet is the type of Graph.constraints — re-exported from
// pkg/graph/temporal as a type alias to avoid extra fully-qualified
// names inside this file.
type ConstraintSet = temporalpkg.ConstraintSet

// checkTemporalConstraints validates relationship r against all configured constraints.
// Returns nil immediately if no constraints are configured (fast path).
// r must be freshly-minted with a valid snowflake ID so relValidFrom is well-defined.
func (c *Core) checkTemporalConstraints(r *types.Relationship, startNode, endNode *types.Node) error {
	if c.constraints.Len() == 0 {
		return nil // fast path: no constraints configured
	}
	return c.constraints.ForEach(func(constraint temporalpkg.TemporalConstraint) error {
		switch constraint.Kind {
		case temporalpkg.ConstraintRelWithinEndpoints:
			return c.checkRelWithinEndpoints(r, startNode, endNode)
		default:
			return fmt.Errorf("%w: %w: kind %d", temporalpkg.ErrTemporalConstraint, temporalpkg.ErrInvalidTemporalConstraint, constraint.Kind)
		}
	})
}

// checkRelWithinEndpoints enforces ConstraintRelWithinEndpoints.
func (c *Core) checkRelWithinEndpoints(r *types.Relationship, startNode, endNode *types.Node) error {
	relFrom := c.relValidFrom(r)
	if err := c.checkRelAgainstEndpoint(r, relFrom, startNode, true); err != nil {
		return err
	}
	return c.checkRelAgainstEndpoint(r, relFrom, endNode, false)
}

// checkRelAgainstEndpoint checks one endpoint's temporal bounds against the relationship.
// isStart=true means startNode; false means endNode (controls which error is returned).
func (c *Core) checkRelAgainstEndpoint(r *types.Relationship, relFrom types.Instant, node *types.Node, isStart bool) error {
	nodeFrom := c.nodeValidFrom(node)
	if relFrom < nodeFrom {
		// The endpoint is valid from where its current life starts, not only
		// from its current row: after an update the earlier versions still
		// answer NodeAt (also from -inf under UntimedUnbounded, backlog 73).
		nodeFrom = c.nodeCurrentLifeStart(node, nodeFrom)
	}

	var nodeTo types.Instant
	if tm := node.Temporal(); tm != nil {
		nodeTo = tm.ValidTo
	}

	// (1)/(2): rel must not start before the node becomes valid.
	if relFrom < nodeFrom {
		if isStart {
			return fmt.Errorf("%w: %w", temporalpkg.ErrTemporalConstraint, temporalpkg.ErrRelBeforeStartNode)
		}
		return fmt.Errorf("%w: %w", temporalpkg.ErrTemporalConstraint, temporalpkg.ErrRelBeforeEndNode)
	}

	// (3)/(4): rel must not start after the node has already expired.
	if nodeTo != 0 && relFrom >= nodeTo {
		if isStart {
			return fmt.Errorf("%w: %w", temporalpkg.ErrTemporalConstraint, temporalpkg.ErrRelAfterStartNode)
		}
		return fmt.Errorf("%w: %w", temporalpkg.ErrTemporalConstraint, temporalpkg.ErrRelAfterEndNode)
	}

	// (5)/(6): if the node has a finite ValidTo, the relationship must also
	// have a finite ValidTo that does not outlive the node.
	if nodeTo != 0 {
		var relTo types.Instant
		if tm := r.Temporal(); tm != nil {
			relTo = tm.ValidTo
		}
		if relTo == 0 || relTo > nodeTo {
			if isStart {
				return fmt.Errorf("%w: %w", temporalpkg.ErrTemporalConstraint, temporalpkg.ErrRelExceedsStartNodeValidity)
			}
			return fmt.Errorf("%w: %w", temporalpkg.ErrTemporalConstraint, temporalpkg.ErrRelExceedsEndNodeValidity)
		}
	}

	return nil
}

func (c *Core) checkNodeCloseTemporalConstraints(id types.NodeID, current *types.Node, closeAt types.Instant) error {
	if c.constraints.Len() == 0 {
		return nil
	}
	return c.constraints.ForEach(func(constraint temporalpkg.TemporalConstraint) error {
		switch constraint.Kind {
		case temporalpkg.ConstraintRelWithinEndpoints:
			return c.checkNodeCloseRelWithinEndpoints(id, current, closeAt)
		default:
			return fmt.Errorf("%w: %w: kind %d", temporalpkg.ErrTemporalConstraint, temporalpkg.ErrInvalidTemporalConstraint, constraint.Kind)
		}
	})
}

func (c *Core) checkNodeCloseRelWithinEndpoints(id types.NodeID, current *types.Node, closeAt types.Instant) error {
	constrained := nodeWithValidTo(current, closeAt)
	outRels, err := c.store.OutgoingRelationships(id, 0)
	if err != nil {
		return err
	}
	inRels, err := c.store.IncomingRelationships(id, 0)
	if err != nil {
		return err
	}
	if !c.storeRowsTrust {
		if err := c.validateOutgoingRelationshipRows(id, 0, outRels); err != nil {
			return err
		}
		if err := c.validateIncomingRelationshipRows(id, 0, inRels); err != nil {
			return err
		}
	}
	for _, r := range outRels {
		if err := c.checkRelAgainstEndpoint(r, c.relValidFrom(r), constrained, true); err != nil {
			return err
		}
	}
	for _, r := range inRels {
		if err := c.checkRelAgainstEndpoint(r, c.relValidFrom(r), constrained, false); err != nil {
			return err
		}
	}
	return nil
}

// nodeCurrentLifeStart is where node's current life starts in valid time:
// the start (storeutil.EntityValidFrom, both untimed modes) of the first
// version recorded after the last hard delete its chain holds (a re-imported
// ID's earlier life does not count; a void row claims nothing), or current,
// node's own start, when that is earlier or the chain cannot be read.
func (c *Core) nodeCurrentLifeStart(node *types.Node, current types.Instant) types.Instant {
	if node.Version() == 0 {
		return current
	}
	history, err := c.getNodeHistory(node.ID())
	if err != nil || len(history) == 0 {
		return current
	}
	var lastDelete types.Instant
	for _, h := range history {
		if tm := h.Temporal(); tm != nil && tm.DeletedAt > lastDelete {
			lastDelete = tm.DeletedAt
		}
	}
	var first *types.Node
	var firstTM *types.TemporalMetadata
	for _, h := range history {
		tm := h.Temporal()
		if tm == nil || tm.Void || (lastDelete != 0 && tm.TxFrom <= lastDelete) {
			continue
		}
		if first == nil || h.Version() < first.Version() {
			first, firstTM = h, tm
		}
	}
	if first == nil {
		return current
	}
	return min(storeutil.EntityValidFrom(first.ID().SnowflakeID(), firstTM), current)
}

func nodeWithValidTo(n *types.Node, validTo types.Instant) *types.Node {
	copy := n.DeepCopy()
	tm := copy.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		copy.SetTemporal(tm)
	}
	tm.ValidTo = validTo
	return copy
}

func relWithValidTo(r *types.Relationship, validTo types.Instant) *types.Relationship {
	copy := r.DeepCopy()
	tm := copy.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		copy.SetTemporal(tm)
	}
	tm.ValidTo = validTo
	return copy
}
