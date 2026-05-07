package graph

import (
	"fmt"

	temporalpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/temporal"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Public temporal-constraint vocabulary lives in `pkg/graph/temporal`.
// The Graph-coupled enforcement methods (checkTemporalConstraints and
// helpers) stay in this file because they need access to *Graph state
// (g.constraints, g.relValidFrom, g.nodeValidFrom).

// ConstraintSet is the type of Graph.constraints — re-exported from
// pkg/graph/temporal as a type alias to avoid extra fully-qualified
// names inside this file.
type ConstraintSet = temporalpkg.ConstraintSet

// checkTemporalConstraints validates relationship r against all configured constraints.
// Returns nil immediately if no constraints are configured (fast path).
// r must be freshly-minted with a valid snowflake ID so relValidFrom is well-defined.
func (g *Graph) checkTemporalConstraints(r *types.Relationship, startNode, endNode *types.Node) error {
	if g.constraints.Len() == 0 {
		return nil // fast path: no constraints configured
	}
	return g.constraints.ForEach(func(c temporalpkg.TemporalConstraint) error {
		switch c.Kind {
		case temporalpkg.ConstraintRelWithinEndpoints:
			return g.checkRelWithinEndpoints(r, startNode, endNode)
		}
		return nil
	})
}

// checkRelWithinEndpoints enforces ConstraintRelWithinEndpoints.
func (g *Graph) checkRelWithinEndpoints(r *types.Relationship, startNode, endNode *types.Node) error {
	relFrom := g.relValidFrom(r)
	if err := g.checkRelAgainstEndpoint(r, relFrom, startNode, true); err != nil {
		return err
	}
	return g.checkRelAgainstEndpoint(r, relFrom, endNode, false)
}

// checkRelAgainstEndpoint checks one endpoint's temporal bounds against the relationship.
// isStart=true means startNode; false means endNode (controls which error is returned).
func (g *Graph) checkRelAgainstEndpoint(r *types.Relationship, relFrom types.Instant, node *types.Node, isStart bool) error {
	nodeFrom := g.nodeValidFrom(node)

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

	// (5)/(6): if rel has an explicit ValidTo and node has a finite ValidTo,
	// the rel must not outlive the node.
	if nodeTo != 0 {
		var relTo types.Instant
		if tm := r.Temporal(); tm != nil {
			relTo = tm.ValidTo
		}
		if relTo != 0 && relTo > nodeTo {
			if isStart {
				return fmt.Errorf("%w: %w", temporalpkg.ErrTemporalConstraint, temporalpkg.ErrRelExceedsStartNodeValidity)
			}
			return fmt.Errorf("%w: %w", temporalpkg.ErrTemporalConstraint, temporalpkg.ErrRelExceedsEndNodeValidity)
		}
	}

	return nil
}
