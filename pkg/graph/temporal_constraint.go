package graph

import (
	"errors"
	"fmt"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// TemporalConstraintKind identifies the type of temporal constraint enforced at write time.
type TemporalConstraintKind uint8

const (
	// ConstraintRelWithinEndpoints enforces that a relationship's validity window
	// is contained within the validity intervals of both endpoint nodes.
	//
	// Checks at write time (r = freshly-minted relationship):
	//   (1) relFrom >= startNode.effectiveValidFrom
	//   (2) relFrom >= endNode.effectiveValidFrom
	//   (3) if startNode.ValidTo != 0: relFrom < startNode.ValidTo
	//   (4) if endNode.ValidTo   != 0: relFrom < endNode.ValidTo
	//   (5) if r.Temporal().ValidTo != 0 AND startNode.ValidTo != 0:
	//           r.ValidTo <= startNode.ValidTo
	//   (6) same for endNode
	ConstraintRelWithinEndpoints TemporalConstraintKind = iota + 1
)

// TemporalConstraint is a single temporal invariant checked at write time.
type TemporalConstraint struct {
	Kind TemporalConstraintKind
}

// ConstraintSet is an immutable-by-convention ordered set of TemporalConstraints.
// The zero value is valid (empty — no constraints).
// Add returns a new copy; the original is never modified.
type ConstraintSet struct {
	items []TemporalConstraint
}

// NewConstraintSet creates a ConstraintSet from the given constraints.
func NewConstraintSet(cs ...TemporalConstraint) ConstraintSet {
	items := make([]TemporalConstraint, len(cs))
	copy(items, cs)
	return ConstraintSet{items: items}
}

// Add returns a new ConstraintSet with c appended. The receiver is not modified.
func (cs ConstraintSet) Add(c TemporalConstraint) ConstraintSet {
	items := make([]TemporalConstraint, len(cs.items)+1)
	copy(items, cs.items)
	items[len(cs.items)] = c
	return ConstraintSet{items: items}
}

// Len returns the number of constraints in the set.
func (cs ConstraintSet) Len() int {
	return len(cs.items)
}

// Items returns a defensive copy of the constraint slice.
// Returns nil if the set is empty.
func (cs ConstraintSet) Items() []TemporalConstraint {
	if len(cs.items) == 0 {
		return nil
	}
	out := make([]TemporalConstraint, len(cs.items))
	copy(out, cs.items)
	return out
}

// Sentinel errors for temporal constraint violations.
// All are wrapped with ErrTemporalConstraint so callers can use errors.Is on either.
var (
	ErrTemporalConstraint          = errors.New("graph: temporal constraint violated")
	ErrRelBeforeStartNode          = errors.New("graph: relationship starts before start node is valid")
	ErrRelBeforeEndNode            = errors.New("graph: relationship starts before end node is valid")
	ErrRelAfterStartNode           = errors.New("graph: relationship starts after start node has expired")
	ErrRelAfterEndNode             = errors.New("graph: relationship starts after end node has expired")
	ErrRelExceedsStartNodeValidity = errors.New("graph: relationship validity exceeds start node validity")
	ErrRelExceedsEndNodeValidity   = errors.New("graph: relationship validity exceeds end node validity")
)

// checkTemporalConstraints validates relationship r against all configured constraints.
// Returns nil immediately if no constraints are configured (fast path).
// r must be freshly-minted with a valid snowflake ID so relValidFrom is well-defined.
func (g *Graph) checkTemporalConstraints(r *types.Relationship, startNode, endNode *types.Node) error {
	if g.constraints.Len() == 0 {
		return nil // fast path: no constraints configured
	}
	for _, c := range g.constraints.Items() {
		switch c.Kind {
		case ConstraintRelWithinEndpoints:
			if err := g.checkRelWithinEndpoints(r, startNode, endNode); err != nil {
				return err
			}
		}
	}
	return nil
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
			return fmt.Errorf("%w: %w", ErrTemporalConstraint, ErrRelBeforeStartNode)
		}
		return fmt.Errorf("%w: %w", ErrTemporalConstraint, ErrRelBeforeEndNode)
	}

	// (3)/(4): rel must not start after the node has already expired.
	if nodeTo != 0 && relFrom >= nodeTo {
		if isStart {
			return fmt.Errorf("%w: %w", ErrTemporalConstraint, ErrRelAfterStartNode)
		}
		return fmt.Errorf("%w: %w", ErrTemporalConstraint, ErrRelAfterEndNode)
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
				return fmt.Errorf("%w: %w", ErrTemporalConstraint, ErrRelExceedsStartNodeValidity)
			}
			return fmt.Errorf("%w: %w", ErrTemporalConstraint, ErrRelExceedsEndNodeValidity)
		}
	}

	return nil
}
