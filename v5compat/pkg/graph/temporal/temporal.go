// Package temporal mirrors the value types of v4 pkg/graph/temporal that the
// paged effective doors take and return.
package temporal

import "github.com/data-insights-ai/rho-tkg/v5compat/pkg/types"

// Pin is the temporal coordinate of the paged effective doors: Tx is required;
// at most one of ValidAt or the ValidStart/ValidEnd interval (0 = open).
type Pin struct {
	Tx         types.Instant
	ValidAt    types.Instant
	ValidStart types.Instant
	ValidEnd   types.Instant
}

// Filter is the property equality of the paged effective doors.
type Filter struct {
	Prop string
	Eq   any
}

// NodeSegment is one effective valid-time segment of a node.
type NodeSegment struct {
	ValidFrom, ValidTo types.Instant
	Node               *types.Node
}

// RelSegment is one effective valid-time segment of a relationship.
type RelSegment struct {
	ValidFrom, ValidTo types.Instant
	Rel                *types.Relationship
}

// ConstraintSet and TemporalConstraint are placeholders for the v4 temporal
// constraint types (the constraints sub-API is not in the first batch).
type (
	ConstraintSet      struct{}
	TemporalConstraint struct{}
)
