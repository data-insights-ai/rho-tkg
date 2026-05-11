// Package temporal contains the public temporal types: snapshots and
// diffs (Graph.Snapshot/Graph.DiffSnapshots return types) plus the
// constraint vocabulary (ConstraintSet, TemporalConstraint and friends)
// that callers configure on a Graph to enforce write-time invariants
// between relationships and their endpoints.
//
// The Graph-coupled enforcement code (checkTemporalConstraints) lives on
// *Graph in pkg/graph; the symbols here are pure data.
package temporal

import "errors"

// Sentinel errors for temporal constraint violations.
// All are wrapped with ErrTemporalConstraint so callers can use errors.Is on either.
var (
	ErrTemporalConstraint          = errors.New("graph: temporal constraint violated")
	ErrInvalidTemporalConstraint   = errors.New("graph: invalid temporal constraint")
	ErrRelBeforeStartNode          = errors.New("graph: relationship starts before start node is valid")
	ErrRelBeforeEndNode            = errors.New("graph: relationship starts before end node is valid")
	ErrRelAfterStartNode           = errors.New("graph: relationship starts after start node has expired")
	ErrRelAfterEndNode             = errors.New("graph: relationship starts after end node has expired")
	ErrRelExceedsStartNodeValidity = errors.New("graph: relationship validity exceeds start node validity")
	ErrRelExceedsEndNodeValidity   = errors.New("graph: relationship validity exceeds end node validity")
)
