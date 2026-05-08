// Package constraints is a sub-API accessor for temporal-constraint
// management. The constraint types live in pkg/graph/temporal.
package constraints

import (
	temporalpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/temporal"
)

// Ops is the subset of *core.ConstraintOps the constraints sub-API forwards to.
type Ops interface {
	Set(cs temporalpkg.ConstraintSet)
	Add(c temporalpkg.TemporalConstraint)
	Get() temporalpkg.ConstraintSet
}

// API is the constraints sub-API accessor.
type API struct{ ops Ops }

// New constructs a constraints sub-API.
func New(ops Ops) *API { return &API{ops: ops} }

// Set replaces the entire constraint set.
func (a *API) Set(cs temporalpkg.ConstraintSet) { a.ops.Set(cs) }

// Add appends a single constraint.
func (a *API) Add(c temporalpkg.TemporalConstraint) { a.ops.Add(c) }

// Get returns the current constraint set.
func (a *API) Get() temporalpkg.ConstraintSet { return a.ops.Get() }
