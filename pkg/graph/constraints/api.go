// Package constraints is a sub-API accessor for temporal-constraint
// management. The constraint types live in pkg/graph/temporal.
package constraints

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/grapherr"
	temporalpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/temporal"
)

// Ops is the subset of *core.ConstraintOps the constraints sub-API forwards to.
type Ops interface {
	Set(cs temporalpkg.ConstraintSet) error
	Add(c temporalpkg.TemporalConstraint) error
	Get() temporalpkg.ConstraintSet
}

// API is the constraints sub-API accessor.
type API struct {
	ops Ops
	ok  bool
}

// New constructs a constraints sub-API.
func New(ops Ops) *API { return &API{ops: ops, ok: !grapherr.IsNil(ops)} }

func (a *API) ready() (Ops, error) {
	if a == nil || !a.ok {
		return nil, grapherr.ErrNilGraph
	}
	return a.ops, nil
}

// Set replaces the entire constraint set.
func (a *API) Set(cs temporalpkg.ConstraintSet) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.Set(cs)
}

// Add appends a single constraint.
func (a *API) Add(c temporalpkg.TemporalConstraint) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.Add(c)
}

// Get returns the current constraint set.
func (a *API) Get() temporalpkg.ConstraintSet {
	if a == nil || !a.ok {
		return temporalpkg.ConstraintSet{}
	}
	return a.ops.Get()
}
