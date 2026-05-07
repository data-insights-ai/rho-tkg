// Package constraintsapi is a sub-API accessor for temporal-constraint
// management. The constraint types live in pkg/graph/temporal.
package constraintsapi

import (
	temporalpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/temporal"
)

// Core is the subset of *graph.Graph methods the constraints sub-API forwards to.
type Core interface {
	SetTemporalConstraints(cs temporalpkg.ConstraintSet)
	AddTemporalConstraint(c temporalpkg.TemporalConstraint)
	TemporalConstraints() temporalpkg.ConstraintSet
}

// API is the constraints sub-API accessor.
type API struct{ c Core }

// New constructs a constraints sub-API.
func New(c Core) *API { return &API{c: c} }

// Set replaces the entire constraint set. Forwards to Graph.SetTemporalConstraints.
func (a *API) Set(cs temporalpkg.ConstraintSet) { a.c.SetTemporalConstraints(cs) }

// Add appends a single constraint. Forwards to Graph.AddTemporalConstraint.
func (a *API) Add(c temporalpkg.TemporalConstraint) { a.c.AddTemporalConstraint(c) }

// Get returns the current constraint set. Forwards to Graph.TemporalConstraints.
func (a *API) Get() temporalpkg.ConstraintSet { return a.c.TemporalConstraints() }
