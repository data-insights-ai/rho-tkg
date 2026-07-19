// Package constraints is a sub-API accessor for temporal-constraint
// management. The constraint types live in pkg/graph/temporal.
package constraints

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	temporalpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
)

// Ops is the subset of *core.ConstraintOps the constraints sub-API forwards to.
type Ops interface {
	Set(cs temporalpkg.ConstraintSet) error
	Add(c temporalpkg.TemporalConstraint) error
	Get() temporalpkg.ConstraintSet
}

// DryRunOps is the dry-run validation surface of *core.ConstraintOps, type-
// asserted from Ops (mirroring UniqueOps) so the base Ops interface stays minimal.
type DryRunOps interface {
	DryRunValidate(ctx context.Context, facts DryRunFacts) ([]DryRunViolation, error)
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

// DryRunValidate validates a proposed fact set against the configured unique +
// temporal constraints and returns every violation WITHOUT asserting anything —
// no writes, no ID mint, no events, no LSN burn, and no permanent UniqueForever
// claim (unlike the Tx+rollback emulation, which cannot release a forever claim
// it made). An empty result means the fact set would be accepted under the
// current committed state.
func (a *API) DryRunValidate(ctx context.Context, facts DryRunFacts) ([]DryRunViolation, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	do, ok := ops.(DryRunOps)
	if !ok {
		// The underlying Ops implementation doesn't support dry-run validation —
		// a capability gap, not a nil/unwired graph (BACKLOG 8c: ErrNilGraph was
		// the wrong sentinel here; a.ready() already returned it for the actual
		// nil-graph case above).
		return nil, storepkg.ErrCapabilityNotSupported
	}
	return do.DryRunValidate(ctx, facts)
}
