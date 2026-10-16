// Package constraints mirrors the v4 import path pkg/graph/constraints: unique and temporal constraints. Every method keeps its
// v4 signature and reports ErrNotSupportedInV5 (declined or not in the first
// batch of WP-11); the types are placeholders so consumers compile.
package constraints

import (
	"context"
	"fmt"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/temporal"
)

// ErrNotSupportedInV5 is the class of every declined door.
var ErrNotSupportedInV5 = store.ErrNotSupportedInV5

func declined(door string) error { return fmt.Errorf("%w: "+door, ErrNotSupportedInV5) }

// Unique-constraint sentinels (the v4 names).
var (
	ErrUniqueViolationExisting = store.ErrUniqueViolationExisting
	ErrUniqueConstraintExists  = store.ErrUniqueConstraintExists
)

// Source installs the runtime unique declaration of the compat graph.
type Source interface {
	CreateUnique(ctx context.Context, label, propertyKey string) error
}

// API is the sub-API handle returned by the graph accessor.
type API struct{ src Source }

// New builds the sub-API over the graph's unique declaration.
func New(src Source) *API { return &API{src: src} }

// DryRunFacts is a placeholder for the v4 type of the same name.
type DryRunFacts struct{}

// DryRunViolation is a placeholder for the v4 type of the same name.
type DryRunViolation struct{}

// UniqueConstraint is a placeholder for the v4 type of the same name.
type UniqueConstraint struct{}

// Set is declined.
func (a *API) Set(cs temporal.ConstraintSet) error {
	return declined("constraints.Set")
}

// Add is declined.
func (a *API) Add(c temporal.TemporalConstraint) error {
	return declined("constraints.Add")
}

// Get is declined.
func (a *API) Get() (r0 temporal.ConstraintSet) {
	return
}

// DryRunValidate is declined.
func (a *API) DryRunValidate(ctx context.Context, facts DryRunFacts) (r0 []DryRunViolation, err error) {
	err = declined("constraints.DryRunValidate")
	return
}

// CreateUnique makes propertyKey unique (v5 Graph.DeclareUnique): over all
// nodes, whatever their label, for the int, string and bool values of the key.
// Existing duplicates refuse it (ErrUniqueViolationExisting), a second call is
// ErrUniqueConstraintExists, and a write of a held value is ErrUniqueViolation.
// The label is not part of the claim (v5 uniqueness is per key).
func (a *API) CreateUnique(ctx context.Context, label, propertyKey string) error {
	if a.src == nil {
		return declined("constraints.CreateUnique")
	}
	return a.src.CreateUnique(ctx, label, propertyKey)
}

// CreateUniqueForever is declined.
func (a *API) CreateUniqueForever(ctx context.Context, label, propertyKey string) error {
	return declined("constraints.CreateUniqueForever")
}

// ReleaseOwnership is declined.
func (a *API) ReleaseOwnership(ctx context.Context, label, propertyKey string, value any) error {
	return declined("constraints.ReleaseOwnership")
}

// DropUnique is declined.
func (a *API) DropUnique(ctx context.Context, label, propertyKey string) error {
	return declined("constraints.DropUnique")
}

// UniqueConstraints is declined.
func (a *API) UniqueConstraints() (r0 []UniqueConstraint) {
	return
}
