package constraints

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
)

// UniqueScope selects which slice of the bitemporal history a unique
// property constraint quantifies over. Only UniqueCurrent is implemented in
// this release; the other values reserve their enum slots so the API does not
// break when they land. Creating a constraint with an unimplemented scope
// returns a clear not-yet-supported error.
type UniqueScope uint8

const (
	// UniqueCurrent (default) forbids two CURRENT nodes carrying the same
	// value for the constrained (label, property). History may hold
	// duplicates; a value freed by supersession or delete is immediately
	// reusable. This matches Neo4j uniqueness semantics.
	UniqueCurrent UniqueScope = iota
	// UniqueForever (value ownership) — reserved, not yet implemented.
	UniqueForever
	// UniqueValidOverlap (temporal uniqueness) — reserved, not yet implemented.
	UniqueValidOverlap
)

// String renders a UniqueScope for diagnostics.
func (s UniqueScope) String() string {
	switch s {
	case UniqueCurrent:
		return "current"
	case UniqueForever:
		return "forever"
	case UniqueValidOverlap:
		return "valid-overlap"
	default:
		return "unknown"
	}
}

// UniqueConstraint describes a registered unique property constraint. Returned
// by UniqueConstraints for introspection.
type UniqueConstraint struct {
	// Label is the node label the constraint binds to. A node is constrained
	// when it carries this label (including via a later AddLabel).
	Label string
	// PropertyKey is the property whose value must be unique across current
	// nodes carrying Label.
	PropertyKey string
	// Scope is the uniqueness scope (v1: always UniqueCurrent).
	Scope UniqueScope
}

// UniqueOps is the subset of *core.ConstraintOps the unique-constraint methods
// forward to. Kept separate from Ops so the temporal-constraint surface stays
// unchanged; the concrete *core.ConstraintOps satisfies both.
type UniqueOps interface {
	CreateUnique(ctx context.Context, label, propertyKey string) error
	CreateUniqueForever(ctx context.Context, label, propertyKey string) error
	ReleaseOwnership(ctx context.Context, label, propertyKey string, value any) error
	DropUnique(ctx context.Context, label, propertyKey string) error
	UniqueConstraints() []UniqueConstraint
}

// CreateUnique registers a unique property constraint on (label, propertyKey)
// with the default UniqueCurrent scope. It validates existing data first: if
// two current nodes already carry the same value the constraint is NOT
// installed and ErrUniqueViolationExisting is returned. Float-typed values are
// rejected with ErrUniqueUnsupportedType. Auto-ensures a property index on
// (label, propertyKey). Requires a backend with MetaKV; rejected on a
// read-only replica.
//
// Enforcement currently covers the STANDALONE node doors (Add / AddWithTx /
// AddByIDIfAbsent / Update / UpdateInPlace / CompareAndSetProperty / AddLabel).
func (a *API) CreateUnique(ctx context.Context, label, propertyKey string) error {
	ops, err := a.uniqueReady()
	if err != nil {
		return err
	}
	return ops.CreateUnique(ctx, label, propertyKey)
}

// CreateUniqueForever registers a UniqueForever value-ownership constraint on
// (label, propertyKey). The first entity to hold a value owns it permanently;
// every other node is barred from that value forever, across supersession, hard
// delete, and reopen. Same install/validation semantics as CreateUnique (rejects
// existing current duplicates with ErrUniqueViolationExisting; floats with
// ErrUniqueUnsupportedType), then seeds ownership from existing current values.
func (a *API) CreateUniqueForever(ctx context.Context, label, propertyKey string) error {
	ops, err := a.uniqueReady()
	if err != nil {
		return err
	}
	return ops.CreateUniqueForever(ctx, label, propertyKey)
}

// ReleaseOwnership removes a UniqueForever ownership claim for value on
// (label, propertyKey), so an operator-corrected value may be reclaimed by a
// different entity. Idempotent. Returns ErrUniqueConstraintNotFound if no
// UniqueForever constraint exists on the pair; ErrUniqueUnsupportedType if value
// is not an indexable non-float scalar.
func (a *API) ReleaseOwnership(ctx context.Context, label, propertyKey string, value any) error {
	ops, err := a.uniqueReady()
	if err != nil {
		return err
	}
	return ops.ReleaseOwnership(ctx, label, propertyKey, value)
}

// DropUnique removes a unique property constraint. Leaves any property index in
// place. Returns ErrUniqueConstraintNotFound if none exists.
func (a *API) DropUnique(ctx context.Context, label, propertyKey string) error {
	ops, err := a.uniqueReady()
	if err != nil {
		return err
	}
	return ops.DropUnique(ctx, label, propertyKey)
}

// UniqueConstraints returns a snapshot of the registered unique constraints.
func (a *API) UniqueConstraints() []UniqueConstraint {
	if a == nil || !a.ok {
		return nil
	}
	uo, ok := a.ops.(UniqueOps)
	if !ok {
		return nil
	}
	return uo.UniqueConstraints()
}

func (a *API) uniqueReady() (UniqueOps, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	uo, ok := ops.(UniqueOps)
	if !ok {
		return nil, grapherr.ErrNilGraph
	}
	return uo, nil
}
