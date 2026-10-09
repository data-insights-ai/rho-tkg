// Package state implements immutable, pure single-component region replacement.
// It stores compact handles to separately owned payloads. It has no graph,
// revision allocator, transaction visibility, history store or certified cuts.
package state

import "github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"

type valueKind uint8

const (
	valueInvalid valueKind = iota
	valueNull
	valuePayload
)

// ValueRef is an immutable graph/store-local opaque uint64 payload handle.
// The embedding engine qualifies its namespace and owns payload storage/lifetime.
// PayloadBytes is a conservative supplied size, not verified payload data. A
// future storage boundary must derive it from stored data, not trust user claims.
// This reducer never dereferences or copies referenced payloads.
type ValueRef struct {
	kind             valueKind
	id, payloadBytes uint64
}

// NewValueRef constructs a non-null payload handle, including zero-byte payloads.
func NewValueRef(id, payloadBytes uint64) (ValueRef, error) {
	if id == 0 {
		return ValueRef{}, ErrInvalidValueRef
	}
	if payloadBytes > hardReferenceBytes {
		return ValueRef{}, ErrResourceLimit
	}
	return ValueRef{kind: valuePayload, id: id, payloadBytes: payloadBytes}, nil
}

// Null constructs canonical present-null value metadata, without a payload.
func Null() ValueRef { return ValueRef{kind: valueNull} }

// ID returns the engine-qualified local handle, zero for null/invalid values.
func (v ValueRef) ID() uint64 { return v.id }

// PayloadBytes returns the conservative caller/engine-supplied size.
func (v ValueRef) PayloadBytes() uint64 { return v.payloadBytes }

// IsNull distinguishes canonical null from a payload handle or absence.
func (v ValueRef) IsNull() bool { return v.kind == valueNull }
func (v ValueRef) validate() error {
	switch v.kind {
	case valuePayload:
		if v.id == 0 {
			return ErrInvalidValueRef
		}
		if v.payloadBytes > hardReferenceBytes {
			return ErrResourceLimit
		}
	case valueNull:
		if v.id != 0 || v.payloadBytes != 0 {
			return ErrInvalidValueRef
		}
	default:
		return ErrInvalidValueRef
	}
	return nil
}

// Revision holds caller-supplied store-local revision/provenance references.
// IDs have no clock, global order or certified-cut meaning. Zero provenance
// means no provenance reference was supplied; a revision ID must be nonzero.
type Revision struct{ id, provenance uint64 }

// NewRevision validates supplied revision identity without allocating one.
func NewRevision(id, provenance uint64) (Revision, error) {
	if id == 0 {
		return Revision{}, ErrInvalidRevision
	}
	return Revision{id: id, provenance: provenance}, nil
}

// ID returns the supplied revision identity.
func (r Revision) ID() uint64 { return r.id }

// Provenance returns its optional store-local provenance reference.
func (r Revision) Provenance() uint64 { return r.provenance }

// Cell distinguishes present-null, present payload, explicit retraction and
// never-asserted absence. A zero Cell is never-asserted absent; explicit Unset
// cells have Present=false and retain their supplied revision/provenance.
type Cell struct {
	present  bool
	value    ValueRef
	revision Revision
}

// Present reports whether a value was asserted, including null.
func (c Cell) Present() bool { return c.present }

// Value returns a payload/null handle only when Present is true.
func (c Cell) Value() ValueRef { return c.value }

// Revision returns retraction/set provenance, or zero for never-asserted absence.
func (c Cell) Revision() Revision { return c.revision }
func (c Cell) references() uint64 {
	if c.present {
		return c.value.payloadBytes
	}
	return 0
}

// Piece is one atomic scope with full immutable cell/revision identity.
type Piece struct {
	scope      temporal.Scope
	cell       Cell
	scopeBytes int
}

// Scope returns the immutable atomic placement.
func (p Piece) Scope() temporal.Scope { return p.scope }

// Cell returns its immutable assertion or explicit retraction.
func (p Piece) Cell() Cell { return p.cell }

// Change is one exact changed region and its before/after cell metadata.
// Equal values with a different revision still produce a change. Gap before
// images are zero absent cells; both images retain supplied revision identities.
type Change struct {
	scope         temporal.Scope
	before, after Cell
	scopeBytes    int
}

// Scope returns this atomic changed region.
func (c Change) Scope() temporal.Scope { return c.scope }

// Before returns the prior value/retraction or never-asserted absence.
func (c Change) Before() Cell { return c.before }

// After returns the supplied new value or explicit retraction.
func (c Change) After() Cell { return c.after }

// Result owns bounded next-state and change metadata. Its embedding engine
// decides whether/how to commit them atomically and budgets the total result.
type Result struct {
	state       State
	changes     []Change
	changeUsage Usage
}

// State returns the immutable resulting component state.
func (r Result) State() State { return r.state }

// Changes returns a defensive copy of before/after changed-region metadata.
func (r Result) Changes() []Change { return cloneChanges(r.changes) }

// ChangeUsage returns its conservative change-output accounting.
func (r Result) ChangeUsage() Usage { return r.changeUsage }
