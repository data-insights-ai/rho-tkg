package types

import (
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// GraphID qualifies graph-local values/configuration independently of temporal
// AxisID. This value layer neither mints IDs nor proves ownership/readiness.
// Zero is invalid for a default-axis designation.
type GraphID [16]byte

var (
	// ErrInvalidGraphIdentity refuses an absent graph identity; no default graph
	// is selected implicitly.
	ErrInvalidGraphIdentity = errors.New("types: invalid graph identity")
	// ErrDefaultAxisMismatch refuses a different graph/axis designation or a
	// non-default axis policy. A graph-only mismatch carries no temporal cause.
	ErrDefaultAxisMismatch = errors.New("types: default-axis designation mismatch")
)

// The suffix versions the POSIX coordinate convention, independently of the
// AxisDescriptor format version. This defines coordinates, not a leap-aware
// elapsed metric: 0 is 1970-01-01T00:00:00Z, one unit is one POSIX millisecond.
const defaultAxisReference = "posix-unix-epoch:1970-01-01T00:00:00Z:v1"

// DefaultAxisBinding designates one explicit graph's compatibility default:
// RationalQ/millisecond on the versioned POSIX reference. Copies are immutable.
// A position still carries axis identity only; callers check the graph binding
// separately. This is pure configuration, not a persisted root, lease, grant,
// registry or replication capability. The embedding storage layer must preserve
// and validate the complete graph/axis designation when it installs one.
type DefaultAxisBinding struct {
	graph GraphID
	axis  temporal.Axis
}

// NewDefaultAxisBinding constructs an explicit graph-local default designation.
// The caller supplies graph and axis identities; neither is derived/allocated.
// Error precedence: invalid limits, zero graph, then NewAxis admission (axis ID,
// descriptor policy). Refusal returns the zero binding. The default axis remains
// Q; Instant is its finite integral int64-ms codec, not its mathematical domain.
func NewDefaultAxisBinding(graph GraphID, axisID temporal.AxisID, l temporal.Limits) (DefaultAxisBinding, error) {
	if err := l.Validate(); err != nil {
		return DefaultAxisBinding{}, err
	}
	if graph == (GraphID{}) {
		return DefaultAxisBinding{}, ErrInvalidGraphIdentity
	}
	axis, err := temporal.NewAxis(temporal.AxisDescriptor{ID: axisID, Profile: temporal.ProfileRationalQ, Version: 1, Reference: defaultAxisReference, CanonicalUnit: "millisecond"}, l)
	if err != nil {
		return DefaultAxisBinding{}, err
	}
	return DefaultAxisBinding{graph: graph, axis: axis}, nil
}

// BindDefaultAxis checks an already reconstructed complete axis before making a
// graph-local designation. It does not convert units/origins or change an axis.
// Error precedence: invalid limits, zero graph, axis structure/descriptor budget,
// unit, profile, then exact reference convention. Existing explicit Z/ms Instant
// helpers remain valid independently; Z is not this default Q designation.
func BindDefaultAxis(graph GraphID, axis temporal.Axis, l temporal.Limits) (DefaultAxisBinding, error) {
	if err := l.Validate(); err != nil {
		return DefaultAxisBinding{}, err
	}
	if graph == (GraphID{}) {
		return DefaultAxisBinding{}, ErrInvalidGraphIdentity
	}
	if err := checkDefaultAxisPolicy(axis, l); err != nil {
		return DefaultAxisBinding{}, err
	}
	return DefaultAxisBinding{graph: graph, axis: axis}, nil
}

// Graph returns the graph identity by value; zero bindings return zero.
func (b DefaultAxisBinding) Graph() GraphID { return b.graph }

// Axis returns the immutable explicit axis; zero bindings return an invalid axis.
func (b DefaultAxisBinding) Axis() temporal.Axis { return b.axis }

// Check validates both designations before checking graph and complete axis
// equality. Error precedence: invalid limits, either zero graph, receiver axis
// policy, supplied axis policy, graph mismatch, then axis identity/definition.
// It verifies a pure value predicate, never storage/ownership authority. A
// graph-only mismatch returns ErrDefaultAxisMismatch without ErrAxisMismatch.
func (b DefaultAxisBinding) Check(graph GraphID, axis temporal.Axis, l temporal.Limits) error {
	if err := l.Validate(); err != nil {
		return err
	}
	if b.graph == (GraphID{}) || graph == (GraphID{}) {
		return ErrInvalidGraphIdentity
	}
	if err := checkDefaultAxisPolicy(b.axis, l); err != nil {
		return err
	}
	if err := checkDefaultAxisPolicy(axis, l); err != nil {
		return err
	}
	if b.graph != graph {
		return ErrDefaultAxisMismatch
	}
	if b.axis.Descriptor() != axis.Descriptor() || b.axis.DefinitionHash() != axis.DefinitionHash() {
		return errors.Join(ErrDefaultAxisMismatch, temporal.ErrAxisMismatch)
	}
	return nil
}

func checkDefaultAxisPolicy(axis temporal.Axis, l temporal.Limits) error {
	if err := axis.Validate(l); err != nil {
		return err
	}
	d := axis.Descriptor()
	if d.CanonicalUnit != "millisecond" {
		return errors.Join(ErrDefaultAxisMismatch, temporal.ErrExplicitMappingRequired)
	}
	if d.Profile != temporal.ProfileRationalQ {
		return errors.Join(ErrDefaultAxisMismatch, temporal.ErrIncompatibleDomain)
	}
	if d.Reference != defaultAxisReference {
		return errors.Join(ErrDefaultAxisMismatch, temporal.ErrAxisMismatch)
	}
	return nil
}
