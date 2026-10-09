package graphstore

import (
	"cmp"

	"github.com/data-insights-ai/rho-tkg/v5/internal/assertion"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// AssociationWrite supplies an independently revisioned association. PrimaryFor
// is an explicit optional named binding, never inferred from the first target
// posting. Zero means no binding request; populated partial refs are invalid.
type AssociationWrite struct {
	Spec       assertion.Spec
	PrimaryFor EntityRef
}

// AssociationRead distinguishes checked absence from a retained retraction.
type AssociationRead struct {
	Found  bool
	Record assertion.Record
}

// PrimaryRead retains named metadata even when its association is retracted or
// its target's life is closed. It does not change declared/effective visibility.
type PrimaryRead struct {
	Bound       bool
	Ref         assertion.Ref
	Association AssociationRead
}

// PrimaryBindingChange makes binding creation visible even on association replay.
// The binding itself is immutable; Before is zero for a newly bound entity.
type PrimaryBindingChange struct {
	Entity        EntityRef
	Before, After assertion.Ref
}

// AssociationResult is a bounded staged result, not a receipt or feed envelope.
// A composed graph command must discard its whole Stage on any rejection and
// co-commit these changes with graph changes under the actual base-root guard.
type AssociationResult struct {
	Transitions []assertion.Transition
	Bindings    []PrimaryBindingChange
}

// AssociationQuery explicitly qualifies its target. Zero interpretation/placement
// selects all. NativeWindow requires NativePlacement: it means exact overlap of
// supplied native scope, not possible/definite knowledge or symbolic evaluation.
type AssociationQuery struct {
	Graph          graphstate.GraphID
	Target         assertion.Target
	Interpretation assertion.Interpretation
	Placement      assertion.PlacementKind
	NativeWindow   temporal.Scope
}

// AssociationPage owns bounded records and continuation. Visited counts all
// catalog/scan reads including filtered and missing rows, not only returned facts.
// Continuations bind this exact query, namespace and immutable application root;
// they are not certified cuts, leases or asserted temporal enumeration order.
type AssociationPage struct {
	Records  []assertion.Record
	Next     []byte
	Complete bool
	Visited  int
}

// AssociationLimits bounds aggregate read/output/operation work independently
// of each Record and existing shared Catalog staging caps. Ledgers count encoded
// keys/records, owned capacities, materialized axes/values and fixed headers;
// they are not physical Go heap/RSS measurements. Zero selects defaults.
type AssociationLimits struct {
	Record         assertion.Limits
	MaxOperations  int
	MaxReadRows    int
	MaxReadBytes   int
	MaxOutputBytes int
}

// DefaultAssociationLimits returns provisional local association policy.
func DefaultAssociationLimits() AssociationLimits {
	return AssociationLimits{assertion.DefaultLimits(), 128, 512, 4 << 20, 4 << 20}
}

// Validate checks independent limits before any lookup or staging allocation.
func (l AssociationLimits) Validate() error { _, err := l.resolve(); return err }

func (l AssociationLimits) resolve() (AssociationLimits, error) {
	if err := l.Record.Validate(); err != nil {
		return AssociationLimits{}, err
	}
	d := DefaultAssociationLimits()
	l.MaxOperations = cmp.Or(l.MaxOperations, d.MaxOperations)
	l.MaxReadRows = cmp.Or(l.MaxReadRows, d.MaxReadRows)
	l.MaxReadBytes = cmp.Or(l.MaxReadBytes, d.MaxReadBytes)
	l.MaxOutputBytes = cmp.Or(l.MaxOutputBytes, d.MaxOutputBytes)
	if l.MaxOperations < 1 || l.MaxOperations > 4096 || l.MaxReadRows < 1 || l.MaxReadRows > 65536 || l.MaxReadBytes < 1 || l.MaxReadBytes > 64<<20 || l.MaxOutputBytes < 1 || l.MaxOutputBytes > 64<<20 {
		return AssociationLimits{}, ErrInvalid
	}
	return l, nil
}

func (l AssociationLimits) bounded(c Limits) AssociationLimits {
	l.MaxReadRows = min(l.MaxReadRows, c.MaxReadRows)
	l.MaxReadBytes = min(l.MaxReadBytes, c.MaxReadBytes)
	l.MaxOutputBytes = min(l.MaxOutputBytes, c.MaxReadBytes)
	d := assertion.DefaultLimits()
	l.Record.MaxRoleBytes = min(cmp.Or(l.Record.MaxRoleBytes, d.MaxRoleBytes), c.MaxNameBytes)
	l.Record.MaxRecordBytes = min(cmp.Or(l.Record.MaxRecordBytes, d.MaxRecordBytes), c.MaxRecordBytes-49)
	t, td := l.Record.Temporal, temporal.DefaultLimits()
	t.MaxInputBytes = min(cmp.Or(t.MaxInputBytes, td.MaxInputBytes), c.Temporal.MaxInputBytes)
	t.MaxValueBytes = min(cmp.Or(t.MaxValueBytes, td.MaxValueBytes), c.Temporal.MaxValueBytes)
	t.MaxDescriptorBytes = min(cmp.Or(t.MaxDescriptorBytes, td.MaxDescriptorBytes), c.Temporal.MaxDescriptorBytes)
	t.MaxMagnitudeBits = min(cmp.Or(t.MaxMagnitudeBits, td.MaxMagnitudeBits), c.Temporal.MaxMagnitudeBits)
	t.MaxRegionPieces = min(cmp.Or(t.MaxRegionPieces, td.MaxRegionPieces), c.Temporal.MaxRegionPieces)
	l.Record.Temporal = t
	return l
}
