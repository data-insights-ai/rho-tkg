// Package assertion supplies immutable temporal associations with supplied
// graph-qualified identities. It owns no graph, history store, allocator, cut,
// transaction or solver. Transactional graph attachment remains a store concern.
package assertion

import (
	"cmp"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Errors distinguish malformed associations, immediate-chain conflicts and bounds.
var (
	ErrInvalid              = errors.New("assertion: invalid association")
	ErrNamespace            = errors.New("assertion: graph mismatch")
	ErrRebinding            = errors.New("assertion: identity or target rebinding")
	ErrPredecessor          = errors.New("assertion: invalid immediate predecessor")
	ErrRevisionReuse        = errors.New("assertion: current revision reused with different content")
	ErrKnowledgeAssociation = errors.New("assertion: knowledge requires a compatible native placement")
	ErrResourceLimit        = errors.New("assertion: resource limit")
	ErrInvalidEncoding      = errors.New("assertion: invalid canonical encoding")
	ErrUnknownVersion       = errors.New("assertion: unknown codec version")
)

// ID is supplied stable assertion identity, with no clock or partition bits.
type ID uint64

// Ref qualifies an assertion independently of physical ownership and routing.
type Ref struct {
	Graph graphstate.GraphID
	ID    ID
}

// TargetKind distinguishes graph identity from one independent component.
type TargetKind uint8

// Exactly one target field is active; target existence is not checked here.
const (
	EntityTarget TargetKind = iota + 1
	ComponentTarget
)

// Target is qualified by Ref.Graph. Inactive fields must be zero.
type Target struct {
	Kind      TargetKind
	Entity    graphstate.EntityID
	Component graphstate.ComponentKey
}

// Interpretation is supplied assertion meaning, independent of support shape.
type Interpretation uint8

// Interpretations neither run a solver nor establish state persistence.
const (
	Occurrence Interpretation = iota + 1
	State
	Observation
	Constraint
	Derived
)

// TemporalRole names the meaning of an asserted placement association.
type TemporalRole string

// Common roles are conveniences, not an evaluator registry or closed vocabulary.
const (
	Validity         TemporalRole = "validity"
	OccurrenceTime   TemporalRole = "occurrence_time"
	SourceOccurrence TemporalRole = "source_occurrence"
	ObservationTime  TemporalRole = "observation_time"
	SourceReceipt    TemporalRole = "source_receipt"
	CausalOrder      TemporalRole = "causal_order"
)

// PlacementKind separates absent association, native scope and symbolic bytes.
type PlacementKind uint8

// Unplaced is an axis-qualified native scope, never NoAssociation.
const (
	NoAssociation PlacementKind = iota + 1
	NativePlacement
	SymbolicPlacement
)

// Placement is a strict constructor-input union. Symbolic is preservation only;
// its descriptor is not occupied native support. Inactive fields must be zero.
type Placement struct {
	Kind     PlacementKind
	Native   temporal.Scope
	Symbolic temporal.OpaqueDescriptor
}

// KnowledgeKind separates absent evidence from typed point-placement evidence.
type KnowledgeKind uint8

// PointKnowledge is allowed only with matching native placement axis/definition.
const (
	NoKnowledge KnowledgeKind = iota + 1
	PointKnowledge
)

// Knowledge is independent of occupied scope. Inactive Point must be zero.
type Knowledge struct {
	Kind  KnowledgeKind
	Point temporal.PointKnowledge
}

// Spec supplies one complete association revision. Revision IDs/provenance are
// graph-local supplied handles, not clocks, commit order or certified cuts.
// Retracted revisions preserve their body as retained evidence.
type Spec struct {
	Ref            Ref
	Target         Target
	Revision       state.Revision
	Previous       uint64
	Interpretation Interpretation
	Role           TemporalRole
	Placement      Placement
	Knowledge      Knowledge
	Retracted      bool
}

// Record owns an immutable association. Copying safely shares immutable values.
// Ordinary native/axisless records allocate no symbolic or knowledge object.
// This is a prototype value, not the compact persisted per-fact row layout.
type Record struct {
	ref            Ref
	target         Target
	revision       state.Revision
	previous       uint64
	interpretation Interpretation
	role           TemporalRole
	placement      PlacementKind
	native         temporal.Scope
	symbolic       *temporal.OpaqueDescriptor
	knowledge      *temporal.PointKnowledge
	retracted      bool
}

// Transition retains complete immediate before/after associations. It does not
// install history or produce a certified transaction/change-feed envelope.
type Transition struct {
	before, after Record
	hasBefore     bool
	replay        bool
}

// Limits bounds each record independently. MaxRecordBytes counts its canonical
// envelope plus one complete native axis definition, not Go heap/RSS. Zero uses
// defaults; hard ceilings are 1 MiB/record and 64 KiB/role. Temporal child caps
// remain independent and are narrowed to MaxRecordBytes before child encoding.
// Apply retains at most two bounded records; their combined ledger is <= 2R,
// where R=MaxRecordBytes. Its canonical/child byte scratch is conservatively
// bounded by 6R, plus bounded child arithmetic scratch and fixed headers. This
// finite factor is not a claim that both records fit one per-record budget.
type Limits struct {
	Temporal       temporal.Limits
	MaxRoleBytes   int
	MaxRecordBytes int
}

// DefaultLimits returns provisional per-record policy, not capacity acceptance.
func DefaultLimits() Limits {
	return Limits{Temporal: temporal.DefaultLimits(), MaxRoleBytes: 256, MaxRecordBytes: 128 << 10}
}

// Validate checks all caps before encoding, decoding or owning input.
func (l Limits) Validate() error { _, err := l.resolve(); return err }

func (l Limits) resolve() (Limits, error) {
	if err := l.Temporal.Validate(); err != nil {
		return Limits{}, err
	}
	d := DefaultLimits()
	l.MaxRoleBytes = cmp.Or(l.MaxRoleBytes, d.MaxRoleBytes)
	l.MaxRecordBytes = cmp.Or(l.MaxRecordBytes, d.MaxRecordBytes)
	if l.MaxRoleBytes < 1 || l.MaxRoleBytes > 64<<10 || l.MaxRecordBytes < 1 || l.MaxRecordBytes > 1<<20 {
		return Limits{}, ErrInvalid
	}
	l.Temporal.MaxInputBytes = min(cmp.Or(l.Temporal.MaxInputBytes, d.Temporal.MaxInputBytes), l.MaxRecordBytes)
	l.Temporal.MaxValueBytes = min(cmp.Or(l.Temporal.MaxValueBytes, d.Temporal.MaxValueBytes), l.MaxRecordBytes)
	l.Temporal.MaxDescriptorBytes = min(cmp.Or(l.Temporal.MaxDescriptorBytes, d.Temporal.MaxDescriptorBytes), l.MaxRecordBytes)
	return l, nil
}
