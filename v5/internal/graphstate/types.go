// Package graphstate plans bounded graph-component changes over a supplied
// immutable read view. It owns no database, snapshots, ID allocator or clocks.
package graphstate

import (
	"context"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Planner errors distinguish invalid input, constraints and incomplete readers.
var (
	ErrNamespace         = errors.New("graphstate: graph namespace mismatch")
	ErrInvalidInput      = errors.New("graphstate: invalid input")
	ErrNilView           = errors.New("graphstate: nil view")
	ErrInvalidView       = errors.New("graphstate: invalid or changing read view")
	ErrIncompleteRead    = errors.New("graphstate: incomplete read")
	ErrContradictoryRead = errors.New("graphstate: overlapping or contradictory read")
	ErrResourceLimit     = errors.New("graphstate: resource limit exceeded")
	ErrValidityRequired  = errors.New("graphstate: explicit placement required")
	ErrEmptyMutation     = errors.New("graphstate: empty mutation placement")
	ErrUnsupported       = errors.New("graphstate: unsupported capability")
	ErrNotFound          = errors.New("graphstate: not found")
	ErrAlreadyExists     = errors.New("graphstate: identity already exists")
	ErrOwnerValidity     = errors.New("graphstate: owner life does not cover placement")
	ErrLifecycleOverlap  = errors.New("graphstate: life overlaps another life")
	ErrSchemaMismatch    = errors.New("graphstate: unknown or invalid property schema")
	ErrTypeMismatch      = errors.New("graphstate: property type/cardinality/owner mismatch")
	ErrUniqueOverlap     = errors.New("graphstate: effective uniqueness overlap")
)

// GraphID qualifies every local handle in one read view and output delta.
type GraphID [16]byte

// ViewID is supplied immutable-view identity, never a certified-cut constructor.
type ViewID [16]byte

// EntityID identifies an object in the supplied graph namespace.
type EntityID uint64

// LifeID is qualified by its owner and supplied by the embedding allocator.
type LifeID uint64

// ValueID identifies a canonical typed value in this graph's value dictionary.
type ValueID uint64

// ReadVersion is an opaque per-record/predicate conflict-validation token.
type ReadVersion uint64

// Cursor is an opaque continuation handle; zero is start/end, not a time.
type Cursor uint64

// EntityKind identifies immutable node/relationship records.
type EntityKind uint8

// Entity kinds remain independent of temporal placement.
const (
	Node EntityKind = iota + 1
	Relationship
)

// ReferenceMode selects life-bound state or stable-identity reference semantics.
type ReferenceMode uint8

// Relationship reference modes never silently rebind to a reopened endpoint.
const (
	LifeBound ReferenceMode = iota + 1
	IdentityReference
)

// Visibility explicitly selects declared or endpoint-effective graph reads.
type Visibility uint8

// Visibility modes use the same supplied immutable read view.
const (
	Declared Visibility = iota + 1
	Effective
)

// ComponentKind identifies one independent temporal component.
type ComponentKind uint8

// Presence has Life=0; labels and properties are life-qualified.
const (
	Presence ComponentKind = iota + 1
	Label
	ScalarProperty
	SetMember
)

// ComponentKey is comparable and contains no graph-wide implicit allocation.
type ComponentKey struct {
	Owner  EntityID
	Life   LifeID
	Kind   ComponentKind
	Name   string
	Member ValueID
}

// EntityRecord retains immutable identity/endpoints, native axis and optional
// supplied interpretation/role. Lifecycle changes do not revise declarations.
type EntityRecord struct {
	ID             EntityID
	Kind           EntityKind
	Axis           temporal.Axis
	Type           string
	Source, Target EntityID
	Mode           ReferenceMode
	Interpretation Interpretation
	TemporalRole   TemporalRole
}

// LifeRecord retains endpoint bindings even after all its presence is closed.
type LifeRecord struct {
	Owner                  EntityID
	Life                   LifeID
	SourceLife, TargetLife LifeID
}

// Cardinality declares scalar replacement or independent set memberships.
type Cardinality uint8

// Cardinalities are schema contracts, not dynamically inferred from values.
const (
	ScalarCardinality Cardinality = iota + 1
	SetCardinality
)

// UniqueMode distinguishes scalar-value from member-wise uniqueness.
type UniqueMode uint8

// Whole-set uniqueness is outside this slice and unknown modes decline.
const (
	UniqueNone UniqueMode = iota
	UniqueScalar
	UniqueMembers
)

// PropertyDefinition is read and versioned through the supplied schema view.
type PropertyDefinition struct {
	Name        string
	Owner       EntityKind
	Type        ScalarKind
	Cardinality Cardinality
	Unique      UniqueMode
}

// EntityRead identifies the immutable view and opaque local record version.
type EntityRead struct {
	View    ViewID
	Version ReadVersion
	Found   bool
	Record  EntityRecord
}

// LifeRead returns immutable metadata, including checked absence.
type LifeRead struct {
	View    ViewID
	Version ReadVersion
	Found   bool
	Record  LifeRecord
}

// PropertyRead returns one owner-kind-qualified definition without a fallback.
type PropertyRead struct {
	View    ViewID
	Version ReadVersion
	Found   bool
	Record  PropertyDefinition
}

// ValueRead binds exact typed dictionary content to a canonical local handle.
type ValueRead struct {
	View    ViewID
	Version ReadVersion
	Found   bool
	ID      ValueID
	Value   Scalar
}

// ReadBudget is a per-page bound; the planner also charges aggregate reads.
type ReadBudget struct{ Rows, Bytes int }

// ComponentQuery requests owned replacement coverage and read-only merge context.
type ComponentQuery struct {
	Key          ComponentKey
	Window       temporal.Scope
	MergeContext bool
}

// ComponentPage owns disjoint coverage within the requested window. Data pieces
// lie in Owned. MergeContext lies outside Owned and never produces writes/CDC.
type ComponentPage struct {
	View         ViewID
	Version      ReadVersion
	Owned        temporal.Scope
	Data         state.State
	MergeContext []state.Piece
	Next         Cursor
	Complete     bool
}

// KeyPredicate is an exact component-prefix dependency, including empty results.
type KeyPredicate struct {
	Owner EntityID
	Life  LifeID
	Kind  ComponentKind
	Name  string
}

// KeyPage returns bounded component keys and complete continuation metadata.
type KeyPage struct {
	View     ViewID
	Version  ReadVersion
	Keys     []ComponentKey
	Next     Cursor
	Complete bool
}

// UniquePredicate requests a complete candidate superset for one typed value.
type UniquePredicate struct {
	Definition PropertyDefinition
	Value      Scalar
	Window     temporal.Scope
}

// UniqueClaim identifies a candidate whose final effective support is rechecked.
type UniqueClaim struct {
	Owner EntityID
	Life  LifeID
	Key   ComponentKey
}

// ClaimPage must not omit candidates or label partial coverage complete.
type ClaimPage struct {
	View     ViewID
	Version  ReadVersion
	Claims   []UniqueClaim
	Next     Cursor
	Complete bool
}

// IncidentPredicate includes absent endpoint-posting ranges in the footprint.
type IncidentPredicate struct {
	Endpoint EntityID
	Life     LifeID
	Window   temporal.Scope
}

// EntityPage is a bounded incident-relationship candidate page.
type EntityPage struct {
	View     ViewID
	Version  ReadVersion
	Entities []EntityID
	Next     Cursor
	Complete bool
}

// ReadView must bind all records/pages to Identity and Graph. The embedding
// storage provides immutable snapshot meaning and verifies dependencies at
// commit; matching local counters alone never establish a database cut.
// Interface dispatch occurs per bounded read/page, not per fact value.
type ReadView interface {
	Graph() GraphID
	Identity() ViewID
	Entity(context.Context, EntityID) (EntityRead, error)
	Life(context.Context, EntityID, LifeID) (LifeRead, error)
	Property(context.Context, EntityKind, string) (PropertyRead, error)
	Value(context.Context, ValueID) (ValueRead, error)
	ValueIdentity(context.Context, Scalar) (ValueRead, error)
	ComponentPage(context.Context, ComponentQuery, Cursor, ReadBudget) (ComponentPage, error)
	ComponentKeys(context.Context, KeyPredicate, Cursor, ReadBudget) (KeyPage, error)
	UniqueCandidates(context.Context, UniquePredicate, Cursor, ReadBudget) (ClaimPage, error)
	IncidentRelationships(context.Context, IncidentPredicate, Cursor, ReadBudget) (EntityPage, error)
}

// OperationKind names semantic graph mutations, with no automatic time stamping.
type OperationKind uint8

// Graph operation kinds share component replacement and declared-life checks.
const (
	CreateNode OperationKind = iota + 1
	CreateRelationship
	Reopen
	Close
	Correct
	AddLabel
	RemoveLabel
	Set
	Unset
	Add
	Remove
)

// Operation is a concrete typed union. Create/reopen supply a fresh Life; ValueID
// is a supplied fresh dictionary handle used only if ValueIdentity is absent.
type Operation struct {
	Kind    OperationKind
	Owner   EntityID
	Life    LifeID
	Scope   temporal.Scope
	Record  EntityRecord
	Binding LifeRecord
	Name    string
	Value   Scalar
	ValueID ValueID
	Present bool
}

// DependencyKind names keys/ranges/predicates that storage must validate/lock.
type DependencyKind uint8

// Dependency kinds include negative reads and predicate phantoms.
const (
	EntityDependency DependencyKind = iota + 1
	LifeDependency
	SchemaDependency
	ValueDependency
	ValueIdentityDependency
	ComponentDependency
	PrefixDependency
	UniquenessDependency
	IncidentDependency
)

// Dependency records the full requested predicate plus source/view/version.
type Dependency struct {
	Kind      DependencyKind
	View      ViewID
	Version   ReadVersion
	Absent    bool
	Owner     EntityID
	OwnerKind EntityKind // SchemaDependency qualifies Name, including checked absence.
	Life      LifeID
	Name      string
	ValueID   ValueID
	Value     Scalar
	Key       ComponentKey
	Window    temporal.Scope
	Prefix    KeyPredicate
	Unique    UniquePredicate
	Incident  IncidentPredicate
}

// ComponentPatch replaces only Owned, retaining complete exact before/after CDC.
type ComponentPatch struct {
	Key     ComponentKey
	Owned   temporal.Scope
	State   state.State
	Changes []state.Change
}

// ValueWrite proposes typed canonical dictionary content under supplied identity.
type ValueWrite struct {
	ID    ValueID
	Value Scalar
}

// Delta is bounded transaction-local metadata; storage installs it atomically.
// Previous views/history are not copied or mutated by production planning.
type Delta struct {
	Graph        GraphID
	View         ViewID
	Entities     []EntityRecord
	Lives        []LifeRecord
	Values       []ValueWrite
	Patches      []ComponentPatch
	Dependencies []Dependency
}

// EndpointStatus distinguishes known active lifecycle from an unmapped axis.
type EndpointStatus struct {
	ID              EntityID
	Known, Active   bool
	Life, BoundLife LifeID
}

// PropertyValue projects a scalar or set sorted by canonical typed key bytes.
// This is deterministic enumeration, not numeric or temporal precedence.
type PropertyValue struct {
	Name        string
	Cardinality Cardinality
	Scalar      Scalar
	Members     []Scalar
}

// Projection exposes stable identity separately from active/effective visibility.
type Projection struct {
	Record         EntityRecord
	Exists, Active bool
	Life           LifeID
	Labels         []string
	Properties     []PropertyValue
	Endpoints      [2]EndpointStatus
	Dependencies   []Dependency
}
