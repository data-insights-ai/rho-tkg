// Package store mirrors v4 pkg/graph/store for the names consumers import:
// QueryOpts and the store sentinels. The backends (memory, sharded, tiered,
// badger) are declined (ErrNotSupportedInV5) except memory.New, which selects the
// in-memory graph.
package store

import (
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/types"
)

// ErrInvalidStoreMutation reports a malformed write (empty name, nil entity).
var ErrInvalidStoreMutation = errors.New("store: invalid store mutation")

// ErrNotSupportedInV5 is the class of every declined v5compat door (see
// graph.ErrNotSupportedInV5, the same value).
var ErrNotSupportedInV5 = errors.New("graph: not supported in v5compat")

// ErrTxDone reports use of a finished transaction.
var ErrTxDone = errors.New("store: transaction already finished")

// Store marks a v4 backend handed to graph.Config.Store. Only memory.New
// satisfies it in v5compat.
type Store interface{ IsV5CompatMemory() bool }

// QueryOpts controls pagination and temporal filtering of the list doors.
// Zero values mean "return all".
type QueryOpts struct {
	Limit int            // max results; 0 = no limit
	After types.EntityID // IDs strictly above After; 0 = from the start

	// ValidAt takes precedence over ValidStart/ValidEnd. ValidEnd 0 = open.
	ValidAt    types.Instant
	ValidStart types.Instant
	ValidEnd   types.Instant

	// TxAt: versions recorded by that transaction time, with the implicit
	// valid-at-now filter (v4 semantics). TxPin: the belief state at that time,
	// no valid-time filtering; exclusive with every valid-time option and TxAt.
	TxAt  types.Instant
	TxPin types.Instant
}

// DistanceMetric determines how similarity is measured between vectors.
type DistanceMetric uint8

// Distance metrics for vector indexes (declined: the vector index).
const (
	DistanceCosine DistanceMetric = iota + 1
	DistanceEuclidean
)

// Placeholders for the v4 statistics, index-option and change-feed types so the
// declined sub-APIs keep their v4 signatures. They carry no data.
type (
	PropertyTypeClassCounts struct{}
	RelTypeDegreeStats      struct{}
	ReadCosts               struct{}
	HistoryCounts           struct{}
	PropertyStats           struct{}
	PropertyNDV             struct{}
	PropertyBounds          struct{}
	MemberTimeStats         struct{}
	VectorIndexOptions      struct{}
	VectorIndexInfo         struct{}
	PropertyIndexOptions    struct{}
	ChangeRecord            struct{}
	RegistrySnapshot        struct{}
	IDSlotLeaseRecord       struct{}
)

// Unique-constraint sentinels (the v4 names, re-exported by graph).
var (
	// ErrUniqueViolation reports a write that would give two nodes one claim.
	ErrUniqueViolation = errors.New("graph: unique constraint violation")
	// ErrUniqueViolationExisting reports a CreateUnique over existing duplicates.
	ErrUniqueViolationExisting = errors.New("graph: unique constraint violated by existing data")
	// ErrUniqueConstraintExists reports a CreateUnique of a declared constraint.
	ErrUniqueConstraintExists = errors.New("graph: unique constraint already exists")
)
