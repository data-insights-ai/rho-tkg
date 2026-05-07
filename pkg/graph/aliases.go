package graph

import (
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/badgerstore"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/events"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/integrity"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/memorystore"
	snowflakepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/snowflake"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/store"
	temporalpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/temporal"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/tieredstore"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// This file aliases the Store-contract types and helpers, plus selected
// in-memory index types, back into the pkg/graph namespace. The
// implementations live in pkg/graph/internal/store and
// pkg/graph/internal/index after the structural restructure; these
// aliases preserve the public API surface (`graph.Store`,
// `graph.QueryOpts`, the sentinel errors callers compare against,
// `graph.OntologyMapping`, `graph.EntityClass`, etc.) and let internal
// pkg/graph code keep using the unqualified names that existed before
// the move.
//
// No new symbols are added here. Anything not aliased was already
// either internal to pkg/graph (and stays so) or already exported via a
// different mechanism.

// --- Persistence-contract types ---

type (
	// Store is the persistence contract for the graph layer.
	Store = store.Store
	// QueryOpts controls pagination and temporal filtering.
	QueryOpts = store.QueryOpts
	// ShardDepth controls which shard tiers are included in merge queries.
	ShardDepth = store.ShardDepth
	// RelTombstone packages a relationship's tombstone data.
	RelTombstone = store.RelTombstone
	// DistanceMetric determines how similarity is measured between two vectors.
	DistanceMetric = store.DistanceMetric
	// IDComponents holds the decomposed fields of a snowflake ID.
	IDComponents = snowflakepkg.IDComponents
)

// --- ShardDepth constants ---

const (
	DepthAll  = store.DepthAll  // All tiers (default).
	DepthHot  = store.DepthHot  // Hot shard only.
	DepthWarm = store.DepthWarm // Hot + warm shards.
)

// --- DistanceMetric constants ---

const (
	DistanceCosine    = store.DistanceCosine
	DistanceEuclidean = store.DistanceEuclidean
)

// --- Store sentinel errors ---

var (
	ErrNodeNotFound          = store.ErrNodeNotFound
	ErrRelNotFound           = store.ErrRelNotFound
	ErrNodeExists            = store.ErrNodeExists
	ErrRelExists             = store.ErrRelExists
	ErrVersionNotFound       = store.ErrVersionNotFound
	ErrNoVersionValidAt      = store.ErrNoVersionValidAt
	ErrIndexExists           = store.ErrIndexExists
	ErrIndexNotFound         = store.ErrIndexNotFound
	ErrTemporalIndexExists   = store.ErrTemporalIndexExists
	ErrTemporalIndexNotFound = store.ErrTemporalIndexNotFound
	ErrTxDone                = store.ErrTxDone
	ErrStoreClosed           = store.ErrStoreClosed
)

// --- Snowflake helpers ---

// DecomposeID extracts the creation time, node ID, and sequence number
// from a snowflake ID. Re-exported from pkg/graph/internal/snowflake.
func DecomposeID(id snowflake.ID) IDComponents {
	return snowflakepkg.DecomposeID(id)
}

// snowflakeEpoch and snowflakeLayout remain available to legacy
// pkg/graph code that referenced them as package-level identifiers.
// Both now forward to the canonical definition in internal/snowflake.
var (
	snowflakeEpoch  time.Time        = snowflakepkg.Epoch
	snowflakeLayout snowflake.Layout = snowflakepkg.Layout
)

// --- In-memory index types (re-exported public API) ---

type (
	// EntityClass distinguishes reference entities from event entities.
	EntityClass = indexpkg.EntityClass
	// OntologyMapping classifies entity labels as reference or event.
	OntologyMapping = indexpkg.OntologyMapping
)

// EntityClass constants.
const (
	ClassEvent     = indexpkg.ClassEvent
	ClassReference = indexpkg.ClassReference
)

// NewOntologyMapping creates an OntologyMapping that classifies the given
// label names as ClassReference. All other labels default to ClassEvent.
func NewOntologyMapping(refLabels []string) *OntologyMapping {
	return indexpkg.NewOntologyMapping(refLabels)
}

// --- Vector-index sentinel errors (public API surface) ---

var (
	ErrVectorIndexExists   = indexpkg.ErrVectorIndexExists
	ErrVectorIndexNotFound = indexpkg.ErrVectorIndexNotFound
	ErrDimensionMismatch   = indexpkg.ErrDimensionMismatch
)

// --- Registry sentinel errors (public API surface) ---

var (
	ErrEmptyName        = indexpkg.ErrEmptyName
	ErrRegistryNotEmpty = indexpkg.ErrRegistryNotEmpty
)

// --- Pagination helpers (package-private wrappers) ---
//
// The implementations live in internal/store as exported functions; these
// thin wrappers preserve the original lowercase identifiers used inside
// pkg/graph so existing call sites and tests don't need to be rewritten.

func paginateIDs(ids []snowflake.ID, after types.EntityID, limit int) []snowflake.ID {
	return store.PaginateIDs(ids, after, limit)
}

func paginateNodes(nodes []*types.Node, after types.EntityID, limit int) []*types.Node {
	return store.PaginateNodes(nodes, after, limit)
}

func paginateRels(rels []*types.Relationship, after types.EntityID, limit int) []*types.Relationship {
	return store.PaginateRels(rels, after, limit)
}

func paginateNodeIDs(ids []types.NodeID, after types.EntityID, limit int) []types.NodeID {
	return store.PaginateNodeIDs(ids, after, limit)
}

func paginateRelIDs(ids []types.RelID, after types.EntityID, limit int) []types.RelID {
	return store.PaginateRelIDs(ids, after, limit)
}

func toNodeIDs(ids []snowflake.ID) []types.NodeID { return store.ToNodeIDs(ids) }

func toRelIDs(ids []snowflake.ID) []types.RelID { return store.ToRelIDs(ids) }

func sortNodesByID(nodes []*types.Node) { store.SortNodesByID(nodes) }

func sortRelsByID(rels []*types.Relationship) { store.SortRelsByID(rels) }

// --- Concrete Store implementation re-exports ---

// MemoryStore is the thread-safe in-memory Store implementation. The
// canonical type lives in pkg/graph/internal/memorystore.
type MemoryStore = memorystore.MemoryStore

// NewMemoryStore constructs an empty MemoryStore.
func NewMemoryStore() *MemoryStore { return memorystore.NewMemoryStore() }

// BadgerStore is the persistent Store implementation backed by Badger v4.
// The canonical type lives in pkg/graph/internal/badgerstore.
type BadgerStore = badgerstore.BadgerStore

// BadgerStoreConfig configures a BadgerStore. The canonical type lives in
// pkg/graph/internal/badgerstore.
type BadgerStoreConfig = badgerstore.BadgerStoreConfig

// NewBadgerStore opens a Badger database with the given configuration and
// rebuilds in-memory indexes from persisted data.
func NewBadgerStore(cfg BadgerStoreConfig) (*BadgerStore, error) {
	return badgerstore.NewBadgerStore(cfg)
}

// RelDeleteInfo holds pre-read relationship metadata for two-phase cascade
// delete inside BadgerStore. Re-exported because TieredStore reaches into the
// BadgerStore-internal partial-write helpers (`PutRelEntityAndOut`,
// `DeleteRelIncoming`, etc.) for cross-shard relationship routing.
type RelDeleteInfo = badgerstore.RelDeleteInfo

// --- TieredStore re-exports ---

// TieredStore is the multi-shard Store implementation that routes entities
// across a reference shard, time-windowed event shards, and an optional
// reference archive. The canonical type lives in
// pkg/graph/internal/tieredstore.
type TieredStore = tieredstore.TieredStore

// TieredStoreConfig configures a TieredStore.
type TieredStoreConfig = tieredstore.TieredStoreConfig

// ShardInfo describes a shard in a TieredStore.
type ShardInfo = tieredstore.ShardInfo

// VerifyResult is the result of TieredStore.VerifyShard.
type VerifyResult = tieredstore.VerifyResult

// RepairResult is the result of TieredStore.RunRepair.
type RepairResult = tieredstore.RepairResult

// ShardEntry / ShardCatalog / ShardKind / ShardTier and their constants are
// part of the TieredStore catalog API.
type (
	ShardEntry   = tieredstore.ShardEntry
	ShardCatalog = tieredstore.ShardCatalog
	ShardKind    = tieredstore.ShardKind
	ShardTier    = tieredstore.ShardTier
)

const (
	ShardReference = tieredstore.ShardReference
	ShardEvent     = tieredstore.ShardEvent

	TierHot  = tieredstore.TierHot
	TierWarm = tieredstore.TierWarm
	TierCold = tieredstore.TierCold
)

// NewTieredStore constructs a TieredStore from cfg.
func NewTieredStore(cfg TieredStoreConfig) (*TieredStore, error) {
	return tieredstore.NewTieredStore(cfg)
}

// NewShardCatalog constructs an empty ShardCatalog backed by the JSON file at
// path. Re-exported for tests.
func NewShardCatalog(path string) *ShardCatalog { return tieredstore.NewShardCatalog(path) }

// TieredStore-specific sentinel errors.
var (
	ErrEventPropertyIndex        = tieredstore.ErrEventPropertyIndex
	ErrPrimaryLabelClassMutation = tieredstore.ErrPrimaryLabelClassMutation
	ErrNotReferenceEntity        = tieredstore.ErrNotReferenceEntity
	ErrCrossShardArchiveRel      = tieredstore.ErrCrossShardArchiveRel
)

// MigrateFromBadger copies all current and historical entities from src to dst.
// Re-exported for use by the Graph layer's migration helpers.
func MigrateFromBadger(src *BadgerStore, dst *TieredStore, labels *indexpkg.LabelRegistry) error {
	return tieredstore.MigrateFromBadger(src, dst, labels)
}

// EventShard is the per-shard wrapper TieredStore uses to track its hot,
// warm, and cold event shards. Re-exported because tests in pkg/graph need
// to refer to the type by name (e.g., to range over `ts.EventShardsForTest()`).
type EventShard = tieredstore.EventShard

// --- Lifecycle event re-exports ---
//
// The canonical types live in pkg/graph/internal/events. These aliases keep
// the historical public API (`graph.EventBus`, `graph.AsyncEventBus`,
// `graph.Event`, the `EventNode*`/`EventRel*` constants, `EventPriority`
// constants, and the `BackpressureStrategy` constants) usable by external
// callers (notably tkgd-v3) without import-path churn.

type (
	// Event is a lifecycle notification emitted after a successful graph mutation.
	Event = events.Event
	// EventType classifies a graph lifecycle event.
	EventType = events.EventType
	// EventPriority controls the delivery queue for AsyncEventBus.
	EventPriority = events.EventPriority
	// EventHandler is a callback invoked for each published event.
	EventHandler = events.EventHandler
	// EventBus dispatches graph lifecycle events to registered subscribers synchronously.
	EventBus = events.EventBus
	// AsyncEventBus delivers graph lifecycle events asynchronously via a worker pool.
	AsyncEventBus = events.AsyncEventBus
	// AsyncEventBusConfig configures an AsyncEventBus.
	AsyncEventBusConfig = events.AsyncEventBusConfig
	// BackpressureStrategy controls AsyncEventBus behaviour when the queue is full.
	BackpressureStrategy = events.BackpressureStrategy
)

// EventType constants.
const (
	EventNodeCreate = events.EventNodeCreate
	EventNodeUpdate = events.EventNodeUpdate
	EventNodeDelete = events.EventNodeDelete
	EventRelCreate  = events.EventRelCreate
	EventRelUpdate  = events.EventRelUpdate
	EventRelDelete  = events.EventRelDelete
)

// EventPriority constants.
const (
	PriorityNormal   = events.PriorityNormal
	PriorityHigh     = events.PriorityHigh
	PriorityCritical = events.PriorityCritical
	PriorityLow      = events.PriorityLow
	PriorityDeferred = events.PriorityDeferred
)

// BackpressureStrategy constants.
const (
	BackpressureBlock      = events.BackpressureBlock
	BackpressureDropOldest = events.BackpressureDropOldest
	BackpressureDropLatest = events.BackpressureDropLatest
)

// NewEventBus creates an EventBus ready for use.
func NewEventBus() *EventBus { return events.NewEventBus() }

// NewAsyncEventBus creates and starts an AsyncEventBus with the given configuration.
func NewAsyncEventBus(cfg AsyncEventBusConfig) *AsyncEventBus {
	return events.NewAsyncEventBus(cfg)
}

// --- Integrity hash re-exports ---
//
// The canonical implementations live in pkg/graph/internal/integrity. These
// aliases preserve the historical public API (`graph.ComputeNodeHash`,
// `graph.ComputeRelHash`) used by external callers (notably tkgd-v3) and let
// internal pkg/graph code keep using the unqualified names that existed
// before the move.

// ComputeNodeHash computes a SHA-256 hash of the node's content.
// The hash covers: id, version, sorted labels, and sorted properties.
// Returns the hex-encoded hash string (64 characters).
var ComputeNodeHash = integrity.ComputeNodeHash

// ComputeRelHash computes a SHA-256 hash of the relationship's content.
// The hash covers: id, version, type name, start ID, end ID, and sorted properties.
// Returns the hex-encoded hash string (64 characters).
var ComputeRelHash = integrity.ComputeRelHash

// --- Temporal-constraint re-exports ---
//
// The pure-data types live in pkg/graph/internal/temporal. The Graph-coupled
// enforcement methods (`checkTemporalConstraints` and friends) stay in
// pkg/graph/temporal_constraint.go. These aliases preserve the historical
// public API (`graph.ConstraintSet`, `graph.TemporalConstraint`,
// `graph.ConstraintRelWithinEndpoints`, the seven sentinel errors) used by
// external callers and let internal pkg/graph code keep using the unqualified
// names that existed before the move.

type (
	// TemporalConstraintKind identifies the type of temporal constraint enforced at write time.
	TemporalConstraintKind = temporalpkg.TemporalConstraintKind
	// TemporalConstraint is a single temporal invariant checked at write time.
	TemporalConstraint = temporalpkg.TemporalConstraint
	// ConstraintSet is an immutable-by-convention ordered set of TemporalConstraints.
	ConstraintSet = temporalpkg.ConstraintSet
)

// TemporalConstraintKind constants.
const (
	// ConstraintRelWithinEndpoints enforces that a relationship's validity window
	// is contained within the validity intervals of both endpoint nodes.
	ConstraintRelWithinEndpoints = temporalpkg.ConstraintRelWithinEndpoints
)

// NewConstraintSet creates a ConstraintSet from the given constraints.
func NewConstraintSet(cs ...TemporalConstraint) ConstraintSet {
	return temporalpkg.NewConstraintSet(cs...)
}

// Temporal-constraint sentinel errors.
var (
	ErrTemporalConstraint          = temporalpkg.ErrTemporalConstraint
	ErrRelBeforeStartNode          = temporalpkg.ErrRelBeforeStartNode
	ErrRelBeforeEndNode            = temporalpkg.ErrRelBeforeEndNode
	ErrRelAfterStartNode           = temporalpkg.ErrRelAfterStartNode
	ErrRelAfterEndNode             = temporalpkg.ErrRelAfterEndNode
	ErrRelExceedsStartNodeValidity = temporalpkg.ErrRelExceedsStartNodeValidity
	ErrRelExceedsEndNodeValidity   = temporalpkg.ErrRelExceedsEndNodeValidity
)
