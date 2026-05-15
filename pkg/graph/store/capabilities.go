package store

import (
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// Capability interfaces — narrowed views of the full Store contract.
//
// The full Store interface is a 65-method aggregate. Splitting it into
// behavioural clusters (this file) lets new backends implement only the
// capabilities they actually support and lets call sites depend on the
// narrowest surface that gets the job done.
//
// All capability interfaces are EMBEDDED into Store (see store.go), so every
// existing implementation continues to satisfy Store as a whole — this is a
// purely additive refactor. Future migrations can convert specific call
// sites to depend on the narrowest capability without touching the
// implementations.
//
// Optional capabilities (DepthHistoryIterationCapability,
// HistoryVersionPageCapability, PropertyIndexCapability,
// TemporalIndexCapability, VectorIndexCapability, HighFrequencyIndexCapability)
// can be removed from a backend's Store implementation in a future major
// version; consumers that rely on them type-assert before calling.

// Lifecycle is the always-mandatory housekeeping subset every backend must
// implement. Close releases resources; Clear truncates without removing
// registries (a graph-layer concern). Nil concrete store receivers return
// ErrNilStore from lifecycle methods rather than panicking.
type Lifecycle interface {
	// Clear removes all entities, indexes, history, and counters.
	// Registries (label/reltype tokens) are a graph-layer concern and
	// untouched.
	Clear() error
	// Close releases any resources held by the store. Idempotent.
	Close() error
}

// NodeCRUDCapability is the node-mutation surface. Every backend must
// implement it — node CRUD plus the label-mutation helpers required by the
// graph's history-aware label flows. Label-mutation helpers validate that the
// supplied current row adds or removes exactly the requested token.
type NodeCRUDCapability interface {
	PutNode(n *types.Node) error
	GetNode(id types.NodeID) (*types.Node, error)
	ReplaceNode(n *types.Node) error
	// DeleteNode removes only an unconnected node row. Backends return
	// ErrInvalidStoreMutation instead of orphaning connected relationships.
	DeleteNode(id types.NodeID) error

	// DeleteNodeCascade removes the node and every relationship it
	// participates in. Used when the graph layer needs a single-call
	// delete that the store can implement atomically.
	DeleteNodeCascade(id types.NodeID) error

	RemoveNodeLabelToken(id types.NodeID, tok uint16, updatedNode *types.Node) error
	AddNodeLabelToken(id types.NodeID, tok uint16, updatedNode *types.Node) error
}

// NodeIntegrityHashCapability is OPTIONAL. Backends that can expose a live
// node's integrity hash without returning the full node can implement this to
// let relationship creation capture endpoint hashes without forcing a defensive
// node deep copy. The method must still validate that the node currently exists.
type NodeIntegrityHashCapability interface {
	NodeIntegrityHash(id types.NodeID) (string, error)
}

// EndpointIntegrityHashCapability is OPTIONAL. It is the batched form of
// NodeIntegrityHashCapability for relationship creation paths that need both
// endpoint hashes under one backend read window.
type EndpointIntegrityHashCapability interface {
	EndpointIntegrityHashes(startID, endID types.NodeID) (string, string, error)
}

// RelationshipCRUDCapability is the relationship-mutation surface.
type RelationshipCRUDCapability interface {
	PutRelationship(r *types.Relationship) error
	GetRelationship(id types.RelID) (*types.Relationship, error)
	ReplaceRelationship(r *types.Relationship) error
	DeleteRelationship(id types.RelID) error
}

// AdjacencyCapability is the outgoing/incoming relationship lookup surface
// plus the batch ForNodes variants. Mandatory because it is on the hot path
// of every traversal-shaped query. Explicit node IDs are all-or-error:
// existing nodes with zero matching relationships return an empty result, but
// any missing requested node returns ErrNodeNotFound.
type AdjacencyCapability interface {
	OutgoingRelationships(nodeID types.NodeID, typeToken uint16) ([]*types.Relationship, error)
	IncomingRelationships(nodeID types.NodeID, typeToken uint16) ([]*types.Relationship, error)

	OutgoingRelationshipsForNodes(nodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error)
	IncomingRelationshipsForNodes(nodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error)
}

// BulkReadCapability is the unfiltered/label-filtered listing surface. The
// graph layer uses it for export, snapshot, and the `*ByLabel` /
// `*ByType` query shapes.
type BulkReadCapability interface {
	NodesByLabel(token uint16, opts QueryOpts) ([]*types.Node, error)
	RelationshipsByType(token uint16, opts QueryOpts) ([]*types.Relationship, error)

	AllNodes(opts QueryOpts) ([]*types.Node, error)
	AllRelationships(opts QueryOpts) ([]*types.Relationship, error)
	GetNodesByIDs(ids []types.NodeID) ([]*types.Node, error)
	GetRelationshipsByIDs(ids []types.RelID) ([]*types.Relationship, error)
}

// BatchCapability is the batched-mutation surface used by BatchBuilder and
// the import path. All-or-nothing: empty/nil input returns nil with zero
// mutations. Delete batches treat duplicate IDs as one requested deletion
// before validation and counter/index updates. DeleteNodesBatch removes only
// unconnected node rows; connected batch deletes must use graph-level cascade
// or history-aware delete flows.
type BatchCapability interface {
	PutNodesBatch(nodes []*types.Node) error
	PutRelationshipsBatch(rels []*types.Relationship) error
	DeleteNodesBatch(ids []types.NodeID) error
	DeleteRelationshipsBatch(ids []types.RelID) error
}

// HistoryCapability is the version-history surface. Every backend must
// implement it because the graph layer applies pre-mutation snapshots on
// every Update*. The atomic ReplaceWith*History and Delete*WithHistory
// methods exist so backends can avoid the "history written, then crash, then
// data write fails" orphan window. DeleteNodeWithHistory relTombstones must
// cover every live connected relationship exactly once.
type HistoryCapability interface {
	ReplaceNodeWithHistory(current *types.Node, prevVersion uint32, prevState *types.Node) error
	ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prevState *types.Relationship) error

	PutNodeVersion(id types.NodeID, version uint32, n *types.Node) error
	GetNodeVersion(id types.NodeID, version uint32) (*types.Node, error)
	GetNodeHistory(id types.NodeID) ([]*types.Node, error)
	TruncateNodeHistory(id types.NodeID, keepVersions int) error

	PutRelVersion(id types.RelID, version uint32, r *types.Relationship) error
	GetRelVersion(id types.RelID, version uint32) (*types.Relationship, error)
	GetRelHistory(id types.RelID) ([]*types.Relationship, error)
	TruncateRelHistory(id types.RelID, keepVersions int) error

	RemoveNodeLabelTokenWithHistory(id types.NodeID, tok uint16, updatedNode *types.Node,
		prevVersion uint32, prevState *types.Node) error
	AddNodeLabelTokenWithHistory(id types.NodeID, tok uint16, updatedNode *types.Node,
		prevVersion uint32, prevState *types.Node) error

	DeleteNodeWithHistory(id types.NodeID, prevNodeVersion uint32, nodeTombstone *types.Node, relTombstones []RelTombstone) error
	DeleteRelWithHistory(id types.RelID, prevVersion uint32, tombstone *types.Relationship) error
}

// TransactionTimeQueryCapability is OPTIONAL. Backends that can inspect
// current rows and version history under one backend read window can implement
// this to avoid materializing every history slice for transaction-time reads.
// Single-entity misses return ErrVersionNotFound; bulk queries omit misses and
// return nil, nil when no entity existed at the requested transaction time.
type TransactionTimeQueryCapability interface {
	NodeAsOf(id types.NodeID, txTime types.Instant) (*types.Node, error)
	RelAsOf(id types.RelID, txTime types.Instant) (*types.Relationship, error)
	NodesAsOf(txTime types.Instant) ([]*types.Node, error)
	RelsAsOf(txTime types.Instant) ([]*types.Relationship, error)
}

// HistoryRollbackTrimCapability is OPTIONAL. It supports transaction rollback
// without eager deep copies of entire history chains. Graph mutation paths
// append superseded versions at the entity's previous Version(); rollback can
// therefore restore the pre-transaction current row and trim history entries
// written at or after that version.
type HistoryRollbackTrimCapability interface {
	TrimNodeHistoryFrom(id types.NodeID, minVersion uint32) error
	TrimRelHistoryFrom(id types.RelID, minVersion uint32) error
}

// HistoryVersionPageCapability is OPTIONAL. Backends that can page through an
// individual entity's history chain implement it so export and other streaming
// readers do not have to materialize every version for one heavily updated
// entity. startVersion is inclusive, and limit 0 returns all remaining versions.
// Results must be sorted by ascending Version().
type HistoryVersionPageCapability interface {
	NodeHistoryVersionsFrom(id types.NodeID, startVersion uint32, limit int) ([]*types.Node, error)
	RelHistoryVersionsFrom(id types.RelID, startVersion uint32, limit int) ([]*types.Relationship, error)
}

// StatsCapability is the counter surface used by `g.Stats().*` and by the
// graph layer's internal accounting.
type StatsCapability interface {
	NodeCount() (int, error)
	RelationshipCount() (int, error)
	NodeCountByLabel(token uint16) (int, error)
	RelCountByType(token uint16) (int, error)
}

// IterationCapability is the iteration surface (ForEach + paginated AllIDs).
// Distinct from BulkReadCapability because iteration returns IDs only — no
// entity deserialisation, no deep copy. The graph layer uses iteration in
// snapshot/diff and on the export path. Nil ForEach callbacks return
// ErrInvalidStoreMutation. Non-nil callbacks are invoked outside backend locks,
// Badger transactions, and Tiered shard checkouts so callback code may call
// back into Store methods. History iterators must not let callback-created
// higher IDs extend the active iteration, and async-buffered stores must make
// pending history deletes visible to ID scans before flush.
type IterationCapability interface {
	AllNodeIDs(opts QueryOpts) ([]types.NodeID, error)
	AllRelIDs(opts QueryOpts) ([]types.RelID, error)

	AllNodeHistoryIDs() ([]types.NodeID, error)
	AllRelHistoryIDs() ([]types.RelID, error)

	AllNodeHistoryIDsFrom(after types.NodeID, limit int) ([]types.NodeID, error)
	AllRelHistoryIDsFrom(after types.RelID, limit int) ([]types.RelID, error)

	ForEachNodeID(fn func(types.NodeID) bool) error
	ForEachRelID(fn func(types.RelID) bool) error
	ForEachNodeHistoryID(fn func(types.NodeID) bool) error
	ForEachRelHistoryID(fn func(types.RelID) bool) error
}

// DepthHistoryIterationCapability is OPTIONAL. Tiered backends implement it
// so graph-layer history-aware queries can combine temporal filters with
// shard-depth filtering without scanning history from tiers the caller
// excluded. Single-shard backends may omit it because every valid depth
// selector maps to the same shard. Nil callbacks return ErrInvalidStoreMutation.
type DepthHistoryIterationCapability interface {
	ForEachNodeHistoryIDByDepth(depth ShardDepth, fn func(types.NodeID) bool) error
	ForEachRelHistoryIDByDepth(depth ShardDepth, fn func(types.RelID) bool) error
}

// DeletedIterationCapability is OPTIONAL. Backends implement it so the graph
// layer can fold ONLY deleted IDs (entities with history rows but no current
// row) onto a narrow indexed candidate list when answering temporal adjacency
// or label/property queries at a point/interval in the past. Without it the
// graph layer falls back to iterating the full history set — correct but
// O(total history) per query regardless of how narrow the candidate list is.
//
// Implementations must visit every ID that has at least one history version
// AND no current row, and must not visit any ID that currently exists. Nil
// callbacks return ErrInvalidStoreMutation. Callbacks are invoked outside
// backend locks (same contract as ForEachNodeHistoryID).
type DeletedIterationCapability interface {
	ForEachDeletedNodeID(fn func(types.NodeID) bool) error
	ForEachDeletedRelID(fn func(types.RelID) bool) error
}

// DepthDeletedIterationCapability is the depth-aware counterpart of
// DeletedIterationCapability. Tiered backends implement it so history-aware
// queries can combine deleted-only iteration with shard-depth filtering.
// Single-shard backends do not need it because every valid depth maps to the
// same shard. Nil callbacks return ErrInvalidStoreMutation.
type DepthDeletedIterationCapability interface {
	ForEachDeletedNodeIDByDepth(depth ShardDepth, fn func(types.NodeID) bool) error
	ForEachDeletedRelIDByDepth(depth ShardDepth, fn func(types.RelID) bool) error
}

// PropertyIndexCapability is OPTIONAL. Backends that do not implement
// property indexes should omit it; consumers that need property-indexed
// lookup should type-assert and surface ErrCapabilityNotSupported (or a
// purpose-specific sentinel) when the assertion fails.
//
// Today every Store implementation in this repository implements it; the
// optional-ness is forward-looking for future read-only or analytic
// backends that have no use for write-side index management.
type PropertyIndexCapability interface {
	CreatePropertyIndex(labelToken uint16, propertyKey string) error
	DropPropertyIndex(labelToken uint16, propertyKey string) error
	NodesByLabelAndProperty(labelToken uint16, key string, value any, opts QueryOpts) ([]*types.Node, error)
}

// TemporalIndexCapability is OPTIONAL — see the note on
// PropertyIndexCapability.
type TemporalIndexCapability interface {
	CreateTemporalIndex(labelToken uint16) error
	DropTemporalIndex(labelToken uint16) error
}

// VectorIndexCapability is OPTIONAL. The reference implementation is a
// brute-force in-memory k-NN. Backends backed by a remote vector store
// should plug in here without having to implement the rest of Store.
// Implementations must reject indexed vectors and search queries containing
// NaN or infinity with ErrInvalidVectorValue.
type VectorIndexCapability interface {
	CreateVectorIndex(labelToken uint16, propertyKey string, dims int, metric DistanceMetric) error
	DropVectorIndex(labelToken uint16, propertyKey string) error
	SearchNearestNodes(labelToken uint16, propertyKey string, query []float32, k int, opts QueryOpts) ([]*types.Node, error)
}

// FilteredVectorSearchCapability is OPTIONAL but strongly recommended for
// any backend that exposes vector search under temporal filters or any
// other graph-layer eligibility predicate. With it, the graph pushes the
// eligibility filter into the search BEFORE the k-cut so the top-k is
// taken from the eligible-only set.
//
// Backends that cannot natively pre-filter still expose useful behaviour
// through `SearchNearestNodes`; the graph-layer fallback performs
// iterative over-fetch (k → 2k → 4k …, clamped to the package-internal
// overfetchCeiling = 65536) until k eligible results are accumulated or
// the backend exhausts. For k > overfetchCeiling, the fallback returns
// at most overfetchCeiling eligible matches; backends that need to serve
// larger k correctly should implement this capability so the graph can
// skip the loop and rely on the backend's pre-filtered top-k.
//
// snowflake.ID is the raw entity-ID type used by the predicate so the
// hook is independent of the typed NodeID/RelID wrappers.
type FilteredVectorSearchCapability interface {
	SearchNearestFiltered(labelToken uint16, propertyKey string, query []float32, k int, filter func(rawID snowflake.ID) bool) ([]snowflake.ID, error)
}

// HighFrequencyIndexCapability is OPTIONAL — see the note on
// PropertyIndexCapability. Implementations that have no need for the
// time-bucketed amortised insertion path may omit it. Implementations must
// reject bucket sizes that are not positive whole milliseconds with
// ErrInvalidTemporalIndexConfig.
type HighFrequencyIndexCapability interface {
	CreateHighFrequencyIndex(labelToken uint16, bucketSize time.Duration) error
	DropHighFrequencyIndex(labelToken uint16) error
}

// MandatoryStore is the embedded composition of capabilities every
// implementation MUST satisfy. The graph layer's Core type narrows its
// store field to MandatoryStore and type-asserts for optional capabilities
// at the call sites that need them, returning ErrCapabilityNotSupported
// (wrapped with the missing-capability name) on assertion miss.
//
// Out-of-tree backends that need to implement only a subset of
// functionality can satisfy MandatoryStore alone; they will work for the
// graph's core CRUD/history/iteration/stats surface and will surface
// ErrCapabilityNotSupported for index operations they do not support.
type MandatoryStore interface {
	Lifecycle
	NodeCRUDCapability
	RelationshipCRUDCapability
	AdjacencyCapability
	BulkReadCapability
	BatchCapability
	HistoryCapability
	StatsCapability
	IterationCapability
}
