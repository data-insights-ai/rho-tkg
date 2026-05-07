// Package store defines the persistence contract for the graph layer.
// External callers (notably tkgd-v3) implement this interface to plug
// in custom backends; production implementations live in
// pkg/graph/store/{memory,badger,tiered}.
package store

import (
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Store is the persistence contract for the graph layer.
// Implementations handle entity storage and index maintenance.
// Keys are typed entity IDs (types.NodeID / types.RelID).
type Store interface {
	// Node operations
	PutNode(n *types.Node) error
	GetNode(id types.NodeID) (*types.Node, error)
	ReplaceNode(n *types.Node) error
	DeleteNode(id types.NodeID) error

	// Relationship operations
	PutRelationship(r *types.Relationship) error
	GetRelationship(id types.RelID) (*types.Relationship, error)
	ReplaceRelationship(r *types.Relationship) error
	DeleteRelationship(id types.RelID) error

	// Index queries
	NodesByLabel(token uint16, opts QueryOpts) ([]*types.Node, error)
	RelationshipsByType(token uint16, opts QueryOpts) ([]*types.Relationship, error)

	// Adjacency queries — token 0 means "all types"
	OutgoingRelationships(nodeID types.NodeID, typeToken uint16) ([]*types.Relationship, error)
	IncomingRelationships(nodeID types.NodeID, typeToken uint16) ([]*types.Relationship, error)

	// OutgoingRelationshipsForNodes returns outgoing relationships for multiple nodes
	// in a single batched operation. Returns a map from nodeID to its outgoing
	// relationships (sorted by ID). Nodes with zero outgoing rels are absent from
	// the map. nil/empty nodeIDs returns nil, nil.
	OutgoingRelationshipsForNodes(nodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error)

	// IncomingRelationshipsForNodes returns incoming relationships for multiple nodes
	// in a single batched operation. Returns a map from nodeID to its incoming
	// relationships (sorted by ID). Nodes with zero incoming rels are absent from
	// the map. nil/empty nodeIDs returns nil, nil.
	IncomingRelationshipsForNodes(nodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error)

	// Bulk queries
	AllNodes(opts QueryOpts) ([]*types.Node, error)
	AllRelationships(opts QueryOpts) ([]*types.Relationship, error)
	GetNodesByIDs(ids []types.NodeID) ([]*types.Node, error)
	GetRelationshipsByIDs(ids []types.RelID) ([]*types.Relationship, error)

	// Batch operations — two-phase (validate then apply), all-or-nothing.
	// Empty/nil input returns nil error with zero mutations.
	PutNodesBatch(nodes []*types.Node) error
	PutRelationshipsBatch(rels []*types.Relationship) error
	DeleteNodesBatch(ids []types.NodeID) error
	DeleteRelationshipsBatch(ids []types.RelID) error

	// Atomic replace + history — atomically writes a version history entry
	// and replaces the current entity data. Prevents orphaned history entries
	// if a crash occurs between the two operations.
	// prevVersion is the version number for the history snapshot.
	// prevState is the pre-mutation entity snapshot (deep-copied by caller).
	ReplaceNodeWithHistory(current *types.Node, prevVersion uint32, prevState *types.Node) error
	ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prevState *types.Relationship) error

	// Version history — Node
	PutNodeVersion(id types.NodeID, version uint32, n *types.Node) error
	GetNodeVersion(id types.NodeID, version uint32) (*types.Node, error)
	GetNodeHistory(id types.NodeID) ([]*types.Node, error)
	TruncateNodeHistory(id types.NodeID, keepVersions int) error

	// Version history — Relationship
	PutRelVersion(id types.RelID, version uint32, r *types.Relationship) error
	GetRelVersion(id types.RelID, version uint32) (*types.Relationship, error)
	GetRelHistory(id types.RelID) ([]*types.Relationship, error)
	TruncateRelHistory(id types.RelID, keepVersions int) error

	// Cascade operations
	DeleteNodeCascade(id types.NodeID) error

	// DeleteNodeWithHistory atomically combines PutRelVersion×N + PutNodeVersion + DeleteNodeCascade
	// into a single storage transaction. Eliminates orphaned tombstone history entries on crash.
	// nodeTombstone must be a pre-built deep copy with DeletedAt/ValidTo/TxFrom/TxTo set.
	// relTombstones is the pre-built list of all connected relationship tombstones.
	DeleteNodeWithHistory(id types.NodeID, prevNodeVersion uint32, nodeTombstone *types.Node, relTombstones []RelTombstone) error

	// DeleteRelWithHistory atomically combines PutRelVersion + DeleteRelationship
	// into a single storage transaction.
	// tombstone must be a pre-built deep copy with DeletedAt/ValidTo/TxFrom/TxTo set.
	DeleteRelWithHistory(id types.RelID, prevVersion uint32, tombstone *types.Relationship) error

	// Counts
	NodeCount() (int, error)
	RelationshipCount() (int, error)
	NodeCountByLabel(token uint16) (int, error)
	RelCountByType(token uint16) (int, error)

	// Property indexes — index node properties for O(1) lookup.
	// CreatePropertyIndex creates an index on the given label/property combination.
	// Scans existing nodes to populate the index. Returns ErrIndexExists on duplicate.
	CreatePropertyIndex(labelToken uint16, propertyKey string) error
	// DropPropertyIndex removes a property index. Returns ErrIndexNotFound if not found.
	DropPropertyIndex(labelToken uint16, propertyKey string) error
	// NodesByLabelAndProperty returns nodes matching the label and property value.
	// Uses the index if one exists; falls back to label scan + property filter otherwise.
	NodesByLabelAndProperty(labelToken uint16, key string, value any, opts QueryOpts) ([]*types.Node, error)

	// Temporal indexes — index nodes by validity interval for O(log n + k) temporal queries.
	// CreateTemporalIndex creates a temporal index on nodes with the given label token.
	// Scans existing nodes to populate the index. Returns ErrTemporalIndexExists on duplicate.
	CreateTemporalIndex(labelToken uint16) error
	// DropTemporalIndex removes a temporal index. Returns ErrTemporalIndexNotFound if not found.
	DropTemporalIndex(labelToken uint16) error

	// CreateHighFrequencyIndex creates a time-bucketed high-frequency index on nodes
	// with the given label token. Provides O(1) amortized insertion versus the
	// sorted-slice temporal index's O(log n). Designed for thousands of event
	// writes per second into event shards.
	// Only one temporal index type can exist per label at a time — returns
	// ErrTemporalIndexExists if any temporal index already exists for this label.
	// Not persisted; must be rebuilt via CreateHighFrequencyIndex after restart.
	CreateHighFrequencyIndex(labelToken uint16, bucketSize time.Duration) error
	// DropHighFrequencyIndex removes the high-frequency index for the given label.
	// Returns ErrTemporalIndexNotFound if no high-frequency index exists.
	DropHighFrequencyIndex(labelToken uint16) error

	// RemoveNodeLabelToken removes tok from the label index for id and persists updatedNode.
	// No version bump; no history entry. The graph layer has already applied the label
	// removal to updatedNode (via RemoveLabelTokenRaw) and recomputed the hash.
	// Returns ErrNodeNotFound if the node does not exist.
	RemoveNodeLabelToken(id types.NodeID, tok uint16, updatedNode *types.Node) error

	// RemoveNodeLabelTokenWithHistory atomically removes tok from the label index,
	// writes a version history entry for prevState, and persists updatedNode.
	// Eliminates the crash window between PutNodeVersion and RemoveNodeLabelToken.
	// Returns ErrNodeNotFound if the node does not exist.
	RemoveNodeLabelTokenWithHistory(id types.NodeID, tok uint16, updatedNode *types.Node,
		prevVersion uint32, prevState *types.Node) error

	// AddNodeLabelTokenWithHistory atomically adds tok to the label index,
	// writes a version history entry for prevState, and persists updatedNode.
	// The graph layer has already added the label to updatedNode (via AddLabelTokenRaw),
	// bumped the version, and recomputed the hash.
	// Returns ErrNodeNotFound if the node does not exist.
	AddNodeLabelTokenWithHistory(id types.NodeID, tok uint16, updatedNode *types.Node,
		prevVersion uint32, prevState *types.Node) error

	// AddNodeLabelToken adds tok to the label index for id and persists updatedNode.
	// No version bump; no history entry. The graph layer has already applied the label
	// addition to updatedNode. Used by transaction rollback to reverse label deltas
	// without polluting version history.
	// Returns ErrNodeNotFound if the node does not exist.
	AddNodeLabelToken(id types.NodeID, tok uint16, updatedNode *types.Node) error

	// AllNodeIDs returns the IDs of all current nodes, with optional pagination.
	// Returns only IDs — no entity deserialization or deep copy.
	AllNodeIDs(opts QueryOpts) ([]types.NodeID, error)

	// AllRelIDs returns the IDs of all current relationships, with optional pagination.
	// Returns only IDs — no entity deserialization or deep copy.
	AllRelIDs(opts QueryOpts) ([]types.RelID, error)

	// AllNodeHistoryIDs returns the IDs of all nodes that have version history entries.
	// This includes deleted nodes whose history was preserved.
	//
	// Loads the entire ID set into memory. For graphs with deep history, prefer
	// AllNodeHistoryIDsFrom which supports cursor-based pagination.
	AllNodeHistoryIDs() ([]types.NodeID, error)

	// AllRelHistoryIDs returns the IDs of all relationships that have version history entries.
	//
	// Loads the entire ID set into memory. For graphs with deep history, prefer
	// AllRelHistoryIDsFrom which supports cursor-based pagination.
	AllRelHistoryIDs() ([]types.RelID, error)

	// AllNodeHistoryIDsFrom returns IDs of all nodes that have version history,
	// sorted ascending, starting STRICTLY AFTER `after` (exclusive). At most
	// `limit` IDs are returned. Pass `after = types.NodeID(0)` (zero value) for
	// the first page; pass the last returned ID to resume. limit ≤ 0 means
	// "all remaining".
	//
	// Bounded-RAM alternative to AllNodeHistoryIDs(). For exporting a graph
	// with deep history, callers should iterate in pages.
	//
	// On TieredStore, this iterates shards sequentially via checkout/checkin —
	// only one shard's history-ID iterator is open at any time. Cross-shard
	// dedup uses a `seen` set bounded by the IDs returned in the current call,
	// not by the total graph size.
	AllNodeHistoryIDsFrom(after types.NodeID, limit int) ([]types.NodeID, error)

	// AllRelHistoryIDsFrom is the relationship-history equivalent of
	// AllNodeHistoryIDsFrom. Same semantics: ascending order, exclusive
	// cursor, limit ≤ 0 means all remaining.
	AllRelHistoryIDsFrom(after types.RelID, limit int) ([]types.RelID, error)

	// ForEachNodeID iterates over all current node IDs, calling fn for each.
	// Iteration stops early if fn returns false. No ordering guarantee.
	// The callback must NOT call other methods on this Store (lock reentrancy).
	ForEachNodeID(fn func(types.NodeID) bool) error

	// ForEachRelID iterates over all current relationship IDs, calling fn for each.
	// Iteration stops early if fn returns false. No ordering guarantee.
	// The callback must NOT call other methods on this Store (lock reentrancy).
	ForEachRelID(fn func(types.RelID) bool) error

	// ForEachNodeHistoryID iterates over all node IDs with version history entries.
	// Iteration stops early if fn returns false. No ordering guarantee.
	// The callback must NOT call other methods on this Store (lock reentrancy).
	ForEachNodeHistoryID(fn func(types.NodeID) bool) error

	// ForEachRelHistoryID iterates over all relationship IDs with version history entries.
	// Iteration stops early if fn returns false. No ordering guarantee.
	// The callback must NOT call other methods on this Store (lock reentrancy).
	ForEachRelHistoryID(fn func(types.RelID) bool) error

	// Vector indexes — in-memory brute-force k-NN index on node properties.
	// Not persisted; the index must be rebuilt from nodes after restart.
	// CreateVectorIndex creates an index for nodes with the given label token,
	// on the given property key, expecting vectors of length dims.
	// Returns ErrVectorIndexExists on duplicate.
	CreateVectorIndex(labelToken uint16, propertyKey string, dims int, metric DistanceMetric) error
	// DropVectorIndex removes a vector index. Returns ErrVectorIndexNotFound if not found.
	DropVectorIndex(labelToken uint16, propertyKey string) error
	// SearchNearestNodes returns the k closest nodes by vector distance.
	// Returns nil (no error) if the index is empty.
	// Returns ErrVectorIndexNotFound if no index exists for label/propertyKey.
	// Returns ErrDimensionMismatch if query length differs from the index's dimensions.
	SearchNearestNodes(labelToken uint16, propertyKey string, query []float32, k int, opts QueryOpts) ([]*types.Node, error)

	// Clear removes all entities, indexes, history, and counters.
	// Preserves nothing. Registries are a Graph-layer concern (not cleared).
	// After Clear(), the store is in the same state as NewMemoryStore() / fresh Badger.
	Clear() error

	// Close releases any resources held by the store.
	// Safe to call multiple times. No-op for stores without resources.
	Close() error
}

// RelTombstone packages a relationship's tombstone data for use in atomic delete-with-history.
// PrevVersion is the history slot to write the tombstone to (matches r.Version() before deletion).
// Tombstone is a pre-built deep copy with DeletedAt, ValidTo, TxFrom, TxTo populated by the caller.
type RelTombstone struct {
	ID          types.RelID
	PrevVersion uint32
	Tombstone   *types.Relationship
}
