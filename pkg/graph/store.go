package graph

import (
	"errors"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// ShardDepth controls which shard tiers are included in merge queries.
// Zero (DepthAll) includes all tiers — backward-compatible default.
type ShardDepth byte

const (
	DepthAll  ShardDepth = 0 // All tiers (default, backward-compatible).
	DepthHot  ShardDepth = 1 // Hot shard only.
	DepthWarm ShardDepth = 2 // Hot + warm shards.
)

// QueryOpts controls pagination and temporal filtering for unbounded query methods.
// Zero values mean "return all" — backward-compatible with existing callers.
type QueryOpts struct {
	Limit int          // Max results. 0 = no limit.
	After snowflake.ID // Return entities with ID > After. 0 = from start.

	// Temporal filters — zero values = no filter (backward-compatible).
	// ValidAt takes precedence if both ValidAt and ValidStart/ValidEnd are set.
	ValidAt    types.Instant // Point-in-time filter. 0 = disabled.
	ValidStart types.Instant // Interval filter start. Both must be > 0 for interval filter.
	ValidEnd   types.Instant // Interval filter end. 0 = disabled.

	// Depth controls which shard tiers to query. 0 (DepthAll) = all tiers.
	// Only used by TieredStore; single-shard stores ignore this field.
	Depth ShardDepth
}

// Store is the persistence contract for the graph layer.
// Implementations handle entity storage and index maintenance.
// Keys are snowflake.ID — the bridge from opaque entity IDs.
type Store interface {
	// Node operations
	PutNode(n *types.Node) error
	GetNode(id snowflake.ID) (*types.Node, error)
	ReplaceNode(n *types.Node) error
	DeleteNode(id snowflake.ID) error

	// Relationship operations
	PutRelationship(r *types.Relationship) error
	GetRelationship(id snowflake.ID) (*types.Relationship, error)
	ReplaceRelationship(r *types.Relationship) error
	DeleteRelationship(id snowflake.ID) error

	// Index queries
	NodesByLabel(token uint16, opts QueryOpts) ([]*types.Node, error)
	RelationshipsByType(token uint16, opts QueryOpts) ([]*types.Relationship, error)

	// Adjacency queries — token 0 means "all types"
	OutgoingRelationships(nodeID snowflake.ID, typeToken uint16) ([]*types.Relationship, error)
	IncomingRelationships(nodeID snowflake.ID, typeToken uint16) ([]*types.Relationship, error)

	// Bulk queries
	AllNodes(opts QueryOpts) ([]*types.Node, error)
	AllRelationships(opts QueryOpts) ([]*types.Relationship, error)
	GetNodesByIDs(ids []snowflake.ID) ([]*types.Node, error)
	GetRelationshipsByIDs(ids []snowflake.ID) ([]*types.Relationship, error)

	// Batch operations — two-phase (validate then apply), all-or-nothing.
	// Empty/nil input returns nil error with zero mutations.
	PutNodesBatch(nodes []*types.Node) error
	PutRelationshipsBatch(rels []*types.Relationship) error
	DeleteNodesBatch(ids []snowflake.ID) error
	DeleteRelationshipsBatch(ids []snowflake.ID) error

	// Atomic replace + history — atomically writes a version history entry
	// and replaces the current entity data. Prevents orphaned history entries
	// if a crash occurs between the two operations.
	// prevVersion is the version number for the history snapshot.
	// prevState is the pre-mutation entity snapshot (deep-copied by caller).
	ReplaceNodeWithHistory(current *types.Node, prevVersion uint32, prevState *types.Node) error
	ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prevState *types.Relationship) error

	// Version history — Node
	PutNodeVersion(id snowflake.ID, version uint32, n *types.Node) error
	GetNodeVersion(id snowflake.ID, version uint32) (*types.Node, error)
	GetNodeHistory(id snowflake.ID) ([]*types.Node, error)
	TruncateNodeHistory(id snowflake.ID, keepVersions int) error

	// Version history — Relationship
	PutRelVersion(id snowflake.ID, version uint32, r *types.Relationship) error
	GetRelVersion(id snowflake.ID, version uint32) (*types.Relationship, error)
	GetRelHistory(id snowflake.ID) ([]*types.Relationship, error)
	TruncateRelHistory(id snowflake.ID, keepVersions int) error

	// Cascade operations
	DeleteNodeCascade(id snowflake.ID) error

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

	// AllNodeIDs returns the IDs of all current nodes, with optional pagination.
	// Returns only IDs — no entity deserialization or deep copy.
	AllNodeIDs(opts QueryOpts) ([]snowflake.ID, error)

	// AllRelIDs returns the IDs of all current relationships, with optional pagination.
	// Returns only IDs — no entity deserialization or deep copy.
	AllRelIDs(opts QueryOpts) ([]snowflake.ID, error)

	// AllNodeHistoryIDs returns the IDs of all nodes that have version history entries.
	// This includes deleted nodes whose history was preserved.
	AllNodeHistoryIDs() ([]snowflake.ID, error)

	// AllRelHistoryIDs returns the IDs of all relationships that have version history entries.
	AllRelHistoryIDs() ([]snowflake.ID, error)

	// ForEachNodeID iterates over all current node IDs, calling fn for each.
	// Iteration stops early if fn returns false. No ordering guarantee.
	// The callback must NOT call other methods on this Store (lock reentrancy).
	ForEachNodeID(fn func(snowflake.ID) bool) error

	// ForEachRelID iterates over all current relationship IDs, calling fn for each.
	// Iteration stops early if fn returns false. No ordering guarantee.
	// The callback must NOT call other methods on this Store (lock reentrancy).
	ForEachRelID(fn func(snowflake.ID) bool) error

	// ForEachNodeHistoryID iterates over all node IDs with version history entries.
	// Iteration stops early if fn returns false. No ordering guarantee.
	// The callback must NOT call other methods on this Store (lock reentrancy).
	ForEachNodeHistoryID(fn func(snowflake.ID) bool) error

	// ForEachRelHistoryID iterates over all relationship IDs with version history entries.
	// Iteration stops early if fn returns false. No ordering guarantee.
	// The callback must NOT call other methods on this Store (lock reentrancy).
	ForEachRelHistoryID(fn func(snowflake.ID) bool) error

	// Clear removes all entities, indexes, history, and counters.
	// Preserves nothing. Registries are a Graph-layer concern (not cleared).
	// After Clear(), the store is in the same state as NewMemoryStore() / fresh Badger.
	Clear() error

	// Close releases any resources held by the store.
	// Safe to call multiple times. No-op for stores without resources.
	Close() error
}

// Sentinel errors for store operations.
var (
	ErrNodeNotFound     = errors.New("graph: node not found")
	ErrRelNotFound      = errors.New("graph: relationship not found")
	ErrNodeExists       = errors.New("graph: node already exists")
	ErrRelExists        = errors.New("graph: relationship already exists")
	ErrVersionNotFound  = errors.New("graph: version not found")
	ErrNoVersionValidAt = errors.New("graph: no version valid at the given time")
	ErrIndexExists              = errors.New("graph: property index already exists")
	ErrIndexNotFound            = errors.New("graph: property index not found")
	ErrTemporalIndexExists      = errors.New("graph: temporal index already exists")
	ErrTemporalIndexNotFound    = errors.New("graph: temporal index not found")
	ErrTxDone                   = errors.New("graph: transaction already committed or rolled back")
)
