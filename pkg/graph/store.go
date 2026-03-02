package graph

import (
	"errors"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

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
	NodesByLabel(token uint16) ([]*types.Node, error)
	RelationshipsByType(token uint16) ([]*types.Relationship, error)

	// Adjacency queries — token 0 means "all types"
	OutgoingRelationships(nodeID snowflake.ID, typeToken uint16) ([]*types.Relationship, error)
	IncomingRelationships(nodeID snowflake.ID, typeToken uint16) ([]*types.Relationship, error)

	// Bulk queries
	AllNodes() ([]*types.Node, error)
	AllRelationships() ([]*types.Relationship, error)
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
	NodesByLabelAndProperty(labelToken uint16, key string, value any) ([]*types.Node, error)

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
	ErrIndexExists      = errors.New("graph: property index already exists")
	ErrIndexNotFound    = errors.New("graph: property index not found")
)
