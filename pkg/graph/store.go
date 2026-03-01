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

	// Close releases any resources held by the store.
	// Safe to call multiple times. No-op for stores without resources.
	Close() error
}

// Sentinel errors for store operations.
var (
	ErrNodeNotFound = errors.New("graph: node not found")
	ErrRelNotFound  = errors.New("graph: relationship not found")
	ErrNodeExists   = errors.New("graph: node already exists")
	ErrRelExists       = errors.New("graph: relationship already exists")
	ErrVersionNotFound = errors.New("graph: version not found")
)
