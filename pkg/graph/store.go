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
	DeleteNode(id snowflake.ID) error

	// Relationship operations
	PutRelationship(r *types.Relationship) error
	GetRelationship(id snowflake.ID) (*types.Relationship, error)
	DeleteRelationship(id snowflake.ID) error

	// Index queries
	NodesByLabel(token uint16) []*types.Node
	RelationshipsByType(token uint16) []*types.Relationship

	// Adjacency queries — token 0 means "all types"
	OutgoingRelationships(nodeID snowflake.ID, typeToken uint16) []*types.Relationship
	IncomingRelationships(nodeID snowflake.ID, typeToken uint16) []*types.Relationship

	// Counts
	NodeCount() int
	RelationshipCount() int
}

// Sentinel errors for store operations.
var (
	ErrNodeNotFound = errors.New("graph: node not found")
	ErrRelNotFound  = errors.New("graph: relationship not found")
	ErrNodeExists   = errors.New("graph: node already exists")
	ErrRelExists    = errors.New("graph: relationship already exists")
)
