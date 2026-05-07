package graph

import (
	"context"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Entity management ---

// AddNode creates a new node with the given labels and properties.
// Labels are resolved to tokens (created if needed). Properties are bulk-validated
// and sorted in O(N log N). Returns the created node with a generated snowflake ID.
func (g *Graph) AddNode(labels []string, props map[string]any) (*types.Node, error) {
	return g.AddNodeWithContext(context.Background(), labels, props)
}

// AddRelationship creates a new directed relationship between two nodes.
// The type name is resolved to a token (created if needed). Properties are
// bulk-validated and sorted in O(N log N).
func (g *Graph) AddRelationship(typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	return g.AddRelationshipWithContext(context.Background(), typeName, startNode, endNode, props)
}

// AddRelationshipByID creates a relationship using endpoint node IDs
// without fetching the endpoint nodes. See AddRelationshipByIDWithContext for details.
func (g *Graph) AddRelationshipByID(typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error) {
	return g.AddRelationshipByIDWithContext(context.Background(), typeName, startID, endID, props)
}

// AddRelationshipByIDIfAbsent atomically creates a relationship using endpoint
// node IDs only if no relationship of the same type between the same endpoints
// already exists. See AddRelationshipByIDIfAbsentWithContext for details.
func (g *Graph) AddRelationshipByIDIfAbsent(typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error) {
	return g.AddRelationshipByIDIfAbsentWithContext(context.Background(), typeName, startID, endID, props)
}

// DeleteNode atomically removes a node and all connected relationships.
// Acquires the entity lock for the node to prevent write-skew with concurrent
// AddRelationship targeting the same node.
// Returns ErrNodeNotFound if the node does not exist.
func (g *Graph) DeleteNode(id types.NodeID) error {
	return g.DeleteNodeWithContext(context.Background(), id)
}

// DeleteRelationship removes a relationship from the store.
// Returns ErrRelNotFound if the relationship does not exist.
func (g *Graph) DeleteRelationship(id types.RelID) error {
	return g.DeleteRelationshipWithContext(context.Background(), id)
}

// --- Update operations ---

// UpdateNode applies property updates to an existing node.
// The updates map keys are property names; values are the new values.
// A nil value deletes the property. Keys with the "tkg_" prefix are rejected.
// Returns the updated node. Empty updates map is a no-op (no version bump).
func (g *Graph) UpdateNode(id types.NodeID, updates map[string]any) (*types.Node, error) {
	return g.UpdateNodeWithContext(context.Background(), id, updates)
}

// UpdateRelationship applies property updates to an existing relationship.
// The updates map keys are property names; values are the new values.
// A nil value deletes the property. Keys with the "tkg_" prefix are rejected.
// Returns the updated relationship. Empty updates map is a no-op.
func (g *Graph) UpdateRelationship(id types.RelID, updates map[string]any) (*types.Relationship, error) {
	return g.UpdateRelationshipWithContext(context.Background(), id, updates)
}

// SetNodeProperty sets a single property on an existing node.
func (g *Graph) SetNodeProperty(id types.NodeID, key string, value any) error {
	_, err := g.UpdateNode(id, map[string]any{key: value})
	return err
}

// DeleteNodeProperty removes a single property from an existing node.
func (g *Graph) DeleteNodeProperty(id types.NodeID, key string) error {
	_, err := g.UpdateNode(id, map[string]any{key: nil})
	return err
}

// SetRelationshipProperty sets a single property on an existing relationship.
func (g *Graph) SetRelationshipProperty(id types.RelID, key string, value any) error {
	_, err := g.UpdateRelationship(id, map[string]any{key: value})
	return err
}

// DeleteRelationshipProperty removes a single property from an existing relationship.
func (g *Graph) DeleteRelationshipProperty(id types.RelID, key string) error {
	_, err := g.UpdateRelationship(id, map[string]any{key: nil})
	return err
}

// --- Version history passthrough ---

// GetNodeHistory returns all version history snapshots for the given node.
func (g *Graph) GetNodeHistory(id types.NodeID) ([]*types.Node, error) {
	return g.store.GetNodeHistory(id)
}

// GetRelHistory returns all version history snapshots for the given relationship.
func (g *Graph) GetRelHistory(id types.RelID) ([]*types.Relationship, error) {
	return g.store.GetRelHistory(id)
}
