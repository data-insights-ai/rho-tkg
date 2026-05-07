package core

import (
	"context"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Entity management ---

// AddNode creates a new node with the given labels and properties.
// Labels are resolved to tokens (created if needed). Properties are bulk-validated
// and sorted in O(N log N). Returns the created node with a generated snowflake ID.
func (c *Core) AddNode(labels []string, props map[string]any) (*types.Node, error) {
	return c.AddNodeWithContext(context.Background(), labels, props)
}

// AddRelationship creates a new directed relationship between two nodes.
// The type name is resolved to a token (created if needed). Properties are
// bulk-validated and sorted in O(N log N).
func (c *Core) AddRelationship(typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	return c.AddRelationshipWithContext(context.Background(), typeName, startNode, endNode, props)
}

// AddRelationshipByID creates a relationship using endpoint node IDs
// without fetching the endpoint nodes. See AddRelationshipByIDWithContext for details.
func (c *Core) AddRelationshipByID(typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error) {
	return c.AddRelationshipByIDWithContext(context.Background(), typeName, startID, endID, props)
}

// AddRelationshipByIDIfAbsent atomically creates a relationship using endpoint
// node IDs only if no relationship of the same type between the same endpoints
// already exists. See AddRelationshipByIDIfAbsentWithContext for details.
func (c *Core) AddRelationshipByIDIfAbsent(typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error) {
	return c.AddRelationshipByIDIfAbsentWithContext(context.Background(), typeName, startID, endID, props)
}

// DeleteNode atomically removes a node and all connected relationships.
// Acquires the entity lock for the node to prevent write-skew with concurrent
// AddRelationship targeting the same node.
// Returns storepkg.ErrNodeNotFound if the node does not exist.
func (c *Core) DeleteNode(id types.NodeID) error {
	return c.DeleteNodeWithContext(context.Background(), id)
}

// DeleteRelationship removes a relationship from the store.
// Returns storepkg.ErrRelNotFound if the relationship does not exist.
func (c *Core) DeleteRelationship(id types.RelID) error {
	return c.DeleteRelationshipWithContext(context.Background(), id)
}

// --- Update operations ---

// UpdateNode applies property updates to an existing node.
// The updates map keys are property names; values are the new values.
// A nil value deletes the property. Keys with the "tkg_" prefix are rejected.
// Returns the updated node. Empty updates map is a no-op (no version bump).
func (c *Core) UpdateNode(id types.NodeID, updates map[string]any) (*types.Node, error) {
	return c.UpdateNodeWithContext(context.Background(), id, updates)
}

// UpdateRelationship applies property updates to an existing relationship.
// The updates map keys are property names; values are the new values.
// A nil value deletes the property. Keys with the "tkg_" prefix are rejected.
// Returns the updated relationship. Empty updates map is a no-op.
func (c *Core) UpdateRelationship(id types.RelID, updates map[string]any) (*types.Relationship, error) {
	return c.UpdateRelationshipWithContext(context.Background(), id, updates)
}

// SetNodeProperty sets a single property on an existing node.
func (c *Core) SetNodeProperty(id types.NodeID, key string, value any) error {
	_, err := c.UpdateNode(id, map[string]any{key: value})
	return err
}

// DeleteNodeProperty removes a single property from an existing node.
func (c *Core) DeleteNodeProperty(id types.NodeID, key string) error {
	_, err := c.UpdateNode(id, map[string]any{key: nil})
	return err
}

// SetRelationshipProperty sets a single property on an existing relationship.
func (c *Core) SetRelationshipProperty(id types.RelID, key string, value any) error {
	_, err := c.UpdateRelationship(id, map[string]any{key: value})
	return err
}

// DeleteRelationshipProperty removes a single property from an existing relationship.
func (c *Core) DeleteRelationshipProperty(id types.RelID, key string) error {
	_, err := c.UpdateRelationship(id, map[string]any{key: nil})
	return err
}

// --- Version history passthrough ---

// GetNodeHistory returns all version history snapshots for the given node.
func (c *Core) GetNodeHistory(id types.NodeID) ([]*types.Node, error) {
	return c.store.GetNodeHistory(id)
}

// GetRelHistory returns all version history snapshots for the given relationship.
func (c *Core) GetRelHistory(id types.RelID) ([]*types.Relationship, error) {
	return c.store.GetRelHistory(id)
}
