package core

import (
	"context"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Entity management ---

// Add creates a new node with the given labels and properties.
// Labels are resolved to tokens (created if needed). Properties are bulk-validated
// and sorted in O(N log N). Returns the created node with a generated snowflake ID.
func (n *NodeOps) Add(labels []string, props map[string]any) (*types.Node, error) {
	c := n.c
	return c.Nodes.AddWithContext(context.Background(), labels, props)
}

// Add creates a new directed relationship between two nodes.
// The type name is resolved to a token (created if needed). Properties are
// bulk-validated and sorted in O(N log N).
func (r *RelOps) Add(typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	c := r.c
	return c.Rels.AddWithContext(context.Background(), typeName, startNode, endNode, props)
}

// AddByID creates a relationship using endpoint node IDs
// without fetching the endpoint nodes. See AddRelationshipByIDWithContext for details.
func (r *RelOps) AddByID(typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error) {
	c := r.c
	return c.Rels.AddByIDWithContext(context.Background(), typeName, startID, endID, props)
}

// AddByIDIfAbsent atomically creates a relationship using endpoint
// node IDs only if no relationship of the same type between the same endpoints
// already exists. See AddRelationshipByIDIfAbsentWithContext for details.
func (r *RelOps) AddByIDIfAbsent(typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error) {
	c := r.c
	return c.Rels.AddByIDIfAbsentWithContext(context.Background(), typeName, startID, endID, props)
}

// Delete atomically removes a node and all connected relationships.
// Acquires the entity lock for the node to prevent write-skew with concurrent
// AddRelationship targeting the same node.
// Returns storepkg.ErrNodeNotFound if the node does not exist.
func (n *NodeOps) Delete(id types.NodeID) error {
	c := n.c
	return c.Nodes.DeleteWithContext(context.Background(), id)
}

// Delete removes a relationship from the store.
// Returns storepkg.ErrRelNotFound if the relationship does not exist.
func (r *RelOps) Delete(id types.RelID) error {
	c := r.c
	return c.Rels.DeleteWithContext(context.Background(), id)
}

// --- Update operations ---

// Update applies property updates to an existing node.
// The updates map keys are property names; values are the new values.
// A nil value deletes the property. Keys with the "tkg_" prefix are rejected.
// Returns the updated node. Empty updates map is a no-op (no version bump).
func (n *NodeOps) Update(id types.NodeID, updates map[string]any) (*types.Node, error) {
	c := n.c
	return c.Nodes.UpdateWithContext(context.Background(), id, updates)
}

// Update applies property updates to an existing relationship.
// The updates map keys are property names; values are the new values.
// A nil value deletes the property. Keys with the "tkg_" prefix are rejected.
// Returns the updated relationship. Empty updates map is a no-op.
func (r *RelOps) Update(id types.RelID, updates map[string]any) (*types.Relationship, error) {
	c := r.c
	return c.Rels.UpdateWithContext(context.Background(), id, updates)
}

// SetProperty sets a single property on an existing node.
func (n *NodeOps) SetProperty(id types.NodeID, key string, value any) error {
	c := n.c
	_, err := c.Nodes.Update(id, map[string]any{key: value})
	return err
}

// DeleteProperty removes a single property from an existing node.
func (n *NodeOps) DeleteProperty(id types.NodeID, key string) error {
	c := n.c
	_, err := c.Nodes.Update(id, map[string]any{key: nil})
	return err
}

// SetProperty sets a single property on an existing relationship.
func (r *RelOps) SetProperty(id types.RelID, key string, value any) error {
	c := r.c
	_, err := c.Rels.Update(id, map[string]any{key: value})
	return err
}

// DeleteProperty removes a single property from an existing relationship.
func (r *RelOps) DeleteProperty(id types.RelID, key string) error {
	c := r.c
	_, err := c.Rels.Update(id, map[string]any{key: nil})
	return err
}

// --- Version history passthrough ---

// History returns all version history snapshots for the given node.
func (n *NodeOps) History(id types.NodeID) ([]*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	var history []*types.Node
	err := c.readUnderRLock(func() error {
		var err error
		history, err = c.getNodeHistory(id)
		return err
	})
	return history, err
}

// History returns all version history snapshots for the given relationship.
func (r *RelOps) History(id types.RelID) ([]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	var history []*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		history, err = c.getRelHistory(id)
		return err
	})
	return history, err
}
