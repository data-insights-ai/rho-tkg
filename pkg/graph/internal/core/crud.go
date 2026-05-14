package core

import (
	"context"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Convenience helpers (single-property update) ---
//
// API 4.0: the previous separate (Add, AddWithContext), (Delete, DeleteWithContext),
// (Update, UpdateWithContext), (UpdateInPlace, UpdateInPlaceWithContext),
// (Get, GetWithContext), (CompareAndSetProperty, CompareAndSetPropertyWithContext)
// pairs collapsed into one context-aware method each. The previous *WithContext
// variants were renamed to drop the suffix; the old short forms were removed.
// Pass context.Background() for the previous no-ctx behavior.

// SetProperty sets a single property on an existing node. Internally delegates
// to Update; ctx is honoured by the Update lifecycle gates (S1).
func (n *NodeOps) SetProperty(ctx context.Context, id types.NodeID, key string, value any) error {
	c := n.c
	_, err := c.Nodes.Update(ctx, id, map[string]any{key: value})
	return err
}

// DeleteProperty removes a single property from an existing node. Internally
// delegates to Update; ctx is honoured by the Update lifecycle gates (S1).
func (n *NodeOps) DeleteProperty(ctx context.Context, id types.NodeID, key string) error {
	c := n.c
	_, err := c.Nodes.Update(ctx, id, map[string]any{key: nil})
	return err
}

// SetProperty sets a single property on an existing relationship. Internally
// delegates to Update; ctx is honoured by the Update lifecycle gates (S1).
func (r *RelOps) SetProperty(ctx context.Context, id types.RelID, key string, value any) error {
	c := r.c
	_, err := c.Rels.Update(ctx, id, map[string]any{key: value})
	return err
}

// DeleteProperty removes a single property from an existing relationship.
// Internally delegates to Update; ctx is honoured by the Update lifecycle gates (S1).
func (r *RelOps) DeleteProperty(ctx context.Context, id types.RelID, key string) error {
	c := r.c
	_, err := c.Rels.Update(ctx, id, map[string]any{key: nil})
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
