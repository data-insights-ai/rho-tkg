package core

import (
	"context"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Convenience helpers (single-property update) ---
//
// Each method is context-aware; pass context.Background() when no context is needed.

// SetProperty sets a single property on an existing node. Internally delegates
// to Update; ctx is honoured by the Update lifecycle gates.
func (n *NodeOps) SetProperty(ctx context.Context, id types.NodeID, key string, value any) error {
	c := n.c
	_, err := c.Nodes.Update(ctx, id, map[string]any{key: value})
	return err
}

// DeleteProperty removes a single property from an existing node. Internally
// delegates to Update; ctx is honoured by the Update lifecycle gates.
func (n *NodeOps) DeleteProperty(ctx context.Context, id types.NodeID, key string) error {
	c := n.c
	_, err := c.Nodes.Update(ctx, id, map[string]any{key: nil})
	return err
}

// SetProperty sets a single property on an existing relationship. Internally
// delegates to Update; ctx is honoured by the Update lifecycle gates.
func (r *RelOps) SetProperty(ctx context.Context, id types.RelID, key string, value any) error {
	c := r.c
	_, err := c.Rels.Update(ctx, id, map[string]any{key: value})
	return err
}

// DeleteProperty removes a single property from an existing relationship.
// Internally delegates to Update; ctx is honoured by the Update lifecycle gates.
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
