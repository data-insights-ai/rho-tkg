package core

import (
	"context"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Lend returns the node's current row like Get, but without Get's defensive
// copy: the store's own FROZEN row, the same shared pointer a label scan or
// GetByIDs hands out.
//
// Lifetime: the row is never written after it is lent (a write stores a new
// row and leaves this one as it was), so it may be held and read from any
// goroutine for as long as the caller wants. It is a snapshot of the entity at
// the read; it does not follow later writes. A caller that keeps lent rows
// across writes re-checks NodeMutationEpoch (or its own key) to know whether
// they are still current.
//
// Mutation: the row is frozen. Its error-returning mutators fail with
// types.ErrFrozenNode and its no-error mutators panic; DeepCopy returns a
// mutable copy. Never mutate a lent row through any other means.
//
// On memory, badger and tiered stores no copy is made. On any other store
// (sharded, a wrapper, an external backend) the row is the defensive copy Get
// makes, frozen before it is returned, so the contract is the same everywhere.
// Errors as Get: ErrNodeNotFound, ErrInvalidStoreMutation for a non-positive
// ID, ErrGraphClosed, the context's error.
func (n *NodeOps) Lend(ctx context.Context, id types.NodeID) (*types.Node, error) {
	c := n.c
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	c.mu.RLock()
	if c.closed.Load() {
		c.mu.RUnlock()
		return nil, ErrGraphClosed
	}
	var (
		node *types.Node
		err  error
	)
	if lender, ok := c.store.(storepkg.EntityLendCapability); ok && c.storeRowsTrust {
		node, err = lender.LendNode(id)
	} else if node, err = c.getCurrentNode(id); err == nil {
		node.Freeze()
	}
	c.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	if ctxErr := checkCtx(ctx); ctxErr != nil {
		return nil, ctxErr
	}
	c.opNodeReads.Add(1)
	return node, nil
}

// Lend is NodeOps.Lend for relationships: the current row frozen and without
// a copy on memory, badger and tiered stores (a frozen copy elsewhere); the
// same lifetime and mutation rules (types.ErrFrozenRelationship). On a
// memory store a relationship of a sealed segment (ADR-0011) is decoded per
// call. Errors as Get: ErrRelNotFound, ErrInvalidStoreMutation,
// ErrGraphClosed, the context's error.
func (r *RelOps) Lend(ctx context.Context, id types.RelID) (*types.Relationship, error) {
	c := r.c
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	c.mu.RLock()
	if c.closed.Load() {
		c.mu.RUnlock()
		return nil, ErrGraphClosed
	}
	var (
		rel *types.Relationship
		err error
	)
	if lender, ok := c.store.(storepkg.EntityLendCapability); ok && c.storeRowsTrust {
		rel, err = lender.LendRelationship(id)
	} else if rel, err = c.getCurrentRelationship(id); err == nil {
		rel.Freeze()
	}
	c.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	if ctxErr := checkCtx(ctx); ctxErr != nil {
		return nil, ctxErr
	}
	c.opRelReads.Add(1)
	return rel, nil
}
