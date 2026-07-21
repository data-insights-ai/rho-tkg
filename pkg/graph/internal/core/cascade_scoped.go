package core

import (
	"context"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// This file wires the four store doors the bitemporal cascade
// (cascadeNodeVersionInterval / cascadeRelVersionInterval in
// temporal_cascade.go) calls through their BACKLOG 11f Batch E scoped
// siblings — PutNodeVersion/ReplaceNode/PutRelVersion/ReplaceRelationship —
// mirroring putGeneratedNode's exact routing pattern. FOUNDATION ONLY:
// nothing constructs a token-carrying ctx yet, so every branch here is
// currently dead in production; token == 0 (every real call today) falls
// straight through to the unscoped door, byte-identical to before this
// batch. The cascade's own append-only history/current-row logic in
// temporal_cascade.go is UNCHANGED — these wrappers only decide where the
// resulting change-log record lands, never what gets written.

func (c *Core) putNodeVersionScopedAware(ctx context.Context, id types.NodeID, version uint32, n *types.Node) error {
	if token, ok := scopeTokenFrom(ctx); ok && token != 0 {
		if scoped, ok := c.store.(storepkg.ScopedCascadeCapability); ok {
			return scoped.PutNodeVersionScoped(id, version, n, token)
		}
	}
	return c.store.PutNodeVersion(id, version, n)
}

func (c *Core) replaceNodeScopedAware(ctx context.Context, n *types.Node) error {
	if token, ok := scopeTokenFrom(ctx); ok && token != 0 {
		if scoped, ok := c.store.(storepkg.ScopedCascadeCapability); ok {
			return scoped.ReplaceNodeScoped(n, token)
		}
	}
	return c.store.ReplaceNode(n)
}

func (c *Core) putRelVersionScopedAware(ctx context.Context, id types.RelID, version uint32, r *types.Relationship) error {
	if token, ok := scopeTokenFrom(ctx); ok && token != 0 {
		if scoped, ok := c.store.(storepkg.ScopedCascadeCapability); ok {
			return scoped.PutRelVersionScoped(id, version, r, token)
		}
	}
	return c.store.PutRelVersion(id, version, r)
}

func (c *Core) replaceRelationshipScopedAware(ctx context.Context, r *types.Relationship) error {
	if token, ok := scopeTokenFrom(ctx); ok && token != 0 {
		if scoped, ok := c.store.(storepkg.ScopedCascadeCapability); ok {
			return scoped.ReplaceRelationshipScoped(r, token)
		}
	}
	return c.store.ReplaceRelationship(r)
}
