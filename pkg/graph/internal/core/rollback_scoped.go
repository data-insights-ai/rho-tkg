package core

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// This file wires GraphTx.Rollback's reverse-mutation path through the
// BACKLOG 11f Batch F store.ScopedRollbackCapability doors. Unlike the
// forward-path *ScopedAware helpers (cascade_scoped.go et al.), which route
// via a ctx-carried token (scopeTokenFrom), these take the token directly —
// Rollback's helper methods have no ctx parameter to thread one through, and
// they already have direct access to tx.g and tx.scopeToken. Each helper
// checks c.scopedChangeLog (the cached, pre-type-asserted capability set at
// Core construction — see scopedTxCapability) rather than re-type-asserting
// c.store, since c.scopedChangeLog IS that assertion result. token == 0
// (foundation-only today, since nothing constructs a nonzero
// GraphTx.scopeToken until BeginTx does) or c.scopedChangeLog == nil (store
// doesn't support the full contract) both fall straight through to the
// plain unscoped door, byte-identical to before this batch.

func (c *Core) deleteRelationshipScopedAware(token uint64, id types.RelID) error {
	if token != 0 && c.scopedChangeLog != nil {
		return c.scopedChangeLog.DeleteRelationshipScoped(id, token)
	}
	return c.store.DeleteRelationship(id)
}

func (c *Core) deleteNodeCascadeScopedAware(token uint64, id types.NodeID) error {
	if token != 0 && c.scopedChangeLog != nil {
		return c.scopedChangeLog.DeleteNodeCascadeScoped(id, token)
	}
	return c.store.DeleteNodeCascade(id)
}

func (c *Core) truncateNodeHistoryScopedAware(token uint64, id types.NodeID, keepVersions int) error {
	if token != 0 && c.scopedChangeLog != nil {
		return c.scopedChangeLog.TruncateNodeHistoryScoped(id, keepVersions, token)
	}
	return c.store.TruncateNodeHistory(id, keepVersions)
}

func (c *Core) truncateRelHistoryScopedAware(token uint64, id types.RelID, keepVersions int) error {
	if token != 0 && c.scopedChangeLog != nil {
		return c.scopedChangeLog.TruncateRelHistoryScoped(id, keepVersions, token)
	}
	return c.store.TruncateRelHistory(id, keepVersions)
}

func (c *Core) addNodeLabelTokenScopedAware(token uint64, id types.NodeID, tok uint16, updatedNode *types.Node) error {
	if token != 0 && c.scopedChangeLog != nil {
		return c.scopedChangeLog.AddNodeLabelTokenScoped(id, tok, updatedNode, token)
	}
	return c.store.AddNodeLabelToken(id, tok, updatedNode)
}

func (c *Core) removeNodeLabelTokenScopedAware(token uint64, id types.NodeID, tok uint16, updatedNode *types.Node) error {
	if token != 0 && c.scopedChangeLog != nil {
		return c.scopedChangeLog.RemoveNodeLabelTokenScoped(id, tok, updatedNode, token)
	}
	return c.store.RemoveNodeLabelToken(id, tok, updatedNode)
}

func (c *Core) trimNodeHistoryFromScopedAware(token uint64, id types.NodeID, minVersion uint32) error {
	if token != 0 && c.scopedChangeLog != nil {
		return c.scopedChangeLog.TrimNodeHistoryFromScoped(id, minVersion, token)
	}
	return c.historyTrim.TrimNodeHistoryFrom(id, minVersion)
}

func (c *Core) trimRelHistoryFromScopedAware(token uint64, id types.RelID, minVersion uint32) error {
	if token != 0 && c.scopedChangeLog != nil {
		return c.scopedChangeLog.TrimRelHistoryFromScoped(id, minVersion, token)
	}
	return c.historyTrim.TrimRelHistoryFrom(id, minVersion)
}

// putNodeVersionScopedAwareToken / replaceNodeScopedAwareToken /
// putRelVersionScopedAwareToken / replaceRelationshipScopedAwareToken are
// the direct-token counterparts of cascade_scoped.go's ctx-based helpers,
// used by Rollback's restoreNodeHistory/restoreRelHistory/
// restoreUpdatedNode/restoreDeletedNodeRow/restoreDeletedRelRow, which have
// no ctx to thread scopeTokenFrom through.
func (c *Core) putNodeVersionScopedAwareToken(token uint64, id types.NodeID, version uint32, n *types.Node) error {
	if token != 0 && c.scopedChangeLog != nil {
		return c.scopedChangeLog.PutNodeVersionScoped(id, version, n, token)
	}
	return c.store.PutNodeVersion(id, version, n)
}

func (c *Core) replaceNodeScopedAwareToken(token uint64, n *types.Node) error {
	if token != 0 && c.scopedChangeLog != nil {
		return c.scopedChangeLog.ReplaceNodeScoped(n, token)
	}
	return c.store.ReplaceNode(n)
}

func (c *Core) putRelVersionScopedAwareToken(token uint64, id types.RelID, version uint32, r *types.Relationship) error {
	if token != 0 && c.scopedChangeLog != nil {
		return c.scopedChangeLog.PutRelVersionScoped(id, version, r, token)
	}
	return c.store.PutRelVersion(id, version, r)
}

func (c *Core) replaceRelationshipScopedAwareToken(token uint64, r *types.Relationship) error {
	if token != 0 && c.scopedChangeLog != nil {
		return c.scopedChangeLog.ReplaceRelationshipScoped(r, token)
	}
	return c.store.ReplaceRelationship(r)
}

// putNodeScopedAwareToken / putRelationshipScopedAwareToken are the
// direct-token counterparts used by Rollback's restoreDeletedNodeRow /
// restoreDeletedRelRow for the "row never existed at rollback time" branch
// (a plain PutNode/PutRelationship, not a replace).
func (c *Core) putNodeScopedAwareToken(token uint64, n *types.Node) error {
	if token != 0 && c.scopedChangeLog != nil {
		return c.scopedChangeLog.PutNodeScoped(n, token)
	}
	return c.store.PutNode(n)
}

func (c *Core) putRelationshipScopedAwareToken(token uint64, r *types.Relationship) error {
	if token != 0 && c.scopedChangeLog != nil {
		return c.scopedChangeLog.PutRelationshipScoped(r, token)
	}
	return c.store.PutRelationship(r)
}
