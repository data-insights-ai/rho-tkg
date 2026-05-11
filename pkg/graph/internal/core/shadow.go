package core

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// NodeProperty resolves a property key on a node.
// For non-tkg_ keys, delegates to the node's PropertySlice.
// For tkg_ shadow keys, resolves from the node's metadata via the graph layer.
// Returns (nil, false) for unknown shadow keys or inapplicable keys (e.g., tkg_type on a node).
func (r *ResolveOps) NodeProperty(n *types.Node, key string) (any, bool) {
	if n == nil {
		return nil, false
	}
	c := r.c
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return nil, false
	}
	if !types.IsShadowKey(key) {
		return n.GetProperty(key)
	}

	switch key {
	// Structural
	case types.ShadowLabels:
		return c.nodeLabelsUnlocked(n), true
	case types.ShadowType:
		return nil, false // rel-only

	// Temporal
	case types.ShadowValidFrom:
		if tm := n.Temporal(); tm != nil {
			return tm.ValidFrom, true
		}
		return nil, false
	case types.ShadowValidTo:
		if tm := n.Temporal(); tm != nil {
			return tm.ValidTo, true
		}
		return nil, false
	case types.ShadowTxFrom:
		if tm := n.Temporal(); tm != nil {
			return tm.TxFrom, true
		}
		return nil, false
	case types.ShadowTxTo:
		if tm := n.Temporal(); tm != nil {
			return tm.TxTo, true
		}
		return nil, false
	case types.ShadowCreatedAt:
		if tm := n.Temporal(); tm != nil && tm.CreatedAt != 0 {
			return tm.CreatedAt, true
		}
		return types.Instant(c.nodeIDGen.CreatedAt(n.ID().SnowflakeID()).UnixMilli()), true
	case types.ShadowUpdatedAt:
		if tm := n.Temporal(); tm != nil {
			return tm.UpdatedAt, true
		}
		return nil, false
	case types.ShadowDeletedAt:
		if tm := n.Temporal(); tm != nil {
			return tm.DeletedAt, true
		}
		return nil, false

	// Provenance
	case types.ShadowCreatedBy:
		if tm := n.Temporal(); tm != nil {
			return tm.CreatedBy, true
		}
		return nil, false
	case types.ShadowUpdatedBy:
		if tm := n.Temporal(); tm != nil {
			return tm.UpdatedBy, true
		}
		return nil, false
	case types.ShadowVersion:
		return n.Version(), true

	// Integrity
	case types.ShadowHash:
		if ig := n.Integrity(); ig != nil {
			return ig.Hash, true
		}
		return nil, false
	case types.ShadowPrevHash:
		if ig := n.Integrity(); ig != nil {
			return ig.PrevHash, true
		}
		return nil, false
	case types.ShadowFromHash, types.ShadowToHash:
		return nil, false // rel-only
	case types.ShadowAuthorID:
		if ig := n.Integrity(); ig != nil {
			return ig.AuthorID, true
		}
		return nil, false
	case types.ShadowSignature:
		if ig := n.Integrity(); ig != nil {
			return ig.Signature, true
		}
		return nil, false
	case types.ShadowAuthorizedBy:
		if ig := n.Integrity(); ig != nil {
			return ig.AuthorizedBy, true
		}
		return nil, false
	case types.ShadowAuthLevel:
		if ig := n.Integrity(); ig != nil {
			return ig.AuthorizationLevel, true
		}
		return nil, false

	// Version chain
	case types.ShadowBaseEntity:
		if tm := n.Temporal(); tm != nil {
			return tm.BaseEntityID(), true
		}
		return nil, false

	default:
		return nil, false
	}
}

// RelProperty resolves a property key on a relationship.
// For non-tkg_ keys, delegates to the relationship's PropertySlice.
// For tkg_ shadow keys, resolves from the relationship's metadata via the graph layer.
// Returns (nil, false) for unknown shadow keys or inapplicable keys (e.g., tkg_labels on a rel).
func (r *ResolveOps) RelProperty(rel *types.Relationship, key string) (any, bool) {
	if rel == nil {
		return nil, false
	}
	c := r.c
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return nil, false
	}
	if !types.IsShadowKey(key) {
		return rel.GetProperty(key)
	}

	switch key {
	// Structural
	case types.ShadowLabels:
		return nil, false // node-only
	case types.ShadowType:
		return c.relTypeUnlocked(rel), true

	// Temporal
	case types.ShadowValidFrom:
		if tm := rel.Temporal(); tm != nil {
			return tm.ValidFrom, true
		}
		return nil, false
	case types.ShadowValidTo:
		if tm := rel.Temporal(); tm != nil {
			return tm.ValidTo, true
		}
		return nil, false
	case types.ShadowTxFrom:
		if tm := rel.Temporal(); tm != nil {
			return tm.TxFrom, true
		}
		return nil, false
	case types.ShadowTxTo:
		if tm := rel.Temporal(); tm != nil {
			return tm.TxTo, true
		}
		return nil, false
	case types.ShadowCreatedAt:
		if tm := rel.Temporal(); tm != nil && tm.CreatedAt != 0 {
			return tm.CreatedAt, true
		}
		return types.Instant(c.relIDGen.CreatedAt(rel.ID().SnowflakeID()).UnixMilli()), true
	case types.ShadowUpdatedAt:
		if tm := rel.Temporal(); tm != nil {
			return tm.UpdatedAt, true
		}
		return nil, false
	case types.ShadowDeletedAt:
		if tm := rel.Temporal(); tm != nil {
			return tm.DeletedAt, true
		}
		return nil, false

	// Provenance
	case types.ShadowCreatedBy:
		if tm := rel.Temporal(); tm != nil {
			return tm.CreatedBy, true
		}
		return nil, false
	case types.ShadowUpdatedBy:
		if tm := rel.Temporal(); tm != nil {
			return tm.UpdatedBy, true
		}
		return nil, false
	case types.ShadowVersion:
		return rel.Version(), true

	// Integrity
	case types.ShadowHash:
		if ig := rel.Integrity(); ig != nil {
			return ig.Hash, true
		}
		return nil, false
	case types.ShadowPrevHash:
		if ig := rel.Integrity(); ig != nil {
			return ig.PrevHash, true
		}
		return nil, false
	case types.ShadowFromHash:
		if ig := rel.Integrity(); ig != nil {
			return ig.FromNodeHash, true
		}
		return nil, false
	case types.ShadowToHash:
		if ig := rel.Integrity(); ig != nil {
			return ig.ToNodeHash, true
		}
		return nil, false
	case types.ShadowAuthorID:
		if ig := rel.Integrity(); ig != nil {
			return ig.AuthorID, true
		}
		return nil, false
	case types.ShadowSignature:
		if ig := rel.Integrity(); ig != nil {
			return ig.Signature, true
		}
		return nil, false
	case types.ShadowAuthorizedBy:
		if ig := rel.Integrity(); ig != nil {
			return ig.AuthorizedBy, true
		}
		return nil, false
	case types.ShadowAuthLevel:
		if ig := rel.Integrity(); ig != nil {
			return ig.AuthorizationLevel, true
		}
		return nil, false

	// Version chain
	case types.ShadowBaseEntity:
		if tm := rel.Temporal(); tm != nil {
			return tm.BaseEntityID(), true
		}
		return nil, false

	default:
		return nil, false
	}
}
