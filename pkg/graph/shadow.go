package graph

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// ResolveNodeProperty resolves a property key on a node.
// For non-tkg_ keys, delegates to the node's PropertySlice.
// For tkg_ shadow keys, resolves from the node's metadata via the graph layer.
// Returns (nil, false) for unknown shadow keys or inapplicable keys (e.g., tkg_type on a node).
func (g *Graph) ResolveNodeProperty(n *types.Node, key string) (any, bool) {
	if !types.IsShadowKey(key) {
		return n.GetProperty(key)
	}

	switch key {
	// Structural
	case types.ShadowLabels:
		return g.NodeLabels(n), true
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
		// Derive from snowflake ID — always available, millisecond precision.
		parts := g.nodeIDGen.Decompose(n.InternalID().SnowflakeID())
		return types.Instant(snowflakeEpoch.UnixMilli() + parts.Time), true
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

	// Version chain
	case types.ShadowBaseEntity:
		if tm := n.Temporal(); tm != nil {
			return tm.BaseEntityID().SnowflakeID(), true
		}
		return nil, false

	default:
		return nil, false
	}
}

// ResolveRelProperty resolves a property key on a relationship.
// For non-tkg_ keys, delegates to the relationship's PropertySlice.
// For tkg_ shadow keys, resolves from the relationship's metadata via the graph layer.
// Returns (nil, false) for unknown shadow keys or inapplicable keys (e.g., tkg_labels on a rel).
func (g *Graph) ResolveRelProperty(r *types.Relationship, key string) (any, bool) {
	if !types.IsShadowKey(key) {
		return r.GetProperty(key)
	}

	switch key {
	// Structural
	case types.ShadowLabels:
		return nil, false // node-only
	case types.ShadowType:
		return g.RelationshipType(r), true

	// Temporal
	case types.ShadowValidFrom:
		if tm := r.Temporal(); tm != nil {
			return tm.ValidFrom, true
		}
		return nil, false
	case types.ShadowValidTo:
		if tm := r.Temporal(); tm != nil {
			return tm.ValidTo, true
		}
		return nil, false
	case types.ShadowTxFrom:
		if tm := r.Temporal(); tm != nil {
			return tm.TxFrom, true
		}
		return nil, false
	case types.ShadowTxTo:
		if tm := r.Temporal(); tm != nil {
			return tm.TxTo, true
		}
		return nil, false
	case types.ShadowCreatedAt:
		if tm := r.Temporal(); tm != nil && tm.CreatedAt != 0 {
			return tm.CreatedAt, true
		}
		// Derive from snowflake ID — always available, millisecond precision.
		parts := g.relIDGen.Decompose(r.InternalID().SnowflakeID())
		return types.Instant(snowflakeEpoch.UnixMilli() + parts.Time), true
	case types.ShadowUpdatedAt:
		if tm := r.Temporal(); tm != nil {
			return tm.UpdatedAt, true
		}
		return nil, false
	case types.ShadowDeletedAt:
		if tm := r.Temporal(); tm != nil {
			return tm.DeletedAt, true
		}
		return nil, false

	// Provenance
	case types.ShadowCreatedBy:
		if tm := r.Temporal(); tm != nil {
			return tm.CreatedBy, true
		}
		return nil, false
	case types.ShadowUpdatedBy:
		if tm := r.Temporal(); tm != nil {
			return tm.UpdatedBy, true
		}
		return nil, false
	case types.ShadowVersion:
		return r.Version(), true

	// Integrity
	case types.ShadowHash:
		if ig := r.Integrity(); ig != nil {
			return ig.Hash, true
		}
		return nil, false
	case types.ShadowPrevHash:
		if ig := r.Integrity(); ig != nil {
			return ig.PrevHash, true
		}
		return nil, false

	// Version chain
	case types.ShadowBaseEntity:
		if tm := r.Temporal(); tm != nil {
			return tm.BaseEntityID().SnowflakeID(), true
		}
		return nil, false

	default:
		return nil, false
	}
}
