package graph

import (
	"fmt"

	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// MigrateFromBadger copies all nodes and relationships from a single BadgerStore
// into a TieredStore. Entities are routed by the TieredStore's ontology — reference
// labels go to refShard, event labels go to the hot event shard.
//
// Uses paginated iteration (ForEachNodeID + GetNode) instead of materializing all
// entities into memory, making it safe for large graphs.
//
// No history migration: hash chains would need re-creation. This handles the 95%
// case of migrating a single-BadgerStore deployment to tiered layout.
//
// The label registry must be wired to the TieredStore before calling this function
// (via SetLabelRegistry).
func MigrateFromBadger(src *BadgerStore, dst *TieredStore, labels *indexpkg.LabelRegistry) error {
	// Wire ontology for routing.
	dst.SetLabelRegistry(labels)

	// Migrate nodes one at a time via ForEachNodeID.
	var migrateErr error
	if err := src.ForEachNodeID(func(id types.NodeID) bool {
		n, err := src.GetNode(id)
		if err != nil {
			migrateErr = fmt.Errorf("graph: migrate: get node %d: %w", id, err)
			return false
		}
		if err := dst.PutNode(n); err != nil {
			migrateErr = fmt.Errorf("graph: migrate: put node %d: %w", id, err)
			return false
		}
		return true
	}); err != nil {
		return fmt.Errorf("graph: migrate: iterate nodes: %w", err)
	}
	if migrateErr != nil {
		return migrateErr
	}

	// Migrate relationships one at a time via ForEachRelID.
	if err := src.ForEachRelID(func(id types.RelID) bool {
		r, err := src.GetRelationship(id)
		if err != nil {
			migrateErr = fmt.Errorf("graph: migrate: get rel %d: %w", id, err)
			return false
		}
		if err := dst.PutRelationship(r); err != nil {
			migrateErr = fmt.Errorf("graph: migrate: put rel %d: %w", id, err)
			return false
		}
		return true
	}); err != nil {
		return fmt.Errorf("graph: migrate: iterate rels: %w", err)
	}
	if migrateErr != nil {
		return migrateErr
	}

	return nil
}
