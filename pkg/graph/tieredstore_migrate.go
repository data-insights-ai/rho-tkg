package graph

import "fmt"

// MigrateFromBadger copies all nodes and relationships from a single BadgerStore
// into a TieredStore. Entities are routed by the TieredStore's ontology — reference
// labels go to refShard, event labels go to the hot event shard.
//
// No history migration: hash chains would need re-creation. This handles the 95%
// case of migrating a single-BadgerStore deployment to tiered layout.
//
// The label registry must be wired to the TieredStore before calling this function
// (via SetLabelRegistry).
func MigrateFromBadger(src *BadgerStore, dst *TieredStore, labels *labelRegistry) error {
	// Wire ontology for routing.
	dst.SetLabelRegistry(labels)

	// Migrate all nodes.
	nodes, err := src.AllNodes(QueryOpts{})
	if err != nil {
		return fmt.Errorf("graph: migrate: read nodes: %w", err)
	}
	for _, n := range nodes {
		if err := dst.PutNode(n); err != nil {
			return fmt.Errorf("graph: migrate: put node %d: %w", n.InternalID().SnowflakeID(), err)
		}
	}

	// Migrate all relationships.
	rels, err := src.AllRelationships(QueryOpts{})
	if err != nil {
		return fmt.Errorf("graph: migrate: read rels: %w", err)
	}
	for _, r := range rels {
		if err := dst.PutRelationship(r); err != nil {
			return fmt.Errorf("graph: migrate: put rel %d: %w", r.InternalID().SnowflakeID(), err)
		}
	}

	return nil
}
