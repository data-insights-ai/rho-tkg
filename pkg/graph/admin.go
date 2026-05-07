package graph

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// ArchiveNode moves a reference node and its relationships from the reference
// shard to the reference archive. Only available with TieredStore.
// Returns ErrNodeNotFound if the node is not in the reference shard.
//
// Concurrency: takes g.mu.Lock — same exclusion class as a transaction.
// ArchiveNode reads adjacency, then runs cascade; without this lock a
// concurrent AddRelationship between the pre-scan and the cascade can
// create a cross-shard rel touching the soon-to-be-archived node, which
// the pre-scan misses and the cascade then partially destroys (the
// rel's adjacency entries on the partner shard would dangle). Archiving
// is a rare, batch-style admin operation; serializing against all
// writers is acceptable and mirrors the tx/batch lock discipline.
func (g *Graph) ArchiveNode(id types.NodeID) error {
	if ts, ok := g.store.(*TieredStore); ok {
		g.mu.Lock()
		defer g.mu.Unlock()
		return ts.ArchiveNode(id)
	}
	return ErrNotTieredStore
}

// RestoreNode moves a reference node and its relationships from the reference
// archive back to the reference shard. Only available with TieredStore.
// Returns ErrNodeNotFound if the node is not in the archive.
//
// Concurrency: takes g.mu.Lock — see ArchiveNode for the rationale.
func (g *Graph) RestoreNode(id types.NodeID) error {
	if ts, ok := g.store.(*TieredStore); ok {
		g.mu.Lock()
		defer g.mu.Unlock()
		return ts.RestoreNode(id)
	}
	return ErrNotTieredStore
}

// DecomposeID extracts the creation time, node ID, and sequence number from
// a snowflake ID. Works with any store type.
func (g *Graph) DecomposeID(id snowflake.ID) IDComponents {
	return DecomposeID(id)
}

// ForceRotate triggers a hot-shard rotation. Only available with TieredStore.
func (g *Graph) ForceRotate() error {
	if ts, ok := g.store.(*TieredStore); ok {
		return ts.ForceRotate()
	}
	return ErrNotTieredStore
}

// ListShards returns information about all shards. Only available with TieredStore.
func (g *Graph) ListShards() ([]ShardInfo, error) {
	if ts, ok := g.store.(*TieredStore); ok {
		return ts.ListShards()
	}
	return nil, ErrNotTieredStore
}

// RebuildCatalog reconstructs the shard catalog from live state.
// Only available with TieredStore.
func (g *Graph) RebuildCatalog() error {
	if ts, ok := g.store.(*TieredStore); ok {
		return ts.RebuildCatalog()
	}
	return ErrNotTieredStore
}

// RunRepair scans for cross-shard consistency issues and fixes them.
// Only available with TieredStore.
func (g *Graph) RunRepair() (*RepairResult, error) {
	if ts, ok := g.store.(*TieredStore); ok {
		return ts.RunRepair()
	}
	return nil, ErrNotTieredStore
}

// VerifyShard runs hash chain verification on all entities in a shard.
// Only available with TieredStore.
func (g *Graph) VerifyShard(shardName string) (*VerifyResult, error) {
	if ts, ok := g.store.(*TieredStore); ok {
		return ts.VerifyShard(g, shardName)
	}
	return nil, ErrNotTieredStore
}

// Reset atomically clears all entities, indexes, history, and counters from
// the graph while preserving registries (label and relationship type tokens).
// Acquires the graph write lock to prevent concurrent operations.
func (g *Graph) Reset() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.store.Clear()
}
