package core

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Archive moves a reference node and its relationships from the reference
// shard to the reference archive. Only available with tiered.Store.
// Returns storepkg.ErrNodeNotFound if the node is not in the reference shard.
//
// Concurrency: takes c.mu.Lock — same exclusion class as a transaction.
// Archive reads adjacency, then runs cascade; without this lock a
// concurrent AddRelationship between the pre-scan and the cascade can
// create a cross-shard rel touching the soon-to-be-archived node, which
// the pre-scan misses and the cascade then partially destroys (the
// rel's adjacency entries on the partner shard would dangle). Archiving
// is a rare, batch-style admin operation; serializing against all
// writers is acceptable and mirrors the tx/batch lock discipline.
func (a *AdminOps) Archive(id types.NodeID) error {
	c := a.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if ts, ok := c.store.(*tiered.Store); ok {
		c.mu.Lock()
		defer c.mu.Unlock()
		return ts.ArchiveNode(id)
	}
	return ErrNotTieredStore
}

// Restore moves a reference node and its relationships from the reference
// archive back to the reference shard. Only available with tiered.Store.
// Returns storepkg.ErrNodeNotFound if the node is not in the archive.
//
// Concurrency: takes c.mu.Lock — see Archive for the rationale.
func (a *AdminOps) Restore(id types.NodeID) error {
	c := a.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if ts, ok := c.store.(*tiered.Store); ok {
		c.mu.Lock()
		defer c.mu.Unlock()
		return ts.RestoreNode(id)
	}
	return ErrNotTieredStore
}

// DecomposeID extracts the creation time, node ID, and sequence number from
// a snowflake ID. Works with any store type.
func (a *AdminOps) DecomposeID(id snowflake.ID) IDComponents {
	return DecomposeID(id)
}

// ForceRotate triggers a hot-shard rotation. Only available with tiered.Store.
//
// Concurrency: takes c.mu.Lock — ForceRotate mutates live shard topology and
// must be in the same exclusion class as Reset/Archive/Restore. Without
// this lock, Reset (which takes c.mu.Lock) can race ForceRotate's
// snapshot-then-clear and miss the new hot shard.
func (a *AdminOps) ForceRotate() error {
	c := a.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if ts, ok := c.store.(*tiered.Store); ok {
		c.mu.Lock()
		defer c.mu.Unlock()
		return ts.ForceRotate()
	}
	return ErrNotTieredStore
}

// ListShards returns information about all shards. Only available with tiered.Store.
//
// Concurrency: takes c.mu.RLock — read-only inspection that must observe a
// consistent topology, so it joins the tx/batch exclusion class as a reader.
func (a *AdminOps) ListShards() ([]tiered.ShardInfo, error) {
	c := a.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if ts, ok := c.store.(*tiered.Store); ok {
		c.mu.RLock()
		defer c.mu.RUnlock()
		return ts.ListShards()
	}
	return nil, ErrNotTieredStore
}

// RebuildCatalog reconstructs the shard catalog from live state.
// Only available with tiered.Store.
//
// Concurrency: takes c.mu.Lock — rewrites the persisted catalog and must
// not race a concurrent ForceRotate or any tx/batch that could change shard
// state mid-rebuild.
func (a *AdminOps) RebuildCatalog() error {
	c := a.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if ts, ok := c.store.(*tiered.Store); ok {
		c.mu.Lock()
		defer c.mu.Unlock()
		return ts.RebuildCatalog()
	}
	return ErrNotTieredStore
}

// Repair scans for cross-shard consistency issues and fixes them.
// Only available with tiered.Store.
//
// Concurrency: takes c.mu.Lock — Phase 2 of repair reads a relationship
// from one shard and may recreate a missing incoming-index entry on
// another shard. Without the graph lock, a concurrent rel delete that
// removes the entity and incoming entry between Repair's read and write
// re-creates an orphaned incoming entry that the delete just removed.
func (a *AdminOps) Repair() (*tiered.RepairResult, error) {
	c := a.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if ts, ok := c.store.(*tiered.Store); ok {
		c.mu.Lock()
		defer c.mu.Unlock()
		return ts.RunRepair()
	}
	return nil, ErrNotTieredStore
}

// VerifyShard runs hash chain verification on all entities in a shard.
// Only available with tiered.Store.
//
// Takes c.mu.RLock. The RLock excludes tx/batch but NOT individual
// standalone mutations (which also use c.mu.RLock — R4-F4), so the
// scan can surface a half-written hash chain on a freshly-updated
// entity. For a strict check (no concurrent writers at all), call
// (*GraphTx).VerifyShard from inside g.Tx.Run; the tx already holds
// c.mu.Lock, so the underlying verifyShardLocked runs without
// re-entering the lock.
func (a *AdminOps) VerifyShard(shardName string) (*tiered.VerifyResult, error) {
	c := a.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.verifyShardLocked(shardName)
}

// verifyShardLocked runs the per-shard hash chain verification. Caller
// must hold c.mu.RLock OR c.mu.Lock — the standalone path uses RLock,
// the tx path uses Lock. verifyShardLocked itself takes no graph-level
// locks. Returns ErrNotTieredStore if the store does not support
// shard-level verification.
func (c *Core) verifyShardLocked(shardName string) (*tiered.VerifyResult, error) {
	if ts, ok := c.store.(*tiered.Store); ok {
		return ts.VerifyShard(c.Hash, shardName)
	}
	return nil, ErrNotTieredStore
}

// Reset atomically clears all entities, indexes, history, and counters from
// the graph while preserving registries (label and relationship type tokens).
// Acquires the graph write lock to prevent concurrent operations.
func (a *AdminOps) Reset() error {
	c := a.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.Clear()
}
