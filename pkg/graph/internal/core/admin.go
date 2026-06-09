package core

import (
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type tieredAdminStore interface {
	ArchiveNode(types.NodeID) error
	RestoreNode(types.NodeID) error
	ForceRotate() error
	ListShards() ([]tiered.ShardInfo, error)
	RebuildCatalog() error
	RunRepair() (*tiered.RepairResult, error)
	VerifyShard(tiered.HashChainVerifier, string) (*tiered.VerifyResult, error)
}

type tieredRegistryLoader interface {
	LoadLabelRegistry(*registrypkg.LabelRegistry) (int, error)
	LoadRelTypeRegistry(*registrypkg.RelTypeRegistry) (int, error)
}

type tieredLabelRegistrySetter interface {
	SetLabelRegistry(*registrypkg.LabelRegistry)
}

func setTieredLabelRegistryIfSupported(store storepkg.MandatoryStore, labels *registrypkg.LabelRegistry) {
	if setter, ok := store.(tieredLabelRegistrySetter); ok {
		setter.SetLabelRegistry(labels)
	}
}

// Archive moves a reference node and its relationships from the reference
// shard to the reference archive. Only available with tiered.Store.
// Returns tiered.ErrNotReferenceEntity when the node exists but is not a
// reference entity, and storepkg.ErrNodeNotFound when the node does not exist
// in an archivable location.
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return ErrGraphClosed
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return err
	}
	if ts, ok := c.store.(tieredAdminStore); ok {
		return ts.ArchiveNode(id)
	}
	return ErrNotTieredStore
}

// Restore moves a reference node and its relationships from the reference
// archive back to the reference shard. Only available with tiered.Store.
// Returns tiered.ErrNotReferenceEntity when the archived row exists but is not
// a reference entity, and storepkg.ErrNodeNotFound when the node is not in the
// archive.
//
// Concurrency: takes c.mu.Lock — see Archive for the rationale.
func (a *AdminOps) Restore(id types.NodeID) error {
	c := a.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return ErrGraphClosed
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return err
	}
	if ts, ok := c.store.(tieredAdminStore); ok {
		return ts.RestoreNode(id)
	}
	return ErrNotTieredStore
}

// DecomposeNodeID extracts the creation time, node ID, and sequence number
// from a NodeID. Works with any store type.
func (a *AdminOps) DecomposeNodeID(id types.NodeID) IDComponents {
	return DecomposeID(id.SnowflakeID())
}

// DecomposeRelID extracts the creation time, node ID, and sequence number
// from a RelID. Works with any store type.
func (a *AdminOps) DecomposeRelID(id types.RelID) IDComponents {
	return DecomposeID(id.SnowflakeID())
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return ErrGraphClosed
	}
	if ts, ok := c.store.(tieredAdminStore); ok {
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
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return nil, ErrGraphClosed
	}
	if ts, ok := c.store.(tieredAdminStore); ok {
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return ErrGraphClosed
	}
	if ts, ok := c.store.(tieredAdminStore); ok {
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return nil, ErrGraphClosed
	}
	if ts, ok := c.store.(tieredAdminStore); ok {
		return ts.RunRepair()
	}
	return nil, ErrNotTieredStore
}

// VerifyShard runs hash chain verification on all entities in a shard.
// Only available with tiered.Store.
//
// Takes c.mu.Lock so standalone mutations, tx/batch, and Reset cannot
// interleave with the hash-chain scan. Code that is already inside a
// transaction must call (*GraphTx).VerifyShard because sync.RWMutex is
// not reentrant.
func (a *AdminOps) VerifyShard(shardName string) (*tiered.VerifyResult, error) {
	c := a.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return nil, ErrGraphClosed
	}
	return c.verifyShardLocked(shardName)
}

// verifyShardLocked runs the per-shard hash chain verification. Caller
// must hold c.mu.Lock. verifyShardLocked itself takes no graph-level locks.
// Returns ErrNotTieredStore if the store does not support shard-level
// verification.
func (c *Core) verifyShardLocked(shardName string) (*tiered.VerifyResult, error) {
	if ts, ok := c.store.(tieredAdminStore); ok {
		return ts.VerifyShard(lockedHashVerifier{c: c}, shardName)
	}
	return nil, ErrNotTieredStore
}

type lockedHashVerifier struct {
	c *Core
}

func (v lockedHashVerifier) VerifyNodeChain(id types.NodeID) (bool, error) {
	return v.c.verifyNodeChainLocked(id)
}

func (v lockedHashVerifier) VerifyRelChain(id types.RelID) (bool, error) {
	return v.c.verifyRelChainLocked(id)
}

// Reset clears all entities, indexes, history, and counters from the graph
// while preserving registries (label and relationship type tokens).
// Acquires the graph write lock to prevent concurrent operations.
func (a *AdminOps) Reset() error {
	c := a.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return ErrGraphClosed
	}
	if err := c.store.Clear(); err != nil {
		return err
	}
	c.restoreOpCounters(opCounterSnapshot{})
	return c.persistRegistries()
}
