package graph

import (
	"fmt"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// ShardInfo describes a shard for admin API queries.
type ShardInfo struct {
	Name      string
	Kind      ShardKind
	Tier      ShardTier
	TimeStart time.Time
	TimeEnd   time.Time
	Nodes     int  // live count from open store, 0 if closed
	Rels      int  // live count from open store, 0 if closed
	Open      bool // whether the store is currently open
	Verified  bool
}

// VerifyResult holds the outcome of a per-shard hash chain verification.
type VerifyResult struct {
	ShardName   string
	NodesOK     int
	RelsOK      int
	NodesFailed int
	RelsFailed  int
	Cached      bool // true if result came from catalog cache
}

// ForceRotate triggers a hot-shard rotation with internal locking.
// Unlike RotateHotShard() which expects the caller to hold ts.mu.Lock,
// ForceRotate acquires the lock internally.
func (ts *TieredStore) ForceRotate() error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.RotateHotShard()
}

// ListShards returns information about all shards in the catalog, enriched
// with live counts from open stores.
func (ts *TieredStore) ListShards() []ShardInfo {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	var infos []ShardInfo

	// Reference shard.
	refEntry, _ := ts.catalog.GetShard("reference")
	refNodes, _ := ts.refShard.NodeCount()
	refRels, _ := ts.refShard.RelationshipCount()
	refInfo := ShardInfo{
		Name:     "reference",
		Kind:     ShardReference,
		Tier:     TierHot,
		Open:     true,
		Nodes:    refNodes,
		Rels:     refRels,
		Verified: refEntry != nil && refEntry.Verified,
	}
	infos = append(infos, refInfo)

	// Archive shard (if open or in catalog).
	if ts.refArchive != nil {
		archiveEntry, _ := ts.catalog.GetShard("archive")
		archNodes, _ := ts.refArchive.NodeCount()
		archRels, _ := ts.refArchive.RelationshipCount()
		archiveInfo := ShardInfo{
			Name:     "archive",
			Kind:     ShardArchive,
			Tier:     TierCold,
			Open:     true,
			Nodes:    archNodes,
			Rels:     archRels,
			Verified: archiveEntry != nil && archiveEntry.Verified,
		}
		infos = append(infos, archiveInfo)
	} else if ts.hasArchiveShard() {
		infos = append(infos, ShardInfo{
			Name: "archive",
			Kind: ShardArchive,
			Tier: TierCold,
			Open: false,
		})
	}

	// Event shards.
	for _, es := range ts.eventShards {
		entry, _ := ts.catalog.GetShard(es.name)
		si := ShardInfo{
			Name:      es.name,
			Kind:      ShardEvent,
			Tier:      es.tier,
			TimeStart: es.timeStart,
			TimeEnd:   es.timeEnd,
			Open:      es.store != nil,
			Verified:  entry != nil && entry.Verified,
		}
		if es.store != nil {
			si.Nodes, _ = es.store.NodeCount()
			si.Rels, _ = es.store.RelationshipCount()
		}
		infos = append(infos, si)
	}

	return infos
}

// RebuildCatalog reconstructs the shard catalog from the live in-memory state.
// Updates node/rel counts and tier info for all open shards.
func (ts *TieredStore) RebuildCatalog() error {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	// Update reference shard.
	refNodes, _ := ts.refShard.NodeCount()
	refRels, _ := ts.refShard.RelationshipCount()
	ts.catalog.UpdateShardStats("reference", refNodes, refRels)

	// Update archive if open.
	if ts.refArchive != nil {
		archNodes, _ := ts.refArchive.NodeCount()
		archRels, _ := ts.refArchive.RelationshipCount()
		ts.catalog.UpdateShardStats("archive", archNodes, archRels)
	}

	// Update event shards.
	for _, es := range ts.eventShards {
		ts.catalog.UpdateShardTier(es.name, es.tier)
		if es.store != nil {
			nc, _ := es.store.NodeCount()
			rc, _ := es.store.RelationshipCount()
			ts.catalog.UpdateShardStats(es.name, nc, rc)
		}
	}

	if !ts.inMemory {
		return ts.catalog.Save()
	}
	return nil
}

// VerifyShard runs hash chain verification on all entities in the named shard.
// Takes a *Graph because hash chain verification needs label/type resolution.
// For immutable shards (warm/cold) that have already been verified, returns
// the cached result without re-scanning.
func (ts *TieredStore) VerifyShard(g *Graph, shardName string) (*VerifyResult, error) {
	// Look up shard in catalog.
	entry, ok := ts.catalog.GetShard(shardName)
	if !ok {
		return nil, fmt.Errorf("graph: shard %q not found in catalog", shardName)
	}

	// Check if immutable and already verified → return cached result.
	isImmutable := entry.Tier == TierWarm || entry.Tier == TierCold
	if isImmutable && entry.Verified {
		return &VerifyResult{
			ShardName: shardName,
			NodesOK:   entry.ApproxNodes,
			RelsOK:    entry.ApproxRels,
			Cached:    true,
		}, nil
	}

	// Get the BadgerStore for this shard.
	store, err := ts.resolveShardStore(shardName)
	if err != nil {
		return nil, err
	}

	// Enumerate all entities.
	nodeIDs, err := store.AllNodeIDs(QueryOpts{})
	if err != nil {
		return nil, fmt.Errorf("graph: verify shard %s: list nodes: %w", shardName, err)
	}
	relIDs, err := store.AllRelIDs(QueryOpts{})
	if err != nil {
		return nil, fmt.Errorf("graph: verify shard %s: list rels: %w", shardName, err)
	}

	result := &VerifyResult{ShardName: shardName}

	// Verify each node.
	for _, id := range nodeIDs {
		ok, err := g.VerifyNodeHashChain(id)
		if err != nil {
			return nil, fmt.Errorf("graph: verify node %d: %w", id, err)
		}
		if ok {
			result.NodesOK++
		} else {
			result.NodesFailed++
		}
	}

	// Verify each relationship.
	for _, id := range relIDs {
		ok, err := g.VerifyRelHashChain(id)
		if err != nil {
			return nil, fmt.Errorf("graph: verify rel %d: %w", id, err)
		}
		if ok {
			result.RelsOK++
		} else {
			result.RelsFailed++
		}
	}

	// Cache result for immutable shards if all passed.
	if isImmutable && result.NodesFailed == 0 && result.RelsFailed == 0 {
		ts.catalog.UpdateShardVerified(shardName, true)
		ts.catalog.UpdateShardStats(shardName, result.NodesOK, result.RelsOK)
		if !ts.inMemory {
			_ = ts.catalog.Save() // best-effort persist
		}
	}

	return result, nil
}

// resolveShardStore returns the BadgerStore for a named shard.
func (ts *TieredStore) resolveShardStore(name string) (*BadgerStore, error) {
	if name == "reference" {
		return ts.refShard, nil
	}
	if name == "archive" {
		if ts.refArchive != nil {
			return ts.refArchive, nil
		}
		if err := ts.ensureRefArchive(); err != nil {
			return nil, err
		}
		return ts.refArchive, nil
	}

	ts.mu.RLock()
	es, ok := ts.eventShards[name]
	ts.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("graph: shard %q not found", name)
	}
	return es.getStore(ts)
}

type namedStore struct {
	name  string
	store *BadgerStore
}

// allShardStoresWithLazyOpen returns all BadgerStore instances, opening cold shards as needed.
func (ts *TieredStore) allShardStoresWithLazyOpen() ([]namedStore, error) {
	var stores []namedStore
	stores = append(stores, namedStore{name: "reference", store: ts.refShard})

	ts.mu.RLock()
	eventShards := make([]*eventShard, 0, len(ts.eventShards))
	for _, es := range ts.eventShards {
		eventShards = append(eventShards, es)
	}
	ts.mu.RUnlock()

	for _, es := range eventShards {
		store, err := es.getStore(ts)
		if err != nil {
			return nil, err
		}
		stores = append(stores, namedStore{name: es.name, store: store})
	}
	return stores, nil
}

// findRelInAnyShardStore locates which BadgerStore owns a relationship entity.
func (ts *TieredStore) findRelInAnyShardStore(relID snowflake.ID) *BadgerStore {
	if ts.refShard.hasRelID(relID) {
		return ts.refShard
	}
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	for _, es := range ts.eventShards {
		if es.store != nil && es.store.hasRelID(relID) {
			return es.store
		}
	}
	return nil
}
