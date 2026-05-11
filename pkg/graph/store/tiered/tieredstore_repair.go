package tiered

import (
	"errors"
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// RepairResult holds the outcome of a cross-shard consistency repair scan.
type RepairResult struct {
	OrphanedInEntries     int // in/ entries without entity → deleted
	MissingInEntries      int // entities without in/ → re-created
	ShardsScanned         int
	CrossShardRelsChecked int
}

// RunRepair scans for cross-shard relationship consistency issues and fixes them.
// Only scans cross-shard relationships (same-shard are atomic).
//
// Phase 1: Find orphaned in/ entries (entity missing)
// For each shard, snapshot all incoming-index entries. For each relID, check if
// the entity exists in any shard. If not → delete the orphaned in/ entry.
//
// Phase 2: Find missing in/ entries (entity exists, in/ missing)
// For each shard, get AllRelIDs. For each rel, read it, resolve start/end shards.
// If cross-shard: check that the end shard's inIdx contains the relID. If missing
// → re-create via PutRelIncoming.
func (ts *Store) RunRepair() (*RepairResult, error) {
	stores, release, err := ts.allShardStoresWithLazyOpen()
	if err != nil {
		return nil, err
	}
	defer release()

	return ts.runRepairStores(stores)
}

func (ts *Store) runRepairStores(stores []namedStore) (*RepairResult, error) {
	result := &RepairResult{
		ShardsScanned: len(stores),
	}

	// Phase 1: Find and remove orphaned in/ entries.
	for _, ns := range stores {
		for _, entry := range ns.store.IncomingIndexEntries() {
			if relationshipRowExists(ns.store, types.RelID(entry.RelID)) {
				continue // same-shard — entity exists here
			}
			// Cross-shard: check the already-pinned shard snapshot for
			// the entity. Re-resolving via a fresh checkoutArchive +
			// ts.eventShards walk would race a concurrent Close that
			// has set closed=true / nil'd refArchive but is still
			// blocked on archiveActiveReqs — the rel exists, but the
			// fresh resolver returns nil and Phase 1 deletes its
			// valid in/ entry as orphaned.
			entityStore := ts.findRelInAnyShardStore(entry.RelID, stores)
			if entityStore != nil {
				continue // entity exists in another shard — not orphaned
			}
			// Orphaned in/ entry: entity doesn't exist anywhere. Purge every
			// local index for the rel ID so stale type/out keys do not keep
			// relIDs and counters alive after repair.
			if err := ns.store.PurgeOrphanRelationshipIndexes(types.RelID(entry.RelID)); err != nil {
				return nil, err
			}
			result.OrphanedInEntries++
		}
	}

	// Phase 2: Find and re-create missing in/ entries.
	for _, ns := range stores {
		relIDs, err := ns.store.AllRelIDs(QueryOpts{})
		if err != nil {
			return nil, err
		}
		for _, relID := range relIDs {
			r, err := ns.store.GetRelationship(relID)
			if err != nil {
				// ErrRelNotFound is a legitimate TOCTOU skip — the rel was
				// deleted between AllRelIDs and GetRelationship. Anything
				// else (I/O failure, closed shard, routing failure) is an
				// operational error: silently swallowing it returns
				// "Repair succeeded" while genuinely needed in/-index
				// repairs were missed. Surface it.
				if errors.Is(err, ErrRelNotFound) {
					continue
				}
				return nil, fmt.Errorf("repair: shard %q: read rel %d: %w", ns.name, relID, err)
			}
			rawRelID := relID.SnowflakeID()

			startID := r.StartNodeID().SnowflakeID()
			endID := r.EndNodeID().SnowflakeID()
			relType := r.TypeToken().Value()

			// Resolve each endpoint from the same pinned shard snapshot
			// this repair pass is already scanning. Fresh live routing can
			// observe refArchive == nil during a concurrent Close even
			// while this snapshot still pins the archive open.
			startShard := ts.findNodeInAnyShardStore(startID, stores)
			if startShard == nil {
				return nil, fmt.Errorf("repair: shard %q: resolve start shard for rel %d: %w", ns.name, relID, ErrNodeNotFound)
			}
			endShard := ts.findNodeInAnyShardStore(endID, stores)
			if endShard == nil {
				return nil, fmt.Errorf("repair: shard %q: resolve end shard for rel %d: %w", ns.name, relID, ErrNodeNotFound)
			}

			if startShard == endShard {
				continue // same-shard — no split write
			}

			result.CrossShardRelsChecked++

			// Cross-shard: verify the end shard has the in/ entry.
			if hasIncomingEntry(endShard, endID, rawRelID) {
				continue // in/ entry exists
			}

			// Missing in/ entry — re-create.
			if err := endShard.PutRelIncoming(endID, startID, relType, rawRelID); err != nil {
				return nil, err
			}
			result.MissingInEntries++
		}
	}

	return result, nil
}

// hasIncomingEntry checks whether the shard's inIdx contains relID for the given nodeID.
func hasIncomingEntry(store *BadgerStore, nodeID, relID snowflake.ID) bool {
	inIDs := store.IncomingRelIDs(nodeID, 0)
	for _, id := range inIDs {
		if id == relID {
			return true
		}
	}
	return false
}
