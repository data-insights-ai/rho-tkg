package graph

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
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
// For each shard, for each nodeID, get incomingRelIDs. For each relID, check if
// the entity exists in any shard. If not → delete the orphaned in/ entry.
//
// Phase 2: Find missing in/ entries (entity exists, in/ missing)
// For each shard, get AllRelIDs. For each rel, read it, resolve start/end shards.
// If cross-shard: check that the end shard's inIdx contains the relID. If missing
// → re-create via putRelIncoming.
func (ts *TieredStore) RunRepair() (*RepairResult, error) {
	stores, release, err := ts.allShardStoresWithLazyOpen()
	if err != nil {
		return nil, err
	}
	defer release()

	result := &RepairResult{
		ShardsScanned: len(stores),
	}

	// Phase 1: Find and remove orphaned in/ entries.
	for _, ns := range stores {
		nodeIDs, err := ns.store.AllNodeIDs(QueryOpts{})
		if err != nil {
			return nil, err
		}
		for _, nodeID := range nodeIDs {
			inRelIDs := ns.store.incomingRelIDs(nodeID, 0)
			for _, relID := range inRelIDs {
				if ns.store.hasRelID(relID) {
					continue // same-shard — entity exists here
				}
				// Cross-shard: check all other shards for the entity.
				entityStore := ts.findRelInAnyShardStore(relID)
				if entityStore != nil {
					continue // entity exists in another shard — not orphaned
				}
				// Orphaned in/ entry: entity doesn't exist anywhere.
				if err := ns.store.deleteIncomingByRelID(nodeID, relID); err != nil {
					return nil, err
				}
				result.OrphanedInEntries++
			}
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
				continue // can't read — skip
			}

			startID := r.StartNodeID().SnowflakeID()
			endID := r.EndNodeID().SnowflakeID()
			relType := r.TypeToken().Value()

			// Resolve which shard owns each endpoint.
			startShard, err := ts.shardForNodeID(startID)
			if err != nil {
				continue
			}
			endShard, err := ts.shardForNodeID(endID)
			if err != nil {
				continue
			}

			if startShard == endShard {
				continue // same-shard — no split write
			}

			result.CrossShardRelsChecked++

			// Cross-shard: verify the end shard has the in/ entry.
			if hasIncomingEntry(endShard, endID, relID) {
				continue // in/ entry exists
			}

			// Missing in/ entry — re-create.
			if err := endShard.putRelIncoming(endID, startID, relType, relID); err != nil {
				return nil, err
			}
			result.MissingInEntries++
		}
	}

	return result, nil
}

// hasIncomingEntry checks whether the shard's inIdx contains relID for the given nodeID.
func hasIncomingEntry(store *BadgerStore, nodeID, relID snowflake.ID) bool {
	inIDs := store.incomingRelIDs(nodeID, 0)
	for _, id := range inIDs {
		if id == relID {
			return true
		}
	}
	return false
}
