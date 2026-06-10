package tiered

import (
	"errors"
	"fmt"
	"log/slog"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RepairResult holds the outcome of a cross-shard consistency repair scan.
type RepairResult struct {
	OrphanedInEntries     int // in/ entries without entity → deleted
	StaleInEntries        int // in/ entries whose entity has different type/end shard → deleted
	MissingInEntries      int // entities without in/ → re-created
	ShardsScanned         int
	CrossShardRelsChecked int
}

// RunRepair scans for cross-shard relationship consistency issues and fixes them.
// Only scans cross-shard relationships (same-shard are atomic).
//
// Phase 1: Find invalid in/ entries.
// For each shard, snapshot all incoming-index entries. For each relID, check if
// the entity exists in any shard. If not, delete the orphaned in/ entry. If it
// exists but points at a different end shard or relationship type, delete the
// stale in/ entry so Phase 2 can recreate the correct one when needed.
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

	// Phase 1: Find and remove orphaned or stale in/ entries.
	for _, ns := range stores {
		for _, entry := range ns.store.IncomingIndexEntries() {
			rel, found, err := relationshipRow(ns.store, types.RelID(entry.RelID))
			if err != nil {
				return nil, fmt.Errorf("repair: shard %q: read incoming rel %d: %w", ns.name, entry.RelID, err)
			}
			entityStore := ns.store
			if !found {
				// Cross-shard: check the already-pinned shard snapshot for
				// the entity. Re-resolving via a fresh checkoutArchive +
				// ts.eventShards walk would race a concurrent Close that
				// has set closed=true / nil'd refArchive but is still
				// blocked on archiveActiveReqs — the rel exists, but the
				// fresh resolver returns nil and Phase 1 deletes its
				// valid in/ entry as orphaned.
				entityStore = ts.findRelInAnyShardStore(entry.RelID, stores)
				if entityStore != nil {
					rel, err = entityStore.GetRelationship(types.RelID(entry.RelID))
					if err != nil {
						return nil, fmt.Errorf("repair: shard %q: read cross-shard incoming rel %d: %w", ns.name, entry.RelID, err)
					}
				}
			}
			if entityStore == nil {
				// Orphaned in/ entry: entity doesn't exist anywhere. Purge every
				// local index for the rel ID so stale type/out keys do not keep
				// relIDs and counters alive after repair.
				if err := ns.store.PurgeOrphanRelationshipIndexes(types.RelID(entry.RelID)); err != nil {
					return nil, err
				}
				result.OrphanedInEntries++
				continue
			}

			endID := rel.EndNodeID().SnowflakeID()
			endShard, err := ts.findNodeInAnyShardStore(endID, stores)
			if err != nil {
				return nil, fmt.Errorf("repair: shard %q: read incoming end node for rel %d: %w", ns.name, entry.RelID, err)
			}
			if endShard == nil {
				return nil, fmt.Errorf("repair: shard %q: resolve incoming end node for rel %d: %w", ns.name, entry.RelID, ErrNodeNotFound)
			}
			if entry.EndID != endID || entry.RelType != rel.TypeToken().Value() || ns.store != endShard {
				if err := ns.store.DeleteIncomingByRelID(entry.EndID, entry.RelID); err != nil {
					return nil, err
				}
				result.StaleInEntries++
				continue
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
			startShard, err := ts.findNodeInAnyShardStore(startID, stores)
			if err != nil {
				return nil, fmt.Errorf("repair: shard %q: read start node for rel %d: %w", ns.name, relID, err)
			}
			if startShard == nil {
				return nil, fmt.Errorf("repair: shard %q: resolve start shard for rel %d: %w", ns.name, relID, ErrNodeNotFound)
			}
			endShard, err := ts.findNodeInAnyShardStore(endID, stores)
			if err != nil {
				return nil, fmt.Errorf("repair: shard %q: read end node for rel %d: %w", ns.name, relID, err)
			}
			if endShard == nil {
				return nil, fmt.Errorf("repair: shard %q: resolve end shard for rel %d: %w", ns.name, relID, ErrNodeNotFound)
			}

			if startShard == endShard {
				continue // same-shard — no split write
			}

			result.CrossShardRelsChecked++

			// Cross-shard: verify the end shard has the in/ entry.
			if hasIncomingEntryOfType(endShard, endID, relType, rawRelID) {
				continue // in/ entry exists
			}

			// Missing in/ entry — re-create.
			if err := endShard.PutRelIncoming(endID, startID, relType, rawRelID); err != nil {
				return nil, err
			}
			result.MissingInEntries++
		}
	}

	// Repaired inconsistencies are evidence of an earlier crash window or a
	// partially-failed cross-shard write — surface them so operators learn a
	// repair happened without inspecting the result programmatically.
	if result.OrphanedInEntries > 0 || result.StaleInEntries > 0 || result.MissingInEntries > 0 {
		slog.Warn("tiered store repair fixed cross-shard inconsistencies",
			"orphaned_in_entries", result.OrphanedInEntries,
			"stale_in_entries", result.StaleInEntries,
			"missing_in_entries", result.MissingInEntries,
			"shards_scanned", result.ShardsScanned,
			"cross_shard_rels_checked", result.CrossShardRelsChecked)
	}

	return result, nil
}

// hasIncomingEntry checks whether the shard's inIdx contains relID for the given nodeID.
func hasIncomingEntry(store *BadgerStore, nodeID, relID snowflake.ID) bool {
	return hasIncomingEntryOfType(store, nodeID, 0, relID)
}

func hasIncomingEntryOfType(store *BadgerStore, nodeID snowflake.ID, relType uint16, relID snowflake.ID) bool {
	inIDs := store.IncomingRelIDs(nodeID, relType)
	for _, id := range inIDs {
		if id == relID {
			return true
		}
	}
	return false
}
