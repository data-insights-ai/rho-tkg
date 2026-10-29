package tiered

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// ShardCacheStats is one open shard's decoded-entity caches (CACHESTATS):
// Nodes and Rels each count the entries and their estimated bytes, clean and
// dirty apart. Every open shard has its own pair, so the store's cache heap is
// the sum of the rows; CacheBudgetBytes bounds each row's clean part.
type ShardCacheStats struct {
	Name  string
	Kind  ShardKind
	Tier  ShardTier
	Nodes storepkg.EntityCacheUsage
	Rels  storepkg.EntityCacheUsage
}

// EntityCacheStats returns a row per open shard: the reference shard, the
// archive when open, then each open event shard (in no set order: key them by
// Name); a closed shard has no
// cache and no row. It walks every cache, O(cached entries), and counts no
// shard access (ShardAccessStats), opens nothing and keeps no shard open.
func (ts *Store) EntityCacheStats() ([]ShardCacheStats, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	row := func(name string, kind ShardKind, tier ShardTier, s *BadgerStore) ShardCacheStats {
		r := ShardCacheStats{Name: name, Kind: kind, Tier: tier}
		r.Nodes, r.Rels = s.EntityCacheUsage()
		return r
	}
	rows := []ShardCacheStats{row("reference", ShardReference, TierHot, ts.refShard)}
	if archive := ts.refArchive.Load(); archive != nil {
		rows = append(rows, row("archive", ShardArchive, TierCold, archive))
	}
	ts.mu.RLock()
	shards := make([]*EventShard, 0, len(ts.eventShards))
	for _, es := range ts.eventShards {
		shards = append(shards, es)
	}
	ts.mu.RUnlock()
	for _, es := range shards {
		es.shardMu.Lock()
		s := es.store
		es.shardMu.Unlock()
		if s != nil {
			rows = append(rows, row(es.name, ShardEvent, es.currentTier(), s))
		}
	}
	return rows, nil
}
