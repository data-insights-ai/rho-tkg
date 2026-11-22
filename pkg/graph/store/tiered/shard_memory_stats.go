package tiered

// ShardMemoryStats is the RAM of a tiered store's open shards by holder
// (MEMACCT), summed per tier as ShardAccessStats' handles are: the reference
// shard, the archive when open, and the event shards by their tier now (a
// demoted shard still open counts cold). A closed shard adds nothing. The
// decoded-entity caches are EntityCacheStats'; what an account leaves out is
// named on badger.MemoryAccount.
type ShardMemoryStats struct {
	Reference MemoryAccount
	Archive   MemoryAccount
	Hot       MemoryAccount
	Warm      MemoryAccount
	Cold      MemoryAccount
}

// MemoryStats returns the open shards' memory accounts by tier
// (ShardMemoryStats). It opens nothing and counts no shard access. Unlike
// ShardAccessStats it is not for a diff around every call: each open store's
// account takes its index lock shared and walks its ID sets' chunk
// directories, one step per 512 to 1,024 live entities, after the shard
// list's lock is released. A closed store reads zero.
func (ts *Store) MemoryStats() ShardMemoryStats {
	var st ShardMemoryStats
	if ts.refShard != nil && !ts.closed.Load() {
		st.Reference = ts.refShard.MemoryAccount()
	}
	if a := ts.refArchive.Load(); a != nil {
		st.Archive = a.MemoryAccount()
	}
	type openShard struct {
		s   *BadgerStore
		mem *MemoryAccount
	}
	var open []openShard
	ts.mu.RLock()
	for _, es := range ts.eventShards {
		s := es.openStore.Load()
		if s == nil {
			continue
		}
		switch es.tierCode.Load() {
		case eventShardTierHot:
			open = append(open, openShard{s, &st.Hot})
		case eventShardTierWarm:
			open = append(open, openShard{s, &st.Warm})
		case eventShardTierCold:
			open = append(open, openShard{s, &st.Cold})
		}
	}
	ts.mu.RUnlock()
	for _, o := range open {
		o.mem.Add(o.s.MemoryAccount())
	}
	return st
}
