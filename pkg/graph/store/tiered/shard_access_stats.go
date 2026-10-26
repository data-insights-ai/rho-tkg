package tiered

// ShardAccessStats counts a tiered store's shard checkouts since New, by the
// shard's tier at the checkout: every read and write pins the shard it
// touches, and a read by ID that misses its candidate shard pins every other
// (routeRelShardFor), so a diff around a caller's work bounds the tiers that
// work touched from above (other callers' accesses in between count too).
type ShardAccessStats struct {
	Reference uint64 // reference shard
	Archive   uint64 // reference archive
	Hot       uint64 // event shards while hot
	Warm      uint64 // event shards while warm
	Cold      uint64 // event shards while cold
	ColdOpens uint64 // lazy opens of a closed cold shard (ShardInfo.ColdOpens summed)
}

// countAccess counts one checkout of es under its current tier.
func (es *EventShard) countAccess() {
	switch es.tierCode.Load() {
	case eventShardTierHot:
		es.accesses[0].Add(1)
	case eventShardTierWarm:
		es.accesses[1].Add(1)
	case eventShardTierCold:
		es.accesses[2].Add(1)
	}
}

// foldDroppedAccesses keeps the counts of es, which a retention drop removed
// from the store, in ShardAccessStats.
func (ts *Store) foldDroppedAccesses(es *EventShard) {
	for i := range es.accesses {
		ts.droppedAccesses[i].Add(es.accesses[i].Load())
	}
	ts.droppedColdOpens.Add(uint64(es.coldOpens.Load())) // #nosec G115 -- a count, never negative
}

// ShardAccessStats returns the store's shard checkouts since New by tier
// (ShardAccessStats). It takes no store-wide lock beyond the shard list's and
// opens nothing: cheap enough to diff around every call.
func (ts *Store) ShardAccessStats() ShardAccessStats {
	st := ShardAccessStats{
		Reference: ts.refAccesses.Load(),
		Archive:   ts.archiveAccesses.Load(),
		Hot:       ts.droppedAccesses[0].Load(),
		Warm:      ts.droppedAccesses[1].Load(),
		Cold:      ts.droppedAccesses[2].Load(),
		ColdOpens: ts.droppedColdOpens.Load(),
	}
	ts.mu.RLock()
	for _, es := range ts.eventShards {
		st.Hot += es.accesses[0].Load()
		st.Warm += es.accesses[1].Load()
		st.Cold += es.accesses[2].Load()
		st.ColdOpens += uint64(es.coldOpens.Load()) // #nosec G115 -- a count, never negative
	}
	ts.mu.RUnlock()
	return st
}
