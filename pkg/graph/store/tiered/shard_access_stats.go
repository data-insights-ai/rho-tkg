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

	// Open badger handles now (a gauge, not a count since New), by the
	// shard's tier at the snapshot: a demoted shard whose handle is still
	// open counts under ColdHandles. The reference shard is always open
	// writable; the archive once opened.
	ReferenceHandles TierHandles
	ArchiveHandles   TierHandles
	HotHandles       TierHandles
	WarmHandles      TierHandles
	ColdHandles      TierHandles

	// Writable checkouts since New, by the event shard's tier at each: every
	// write (a create, a late write, an owner update, a backfill) and every
	// caller that may write. A read-only handle on that tier would have to be
	// upgraded for each (as a cold one is), so WarmWrites is what a read-only
	// warm tier would pay.
	HotWrites  uint64
	WarmWrites uint64
	ColdWrites uint64
}

// TierHandles counts the open badger handles of one tier
// (ShardAccessStats): Writable ones and ReadOnly ones (a cold shard a read
// opened, COLDRO).
type TierHandles struct {
	Writable int
	ReadOnly int
}

// Handle kinds of EventShard.handle.
const (
	handleClosed uint32 = iota
	handleWritable
	handleReadOnly
)

// noteHandle records the kind of es's store in es.handle, which
// ShardAccessStats reads without shardMu. Called wherever a store is
// installed or cleared (setStoreLocked, clearStoreLocked, the read-only
// open, initTier for a shard built with its store). Caller holds es.shardMu
// or owns es alone.
func (es *EventShard) noteHandle() {
	switch {
	case es.store == nil:
		es.handle.Store(handleClosed)
	case es.storeRO:
		es.handle.Store(handleReadOnly)
	default:
		es.handle.Store(handleWritable)
	}
}

// countWrite counts one writable checkout of es under its current tier.
func (es *EventShard) countWrite() {
	if i, ok := tierSlot(es.tierCode.Load()); ok {
		es.writes[i].Add(1)
	}
}

// tierSlot maps an event shard tier code to its index in the per-tier
// counter arrays (hot 0, warm 1, cold 2).
func tierSlot(code uint32) (int, bool) {
	switch code {
	case eventShardTierHot:
		return 0, true
	case eventShardTierWarm:
		return 1, true
	case eventShardTierCold:
		return 2, true
	}
	return 0, false
}

// add counts one handle of kind.
func (h *TierHandles) add(kind uint32) {
	switch kind {
	case handleWritable:
		h.Writable++
	case handleReadOnly:
		h.ReadOnly++
	}
}

// countAccess counts one checkout of es under its current tier.
func (es *EventShard) countAccess() {
	if i, ok := tierSlot(es.tierCode.Load()); ok {
		es.accesses[i].Add(1)
	}
}

// foldDroppedAccesses keeps the counts of es, which a retention drop removed
// from the store, in ShardAccessStats.
func (ts *Store) foldDroppedAccesses(es *EventShard) {
	for i := range es.accesses {
		ts.droppedAccesses[i].Add(es.accesses[i].Load())
		ts.droppedWrites[i].Add(es.writes[i].Load())
	}
	ts.droppedColdOpens.Add(uint64(es.coldOpens.Load())) // #nosec G115 -- a count, never negative
}

// ShardAccessStats returns the store's shard checkouts since New by tier
// (ShardAccessStats). It takes no store-wide lock beyond the shard list's and
// opens nothing: cheap enough to diff around every call.
func (ts *Store) ShardAccessStats() ShardAccessStats {
	st := ShardAccessStats{
		Reference:  ts.refAccesses.Load(),
		Archive:    ts.archiveAccesses.Load(),
		Hot:        ts.droppedAccesses[0].Load(),
		Warm:       ts.droppedAccesses[1].Load(),
		Cold:       ts.droppedAccesses[2].Load(),
		ColdOpens:  ts.droppedColdOpens.Load(),
		HotWrites:  ts.droppedWrites[0].Load(),
		WarmWrites: ts.droppedWrites[1].Load(),
		ColdWrites: ts.droppedWrites[2].Load(),
	}
	if ts.refShard != nil && !ts.closed.Load() {
		st.ReferenceHandles.Writable = 1
	}
	if ts.refArchive.Load() != nil {
		st.ArchiveHandles.Writable = 1
	}
	ts.mu.RLock()
	for _, es := range ts.eventShards {
		st.Hot += es.accesses[0].Load()
		st.Warm += es.accesses[1].Load()
		st.Cold += es.accesses[2].Load()
		st.ColdOpens += uint64(es.coldOpens.Load()) // #nosec G115 -- a count, never negative
		st.HotWrites += es.writes[0].Load()
		st.WarmWrites += es.writes[1].Load()
		st.ColdWrites += es.writes[2].Load()
		kind := es.handle.Load()
		switch es.tierCode.Load() {
		case eventShardTierHot:
			st.HotHandles.add(kind)
		case eventShardTierWarm:
			st.WarmHandles.add(kind)
		case eventShardTierCold:
			st.ColdHandles.add(kind)
		}
	}
	ts.mu.RUnlock()
	return st
}
