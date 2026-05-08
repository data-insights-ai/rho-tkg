package tiered

import (
	"errors"
	"fmt"
	"time"
)

// getStore returns the BadgerStore for this shard, lazily opening it if cold.
// For hot/warm shards: zero overhead (direct pointer return).
// For cold shards: acquires shardMu, opens BadgerStore if nil, updates lastAccess.
func (es *EventShard) getStore(ts *Store) (*BadgerStore, error) {
	if es.tier != TierCold {
		return es.store, nil // hot/warm: zero overhead
	}
	es.shardMu.Lock()
	defer es.shardMu.Unlock()
	if es.store != nil {
		es.lastAccess.Store(time.Now().UnixMilli())
		return es.store, nil
	}
	store, err := ts.openBadgerStoreWithRecovery(es.path)
	if err != nil {
		return nil, fmt.Errorf("graph: lazy-open cold shard %s: %w", es.name, err)
	}
	es.store = store
	es.lastAccess.Store(time.Now().UnixMilli())
	return store, nil
}

// checkoutStore returns the BadgerStore and increments activeReqs to prevent
// idle-close from closing the store while the caller is still using it.
// Caller must call checkinStore() when done (typically via defer).
//
// For hot/warm shards: zero overhead (direct pointer return + atomic increment).
// For cold shards: acquires shardMu, opens if nil, increments activeReqs while
// still holding the lock to prevent TOCTOU race with closeIdleShards.
func (es *EventShard) checkoutStore(ts *Store) (*BadgerStore, error) {
	// Refuse new checkouts after Close has been initiated. Close drains
	// activeReqs per shard before es.store.Close(), so a checkout that
	// races past the drain would see its store closed under it. The
	// activeReqs increment must therefore be guarded by the same flag
	// Close consults; without this gate the spin-wait in Close gives
	// only the illusion of safety. Mirrors ensureRefArchive's
	// ErrStoreClosed semantics.
	if ts.closed.Load() {
		return nil, ErrStoreClosed
	}
	if es.tier != TierCold {
		// Hot/warm: stores are always open, never closed by idle-close.
		es.activeReqs.Add(1)
		es.lastAccess.Store(time.Now().UnixMilli())
		// Re-check closed AFTER the increment so a concurrent Close that
		// observed activeReqs == 0 between our load and our add doesn't
		// proceed to close the store. If we lost the race, decrement and
		// surface ErrStoreClosed to the caller.
		if ts.closed.Load() {
			es.activeReqs.Add(-1)
			return nil, ErrStoreClosed
		}
		return es.store, nil
	}
	// Cold shard: acquire shardMu to atomically open + increment activeReqs.
	// This prevents closeIdleShards from closing the store between open and
	// the activeReqs increment (the v3.0.30 TOCTOU bug).
	es.shardMu.Lock()
	if ts.closed.Load() {
		es.shardMu.Unlock()
		return nil, ErrStoreClosed
	}
	if es.store == nil {
		store, err := ts.openBadgerStoreWithRecovery(es.path)
		if err != nil {
			es.shardMu.Unlock()
			return nil, fmt.Errorf("graph: lazy-open cold shard %s: %w", es.name, err)
		}
		es.store = store
	}
	es.activeReqs.Add(1)
	es.lastAccess.Store(time.Now().UnixMilli())
	store := es.store
	es.shardMu.Unlock()
	return store, nil
}

// checkinStore decrements activeReqs, signalling that the caller is done
// using the store returned by checkoutStore.
func (es *EventShard) checkinStore() {
	es.activeReqs.Add(-1)
}

// checkoutArchive returns the refArchive pointer pinned against a concurrent
// Close. Callers MUST invoke checkin exactly once. If the store has been
// closed or the catalog has no archive shard the returned pointer is nil
// and checkin is a safe no-op — callers should treat nil as "no archive
// available" and skip archive-side work.
//
// Cold-start handling: if the catalog records an archive shard but the
// in-memory pointer is nil (e.g. fresh process restart, no GetNode has
// triggered lazy-open yet), this helper invokes ensureRefArchive so bulk
// scans see archive content without needing a prior point lookup. Without
// this, AllNodes / AllNodeHistoryIDs / similar bulk APIs would silently
// omit archived entities until some unrelated lookup opened the archive.
//
// Concurrency: mirrors EventShard.checkoutStore. Close drains
// archiveActiveReqs before calling archive.Close(), so a checkout active
// at Close-time will be observed and waited for. The post-increment
// closed re-check closes the window where Close already advanced past
// the spin-wait between our load and our increment.
func (ts *Store) checkoutArchive() (*BadgerStore, func(), error) {
	noop := func() {}
	if ts.closed.Load() {
		return nil, noop, nil
	}
	archive := ts.refArchive.Load()
	if archive == nil {
		// Cold-start: open the archive on demand if the catalog says one
		// exists. ensureRefArchive itself refuses to open after Close, so
		// a racing Close still surfaces the right error.
		if !ts.hasArchiveShard() {
			return nil, noop, nil
		}
		if err := ts.ensureRefArchive(); err != nil {
			if errors.Is(err, ErrStoreClosed) {
				return nil, noop, nil
			}
			return nil, noop, err
		}
		archive = ts.refArchive.Load()
		if archive == nil {
			return nil, noop, nil
		}
	}
	ts.archiveActiveReqs.Add(1)
	if ts.closed.Load() {
		ts.archiveActiveReqs.Add(-1)
		return nil, noop, nil
	}
	// Re-load after the increment: Close stores nil into refArchive under
	// archiveMu, and a snapshot taken before the increment may have raced.
	if ts.refArchive.Load() == nil {
		ts.archiveActiveReqs.Add(-1)
		return nil, noop, nil
	}
	return archive, func() { ts.archiveActiveReqs.Add(-1) }, nil
}

// idleCloseLoop periodically checks cold shards and closes those that have been
// idle longer than IdleTimeout. Runs every IdleTimeout/2. Stopped via closeCh.
func (ts *Store) idleCloseLoop() {
	tick := time.NewTicker(ts.idleTimeout / 2)
	defer tick.Stop()

	for {
		select {
		case <-ts.closeCh:
			return
		case <-tick.C:
			ts.closeIdleShards()
		}
	}
}

// closeIdleShards closes cold shards that have been idle longer than idleTimeout.
func (ts *Store) closeIdleShards() {
	nowMs := time.Now().UnixMilli()
	thresholdMs := ts.idleTimeout.Milliseconds()

	ts.mu.RLock()
	var coldShards []*EventShard
	for _, es := range ts.eventShards {
		if es.tier == TierCold {
			coldShards = append(coldShards, es)
		}
	}
	ts.mu.RUnlock()

	for _, es := range coldShards {
		es.shardMu.Lock()
		if es.store != nil && es.activeReqs.Load() == 0 && (nowMs-es.lastAccess.Load()) > thresholdMs {
			_ = es.store.Close() // idle eviction; Close error can't be surfaced to any caller
			es.store = nil
		}
		es.shardMu.Unlock()
	}
}
