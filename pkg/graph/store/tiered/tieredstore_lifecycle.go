package tiered

import (
	"fmt"
	"log/slog"
	"time"
)

// checkoutStore returns the BadgerStore and increments activeReqs to prevent
// idle-close from closing the store while the caller is still using it.
// Caller must call checkinStore() when done (typically via defer).
//
// For hot/warm shards: zero overhead (direct pointer return + atomic increment).
// For cold shards: acquires shardMu, opens if nil, increments activeReqs while
// still holding the lock to prevent TOCTOU race with closeIdleShards.
func (es *EventShard) checkoutStore(ts *Store) (*BadgerStore, error) {
	if err := ts.backgroundError(); err != nil {
		return nil, err
	}
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
	if es.currentTier() != TierCold {
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
		if es.currentTier() == TierCold {
			es.shardMu.Lock()
			if es.store == nil {
				store, err := ts.openBadgerStoreWithRecovery(es.path)
				if err != nil {
					es.activeReqs.Add(-1)
					es.shardMu.Unlock()
					return nil, fmt.Errorf("graph: lazy-open cold shard %s: %w", es.name, err)
				}
				es.store = store
			}
			es.readTransientOpen = false
			store := es.store
			es.shardMu.Unlock()
			return store, nil
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
	es.readTransientOpen = false
	es.activeReqs.Add(1)
	if ts.closed.Load() {
		es.activeReqs.Add(-1)
		es.shardMu.Unlock()
		return nil, ErrStoreClosed
	}
	es.lastAccess.Store(time.Now().UnixMilli())
	store := es.store
	es.shardMu.Unlock()
	return store, nil
}

// checkoutStoreForRead pins a shard for read-only fanout work.
//
// Cold shards that were already open keep the normal idle-close behaviour.
// Cold shards opened only for this checkout are closed again when the returned
// release function runs and no other reader has checked them out. This prevents
// DepthAll scans across many cold shards from accumulating one Badger handle
// per historical shard until the idle-close timer eventually fires.
func (es *EventShard) checkoutStoreForRead(ts *Store) (*BadgerStore, func(), error) {
	noop := func() {}
	if es.currentTier() != TierCold {
		store, err := es.checkoutStore(ts)
		if err != nil {
			return nil, noop, err
		}
		return store, es.checkinStore, nil
	}
	if err := ts.backgroundError(); err != nil {
		return nil, noop, err
	}
	if ts.closed.Load() {
		return nil, noop, ErrStoreClosed
	}

	es.shardMu.Lock()
	if ts.closed.Load() {
		es.shardMu.Unlock()
		return nil, noop, ErrStoreClosed
	}
	if es.store == nil {
		store, err := ts.openBadgerStoreWithRecovery(es.path)
		if err != nil {
			es.shardMu.Unlock()
			return nil, noop, fmt.Errorf("graph: lazy-open cold shard %s: %w", es.name, err)
		}
		es.store = store
		es.readTransientOpen = true
	}
	es.activeReqs.Add(1)
	if ts.closed.Load() {
		es.activeReqs.Add(-1)
		es.shardMu.Unlock()
		return nil, noop, ErrStoreClosed
	}
	es.lastAccess.Store(time.Now().UnixMilli())
	store := es.store
	es.shardMu.Unlock()

	return store, func() { es.checkinReadStore(ts) }, nil
}

// checkoutOpenStoreForRead pins an already-open shard for informational reads
// without lazy-opening a closed cold shard or promoting a transient cold read
// handle into a long-lived open handle.
func (es *EventShard) checkoutOpenStoreForRead(ts *Store) (*BadgerStore, func(), bool, error) {
	noop := func() {}
	if es.currentTier() != TierCold {
		store, err := es.checkoutStore(ts)
		if err != nil {
			return nil, noop, false, err
		}
		return store, es.checkinStore, true, nil
	}
	if err := ts.backgroundError(); err != nil {
		return nil, noop, false, err
	}

	es.shardMu.Lock()
	if ts.closed.Load() {
		es.shardMu.Unlock()
		return nil, noop, false, ErrStoreClosed
	}
	if es.store == nil {
		es.shardMu.Unlock()
		return nil, noop, false, nil
	}
	es.activeReqs.Add(1)
	if ts.closed.Load() {
		es.activeReqs.Add(-1)
		es.shardMu.Unlock()
		return nil, noop, false, ErrStoreClosed
	}
	es.lastAccess.Store(time.Now().UnixMilli())
	store := es.store
	es.shardMu.Unlock()

	return store, func() { es.checkinReadStore(ts) }, true, nil
}

func (es *EventShard) checkinReadStore(ts *Store) {
	es.activeReqs.Add(-1)
	es.shardMu.Lock()
	defer es.shardMu.Unlock()
	if es.store == nil || !es.readTransientOpen || es.activeReqs.Load() != 0 {
		return
	}
	if err := es.store.Close(); err != nil {
		wrapped := fmt.Errorf("graph: close transient cold shard %s: %w", es.name, err)
		ts.recordBackgroundError(wrapped)
		slog.Error("tiered cold shard transient close failed", "shard", es.name, "err", err)
		return
	}
	es.store = nil
	es.readTransientOpen = false
}

// checkoutStoreIfOpen pins a shard only if its BadgerStore is already open.
// Unlike checkoutStore, it never lazy-opens a closed cold shard. This is used
// by negative-path probes where waking every cold shard would turn a cheap
// create into a full historical scan.
func (es *EventShard) checkoutStoreIfOpen(ts *Store) (*BadgerStore, bool, error) {
	if err := ts.backgroundError(); err != nil {
		return nil, false, err
	}
	if es.currentTier() != TierCold {
		store, err := es.checkoutStore(ts)
		return store, err == nil, err
	}

	es.shardMu.Lock()
	if ts.closed.Load() {
		es.shardMu.Unlock()
		return nil, false, ErrStoreClosed
	}
	if es.store == nil {
		es.shardMu.Unlock()
		return nil, false, nil
	}
	es.readTransientOpen = false
	es.activeReqs.Add(1)
	es.lastAccess.Store(time.Now().UnixMilli())
	store := es.store
	es.shardMu.Unlock()

	if ts.closed.Load() {
		es.activeReqs.Add(-1)
		return nil, false, ErrStoreClosed
	}
	return store, true, nil
}

// checkinStore decrements activeReqs, signalling that the caller is done
// using the store returned by checkoutStore.
func (es *EventShard) checkinStore() {
	es.activeReqs.Add(-1)
}

// checkoutRefShard returns refShard pinned against a concurrent Close.
// Callers MUST invoke the returned checkin exactly once. The reference shard
// is not idle-closed, but Close does close it, so long-running scans and
// writes need the same lifecycle gate as event shards and refArchive.
func (ts *Store) checkoutRefShard() (*BadgerStore, func(), error) {
	noop := func() {}
	if ts == nil {
		return nil, noop, ErrNilStore
	}
	if err := ts.backgroundError(); err != nil {
		return nil, noop, err
	}
	if ts.closeCh == nil || ts.refShard == nil {
		return nil, noop, ErrStoreClosed
	}
	if ts.closed.Load() {
		return nil, noop, ErrStoreClosed
	}
	ts.refActiveReqs.Add(1)
	if ts.closed.Load() {
		ts.refActiveReqs.Add(-1)
		return nil, noop, ErrStoreClosed
	}
	return ts.refShard, func() { ts.refActiveReqs.Add(-1) }, nil
}

// checkoutArchive returns the refArchive pointer pinned against a concurrent
// Close. Callers MUST invoke checkin exactly once. If the catalog has no
// archive shard the returned pointer is nil and checkin is a safe no-op —
// callers should treat nil as "no archive available" and skip archive-side
// work. Closed stores return ErrStoreClosed, matching checkoutStore.
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
	if err := ts.checkOpen(); err != nil {
		return nil, noop, err
	}
	if err := ts.backgroundError(); err != nil {
		return nil, noop, err
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
		return nil, noop, ErrStoreClosed
	}
	// Re-load after the increment: Close stores nil into refArchive under
	// archiveMu, and a snapshot taken before the increment may have raced.
	if ts.refArchive.Load() == nil {
		ts.archiveActiveReqs.Add(-1)
		return nil, noop, ErrStoreClosed
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
		if es.currentTier() == TierCold {
			coldShards = append(coldShards, es)
		}
	}
	ts.mu.RUnlock()

	for _, es := range coldShards {
		es.shardMu.Lock()
		if es.store != nil && es.activeReqs.Load() == 0 && (nowMs-es.lastAccess.Load()) > thresholdMs {
			if err := es.store.Close(); err != nil {
				wrapped := fmt.Errorf("graph: idle-close cold shard %s: %w", es.name, err)
				ts.recordBackgroundError(wrapped)
				slog.Error("tiered cold shard idle-close failed", "shard", es.name, "err", err)
			}
			es.store = nil
			es.readTransientOpen = false
		}
		es.shardMu.Unlock()
	}
}
