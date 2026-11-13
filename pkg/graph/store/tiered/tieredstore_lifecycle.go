package tiered

import (
	"fmt"
	"log/slog"
	"time"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// setStoreLocked installs store on es and counts it when es is cold. Caller
// holds es.shardMu.
func (es *EventShard) setStoreLocked(ts *Store, store *BadgerStore) {
	es.store = store
	es.countColdStoreLocked(ts)
}

// countColdStoreLocked counts es's open store in ts.openColdShards once while
// es is cold. A store can become a cold shard's without a cold open — a warm
// shard demoted by rotation keeps its handle — so the demotion calls this too;
// otherwise the close of that handle would decrement a count it never added
// and every later check-in would retain past the cap. Caller holds es.shardMu.
func (es *EventShard) countColdStoreLocked(ts *Store) {
	if es.store != nil && !es.coldCounted && es.currentTier() == TierCold {
		es.coldCounted = true
		ts.openColdShards.Add(1)
	}
}

// openColdLocked lazily opens the closed cold shard es writable, installs and
// counts it (ShardInfo.ColdOpens) and syncs its indexes. A writable shard can
// gain rows, so before it opens its no-rows mark and sealed counts leave the
// saved catalog (label and type doors skip a shard by them). Caller holds
// es.shardMu.
func (es *EventShard) openColdLocked(ts *Store) error {
	if err := es.wakeNoRowsLocked(ts); err != nil {
		return err
	}
	if es.dropCachedCounts(ts) {
		if err := ts.catalog.Save(); err != nil {
			return fmt.Errorf("graph: save catalog before opening sealed shard %s: %w", es.name, err)
		}
	}
	store, err := ts.openColdBadgerStoreWithRecovery(es.path)
	if err != nil {
		return fmt.Errorf("graph: lazy-open cold shard %s: %w", es.name, err)
	}
	es.coldOpens.Add(1)
	es.setStoreLocked(ts, store)
	ts.syncColdShardIndexes(store, es.name)
	return nil
}

// openColdReadOnlyLocked lazily opens the closed cold shard es read-only for
// a reader (COLDRO): about 1 ms and 3 MB of heap where a writable open costs
// about 28 ms and 94 MB (tasks/evidence/COLDRO). It opens writable instead
// when the read-only open fails (an unclean WAL needs a writable recovery) or
// when the shard's index definitions differ from the anchor's, which only a
// writable open syncs. Caller holds es.shardMu.
func (es *EventShard) openColdReadOnlyLocked(ts *Store) error {
	store, err := ts.newShardStore(ts.shardCfg(es.path, true, true))
	if err != nil {
		return es.openColdLocked(ts)
	}
	if match, merr := ts.anchoredIndexesMatch(store); merr != nil || !match {
		if cerr := store.Close(); cerr != nil {
			return fmt.Errorf("graph: close read-only cold shard %s: %w", es.name, cerr)
		}
		return es.openColdLocked(ts)
	}
	es.coldOpens.Add(1)
	es.setStoreLocked(ts, store)
	es.storeRO = true
	return nil
}

// openColdWritableLocked makes es's store writable: it opens the closed cold
// shard, or replaces its read-only handle. A replacement first waits for the
// handle's readers (the caller's own pins, own, excepted) behind the shard's
// gate, as a move does, since the writable open needs the directory lock the
// read-only handle shares. Caller holds es.shardMu, again on return.
func (es *EventShard) openColdWritableLocked(ts *Store, own int64) error {
	if es.store == nil {
		return es.openColdLocked(ts)
	}
	done := make(chan struct{})
	es.moveDone = done
	es.moving.Store(true)
	defer func() {
		es.moveDone = nil
		es.moving.Store(false)
		close(done)
	}()
	for es.activeReqs.Load() > own {
		es.shardMu.Unlock()
		time.Sleep(time.Millisecond)
		es.shardMu.Lock()
	}
	if es.store == nil { // the idle sweep closed it meanwhile
		return es.openColdLocked(ts)
	}
	if err := es.store.Close(); err != nil {
		return fmt.Errorf("graph: close read-only cold shard %s: %w", es.name, err)
	}
	es.clearStoreLocked(ts)
	return es.openColdLocked(ts)
}

// clearStoreLocked drops es's store (the caller closed it) and uncounts it.
// Caller holds es.shardMu.
func (es *EventShard) clearStoreLocked(ts *Store) {
	es.store = nil
	es.storeRO = false
	es.readTransientOpen = false
	if es.coldCounted {
		es.coldCounted = false
		ts.openColdShards.Add(-1)
	}
}

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
		// Hot/warm: stores are always open, never closed by idle-close, so
		// the checkout stamps no access (EVENTCLOCK: a clock read per
		// checkout); a demoted shard's idle time counts from its last cold
		// access.
		es.activeReqs.Add(1)
		// Re-check closed AFTER the increment so a concurrent Close that
		// observed activeReqs == 0 between our load and our add doesn't
		// proceed to close the store. If we lost the race, decrement and
		// surface ErrStoreClosed to the caller.
		if ts.closed.Load() {
			es.activeReqs.Add(-1)
			return nil, ErrStoreClosed
		}
		// A move closes the shard once its pins drain: wait for it, then
		// check out whatever the shard is afterwards (shard_move.go).
		if es.moving.Load() {
			es.activeReqs.Add(-1)
			es.shardMu.Lock()
			es.waitMoveLocked()
			es.shardMu.Unlock()
			return es.checkoutStore(ts)
		}
		if es.currentTier() == TierCold {
			es.shardMu.Lock()
			es.waitMoveLocked()
			if es.store == nil || es.storeRO {
				if err := es.openColdWritableLocked(ts, 1); err != nil {
					es.activeReqs.Add(-1)
					es.shardMu.Unlock()
					return nil, err
				}
			}
			es.readTransientOpen = false
			store := es.store
			es.shardMu.Unlock()
			es.countAccess()
			return store, nil
		}
		es.countAccess()
		return es.store, nil
	}
	// Cold shard: acquire shardMu to atomically open + increment activeReqs.
	// This prevents closeIdleShards from closing the store between open and
	// the activeReqs increment (the v3.0.30 TOCTOU bug).
	es.shardMu.Lock()
	es.waitMoveLocked()
	if ts.closed.Load() {
		es.shardMu.Unlock()
		return nil, ErrStoreClosed
	}
	if es.store == nil || es.storeRO {
		if err := es.openColdWritableLocked(ts, 0); err != nil {
			es.shardMu.Unlock()
			return nil, err
		}
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
	es.countAccess()
	return store, nil
}

// checkoutStoreForRead pins a shard for read-only fanout work.
//
// A cold shard opened here STAYS open, and is closed by the idle timer or by
// the open-handle cap, whichever comes first. It used to be closed again the
// moment its read finished, which bounded handles at the cost of making every
// read of the same old data reopen the same shards — measured through a
// caller as 16.9s to open a case whose signals live in demoted shards, and
// 7.5s again on the very next read.
//
// MaxOpenColdShards is what makes retaining them safe: a DepthAll scan across
// a years-old store no longer accumulates one handle per historical shard,
// because the least recently used are closed once the cap is passed.
func (es *EventShard) checkoutStoreForRead(ts *Store) (*BadgerStore, func(), error) {
	return es.checkoutForRead(ts, false)
}

// checkoutStoreReadOnly is checkoutStoreForRead for a caller that only reads
// the store: while a move copies the shard it gets the read-only handle on the
// source instead of waiting for the move (MOVEREAD). A caller that may write,
// even through a store it found by a read, uses checkoutStoreForRead.
func (es *EventShard) checkoutStoreReadOnly(ts *Store) (*BadgerStore, func(), error) {
	return es.checkoutForRead(ts, true)
}

// checkoutForRead pins es for a read (checkoutStoreForRead, readOnly false;
// checkoutStoreReadOnly, true) and reports the pin to the test-only
// checkoutHook.
func (es *EventShard) checkoutForRead(ts *Store, readOnly bool) (*BadgerStore, func(), error) {
	return es.checkoutForReadCharged(ts, readOnly, nil)
}

// checkoutForReadCharged is checkoutForRead for a read under a budget (RH
// list R2, READBUDGET step 6): when es is closed, the open is charged to opens
// (store.ReadTally.ShardOpen) under the shard's lock, before the open, and a
// refusal returns the budget error with the shard left closed. An open shard
// charges nothing. A nil opens charges nothing.
func (es *EventShard) checkoutForReadCharged(ts *Store, readOnly bool, opens *storecontract.ReadTally) (*BadgerStore, func(), error) {
	store, release, err := es.pinForRead(ts, readOnly, opens)
	if err == nil {
		release = ts.noteCheckout(es, release)
	}
	return store, release, err
}

// checkoutBudgeted is checkoutStoreReadOnly for one worker of a parallel
// fan-out under a budget (READBUDGET step 6): once the read's meter refused
// (store.ReadMeter.Exceeded), the worker stops before its shard; otherwise
// checkoutForReadCharged. A nil opens is checkoutStoreReadOnly.
func (es *EventShard) checkoutBudgeted(ts *Store, opens *storecontract.ReadTally) (*BadgerStore, func(), error) {
	if err := opens.Exceeded(); err != nil {
		return nil, func() {}, err
	}
	return es.checkoutForReadCharged(ts, true, opens)
}

func (es *EventShard) pinForRead(ts *Store, readOnly bool, opens *storecontract.ReadTally) (*BadgerStore, func(), error) {
	noop := func() {}
	if err := ts.backgroundError(); err != nil {
		return nil, noop, err
	}
	if ts.closed.Load() {
		return nil, noop, ErrStoreClosed
	}
	if es.currentTier() != TierCold {
		if !readOnly {
			store, err := es.checkoutStore(ts)
			if err != nil {
				return nil, noop, err
			}
			return store, es.checkinStore, nil
		}
		// Hot/warm: the open store, pinned as checkoutStore does; a moving
		// shard takes the locked path, which reads its source.
		es.activeReqs.Add(1)
		if ts.closed.Load() {
			es.activeReqs.Add(-1)
			return nil, noop, ErrStoreClosed
		}
		if !es.moving.Load() && es.currentTier() != TierCold {
			es.countAccess()
			return es.store, es.checkinStore, nil
		}
		es.activeReqs.Add(-1)
	}

	es.shardMu.Lock()
	if readOnly {
		if store, release, ok := es.waitMoveForReadLocked(); ok {
			es.shardMu.Unlock()
			es.countAccess()
			return store, release, nil
		}
	} else {
		es.waitMoveLocked()
	}
	if ts.closed.Load() {
		es.shardMu.Unlock()
		return nil, noop, ErrStoreClosed
	}
	if es.store == nil || es.storeRO && !readOnly {
		// Every open below (a closed shard, or a read-only handle reopened
		// writable) is charged first; a refusal leaves the shard as it is.
		if err := opens.ShardOpen(); err != nil {
			es.shardMu.Unlock()
			return nil, noop, err
		}
	}
	if es.store != nil && es.storeRO && !readOnly {
		// A caller that may write: the read-only handle gives way to a
		// writable one, which stays as a read's transient open would.
		if err := es.openColdWritableLocked(ts, 0); err != nil {
			es.shardMu.Unlock()
			return nil, noop, err
		}
		es.readTransientOpen = true
	}
	if es.store == nil {
		open := es.openColdLocked
		if readOnly {
			open = es.openColdReadOnlyLocked
		}
		if err := open(ts); err != nil {
			es.shardMu.Unlock()
			return nil, noop, err
		}
		es.readTransientOpen = true
		// A read-only open leaves the sealed catalog entry alone (the shard
		// cannot change through it); a writable one unsealed it in
		// openColdLocked. The in-memory copy goes either way: while the store
		// is open it is the more direct answer.
		es.counts.Store(nil)
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
	es.countAccess()

	// The open-handle cap is NOT enforced here. This checkout is reached from
	// callers that already hold ts.mu — RebuildCatalog among them — and the cap
	// needs ts.mu to see the shard list, so taking it here deadlocks. It runs on
	// the idle sweeper instead, which owns no lock when it starts.
	return store, func() { es.checkinReadStore(ts) }, nil
}

// checkoutOpenStoreForRead pins an already-open shard for informational reads
// without lazy-opening a closed cold shard or promoting a transient cold read
// handle into a long-lived open handle.
//
// It never waits for a move: a moving shard is pinned while its store is still
// open and reported closed once the move has closed it (the close flushed and
// synced it), so DurableFlush and the listing do not stall for a copy.
func (es *EventShard) checkoutOpenStoreForRead(ts *Store) (*BadgerStore, func(), bool, error) {
	noop := func() {}
	if err := ts.backgroundError(); err != nil {
		return nil, noop, false, err
	}
	if es.currentTier() != TierCold {
		es.activeReqs.Add(1)
		if ts.closed.Load() {
			es.activeReqs.Add(-1)
			return nil, noop, false, ErrStoreClosed
		}
		if !es.moving.Load() && es.currentTier() != TierCold {
			es.countAccess()
			return es.store, es.checkinStore, true, nil
		}
		es.activeReqs.Add(-1) // moving or just demoted: the locked path below
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
	es.countAccess()

	return store, func() { es.checkinReadStore(ts) }, true, nil
}

func (es *EventShard) checkinReadStore(ts *Store) {
	es.activeReqs.Add(-1)
	es.shardMu.Lock()
	defer es.shardMu.Unlock()
	if es.store == nil || !es.readTransientOpen || es.activeReqs.Load() != 0 {
		return
	}

	// RETAINED, IF THE CALLER ASKED AND THERE IS ROOM. Closing here bounds
	// handles absolutely, at the cost of paying the open again on every read of
	// the same old data — measured through a caller as 16.9s to open a case
	// whose signals live in demoted shards, then 7.5s again on the next read.
	//
	// With MaxOpenColdShards set, up to that many stay open and the count is
	// still hard-bounded, because the shard that finds no room closes here just
	// as it always did.
	if ts.maxOpenColdShards > 0 && ts.openColdShards.Load() <= int64(ts.maxOpenColdShards) {
		return
	}

	// Same reasoning as the idle-close: record the counts while the store is
	// still open, so the shard can be counted afterwards without reopening.
	sealedCounts := es.snapshotCountsLocked(ts)
	if err := es.store.Close(); err != nil {
		wrapped := fmt.Errorf("graph: close transient cold shard %s: %w", es.name, err)
		ts.recordBackgroundError(wrapped)
		slog.Error("tiered cold shard transient close failed", "shard", es.name, "err", err)
		return
	}
	es.clearStoreLocked(ts)
	if sealedCounts {
		defer ts.saveCatalogBestEffort("seal shard counts on transient close")
	}
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
	es.countAccess()
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
	ts.refAccesses.Add(1)
	return ts.refShard, ts.noteCheckout(checkoutKeyRef, func() { ts.refActiveReqs.Add(-1) }), nil
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
	return ts.checkoutArchiveCharged(nil)
}

// checkoutArchiveCharged is checkoutArchive for a read under a budget (RH list
// R2, READBUDGET step 6): when the catalog has an archive not yet opened, the
// open is charged to opens before it, and a refusal returns the budget error
// with the archive left closed. An archive opened by another read in between
// is charged all the same (the count errs high, never low). A nil opens
// charges nothing.
func (ts *Store) checkoutArchiveCharged(opens *storecontract.ReadTally) (*BadgerStore, func(), error) {
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
		if err := opens.ShardOpen(); err != nil {
			return nil, noop, err
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
	ts.archiveAccesses.Add(1)
	return archive, ts.noteCheckout(checkoutKeyArchive, func() { ts.archiveActiveReqs.Add(-1) }), nil
}

// idleCloseLoop periodically checks cold shards and closes those that have been
// idle longer than IdleTimeout, then moves one shard to its tier's volume.
// Runs every IdleTimeout/2, the value New checked (SetIdleTimeoutForTest(0)
// between New and this goroutine's start panicked NewTicker, 4 Nov). Stopped
// via closeCh.
func (ts *Store) idleCloseLoop(every time.Duration) {
	tick := time.NewTicker(every)
	defer tick.Stop()

	for {
		select {
		case <-ts.closeCh:
			return
		case <-tick.C:
			ts.closeIdleShards()
			// Then trim anything still over the cap. Retained read handles are
			// reclaimed here rather than at check-in, so this is what keeps a
			// scan across a years-old store from holding every shard open.
			ts.enforceColdShardCap()
			ts.sweepMoveOne()
		}
	}
}

// coldShardsClosable reports whether closing a cold shard's store keeps its
// rows. An InMemory shard's BadgerStore is the only copy of them: a lazy open
// of the same path after a close starts empty, so the idle sweep and the
// open-handle cap leave in-memory shards open (their bound is memory, which a
// close would only free by dropping the data).
func (ts *Store) coldShardsClosable() bool { return !ts.inMemory }

// closeEmptyDemotedShardsLocked follows committed rotation under lifecycleMu.RLock
// and ts.mu.Lock, closing the demoted disk shards HoldsNoRows proves empty. It
// never waits for a pin: retained handles use normal idle cleanup.
//
// It runs on the write path (checkRotation) after the rotation committed, so
// it returns nothing: a failed probe keeps the handle for the idle sweep and is
// logged; a failed close records the background-error fence exactly as a
// failed idle close does (checkinReadStore). Neither fails the triggering write
// by itself.
func (ts *Store) closeEmptyDemotedShardsLocked(oldHot *EventShard, demoted []*EventShard) {
	ts.scopeMu.Lock()
	scopeOpen := ts.scopeOpen
	ts.scopeMu.Unlock()
	sealedAny := false
	closeEmpty := func(es *EventShard) {
		es.shardMu.Lock()
		defer es.shardMu.Unlock()
		if es.currentTier() != TierCold {
			return
		}
		es.countColdStoreLocked(ts)
		// A legacy scope may hold log records outside HoldsNoRows' keyspace.
		if !ts.coldShardsClosable() || scopeOpen || es.moving.Load() || es.store == nil || es.activeReqs.Load() != 0 {
			return
		}
		// The cold tier is already published: stale warm checkouts recheck it
		// after pinning and must take shardMu before accessing the handle.
		empty, err := es.store.HoldsNoRows()
		if err != nil {
			slog.Warn("graph: probe demoted shard; handle kept for the idle sweep", "shard", es.name, "err", err)
			return
		}
		if !empty {
			return
		}
		sealedAny = es.snapshotCountsLocked(ts) || sealedAny
		err = es.store.Close()
		es.clearStoreLocked(ts)
		if err != nil {
			// Close may have closed the DB despite its error: detach, and fence
			// later access; the committed topology is not rolled back.
			ts.recordBackgroundError(fmt.Errorf("graph: close empty demoted shard %s: %w", es.name, err))
			slog.Error("tiered empty demoted shard close failed", "shard", es.name, "err", err)
		}
	}
	closeEmpty(oldHot)
	for _, es := range demoted {
		closeEmpty(es)
	}
	if sealedAny {
		ts.saveCatalogBestEffort("seal empty shard counts on demotion")
	}
}

// idleTimeoutValue returns the cold-shard idle timeout.
func (ts *Store) idleTimeoutValue() time.Duration { return time.Duration(ts.idleTimeout.Load()) }

// closeIdleShards closes cold shards that have been idle longer than idleTimeout.
func (ts *Store) closeIdleShards() {
	if !ts.coldShardsClosable() {
		return
	}
	nowMs := time.Now().UnixMilli()
	thresholdMs := ts.idleTimeoutValue().Milliseconds()

	ts.mu.RLock()
	var coldShards []*EventShard
	for _, es := range ts.eventShards {
		if es.currentTier() == TierCold {
			coldShards = append(coldShards, es)
		}
	}
	ts.mu.RUnlock()

	sealedAny := false
	for _, es := range coldShards {
		es.shardMu.Lock()
		if es.store != nil && es.activeReqs.Load() == 0 && ((nowMs-es.lastAccess.Load()) > thresholdMs || es.openEmptyLocked()) {
			// Record what it holds BEFORE closing: after this the shard cannot
			// change, so the snapshot stays true until it is opened again, and
			// count folds stop reopening it just to add up numbers.
			sealedAny = es.snapshotCountsLocked(ts) || sealedAny
			if err := es.store.Close(); err != nil {
				wrapped := fmt.Errorf("graph: idle-close cold shard %s: %w", es.name, err)
				ts.recordBackgroundError(wrapped)
				slog.Error("tiered cold shard idle-close failed", "shard", es.name, "err", err)
			}
			es.clearStoreLocked(ts)
		}
		es.shardMu.Unlock()
	}

	// One save for the whole sweep, outside every shard lock. Sealing is worth
	// persisting even if several shards closed: the alternative is the next
	// start reopening each of them to recount what they held.
	if sealedAny {
		ts.saveCatalogBestEffort("seal shard counts on idle-close")
	}
}

// openEmptyLocked reports whether the open store holds no row. Closing it seals
// that: fan-out reads keep such a shard's last access fresh and its cap slot
// taken until they stop visiting it. Caller holds es.shardMu.
func (es *EventShard) openEmptyLocked() bool {
	none, err := es.store.HoldsNoRows()
	return err == nil && none
}

// saveCatalogBestEffort persists the catalog and logs rather than fails.
//
// Every caller here is adjusting COUNT bookkeeping, which is an optimisation:
// if the save is lost the numbers are recomputed on the next start, slowly but
// correctly. That is not worth failing a close or a checkout over.
func (ts *Store) saveCatalogBestEffort(what string) {
	if err := ts.catalog.Save(); err != nil {
		slog.Error("graph: persist shard catalog", "for", what, "error", err)
	}
}
