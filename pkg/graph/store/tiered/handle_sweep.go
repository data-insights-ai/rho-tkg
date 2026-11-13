package tiered

import (
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"
)

// The handle sweep (R3/WARMMEM, tasks/evidence/R3/design-sweep.txt). A
// rotation leaves the old hot shard warm on its hot-size handle (a memtable
// arena of about 1.3 x MemTableSize) and a demotion leaves a cold shard on the
// handle it had while warm. The sweep, a goroutine off the write path, swaps
// those handles: a warm shard's hot-size handle is closed and reopened
// writable at WarmMemTableSize, a demoted shard still on its hot-size handle
// has it closed (its next read opens it read-only, a late write upgrades it);
// a warm-size handle, a late write's or one the sweep reopened before the
// demotion, is left to IdleTimeout. A swap is a move to the
// same volume without the copy: it takes the shard's move gate (MoveShard),
// waits for the pins to drain, closes, and reopens, holding only the shard's
// shardMu and never ts.mu, so writers to other shards never wait and writers
// to the swapping shard wait one swap.

// handleSweep is the sweep goroutine's state.
type handleSweep struct {
	kick   chan struct{}                           // buffered (1): a rotation or the idle tick wakes the sweep
	done   chan struct{}                           // closed when the loop returns; nil when it never started
	swaps  atomic.Int64                            // warm handles reopened at WarmMemTableSize
	closes atomic.Int64                            // demoted handles closed
	fault  atomic.Pointer[func(step string) error] // test hook (sweepFaultAt)
}

// sweepAction is what the sweep owes a shard's handle.
type sweepAction int

const (
	sweepNone   sweepAction = iota
	sweepReopen             // warm on a hot-size handle: reopen at WarmMemTableSize
	sweepClose              // cold on a hot-size handle (the demoted hot chain): close
)

// startHandleSweep starts the sweep goroutine; Close stops it (closeCh) and
// waits for it. An in-memory store has no sweep: a close would drop the data
// (coldShardsClosable).
func (ts *Store) startHandleSweep() {
	if ts.inMemory {
		return
	}
	ts.sweep.kick = make(chan struct{}, 1)
	ts.sweep.done = make(chan struct{})
	go ts.handleSweepLoop()
}

// stopHandleSweep waits for the sweep goroutine to return; the caller closed
// closeCh and set closing, which ends a swap in progress before its reopen.
func (ts *Store) stopHandleSweep() {
	if ts.sweep.done != nil {
		<-ts.sweep.done
	}
}

// kickHandleSweep wakes the sweep without blocking; a kick already pending
// covers this one.
func (ts *Store) kickHandleSweep() {
	if ts.sweep.kick == nil {
		return
	}
	select {
	case ts.sweep.kick <- struct{}{}:
	default:
	}
}

func (ts *Store) handleSweepLoop() {
	defer close(ts.sweep.done)
	for {
		select {
		case <-ts.closeCh:
			return
		case <-ts.sweep.kick:
			ts.sweepHandles()
		}
	}
}

// sweepHandles runs one pass: every event shard but the hot one, one at a
// time. A shard skipped (a scope open, a pin that outlived Close's start) is
// retried on the next kick.
func (ts *Store) sweepHandles() {
	if ts.inMemory {
		return
	}
	ts.mu.RLock()
	shards := make([]*EventShard, 0, len(ts.eventShards))
	for _, es := range ts.eventShards {
		if es != nil && es != ts.hotShard {
			shards = append(shards, es)
		}
	}
	ts.mu.RUnlock()
	sealed := false
	for _, es := range shards {
		if ts.closing.Load() {
			break
		}
		sealed = ts.sweepShard(es) || sealed
	}
	if sealed {
		ts.saveCatalogBestEffort("seal shard counts on a demoted handle's close")
	}
}

// sweepActionLocked is what the sweep owes es now. Caller holds es.shardMu.
func (ts *Store) sweepActionLocked(es *EventShard) sweepAction {
	if ts.inMemory || ts.closing.Load() || es.store == nil || es.storeRO {
		return sweepNone
	}
	// A scoped shard must not change its handle mid-scope (as
	// closeEmptyDemotedShardsLocked): its buffered records live on the handle.
	ts.scopeMu.Lock()
	scopeOpen := ts.scopeOpen
	ts.scopeMu.Unlock()
	if scopeOpen {
		return sweepNone
	}
	switch es.currentTier() {
	case TierWarm:
		if es.hotArena && ts.warmMemTableSize > 0 {
			return sweepReopen
		}
	case TierCold:
		// Only the demoted hot chain: a handle a late write opened (warm
		// size) is left to IdleTimeout, or a shard with steady late writes
		// would reopen on every kick.
		if es.hotArena {
			return sweepClose
		}
	}
	return sweepNone
}

// sweepShard swaps es's handle when it owes a swap, behind the shard's move
// gate. It reports whether the close sealed the shard's counts (the caller
// saves the catalog once per pass).
func (ts *Store) sweepShard(es *EventShard) (sealed bool) {
	es.shardMu.Lock()
	es.waitMoveLocked()
	if ts.sweepActionLocked(es) == sweepNone {
		es.shardMu.Unlock()
		return false
	}
	done := make(chan struct{})
	es.moveDone = done
	es.moving.Store(true)
	es.shardMu.Unlock()

	var store *BadgerStore // the reopened handle, installed with the gate's release
	defer func() {
		es.shardMu.Lock()
		if store != nil {
			ts.installSweptLocked(es, store)
		}
		ts.endHandleGateLocked(es, done)
		es.shardMu.Unlock()
	}()

	if err := ts.sweepFaultAt("gated"); err != nil {
		return false
	}
	act, sealed, ok := ts.sweepClose(es)
	if !ok {
		return false
	}
	if act == sweepClose {
		ts.sweep.closes.Add(1)
		return sealed
	}
	if err := ts.sweepFaultAt("closed"); err != nil || ts.closing.Load() {
		// Not reopened: cold in memory (the catalog keeps warm), so until
		// Close marks the store closed an access takes the locked path,
		// which handles a closed shard (MoveShard's pattern).
		es.shardMu.Lock()
		es.setTier(TierCold)
		es.shardMu.Unlock()
		return sealed
	}
	s, err := ts.openBadgerStoreWithRecovery(es.path)
	if err != nil {
		es.shardMu.Lock()
		es.setTier(TierCold)
		ts.catalog.UpdateShardTier(es.name, TierCold)
		es.shardMu.Unlock()
		ts.saveCatalogBestEffort("demote a swept shard that did not reopen")
		slog.Error("tiered: swept shard did not reopen; demoted to cold", "shard", es.name, "err", err)
		return sealed
	}
	store = s
	return sealed
}

// sweepClose closes es's handle once its pins drain (new checkouts wait on the
// gate), deciding again under shardMu what the shard owes: a scope opened or a
// promotion meanwhile leaves the handle as it is (ok false). A cold shard's
// counts are sealed first, as on its idle close.
func (ts *Store) sweepClose(es *EventShard) (act sweepAction, sealed, ok bool) {
	for {
		es.shardMu.Lock()
		if es.activeReqs.Load() == 0 {
			break
		}
		es.shardMu.Unlock()
		if ts.closing.Load() {
			return sweepNone, false, false
		}
		time.Sleep(time.Millisecond)
	}
	defer es.shardMu.Unlock()
	if act = ts.sweepActionLocked(es); act == sweepNone {
		return sweepNone, false, false
	}
	sealed = es.currentTier() == TierCold && es.snapshotCountsLocked(ts)
	err := es.store.Close()
	es.clearStoreLocked(ts)
	if err != nil {
		// Close may have closed the DB despite its error: detach, and fence
		// later access (closeEmptyDemotedShardsLocked); a warm shard still
		// reopens, or is demoted when it cannot.
		ts.recordBackgroundError(fmt.Errorf("graph: handle sweep: close shard %s: %w", es.name, err))
		slog.Error("tiered: handle sweep close failed", "shard", es.name, "err", err)
	}
	return act, sealed, true
}

// installSweptLocked installs the reopened handle on es, unless the store
// began closing or the shard was demoted meanwhile: then the handle closes and
// the shard stays closed (cold; a demotion's next open drops the relationship
// temporal indexes a warm open kept). Caller holds es.shardMu.
func (ts *Store) installSweptLocked(es *EventShard, store *BadgerStore) {
	closing := ts.closing.Load()
	if closing || es.currentTier() == TierCold {
		if closing {
			es.setTier(TierCold)
		}
		if err := store.Close(); err != nil {
			slog.Error("tiered: handle sweep: close a reopened handle not installed", "shard", es.name, "err", err)
		}
		return
	}
	es.setStoreLocked(ts, store)
	ts.sweep.swaps.Add(1)
}

// endHandleGateLocked releases a shard's move gate, first bringing its handle
// up to the store's registries: a SetNameRegistries or SetPropertyKeyRegistry
// that ran while the gate was up skipped the shard (it reads es.store under
// shardMu and skips a moving shard), and the handle opened from the registries
// stored before it. The setters store the registry before they take shardMu,
// so one that took it before this release is seen here, and one after it finds
// the shard open and not moving. Caller holds es.shardMu.
func (ts *Store) endHandleGateLocked(es *EventShard, done chan struct{}) {
	if s := es.store; s != nil {
		s.SetNameRegistries(ts.labelNames.Load(), ts.relTypeNames.Load())
		// Only a registry that changed: setting the same one again would
		// re-seed the shard's persisted-key watermark past keys not yet
		// committed.
		if reg := ts.propKeyReg.Load(); reg != nil && s.PropertyKeyRegistry() != reg {
			s.SetPropertyKeyRegistry(reg)
		}
	}
	es.moveDone = nil
	es.moving.Store(false)
	close(done)
}

func (ts *Store) sweepFaultAt(step string) error {
	if f := ts.sweep.fault.Load(); f != nil {
		return (*f)(step)
	}
	return nil
}
