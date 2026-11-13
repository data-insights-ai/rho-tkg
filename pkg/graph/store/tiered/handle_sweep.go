package tiered

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// The handle sweep (R3/WARMMEM, tasks/evidence/R3/design-sweep.txt). A
// rotation leaves the old hot shard warm on its hot-size handle (a memtable
// arena of about 1.3 x MemTableSize) and a demotion leaves a cold shard on the
// handle it had while warm. The sweep, a goroutine off the write path, swaps
// those handles: a warm shard's hot-size handle is closed and reopened
// writable at WarmMemTableSize, a demoted shard still on the handle it had
// while hot or warm (either size) has it closed (its next read opens it
// read-only, a late write upgrades it); a handle a cold read or late write
// opened (coldOpened) is left to IdleTimeout. A swap is a move to the
// same volume without the copy: it takes the shard's move gate (MoveShard),
// waits for the pins to drain, closes, and reopens, holding only the shard's
// shardMu and never ts.mu, so writers to other shards never wait and writers
// to the swapping shard wait one swap.
//
// A pass visits only the shards that owe work (handleSweep.pending), so its
// cost follows the work and not the shards the store retains: a shard enters
// the set when a rotation leaves it on a handle the sweep owes a swap or a
// close (sweepPend), and stays in it while a scope, a pin or a hold defers
// the swap.

// handleSweep is the sweep goroutine's state.
type handleSweep struct {
	kick    chan struct{}                           // buffered (1): a rotation or the idle tick wakes the sweep
	done    chan struct{}                           // closed when the loop returns; nil when it never started
	swaps   atomic.Int64                            // warm handles reopened at WarmMemTableSize
	closes  atomic.Int64                            // demoted handles closed
	fault   atomic.Pointer[func(step string) error] // test hook (sweepFaultAt)
	drain   atomic.Int64                            // test hook: the pin drain deadline in ns; 0 is sweepDrainTimeout
	passes  atomic.Int64                            // passes finished (a test waits on it)
	visited atomic.Int64                            // shards the passes looked at (a test counts what a pass costs)

	// passMu keeps one pass at a time, the goroutine's and a test's: a pass
	// returns once the shards before it are settled.
	passMu sync.Mutex
	// mu guards pending; a leaf lock, taken under ts.mu and shardMu.
	mu sync.Mutex
	// pending holds the shards that owe a swap or a close: the shard a
	// rotation turned warm on its hot-size handle or demoted with its handle
	// open, a cold window shard made open, and any shard a pass could not
	// settle (a scope open, a pin that outlasted the drain deadline).
	pending map[*EventShard]struct{}
}

// sweepDrainTimeout bounds how long a swap waits for its shard's pins to
// drain. New checkouts of the shard wait on the gate meanwhile, so one long
// pin (an open iterator) would stall every reader and writer of the shard; the
// swap gives up instead and the shard is retried on the next kick.
const sweepDrainTimeout = time.Second

func (ts *Store) sweepDrainDeadline() time.Time {
	d := time.Duration(ts.sweep.drain.Load())
	if d <= 0 {
		d = sweepDrainTimeout
	}
	return time.Now().Add(d)
}

// sweepAction is what the sweep owes a shard's handle.
type sweepAction int

const (
	sweepNone   sweepAction = iota
	sweepReopen             // warm on a hot-size handle: reopen at WarmMemTableSize
	sweepClose              // cold on the handle it had while hot or warm (the demoted chain): close
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

// sweepPend adds the shards that owe a handle swap or close to the set the
// next pass visits. Any lock may be held. A store without a sweep (in memory)
// keeps nothing.
func (ts *Store) sweepPend(shards ...*EventShard) {
	if ts.sweep.kick == nil {
		return
	}
	ts.sweep.mu.Lock()
	defer ts.sweep.mu.Unlock()
	if ts.sweep.pending == nil {
		ts.sweep.pending = make(map[*EventShard]struct{}, len(shards))
	}
	for _, es := range shards {
		if es != nil {
			ts.sweep.pending[es] = struct{}{}
		}
	}
}

// takeSweepPending hands the pending shards to a pass and empties the set; a
// shard the pass cannot settle goes back with sweepPend.
func (ts *Store) takeSweepPending() []*EventShard {
	ts.sweep.mu.Lock()
	defer ts.sweep.mu.Unlock()
	if len(ts.sweep.pending) == 0 {
		return nil
	}
	shards := make([]*EventShard, 0, len(ts.sweep.pending))
	for es := range ts.sweep.pending {
		shards = append(shards, es)
	}
	clear(ts.sweep.pending)
	return shards
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

// sweepHandles runs one pass over the pending shards (sweepPend), one at a
// time. A shard with pins held is left to the end of the pass, so it does not
// delay the others; a shard that still owes work after the pass (a scope open,
// a pin past the drain deadline, a swap a test holds) goes back to the set for
// the next kick. Passes do not overlap.
func (ts *Store) sweepHandles() {
	if ts.inMemory {
		return
	}
	ts.sweep.passMu.Lock()
	defer ts.sweep.passMu.Unlock()
	defer ts.sweep.passes.Add(1)
	sealed := false
	var pinned []*EventShard
	for _, es := range ts.takeSweepPending() {
		if ts.closing.Load() {
			break
		}
		ts.sweep.visited.Add(1)
		res := ts.sweepShard(es, false)
		sealed = res.sealed || sealed
		switch {
		case res.pinned:
			pinned = append(pinned, es)
		case res.retry:
			ts.sweepPend(es)
		}
	}
	for _, es := range pinned {
		if ts.closing.Load() {
			break
		}
		res := ts.sweepShard(es, true)
		sealed = res.sealed || sealed
		if res.retry {
			ts.sweepPend(es)
		}
	}
	if sealed {
		ts.saveCatalogBestEffort("seal shard counts on a demoted handle's close")
	}
}

// sweepActionLocked is what the sweep owes es now; deferred reports a swap or
// close owed but not allowed yet (a scope is open), so the shard stays
// pending. Caller holds es.shardMu.
func (ts *Store) sweepActionLocked(es *EventShard) (act sweepAction, deferred bool) {
	if ts.inMemory || ts.closing.Load() || es.store == nil || es.storeRO {
		return sweepNone, false
	}
	switch es.currentTier() {
	case TierWarm:
		if es.hotArena && ts.warmMemTableSize > 0 {
			act = sweepReopen
		}
	case TierCold:
		// Only the demoted chain (the handle the shard had while hot or
		// warm, whatever its size): a handle a cold read or late write
		// opened is left to IdleTimeout, or a shard with steady late writes
		// would reopen on every kick.
		if !es.coldOpened {
			act = sweepClose
		}
	}
	if act == sweepNone {
		return sweepNone, false
	}
	// A scoped shard must not change its handle mid-scope (as
	// closeEmptyDemotedShardsLocked): its buffered records live on the handle.
	ts.scopeMu.Lock()
	scopeOpen := ts.scopeOpen
	ts.scopeMu.Unlock()
	if scopeOpen {
		return sweepNone, true
	}
	return act, false
}

// sweepResult is what a visit of one shard left behind.
type sweepResult struct {
	sealed bool // the close sealed the shard's counts (the pass saves the catalog once)
	retry  bool // the shard still owes work: a scope, a long pin or a held swap deferred it
	pinned bool // pins are held and the visit was not patient: the pass comes back at its end
}

// sweepShard swaps es's handle when it owes a swap, behind the shard's move
// gate. A patient visit waits for the pins to drain up to the deadline; the
// first visit of a pass does not wait: a shard with pins held is reported
// (pinned) before it takes its gate, and the pass settles the others first.
func (ts *Store) sweepShard(es *EventShard, patient bool) (res sweepResult) {
	es.shardMu.Lock()
	es.waitMoveLocked()
	act, deferred := ts.sweepActionLocked(es)
	if act == sweepNone {
		es.shardMu.Unlock()
		res.retry = deferred
		return res
	}
	if !patient && es.activeReqs.Load() != 0 {
		es.shardMu.Unlock()
		res.pinned = true
		return res
	}
	done := make(chan struct{})
	es.moveDone = done
	es.moving.Store(true)
	es.shardMu.Unlock()

	var store *BadgerStore // the reopened handle, installed with the gate's release
	defer func() {
		es.shardMu.Lock()
		switch {
		case store != nil && es.currentTier() == TierHot && !ts.closing.Load():
			ts.installPromotedLocked(es, store)
		case store != nil:
			ts.installSweptLocked(es, store)
		}
		ts.endHandleGateLocked(es, done)
		es.shardMu.Unlock()
	}()

	if err := ts.sweepFaultAt("gated"); err != nil {
		res.retry = true // held: the shard owes its swap still
		return res
	}
	act, sealed, st := ts.sweepClose(es, patient)
	res.sealed = sealed
	switch st {
	case closePinned:
		res.pinned, res.retry = !patient, patient
		return res
	case closeDeferred:
		res.retry = true
		return res
	case closeSettled:
		return res
	}
	if act == sweepClose {
		ts.sweep.closes.Add(1)
		return res
	}
	if err := ts.sweepFaultAt("closed"); err != nil || ts.closing.Load() {
		// Not reopened: cold in memory (the catalog keeps warm), so until
		// Close marks the store closed an access takes the locked path,
		// which handles a closed shard (MoveShard's pattern).
		es.shardMu.Lock()
		es.setTier(TierCold)
		es.shardMu.Unlock()
		return res
	}
	s, err := ts.openBadgerStoreWithRecovery(es.path)
	if err != nil {
		es.shardMu.Lock()
		es.setTier(TierCold)
		ts.catalog.UpdateShardTier(es.name, TierCold)
		es.shardMu.Unlock()
		ts.saveCatalogBestEffort("demote a swept shard that did not reopen")
		slog.Error("tiered: swept shard did not reopen; demoted to cold", "shard", es.name, "err", err)
		return res
	}
	store = s
	return res
}

// closeStatus is how sweepClose ended.
type closeStatus int

const (
	closeDone     closeStatus = iota // the handle is closed
	closeSettled                     // nothing is owed any more (a promotion, Close, a shard without a handle)
	closeDeferred                    // owed, but a scope opened meanwhile
	closePinned                      // the pins did not drain
)

// sweepClose closes es's handle once its pins drain (new checkouts wait on the
// gate). A patient close waits for them up to the deadline (sweepDrainTimeout)
// and then gives up (closePinned, the "abandoned" step, the gate dropped by
// the caller's defer); an impatient one looks once. It decides again under
// shardMu what the shard owes: a scope opened (closeDeferred) or a promotion
// meanwhile leaves the handle as it is. A cold shard's counts are sealed
// first, as on its idle close.
func (ts *Store) sweepClose(es *EventShard, patient bool) (act sweepAction, sealed bool, st closeStatus) {
	var deadline time.Time
	if patient {
		deadline = ts.sweepDrainDeadline()
	}
	for {
		es.shardMu.Lock()
		if es.activeReqs.Load() == 0 {
			break
		}
		es.shardMu.Unlock()
		if ts.closing.Load() {
			return sweepNone, false, closeSettled
		}
		if !patient {
			return sweepNone, false, closePinned
		}
		if time.Now().After(deadline) {
			// A long pin: give up and drop the gate (the caller's defer);
			// the shard is retried on the next kick.
			slog.Info("tiered: handle sweep: pins did not drain, swap retried later", "shard", es.name, "pins", es.activeReqs.Load())
			_ = ts.sweepFaultAt("abandoned")
			return sweepNone, false, closePinned
		}
		time.Sleep(time.Millisecond)
	}
	defer es.shardMu.Unlock()
	act, deferred := ts.sweepActionLocked(es)
	if act == sweepNone {
		if deferred {
			return sweepNone, false, closeDeferred
		}
		return sweepNone, false, closeSettled
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
	return act, sealed, closeDone
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
	// A scope opened between the close and now skipped the closed shard
	// (forEachScopeShard): the reopened handle joins it, or its writes
	// self-commit records the scope's discard cannot reach.
	ts.handNewShardToLogScope(store, es.name)
	ts.sweep.swaps.Add(1)
}

// installPromotedLocked replaces the warm-size handle a swap reopened with a
// hot-size one: a rotation promoted es to hot after the swap's recheck (it
// flips the tier under shardMu and then waits for this gate), and a hot shard
// takes the store's writes. When the hot open fails the shard keeps a
// warm-size handle. Caller holds es.shardMu; the gate is still up.
func (ts *Store) installPromotedLocked(es *EventShard, warm *BadgerStore) {
	if err := warm.Close(); err != nil {
		slog.Error("tiered: handle sweep: close the warm handle of a promoted shard", "shard", es.name, "err", err)
	}
	hot, err := ts.openExistingBadgerStore(es.path)
	hotSize := err == nil
	if err != nil {
		slog.Error("tiered: handle sweep: promoted shard did not reopen at the hot size", "shard", es.name, "err", err)
		if hot, err = ts.openBadgerStoreWithRecovery(es.path); err != nil {
			ts.recordBackgroundError(fmt.Errorf("graph: handle sweep: reopen promoted shard %s: %w", es.name, err))
			return
		}
	}
	es.setStoreLocked(ts, hot)
	es.hotArena = hotSize
	// A scope opened between the swap's close and now skipped the closed
	// shard, as in installSweptLocked: the new handle joins it.
	ts.handNewShardToLogScope(hot, es.name)
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
