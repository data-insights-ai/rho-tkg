package tiered

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badger "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
)

// The handle sweep (R3/WARMMEM). A
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
//
// The sweep also owns a closed cold shard's derived state. That state is four
// facts of the shard's catalog entry: the recovery mark, the sealed counts, the
// no-rows mark and the former-label set. An entry that holds them all is
// settled (ShardEntry.settled): no metered open of the shard is refused and
// every door that can answer or skip from the seal does. A closed cold shard
// whose entry is not settled is owed. Nothing else says so: no flag, no list
// is the truth, and a kill leaves the entry as the next New reads it. Three
// events queue a shard (handleSweep.owed):
//   - New, for every cold shard whose loaded entry is not settled
//     (sweepOweAtOpen);
//   - a close that leaves its seal incomplete (a close on a read's or a
//     write's path walks no history): closeStoreLocked, which every run-time
//     close of an installed handle goes through (sweepOweClosedLocked);
//   - a metered read's open refused for a repair it cannot be charged for
//     (sweepOweRepairLocked).
//
// One step at a time pays it (sweepSettle), the only code that opens a closed
// cold shard for its own sake:
//   - the repair (settleRepairLocked), for a shard marked for recovery: a
//     writable open, which recovers and repairs the directory, the seal, the
//     close, whose image clears the mark. New runs it in the foreground for the
//     shards its catalog marks, so open time follows the marked shards: the
//     handles open writable at the kill, and a shard whose repair was owed
//     or had failed when the process ended (such a mark survives a clean
//     Close);
//   - the build, for any other owed shard: a read-only open as a metered read
//     makes it (it refuses a directory that needs a repair: the step then marks
//     the shard, and the repair is its next step), one walk of the shard's
//     history rows for the set, the seal, the close.
//
// A pass runs one step, after its handle work, saves the catalog and kicks
// itself while shards are queued: a pass is bounded by one shard, a kill loses
// the step in flight, and a rotation's swap waits for one step at most. The
// build runs behind the shard's move gate, not under its shardMu, so a reader
// or writer of that shard waits for the step and nothing else does.

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
	settles atomic.Int64                            // settle steps the passes ran (a test counts them against the passes)

	// afterScopeFlag is a test hook: BeginLogScope runs it between setting the
	// scope's flag and folding the begin over the open shards, where a swap
	// can install a handle and hand it to the scope (set before the store is
	// used by other goroutines).
	afterScopeFlag func()

	// passMu keeps one pass at a time, the goroutine's and a test's: a pass
	// returns once the shards before it are settled.
	passMu sync.Mutex
	// mu guards pending and owed; a leaf lock, taken under ts.mu and shardMu.
	mu sync.Mutex
	// pending holds the shards that owe a swap or a close: the shard a
	// rotation turned warm on its hot-size handle or demoted with its handle
	// open, a cold window shard made open, and any shard a pass could not
	// settle (a scope open, a pin that outlasted the drain deadline).
	pending map[*EventShard]struct{}
	// owed is the queue of closed cold shards that may owe a settle step, in
	// the order they are settled; owedSet holds the same shards once each,
	// true for one a refused metered read handed over (those lead the queue).
	// It is a work list, not the truth: the step reads the shard's catalog
	// entry again under its shardMu, and decides the shard's place in the
	// queue under that lock too, as the close and the refusal that queue a
	// shard do.
	owed    []*EventShard
	owedSet map[*EventShard]bool
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
			if ts.sweepHandles() {
				ts.kickHandleSweep() // one step a pass: the next queued shard
			}
		}
	}
}

// sweepHandles runs one pass over the pending shards (sweepPend), one at a
// time. A shard with pins held is left to the end of the pass, so it does not
// delay the others; a shard that still owes work after the pass (a scope open,
// a pin past the drain deadline, a swap a test holds) goes back to the set for
// the next kick. Then the pass runs at most one settle step for a queued
// closed cold shard (sweepSettleOne) and saves the catalog, so the step is
// durable when the pass ends. Passes do not overlap. It reports that a step
// ran and shards are still queued: the caller runs another pass.
func (ts *Store) sweepHandles() (more bool) {
	if ts.inMemory {
		return false
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
	settled, more := ts.sweepSettleOne()
	if sealed || settled {
		ts.saveCatalogBestEffort("seal shard counts on a demoted handle's close or a settle step")
	}
	return more
}

// sweepOwe queues shards for a settle step at the end of the queue, each
// once. Any lock may be held. A store without a sweep (in memory, or New
// before it starts one) keeps nothing.
func (ts *Store) sweepOwe(shards ...*EventShard) {
	if ts.sweep.kick == nil {
		return
	}
	ts.sweep.mu.Lock()
	defer ts.sweep.mu.Unlock()
	if ts.sweep.owedSet == nil {
		ts.sweep.owedSet = make(map[*EventShard]bool, len(shards))
	}
	for _, es := range shards {
		if es == nil {
			continue
		}
		if _, queued := ts.sweep.owedSet[es]; !queued {
			ts.sweep.owedSet[es] = false
			ts.sweep.owed = append(ts.sweep.owed, es)
		}
	}
}

// sweepOweFirst puts es ahead of the queue, behind the shards refused reads
// handed over that lead it: refused marks es as one of those (a reader is
// waiting on it: sweepOweRepairLocked); without it es is a shard in the middle
// of its steps, which keeps its turn ahead of the shards not yet started but
// never ahead of a refused reader's. The refused shards are the queue's head,
// in the order of their first refusals: one refused again keeps its place.
func (ts *Store) sweepOweFirst(es *EventShard, refused bool) {
	if ts.sweep.kick == nil {
		return
	}
	ts.sweep.mu.Lock()
	defer ts.sweep.mu.Unlock()
	if ts.sweep.owedSet == nil {
		ts.sweep.owedSet = make(map[*EventShard]bool)
	}
	if was, queued := ts.sweep.owedSet[es]; queued {
		if was && refused {
			return
		}
		i := slices.Index(ts.sweep.owed, es)
		ts.sweep.owed = slices.Delete(ts.sweep.owed, i, i+1)
	}
	at := 0
	for at < len(ts.sweep.owed) && ts.sweep.owedSet[ts.sweep.owed[at]] {
		at++
	}
	ts.sweep.owedSet[es] = refused
	ts.sweep.owed = slices.Insert(ts.sweep.owed, at, es)
}

// firstOwed is the first queued shard behind the skip that lead the queue
// (the refused shards a pass looked at and left waiting in place:
// sweepPutBack), nil when there is none. It stays queued while its step runs:
// the step takes it off (sweepUnqueue) or leaves it for another step.
func (ts *Store) firstOwed(skip int) *EventShard {
	ts.sweep.mu.Lock()
	defer ts.sweep.mu.Unlock()
	if skip >= len(ts.sweep.owed) {
		return nil
	}
	return ts.sweep.owed[skip]
}

// sweepUnqueue takes es off the queue; back puts it at its end instead, no
// longer as a refused read's shard (a shard still being written: the next
// one's turn).
func (ts *Store) sweepUnqueue(es *EventShard, back bool) {
	ts.sweep.mu.Lock()
	defer ts.sweep.mu.Unlock()
	if _, queued := ts.sweep.owedSet[es]; !queued {
		return
	}
	i := slices.Index(ts.sweep.owed, es)
	ts.sweep.owed = slices.Delete(ts.sweep.owed, i, i+1)
	if back {
		ts.sweep.owedSet[es] = false
		ts.sweep.owed = append(ts.sweep.owed, es)
		return
	}
	delete(ts.sweep.owedSet, es)
}

// sweepPutBack makes room for the next shard when es cannot have its step now
// (behind a move's or a swap's gate, or while a Clear runs). A refused read's
// shard stays where it is, at the head of the queue (stays true: the pass
// looks behind it), so its repair is the first step once it can run; any other
// shard goes to the end.
func (ts *Store) sweepPutBack(es *EventShard) (stays bool) {
	ts.sweep.mu.Lock()
	refused := ts.sweep.owedSet[es]
	ts.sweep.mu.Unlock()
	if refused {
		return true
	}
	ts.sweepUnqueue(es, true)
	return false
}

// owedQueued is the number of queued shards, and whether es is one of them
// (nil: not asked).
func (ts *Store) owedQueued(es *EventShard) (n int, queued bool) {
	ts.sweep.mu.Lock()
	defer ts.sweep.mu.Unlock()
	_, queued = ts.sweep.owedSet[es]
	return len(ts.sweep.owed), queued
}

// settleGiveUp is how many settle steps in a row the sweep spends on a shard
// without settling it before it leaves the shard alone
// (EventShard.settleTries). A shard that needs every step takes three: a build
// whose open is refused, the repair, the build.
const settleGiveUp = 3

// owedLocked reports that es is owed: a closed cold shard on disk whose
// catalog entry is not settled, which the sweep may still settle
// (settleTries). Caller holds es.shardMu.
func (ts *Store) owedLocked(es *EventShard) bool {
	return ts.settleableLocked(es) && ts.catalog.shardOwes(es.name)
}

// settleableLocked is owedLocked without the catalog lookup: es is a closed
// cold shard on disk that the sweep has not given up on, in a store that is
// not closing. Caller holds es.shardMu.
func (ts *Store) settleableLocked(es *EventShard) bool {
	return !ts.inMemory && !ts.closing.Load() && es.settleTries < settleGiveUp && es.store == nil && es.currentTier() == TierCold
}

// sweepOweClosedLocked queues es when the close that just ended left its seal
// incomplete (closeStoreLocked: no counts, or no former-label set), and kicks
// the sweep unless the shard is still being written (settleQuietLocked: the
// idle tick's kick comes back). An entry that carries the recovery mark is
// not this entrance's: a close that wrote no close image leaves the mark, and
// then New or the first metered read's refusal hands the shard over; a close
// decides nothing about a directory it did not prove clean. Caller holds
// es.shardMu.
func (ts *Store) sweepOweClosedLocked(es *EventShard) {
	if ts.sweep.kick == nil || !ts.settleableLocked(es) {
		return
	}
	if owes, marked := ts.catalog.shardOwed(es.name); !owes || marked {
		return
	}
	ts.sweepOwe(es)
	if ts.settleQuietLocked(es) {
		ts.kickHandleSweep()
	}
}

// settleQuietLocked reports that es's build may run now: no write's handle
// closed at its check-in within the last IdleTimeout. A by-ID write into a
// cold shard at MaxOpenColdShards 0 opens it writable, which drops its seal,
// and closes it at once; a build after each such close would walk the shard's
// whole history once per write of a burst, and hold up the next write behind
// the shard's gate. The build waits, as the idle close of a handle left open
// waits, so a burst costs one walk after it ends. A shard written without
// pause is not built until the writes stop. With IdleTimeout 0 (no idle close,
// no idle tick) there is no wait: the build follows each such close at once.
// Caller holds es.shardMu.
func (ts *Store) settleQuietLocked(es *EventShard) bool {
	return es.wroteAt == 0 || time.Since(time.UnixMilli(es.wroteAt)) >= ts.idleTimeoutValue()
}

// sweepOweRepairLocked hands the closed cold shard es to the sweep after a
// metered read's open of it was refused for a repair the read cannot be
// charged for (openColdReadOnlyLocked: an unclean WAL, a staged flush, a lost
// close image, index definitions to sync). The refusal is what the catalog
// could not know, so it sets the entry's recovery mark, in memory: the entry
// then reads not settled and the shard's step is the repair, whose writable
// open saves the mark before it opens. The shard goes ahead of the queue (a
// reader is waiting on it; behind the shards other refused reads put there)
// and the sweep is kicked at once. The repair is the next step after the pass
// in flight, if one is (its step, and its handle work, which waits for a
// move's copy of a pending shard), and does not wait for the shard's writes
// to stop. Caller holds es.shardMu.
func (ts *Store) sweepOweRepairLocked(es *EventShard) {
	if ts.sweep.kick == nil {
		return
	}
	ts.catalog.SetShardRecover(es.name, true)
	es.settleTries = 0
	ts.sweepOweFirst(es, true)
	ts.kickHandleSweep()
}

// sweepOweAtOpen queues, at the end of New, every cold shard whose catalog
// entry is not settled, the newest window first (the days a cut reads first),
// and kicks the sweep once: the shards New recovered (sealed without the
// build), a shard warm at the last Close and demoted by this New (never
// sealed), a shard a kill left mid-step, a catalog an earlier release wrote
// (no former-label set, no recovery mark). New reads the entries it loaded
// and no file. Runs after startHandleSweep.
func (ts *Store) sweepOweAtOpen() {
	if ts.sweep.kick == nil {
		return
	}
	names := ts.catalog.unsettledColdShards()
	owed := make([]*EventShard, 0, len(names))
	for _, name := range slices.Backward(names) {
		if es := ts.eventShards[name]; es != nil && es.currentTier() == TierCold {
			owed = append(owed, es)
		}
	}
	if len(owed) == 0 {
		return
	}
	ts.sweepOwe(owed...)
	ts.kickHandleSweep()
}

// settleStatus is how a pass's look at one queued shard ended.
type settleStatus int

const (
	settleNothing settleStatus = iota // not owed any more: opened or settled meanwhile, dropped, not cold
	settleGated                       // a move or a swap holds the shard's gate, or the shard is still being written: it stays queued, at the end
	settleWaits                       // as settleGated, for a refused read's shard: it stays queued where it is, at the head
	settleHeld                        // a test holds the step ("settle"): it stays queued
	settleDone                        // the step ran
)

// sweepSettleOne runs at most one settle step: it looks at the queued shards
// in turn until one is owed and runs that one's step. A shard that is no
// longer owed costs its lock and a catalog lookup and leaves the queue (a
// later close queues it again if it must); a shard behind a move's gate, or
// one still being written (settleQuietLocked), goes to the end of the queue
// without a wait, so it delays only itself (a refused read's shard behind a
// gate keeps its place and the pass looks behind it). It reports whether the catalog
// changed (the pass saves) and whether a step ran with shards still queued
// that were not put back (the pass kicks itself; a pass that found only such
// or held shards does not, and the gate's end or the idle tick kicks). Each
// look decides its shard's place in the queue itself (sweepSettle); this only
// counts.
func (ts *Store) sweepSettleOne() (changed, more bool) {
	queued, _ := ts.owedQueued(nil)
	gated, waiting := 0, 0
	for ; queued > 0 && !ts.closing.Load(); queued-- {
		es := ts.firstOwed(waiting)
		if es == nil {
			return false, false
		}
		st, ch := ts.sweepSettle(es)
		_ = ts.sweepFaultAt("looked")
		switch st {
		case settleWaits:
			waiting++
			gated++
		case settleGated:
			gated++
		case settleHeld:
			return false, false
		case settleDone:
			left, _ := ts.owedQueued(nil)
			return ch, left > gated
		}
	}
	return false, false
}

// sweepSettle looks at the queued shard es and runs one settle step for it if
// it is owed now: the repair when its entry is marked for recovery, else the
// build. A step that leaves the shard owed keeps it ahead of the shards not
// yet started (a build whose open found a repair owed, a repair that leaves
// the build), up to settleGiveUp steps in a row; then the shard is left as it
// is until an access opens it writable, a metered read's open of it is refused
// or clears its recovery mark, or a Clear empties it (a directory that cannot
// be opened, a history row that cannot be read: logged each time).
//
// The look decides es's place in the queue before it releases es.shardMu: off
// it (not owed any more, settled, given up), at its end (gated, still being
// written, Clear running; a refused read's shard stays at the head instead),
// or ahead. A close or a refusal of es takes the same
// lock to queue it, so it comes wholly before the look, which then sees what
// it left, or wholly after, when it finds es off the queue and queues it.
// Decided after the unlock, a close in between found es still queued and the
// look then took it off: owed by nobody.
func (ts *Store) sweepSettle(es *EventShard) (st settleStatus, changed bool) {
	es.shardMu.Lock()
	defer es.shardMu.Unlock()
	if es.moveDone != nil || ts.clearing.Load() {
		// No step starts during a Clear: it would compare an emptied shard's
		// index definitions with an anchor Clear has not emptied yet. Clear
		// queues its shards at its end.
		if ts.sweepPutBack(es) {
			return settleWaits, false
		}
		return settleGated, false
	}
	if !ts.owedLocked(es) {
		ts.sweepUnqueue(es, false)
		return settleNothing, false
	}
	if err := ts.sweepFaultAt("settle"); err != nil {
		return settleHeld, false
	}
	entry, _ := ts.catalog.GetShard(es.name)
	repair := entry != nil && entry.Recover
	if !repair && !ts.settleQuietLocked(es) {
		// The build waits for the writes to stop; a repair does not (a
		// reader is refused until it ran).
		ts.sweepUnqueue(es, true)
		return settleGated, false
	}
	ts.sweep.settles.Add(1)
	es.settleTries++
	var err error
	if repair {
		changed, err = ts.settleRepairLocked(es)
	} else {
		changed, err = ts.settleBuildLocked(es)
	}
	if err != nil {
		slog.Error("tiered: handle sweep: settle step failed", "shard", es.name, "step", es.settleTries, "err", err)
	}
	if ts.owedLocked(es) {
		ts.sweepOweFirst(es, false) // its next step before the shards not yet started
		return settleDone, changed
	}
	if !ts.catalog.shardOwes(es.name) {
		es.settleTries = 0
	}
	ts.sweepUnqueue(es, false)
	return settleDone, changed
}

// settleRepairLocked is the repair step of the closed cold shard es, whose
// catalog entry is marked for recovery: it opens writable, which recovers and
// repairs the directory (openColdWritableStoreLocked: the unseal and the mark
// are saved before the open; an unclean WAL is replayed, index definitions are
// synced to the anchor's), seals the counts and closes; the close's image
// clears the mark. It walks no history: the former-label set stays unknown,
// which skips nothing, and the entry stays owed for the build. A missing
// directory is refused before the unseal, so the entry stays exactly as it is
// (changed false: nothing to save). Otherwise the catalog changed (the unseal
// is saved; the seal and the cleared mark are the caller's to save). The
// handle is the sweep's and small: a writer the sweep closed a small handle
// for still gets its large one (growNext is kept). Caller holds es.shardMu; es
// is closed and cold.
func (ts *Store) settleRepairLocked(es *EventShard) (changed bool, err error) {
	if err := ts.requireShardDir(es.path); err != nil {
		return false, err
	}
	grow := es.growNext
	es.growNext = false
	defer func() { es.growNext = grow }()
	if err := es.openColdWritableStoreLocked(ts); err != nil {
		return true, err // unsealed and marked, saved: the next step, refusal or New comes back
	}
	es.snapshotCountsNoBuildLocked(ts)
	return true, es.closeStoreLocked(ts, "settle: close recovered cold shard") // a fault is the fence
}

// settleBuildLocked is the build step of the closed cold shard es, whose entry
// is owed and not marked: it opens the shard read-only as a metered read's
// open does, builds the former-label set (one walk of the shard's history
// rows), seals and closes. The handle is the step's own: not installed on es,
// so no gauge, cold-open count or cap slot shows it, and the open changes no
// catalog field, so a kill inside the step leaves the entry owed as it was.
//
// An open refused for a repair (what a reader would be refused for; a catalog
// an earlier release wrote names no shard for recovery) marks the entry in
// memory: the shard's next step is the repair.
//
// The open and the walk run behind the shard's move gate with shardMu
// released, as a swap's reopen does: a reader or writer of the shard waits for
// the gate, and everything that only looks at the shard under its lock (the
// registry setters, the idle close, the cap pass, the listings) does not wait
// for a walk. Caller holds es.shardMu, again on return; es is closed and cold
// and its gate is down.
func (ts *Store) settleBuildLocked(es *EventShard) (changed bool, err error) {
	if err := ts.requireShardDir(es.path); err != nil {
		return false, err
	}
	done := make(chan struct{})
	es.moveDone = done
	es.moving.Store(true)
	es.shardMu.Unlock()

	store, refused, err := ts.settleOpen(es)
	var walk func() ([]uint16, error) // a failed walk: the set stays unknown
	if err == nil && !refused {
		if err = ts.sweepFaultAt("build"); err == nil {
			var tokens []uint16
			if tokens, err = store.BuildFormerLabelTokens(); err == nil {
				walk = func() ([]uint16, error) { return tokens, nil }
			}
		}
	}

	es.shardMu.Lock()
	switch {
	case store == nil && refused:
		// In memory: the repair, this shard's next step, saves the mark
		// before its writable open.
		ts.catalog.SetShardRecover(es.name, true)
	case store != nil:
		// The seal takes the walk's tokens: the shard cannot have changed
		// since, closed behind its gate. A failed walk still leaves the
		// counts sealed, the set unknown.
		changed = es.sealCountsLocked(ts, store, walk)
		// Closed before the gate drops: a writer waiting on it opens the
		// directory writable.
		if cerr := store.Close(); cerr != nil {
			slog.Error("tiered: handle sweep: close the build step's read-only handle", "shard", es.name, "err", cerr)
		}
	}
	// The gate drops; no handle is installed, so there is no registry to
	// bring up to date (endHandleGateLocked).
	es.moveDone = nil
	es.moving.Store(false)
	close(done)
	return changed, err
}

// sweepBuildHandleBytes stands for what the build step's handle holds besides
// its configured caches: its own structures and what the block cache holds
// over its configured size. Measured 1.2 MiB on a shard of 1,000 logons and
// at most 1.8 MiB up to 1,000,000.
const sweepBuildHandleBytes = 4 << 20

// sweepBuildBytes is ShardAccessStats.SweepBuildBytes: what one build step
// holds by configuration, on the handle settleOpen opens. Nothing in it
// depends on the shard or on the number of shards: steps run one at a time
// (one per pass, passes under passMu), and the walk keeps a token set, not the
// shard's former members.
func (ts *Store) sweepBuildBytes() int64 {
	if ts.inMemory {
		return 0 // no sweep goroutine, no build step
	}
	return transientBlockCache(ts.blockCacheSize) + ts.indexCacheSize + badger.FormerLabelTokenSetBytes + sweepBuildHandleBytes
}

// settleOpen opens the closed cold shard es read-only for a build step, the
// way a metered read's open does (openColdReadOnlyLocked with a tally), on the
// transient configuration. refused reports that the open was refused for a
// repair only a writable open makes (the read's "unsupported" refusal), or
// that the handle opened but its index definitions are not the anchor's; the
// handle is then closed. The tally is the step's own, unlimited and dropped
// with the step: it is the open's switch for "refuse a repair, do not make
// it", not an allowance. No cut is charged: what the step holds, one transient
// handle and the walk's token set, is the store's maintenance, outside a
// cut's allowance and reported as ShardAccessStats.SweepBuildBytes.
func (ts *Store) settleOpen(es *EventShard) (store *BadgerStore, refused bool, err error) {
	_, meter := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{})
	cfg := ts.coldReadCfg(es.path, false)
	cfg.OpenReadTally = meter.Tally("settle")
	if store, err = ts.newShardStore(cfg); err != nil {
		var budget *storecontract.ReadBudgetError
		if errors.As(err, &budget) && budget.Limit == "unsupported" {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("graph: read-only open of cold shard %s: %w", es.name, err)
	}
	if match, merr := ts.anchoredIndexesMatch(store); merr != nil || !match {
		if cerr := store.Close(); cerr != nil {
			return nil, false, fmt.Errorf("graph: close read-only cold shard %s: %w", es.name, cerr)
		}
		return nil, true, nil
	}
	return store, false, nil
}

// settleMarkedAtOpen runs at New, before the first read, the repair step of
// the closed cold shards the catalog marks for recovery (ShardEntry.Recover:
// open writable when the last process ended, or closed without its close
// image): a metered read would be refused by each. The repair walks no
// history (New is the foreground); sweepOweAtOpen queues the shard for its
// build. New collects the marked shards from the catalog it has loaded, so it
// opens no other shard and stats no other directory: the work follows the
// shards open writable in the last run, not the archive. A shard that does
// not recover is logged and stays closed and marked. Runs before the sweep
// and the idle close start.
func (ts *Store) settleMarkedAtOpen(shards []*EventShard) {
	if ts.inMemory || len(shards) == 0 {
		return
	}
	start := time.Now()
	save := false
	for _, es := range shards {
		es.shardMu.Lock()
		if es.store != nil || es.currentTier() != TierCold {
			es.shardMu.Unlock()
			continue
		}
		changed, err := ts.settleRepairLocked(es)
		es.shardMu.Unlock()
		save = save || changed // the seal and the mark's clear
		if err != nil {
			slog.Error("tiered: open: cold shard recovery failed", "shard", es.name, "err", err)
			continue
		}
		ts.openStats.ColdRecovered++
	}
	ts.openStats.ColdRecovery = time.Since(start)
	if save {
		ts.saveCatalogBestEffort("seal and clear the recovery marks of cold shards recovered at open")
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
		// A warm size equal to the hot one owes nothing: the reopen would
		// take the same arena (S1).
		if es.hotArena && ts.warmMemTableSize > 0 && ts.warmMemTableSize < hotMemTableSize(ts.memTableSize) {
			act = sweepReopen
		}
	case TierCold:
		// The demoted chain (the handle the shard had while hot or warm,
		// whatever its size) closes. A handle a cold read or late write
		// opened is left to IdleTimeout until it has committed
		// coldGrowBytes on the small cold memtable (coldGrowOwedLocked);
		// that close happens once per small handle (the next writable open
		// takes MemTableSize), so steady late writes do not reopen on every
		// kick.
		if !es.coldOpened || ts.coldGrowOwedLocked(es) {
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
		restored := false
		es.shardMu.Lock()
		switch {
		case store != nil && es.currentTier() == TierHot && !ts.closing.Load():
			ts.installPromotedLocked(es, store)
		case store != nil:
			ts.installSweptLocked(es, store)
		case es.store == nil && es.currentTier() == TierHot && !ts.closing.Load():
			// The swap closed the handle and left none (a failed open, a
			// stop), and a rotation promoted the shard meanwhile, before or
			// after the swap gave up: a hot shard needs its handle.
			restored = ts.reopenPromotedLocked(es)
		}
		ts.endHandleGateLocked(es, done)
		es.shardMu.Unlock()
		if restored {
			ts.saveCatalogBestEffort("restore the hot tier of a promoted shard whose swap failed")
		}
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
		ts.sweepNotReopened(es, nil)
		return res
	}
	s, err := ts.sweepReopenHandle(es)
	if err != nil {
		ts.sweepNotReopened(es, err)
		return res
	}
	store = s
	return res
}

// sweepReopenHandle opens es's shard at the warm size, after the "reopen" test
// step (an error there stands for a failed open).
func (ts *Store) sweepReopenHandle(es *EventShard) (*BadgerStore, error) {
	if err := ts.sweepFaultAt("reopen"); err != nil {
		return nil, err
	}
	return ts.openBadgerStoreWithRecovery(es.path)
}

// sweepNotReopened settles a shard whose swap closed its handle and does not
// reopen it: the open failed (cause), a test stopped the swap, or Close began.
// The shard keeps the tier it has now, whatever the swap meant to do:
//   - warm: cold in memory, so until Close marks the store closed an access
//     takes the locked path, which opens a closed shard (MoveShard's
//     pattern); a failed open (cause) also saves the demotion, so a restart
//     does not need that open;
//   - hot, a rotation promoted it meanwhile: left hot. The swap's deferred
//     install gives it a hot-size handle before the gate drops
//     (reopenPromotedLocked), because a hot shard takes the store's writes;
//   - cold: as it is.
//
// The gate is still up.
func (ts *Store) sweepNotReopened(es *EventShard, cause error) {
	demoted := false
	es.shardMu.Lock()
	if es.currentTier() == TierWarm {
		es.setTier(TierCold)
		if cause != nil {
			ts.catalog.UpdateShardTier(es.name, TierCold)
			demoted = true
		}
	}
	tier := es.currentTier()
	es.shardMu.Unlock()
	if demoted {
		ts.saveCatalogBestEffort("demote a swept shard that did not reopen")
	}
	if cause != nil {
		slog.Error("tiered: swept shard did not reopen at the warm size", "shard", es.name, "tier", tier, "err", cause)
	}
	_ = ts.sweepFaultAt("unopened")
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
	grow := ts.coldGrowOwedLocked(es) // read before the close: it reads the handle
	// A fault is the fence; a warm shard still reopens, or is demoted when it
	// cannot.
	_ = es.closeStoreLocked(ts, "handle sweep: close shard")
	es.growNext = grow
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
		// The close of a cold shard's writable handle, like every other: a
		// clean one clears the shard's mark and hands the shard, closed and
		// not sealed, to the settle steps (the gate's end kicks). Without
		// that the shard stayed marked with a clean directory, which no close
		// hands over and no read is refused for.
		err := store.Close()
		if err != nil {
			slog.Error("tiered: handle sweep: close a reopened handle not installed", "shard", es.name, "err", err)
		}
		es.noteCloseLocked(ts, store, true, err)
		ts.sweepOweClosedLocked(es)
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
	ts.openPromotedHandleLocked(es)
}

// reopenPromotedLocked gives a promoted shard the swap left without a handle
// (the warm open failed, or the swap stopped after the close) a hot-size one;
// the shard keeps its hot tier. A swap that demoted the shard before the
// rotation promoted it left the catalog naming it cold: that is put right, and
// the result reports it (the caller saves the catalog once it holds no lock).
// A shard that cannot be opened is the store's sticky fault
// (openPromotedHandleLocked). Caller holds es.shardMu; the gate is still up.
func (ts *Store) reopenPromotedLocked(es *EventShard) (catalogChanged bool) {
	slog.Warn("tiered: handle sweep: a promoted shard's swap left it without a handle; reopening it at the hot size", "shard", es.name)
	ts.openPromotedHandleLocked(es)
	if es.store == nil {
		return false
	}
	if e, ok := ts.catalog.GetShard(es.name); ok && e.Tier != TierHot {
		ts.catalog.UpdateShardTier(es.name, TierHot)
		return true
	}
	return false
}

// openPromotedHandleLocked opens the hot-size handle of a promoted shard (the
// recovery open when the hot-size one fails; no handle and the store's sticky
// fault when both fail) and installs it. Caller holds es.shardMu.
func (ts *Store) openPromotedHandleLocked(es *EventShard) {
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
	// A settle step that found the gate up left the shard queued and moved on.
	if _, queued := ts.owedQueued(es); queued {
		ts.kickHandleSweep()
	}
}

func (ts *Store) sweepFaultAt(step string) error {
	if f := ts.sweep.fault.Load(); f != nil {
		return (*f)(step)
	}
	return nil
}
