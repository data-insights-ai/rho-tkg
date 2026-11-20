package tiered

import (
	"slices"
	"time"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Shard holds (CUTCOST N1, N2; store.ReadMeter.HoldShardOpens). Without a
// hold a cold shard a read opens stays open only while fewer than
// Config.MaxOpenColdShards are open; past that every read of it opens and
// closes it again, a write opens it once more on no meter, and a writable
// handle waits for the idle close. So the cost of a caller's reads depended on
// what happened to be open, and the open handles had no bound.
//
// A holding meter's caller gets each cold shard opened once and kept until the
// meter's Release, which closes it: nothing stays open behind the caller, and
// the same reads open the same shards whatever was open before. At most
// Config.MaxOpenColdShards are held for one meter at once (0: no bound).
// ShardHoldStrict refuses one shard more by name; ShardHoldLRU closes the
// shard held longest to make room. A read opens read-only in both, so a shard
// that is only read keeps its seal and owes the handle sweep nothing (CUTCOST
// review 4 F2: a writable open drops the seal, and the sweep then walks the
// shard on no meter). A strict caller names the days it will write
// (HoldShardsAt): those are opened writable, or upgraded when a read of the
// caller's already holds them read-only, on the caller's meter, so its write
// opens nothing. A write of a shard held read-only that was not named (a
// write of another part under the caller's meter, as Sigma's write-back of an
// edge to an event) upgrades the handle as any write does (a hold's pin is not
// a read in flight, so the upgrade does not wait for it), and the upgrade is
// counted on the holding meter: every open of a held shard is in its work.

// holdModeFor is the mode es is to be held in for opens: none when opens does
// not hold, es is not cold, or opens holds es already.
func (ts *Store) holdModeFor(es *EventShard, opens *storecontract.ReadTally) storecontract.ShardHoldMode {
	mode := opens.HoldMode()
	if mode == storecontract.ShardHoldNone || es.currentTier() != TierCold || opens.HoldsShard(es) {
		return storecontract.ShardHoldNone
	}
	return mode
}

// roomForHeld makes room for one more shard held for opens under the store's
// bound: ShardHoldStrict refuses past it (limit "shards held"), ShardHoldLRU
// closes the shards held longest. Caller holds no shard lock.
func (ts *Store) roomForHeld(opens *storecontract.ReadTally, mode storecontract.ShardHoldMode) error {
	bound := ts.maxOpenColdShards
	if bound <= 0 {
		return nil
	}
	for opens.HeldShards() >= bound {
		if mode == storecontract.ShardHoldStrict {
			return opens.RefuseHeld(bound)
		}
		if !opens.ReleaseOldestShard() {
			return nil
		}
	}
	return nil
}

// releaseHeld drops the meter's pin on es and, when nothing else uses the
// shard and it is cold, seals and closes it whatever the open-handle cap says:
// a held shard never outlives its meter.
func (es *EventShard) releaseHeld(ts *Store, opens *storecontract.ReadTally) {
	es.heldBy.CompareAndSwap(opens, nil)
	es.holdPins.Add(-1)
	es.activeReqs.Add(-1)
	es.shardMu.Lock()
	defer es.shardMu.Unlock()
	if es.store == nil || es.currentTier() != TierCold {
		return
	}
	if es.activeReqs.Load() != 0 || es.moving.Load() {
		ts.sweepPend(es) // another caller has it: the handle sweep looks at it when that one is done
		return
	}
	sealed := es.snapshotCountsNoBuildLocked(ts)
	if !es.storeRO {
		es.wroteAt = time.Now().UnixMilli() // a write's close: the build waits for the writes to stop, as at a check-in
	}
	_ = es.closeStoreLocked(ts, "close held cold shard") // a fault is the fence
	if sealed {
		defer ts.saveCatalogBestEffort("seal shard counts on a held shard's close")
	}
}

// windowHold is a strict meter's hold of a calendar window that has no shard
// yet: a place under the bound. es is the shard a write made in the window
// since, which the meter then holds open; nil while none was made.
type windowHold struct {
	opens *storecontract.ReadTally
	es    *EventShard
}

// holdMadeShard gives es, a cold shard just made in the calendar window
// starting at window, to the meter that holds that window: the meter's pin is
// taken here and its Release closes the shard, so the caller leaves it to no
// sweep. False: no meter holds the window. Caller holds ts.mu.Lock.
func (ts *Store) holdMadeShard(window int64, es *EventShard) bool {
	ts.heldWinMu.Lock()
	defer ts.heldWinMu.Unlock()
	h := ts.heldWindows[window]
	if h == nil || h.es != nil || !h.opens.RekeyHeld(h, es) {
		return false
	}
	h.es = es
	es.activeReqs.Add(1)
	es.holdPins.Add(1)
	es.heldBy.Store(h.opens)
	return true
}

// releaseWindow ends the hold h of the window starting at window: the place is
// free again and the shard made in it, if any, is closed as a held shard is.
func (ts *Store) releaseWindow(window int64, h *windowHold) {
	ts.heldWinMu.Lock()
	if ts.heldWindows[window] == h {
		delete(ts.heldWindows, window)
	}
	es := h.es
	ts.heldWinMu.Unlock()
	if es != nil {
		es.releaseHeld(ts, h.opens)
	}
}

// HoldShardsAt holds, for a holding tally, the cold shards whose windows hold
// instants, for a write: the days its caller is about to write. One that
// exists is opened writable (ShardHoldLRU: read-only), charged as any open of
// it (its window, the caller's limit), and kept until the meter's Release; one
// the meter already holds read-only is upgraded, charged as an open too. For a
// strict tally a window that has no shard yet and whose shard would be made
// cold (a day before the store's first, a day the clock skipped) is held as a
// place: it counts under the bound, nothing is made, and the shard a write
// then makes there is the meter's from its first moment, open until Release.
// A caller that will write records of those instants calls it first: every
// cold day its write touches is counted under the bound before anything is
// written, the write finds each shard open, no handle outlives the meter, and
// a caller refused before its write leaves no shard behind. ShardHoldStrict
// refuses, before it opens anything, a set of days that with the shards
// already held passes the bound. A tally that does not hold, an instant on a
// hot or warm shard or on a day that would be made warm: nothing.
func (ts *Store) HoldShardsAt(instants []types.Instant, opens *storecontract.ReadTally) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	mode := opens.HoldMode()
	if mode == storecontract.ShardHoldNone {
		return nil
	}
	var shards []*EventShard
	var windows []int64
	fresh := 0 // of shards: not held yet
	ts.mu.RLock()
	for _, at := range instants {
		t := time.UnixMilli(int64(at))
		if es := shardWindowFor(ts.shardIndex, t); es != nil {
			if es.currentTier() == TierCold && !slices.Contains(shards, es) {
				shards = append(shards, es)
				if !opens.HoldsShard(es) {
					fresh++
				}
			}
			continue
		}
		if !ts.calendar || mode != storecontract.ShardHoldStrict {
			continue // no window shard is made on demand; or a hold that makes room when the shard is opened
		}
		local := t.In(ts.shardLoc)
		start, end := shardWindowStart(local, ts.shardWindow), shardWindowEnd(local, ts.shardWindow)
		for _, es := range ts.shardIndex { // the end makeWindowShardLocked gives the shard: cut to its neighbour
			if es.timeStart.After(local) && es.timeStart.Before(end) {
				end = es.timeStart
			}
		}
		if ts.coldAfter <= 0 || !end.Before(ts.clock().Add(-ts.coldAfter)) {
			continue // made warm: an open shard of the warm tier, no cold handle
		}
		ts.heldWinMu.Lock()
		h := ts.heldWindows[start.UnixMilli()]
		ts.heldWinMu.Unlock()
		if (h == nil || !opens.HoldsShard(h)) && !slices.Contains(windows, start.UnixMilli()) {
			windows = append(windows, start.UnixMilli())
		}
	}
	ts.mu.RUnlock()
	if bound := ts.maxOpenColdShards; mode == storecontract.ShardHoldStrict && bound > 0 && opens.HeldShards()+fresh+len(windows) > bound {
		return opens.RefuseHeld(bound)
	}
	for _, window := range windows {
		h := &windowHold{opens: opens}
		ts.heldWinMu.Lock()
		if ts.heldWindows == nil {
			ts.heldWindows = map[int64]*windowHold{}
		}
		if ts.heldWindows[window] == nil { // another meter's place stays; this one counts its own all the same
			ts.heldWindows[window] = h
		}
		ts.heldWinMu.Unlock()
		opens.HoldShard(h, func() { ts.releaseWindow(window, h) })
	}
	for _, es := range shards {
		_, release, err := es.checkoutForReadCharged(ts, mode == storecontract.ShardHoldLRU, opens)
		if err != nil {
			return err
		}
		release()
	}
	return nil
}
