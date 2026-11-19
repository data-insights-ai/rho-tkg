package badger

import (
	"errors"
	"sync/atomic"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// asOfSnapshot is the store's committed view of the as-of index: the one
// place the write hook and the indexed reads read committed proofs, aliases,
// seek keys and history rows from. Neither opens a Badger read transaction
// of its own: a transaction waits at its open for every commit in flight
// (txn.go:78-95), and under wbMu every writer and overlay reader waits
// behind it.
//
// A view is a read transaction a flush opens after its commit is complete,
// under flushMu and outside wbMu, and installs in the wbMu section that moves
// commitGen and drops the committed rows from the overlay; Clear (after its
// wipe) and open install theirs the same way. A view is current while
// commitGen equals gen. A current view sees every key that is in neither
// pending nor flushing, and never a part of a flush: it was opened between
// two write-buffer commits, which flushMu serialises.
//
// The hook (noteAsOfHistoryLocked) reads pending, flushing, then the current
// view, all under wbMu; a view that is not current is never read, the hook
// invalidates its chain instead (errAsOfSnapStale). An indexed read takes one
// wbMu section (pinAsOfView): if no history key of its chain is buffered and
// the view is current, the view shows the chain exactly as it is at that
// moment, and the read keeps it by reference and reads it with no lock held.
// Otherwise the read declines and the admitted scan answers. So another
// chain's flush never changes how a chain is read: the flush section replaces
// the view and moves commitGen together.
//
// Badger allows one read-only transaction to serve several readers at once
// (iterator.go:453-458; a read-only Get and NewIterator write nothing but the
// atomic iterator count, txn.go:434-487, iterator.go:459-491). Discard needs
// every iterator closed, so a view counts its users: the store's installed
// reference and one per reading call; the last release discards it.
//
// The hook enters no value-log pin (vlogReaders): it reads proofs, aliases
// and the generation, values of at most 41 bytes, which are always inline
// (Config.ValueThreshold is at least 1 KB), and history keys without their
// values. A reading call reads history rows and enters the pin for as long
// as it holds the view (pinAsOfView).
//
// A view holds back compaction of versions committed after it opened, and the
// previous version of every key its own flush or update commit wrote (still
// bounded by one flush's keys), until it is replaced: by every flush that
// commits, by an idle flush tick (async
// stores; under SyncWrites no flush loop runs), by Clear, and after a direct
// commit through update (meta keys, the open image's header) unless flushMu
// is held at that moment (renewAsOfSnapAfterCommit states the bound).
// Versions a WriteBatch writer commits outside these (index backfills,
// open-image chunks) stay held until the next of them. A reader that still
// holds a replaced view holds it back for the length of its read. Guarded by
// wbMu.
type asOfSnapshot struct {
	view *asOfView
	gen  uint64 // commitGen when view was installed

	// The recovery generation never changes while the store is open: only
	// an open's recovery and a staged discard write it, both before a handle
	// exists, and Clear wipes it (refreshAsOfSnap forgets it). Read once.
	generation      uint64
	generationKnown bool
}

// asOfView is one committed read transaction and the number of its users.
type asOfView struct {
	txn  *badgerv4.Txn
	refs atomic.Int32
}

var errAsOfSnapStale = errors.New("graph: as-of hook view is not current")

// openAsOfView opens the next view, holding the store's reference. Caller
// holds flushMu or owns the store (open), never wbMu: no other commit of the
// write buffer can be in flight, and a slow open stalls no writer. Exact
// erasure's flush, the periodic open image's flush (flushIndexLocked with
// keepIndexLock) and Clear open it while holding idxMu. nil on a closed
// store.
func (bs *Store) openAsOfView() *asOfView {
	if bs.dbClosed.Load() {
		return nil
	}
	if h := bs.readTxnOpenTestHook; h != nil {
		h()
	}
	noteAsOfViewOpened()
	v := &asOfView{txn: bs.db.NewTransaction(false)}
	v.refs.Store(1)
	return v
}

// releaseAsOfView gives up one reference; the last one ends the view's
// transaction. Never under wbMu. Once the database is closed its read mark
// died with the oracle and the discard is skipped.
func (bs *Store) releaseAsOfView(v *asOfView) {
	if v == nil || v.refs.Add(-1) != 0 {
		return
	}
	noteAsOfViewEnded()
	if !bs.dbClosed.Load() {
		v.txn.Discard()
	}
}

// replaceLocked installs v as the view of commitGen gen and returns the old
// one, whose reference the caller releases after it releases wbMu. Caller
// holds wbMu.
func (s *asOfSnapshot) replaceLocked(v *asOfView, gen uint64, forgetGeneration bool) *asOfView {
	old := s.view
	s.view, s.gen = v, gen
	if forgetGeneration {
		s.generation, s.generationKnown = 0, false
	}
	return old
}

// refreshAsOfSnap replaces the view with one of the current commitGen.
// Caller holds flushMu (flush and Clear are the only commitGen writers, both
// under it) or owns the store (the end of open), never wbMu. Clear calls it
// while it still holds idxMu, so no history writer appends between its wipe
// and the new view.
func (bs *Store) refreshAsOfSnap(forgetGeneration bool) {
	v := bs.openAsOfView()
	if v == nil {
		return
	}
	bs.wbMu.Lock()
	old := bs.asOfSnap.replaceLocked(v, bs.commitGen.Load(), forgetGeneration)
	bs.wbMu.Unlock()
	bs.releaseAsOfView(old)
}

// renewAsOfSnapAfterCommit installs a view at the store's newest version
// after a direct commit (update), so the old view does not hold back what the
// commit replaced; a SyncWrites store has no flush tick that would. It takes
// flushMu, not only commitGen: Clear moves commitGen before its wipe, so a
// view opened beside it could carry the moved generation over pre-wipe rows.
//
// It never waits for flushMu (a caller inside a flush or Clear, such as a
// staged flush's marker or Clear's restamp under wbMu, would wait for
// itself), so it is skipped whenever anyone holds it: a flush after its view
// opened or one that failed, Clear, Close, the deferred planner-statistics
// scan, exact erasure, a periodic image's flush and encode, a durable
// flush's sync and owner hooks. The view then stays below that commit until
// the next flush that commits, idle flush tick (async stores), update commit
// or Clear; on a SyncWrites store that idles afterwards, until its next
// write. Held back meanwhile: of each key written by the commit the view
// opened after or by a later one, every version from that commit on and the
// newest older one (compaction drops only versions at or below the oldest
// read mark, which stops at the commit before the view's own flush or update
// commit: the view also holds the previous version of every key that commit
// wrote, still bounded by one flush's keys); the set does not grow while
// nothing commits. Skipped
// too on a read-only handle (nothing commits) and while no view is installed
// (open, after Close).
func (bs *Store) renewAsOfSnapAfterCommit() {
	if bs.readOnly || !bs.flushMu.TryLock() {
		return
	}
	defer bs.flushMu.Unlock()
	bs.wbMu.Lock()
	live := bs.asOfSnap.view != nil
	bs.wbMu.Unlock()
	if live {
		bs.refreshAsOfSnap(false)
	}
}

// dropAsOfSnap removes the installed view before Close stops the database's
// oracle. A read that pinned it earlier ends it (releaseAsOfView). Caller
// holds flushMu.
func (bs *Store) dropAsOfSnap() {
	bs.wbMu.Lock()
	old := bs.asOfSnap.replaceLocked(nil, 0, true)
	bs.wbMu.Unlock()
	bs.releaseAsOfView(old)
}

// asOfViewLocked returns the current view for the hook. Caller holds wbMu
// and reads it only under that lock.
func (bs *Store) asOfViewLocked() (*badgerv4.Txn, error) {
	if bs.asOfSnap.view == nil || bs.asOfSnap.gen != bs.commitGen.Load() {
		return nil, errAsOfSnapStale
	}
	return bs.asOfSnap.view.txn, nil
}

// pinAsOfView is the one lock section of an indexed read of prefix's chain
// (prefix: the history key without its version). It returns the current view
// with a reference and a value-log pin, both ended by unpinAsOfView, or nil
// when the index must not answer: the handle does not trust its proofs, a
// history key of the chain is buffered, or the view is not current (inside
// Clear). The chain's proof, alias and seek keys are buffered and flushed
// with the history row that changes them, so the view then shows the whole
// chain as of this section. It opens no Badger transaction and charges
// nothing.
func (bs *Store) pinAsOfView(prefix []byte) (*asOfView, vlogPin, error) {
	if h := bs.asOfViewPinTestHook; h != nil {
		h(false)
	}
	bs.wbMu.Lock()
	if err := bs.checkOpen(); err != nil {
		bs.wbMu.Unlock()
		return nil, 0, err
	}
	v := bs.asOfSnap.view
	if v == nil || bs.asOfSnap.gen != bs.commitGen.Load() || bs.asOfUntrusted ||
		len(bs.pendingByPrefix[string(prefix)]) != 0 || len(bs.flushingByPrefix[string(prefix)]) != 0 {
		bs.wbMu.Unlock()
		return nil, 0, nil
	}
	v.refs.Add(1)
	bs.wbMu.Unlock()
	// Before the first Get on the view: a value-log GC pass that ends later
	// waits for this read (badgerstore_vlog_guard.go).
	pin := bs.vlogReaders.enter()
	if h := bs.asOfViewPinTestHook; h != nil {
		h(true)
	}
	return v, pin, nil
}

// unpinAsOfView ends what pinAsOfView returned, after the read closed its
// iterators.
func (bs *Store) unpinAsOfView(v *asOfView, pin vlogPin) {
	bs.vlogReaders.exit(pin)
	bs.releaseAsOfView(v)
}

// asOfGenerationLocked is the store's recovery generation, read from the
// view on first use. Caller holds wbMu.
func (bs *Store) asOfGenerationLocked() (uint64, error) {
	if bs.asOfSnap.generationKnown {
		return bs.asOfSnap.generation, nil
	}
	raw, err := bs.asOfStoredValueLocked(storepkg.HistoryAsOfGenerationKey)
	if err != nil {
		return 0, err
	}
	generation, err := asOfGeneration(raw)
	if err != nil {
		return 0, err
	}
	bs.asOfSnap.generation, bs.asOfSnap.generationKnown = generation, true
	return generation, nil
}
