package badger

import (
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Value-log GC against open reads (VALUETHRESH F1). badger's GC rewrites a
// file's live values (keeping each entry's version, value.go:217-224) and
// then deletes the file at once unless an iterator is open (value.go:342-355);
// a transaction that only Gets pins nothing. An item a Get returned before the
// rewrite holds the old value pointer, and its Value after the delete is
// logged and comes back empty with a nil error (iterator.go:155-189). A Get
// after the rewrite finds the new pointer, so only reads open while the GC
// pass ran are at risk.
//
// rho's GC pass (runValueLogGC, the only caller of RunValueLogGC) therefore
// holds an iterator of its own, so badger queues the rewritten files instead
// of deleting them, and closes it only after every read that began before the
// pass ended has ended (vlogReaders.drain); the last iterator to close deletes
// the queued files (value.go:367-381). Every read or update transaction rho
// opens while the store runs enters vlogReaders before it takes its read
// timestamp: view, update, newReadTxn. Readers never wait; the GC pass waits
// for them.

// vlogReaders counts the open read transactions per epoch parity.
type vlogReaders struct {
	epoch  atomic.Uint64
	active [2]atomic.Int64
}

// vlogPin is the slot a read entered (its parity + 1; 0 entered none).
type vlogPin uint8

func (r *vlogReaders) enter() vlogPin {
	for {
		e := r.epoch.Load()
		r.active[e&1].Add(1)
		if r.epoch.Load() == e { // else a drain may have passed this slot: enter the new one
			return vlogPin(e&1) + 1
		}
		r.active[e&1].Add(-1)
	}
}

func (r *vlogReaders) exit(p vlogPin) {
	if p != 0 {
		r.active[p-1].Add(-1)
	}
}

// drain returns once every read that entered before it has exited, or when
// stop closes first (stopped). It waits while the slot is above zero: a slot
// below zero (an exit without its enter, a bug) is reported, not waited on,
// so it cannot stall every later pass. One caller at a time (gcMu).
func (r *vlogReaders) drain(stop <-chan struct{}) (waited time.Duration, stopped, negative bool) {
	start := time.Now()
	slot := &r.active[(r.epoch.Add(1)-1)&1]
	for slot.Load() > 0 {
		select {
		case <-stop:
			return time.Since(start), true, false
		case <-time.After(time.Millisecond):
		}
	}
	return time.Since(start), false, slot.Load() < 0
}

// vlogGuardPrefix matches no key (rho's keyspaces start below 0x10), so the
// guard iterator opens no table.
var vlogGuardPrefix = []byte{0xFF, 'v', 'l', 'o', 'g', 'g', 'c'}

// vlogDrainSlow: a pass whose wait for open reads takes longer is logged. A
// read transaction held across a caller's callback (the bulk and batch doors)
// holds the pass up; meanwhile the guard keeps the memtables open when it began and the
// rewritten files (docs/badger.md rule 9).
var vlogDrainSlow = 60 * time.Second

// afterVlogRewrite, when set (tests only), runs after the GC rewrote what it
// would and before the pass waits for open reads.
var afterVlogRewrite func()

// runValueLogGC runs badger's value-log GC until it finds nothing to rewrite
// and returns the number of files it rewrote. The rewritten files are deleted
// when the guard closes: after the reads open during the pass, or at Close.
func (bs *Store) runValueLogGC() int {
	bs.gcMu.Lock()
	defer bs.gcMu.Unlock()
	if bs.readOnly || bs.isClosingOrClosed() || bs.db.IsClosed() {
		// Read-only: badger refuses the GC, and an iterator over no memtable
		// never releases its count (iterator.go:545-548). Closed under the
		// store (a crash simulation): NewIterator panics (iterator.go:464-466).
		return 0
	}
	guard := bs.db.NewTransaction(false)
	it := guard.NewIterator(badgerv4.IteratorOptions{Prefix: vlogGuardPrefix})
	n := 0
	for bs.db.RunValueLogGC(bs.gcRatio) == nil {
		n++ // Keep running until no more garbage to collect.
	}
	if afterVlogRewrite != nil {
		afterVlogRewrite()
	}
	waited, stopped, negative := bs.vlogReaders.drain(bs.stopCh)
	if negative && bs.vlogNegativeLogged.CompareAndSwap(false, true) {
		bs.warnf("graph: value-log GC found a negative open-read count (an exit without its enter); files it rewrote may be deleted under a read")
	}
	if waited > vlogDrainSlow {
		bs.warnf("graph: value-log GC waited %s for open reads (stopped: %t); a read held across a callback pins the rewritten files and a memtable meanwhile", waited.Round(time.Second), stopped)
	}
	it.Close()
	guard.Discard()
	return n
}

// warnf logs a warning to Config.Logger, else to slog.
func (bs *Store) warnf(format string, args ...any) {
	if bs.logger != nil {
		bs.logger.Warningf(format, args...)
		return
	}
	slog.Warn(fmt.Sprintf(format, args...))
}

// update runs fn in a read-write transaction entered in vlogReaders.
func (bs *Store) update(fn func(txn *badgerv4.Txn) error) error {
	pin := bs.vlogReaders.enter()
	defer bs.vlogReaders.exit(pin)
	return bs.db.Update(fn)
}
