package badger

import (
	"bytes"
	"errors"

	badgerv4 "github.com/dgraph-io/badger/v4"
)

// scanSnapshot is the badger read snapshot a multi-row read falls back to when
// the entity cache misses. It re-pins whenever the cache's flush epoch has moved
// since the snapshot opened.
//
// WHY ONE SNAPSHOT IS NOT ENOUGH. Such a read consults the cache per row and
// badger only on a miss. A miss proves the row's latest version is committed
// (dirty entries are never evicted), not that a snapshot opened EARLIER can see
// it: a flush that commits after the snapshot opened, marks the row clean and
// lets the cache evict it leaves the reader with a snapshot holding the
// previous version, or no row at all. The first silently returns a replaced
// version; the second silently drops a live row.
//
// THE RULE. The epoch is read immediately before each snapshot opens, and
// MarkFlushed advances it before any entry turns clean. After a miss, an
// unchanged epoch proves no flush released a row since the snapshot opened, so
// the snapshot holds the missed row's latest version; a changed epoch re-pins
// (one new transaction per flush that lands during the read, never one per
// row). Every fill into the cache from this snapshot goes through
// LoadCleanAt(…, s.pinned) so it is refused if the epoch moves in between.
//
// Not safe for concurrent use: one goroutine per scanSnapshot, like a badger
// transaction. Items returned by seekAfterMiss are valid until the next call.
type scanSnapshot struct {
	bs     *Store
	db     *badgerv4.DB
	epoch  func() uint64 // the flush epoch of the cache the reader consults
	pinned uint64        // epoch read immediately before txn opened
	txn    *badgerv4.Txn
	pin    vlogPin
	it     *badgerv4.Iterator
	last   []byte // the key seekAfterMiss sought last on it (nil: none yet)
	// lendAfterMiss's probe backoff: after a probe run ended, the next
	// probe waits until at most probeBelow keys are left; probeSkip (0: no
	// probe ended yet) doubles with each ended run.
	probeBelow, probeSkip int
}

func newScanSnapshot(bs *Store, epoch func() uint64) *scanSnapshot {
	return &scanSnapshot{bs: bs, db: bs.db, epoch: epoch}
}

// txnAfterMiss returns a read transaction that holds the latest committed
// version of any row the cache has released. Call it only AFTER the cache
// missed for the row about to be read: the epoch must be read after the miss.
func (s *scanSnapshot) txnAfterMiss() *badgerv4.Txn {
	e := s.epoch()
	if s.txn == nil || e != s.pinned {
		s.close()
		s.pinned = e
		s.txn, s.pin = s.bs.newReadTxn()
	}
	return s.txn
}

// anyTxn returns the current transaction, opening one if none is open. For
// reads that do not depend on the cache (history keys under an overlay
// captured before the scan began), any snapshot opened after that capture is
// correct.
func (s *scanSnapshot) anyTxn() *badgerv4.Txn {
	if s.txn == nil {
		s.pinned = s.epoch()
		s.txn, s.pin = s.bs.newReadTxn()
	}
	return s.txn
}

// seekAfterMiss returns the item stored exactly at key, or nil when the key is
// absent. Same precondition as txnAfterMiss. Keys must be sought in ascending
// order between re-pins; the iterator is re-created with each new snapshot.
//
// A target at most seekSteps keys past the iterator's position is reached
// with Next calls (stepTo), a farther one or one below the last target with a
// Seek. PrefetchValues=false is LOAD-BEARING: badger's value prefetch
// re-fills a value window on every Seek, almost all of it discarded.
// prefetch=true is much SLOWER than the per-row Txn.Get path it replaced. Do
// not flip this.
func (s *scanSnapshot) seekAfterMiss(key []byte) *badgerv4.Item {
	txn := s.txnAfterMiss() // a re-pin closes the old iterator
	if s.it == nil {
		iopts := badgerv4.DefaultIteratorOptions
		iopts.PrefetchValues = false
		s.it = txn.NewIterator(iopts)
		noteScanRead(scanReadIter)
	}
	if s.last == nil || bytes.Compare(key, s.last) < 0 || !s.stepTo(key) {
		s.it.Seek(key)
		noteScanRead(scanReadSeek)
	}
	s.last = append(s.last[:0], key...)
	return s.itemAt(key)
}

// itemAt returns the iterator's item when it holds exactly key, else nil.
func (s *scanSnapshot) itemAt(key []byte) *badgerv4.Item {
	if !s.it.Valid() {
		return nil // past end of keyspace — absent
	}
	item := s.it.Item()
	if !bytes.Equal(item.Key(), key) {
		return nil // key not present — deleted / orphaned index entry
	}
	return item
}

// lendAfterMiss is seekAfterMiss for the batch lends (LENDCOST): it returns
// the item stored exactly at key, or nil when the key is absent; same
// precondition as txnAfterMiss. An item is valid until the next call. left
// counts the caller's IDs from this one to the end of its batch; runAhead
// reports that the next ID lies within seekSteps of this one (lendRunAhead).
//
// An iterator merges a table iterator of every table of every level
// (badger v4.9.2 iterator.go:473-486) and a Seek re-seeks each; a Get skips
// the tables whose bloom filter lacks the key (level_handler.go:272-276;
// docs/badger.md § 3). So sparse keys are read by Get: on a store spanning
// L0 to L3, four keys 1,000 rows apart cost 1.7-2x their Gets through the
// iterator (tasks/evidence/LENDCOST/bench.txt).
// The iterator serves runs only:
//   - a run in progress (s.last set) steps forward to key with at most
//     seekSteps Next calls; a key it cannot reach ends the run, so a parked
//     iterator costs one failed step, not one per later sparse key;
//   - a run starts with a Seek (the iterator created on the first) when
//     runAhead proves the next row at most seekSteps keys away, or as a
//     probe when at least lendProbeMin keys are left: rows created in
//     different milliseconds lie next to each other with IDs far apart.
//     A probe that ends at once costs one Seek and seekSteps Nexts; the
//     next waits for the batch to shrink by lendProbeMin, then twice that;
//   - every other key is a Get on the same pinned transaction.
func (s *scanSnapshot) lendAfterMiss(key []byte, left int, runAhead bool) (*badgerv4.Item, error) {
	txn := s.txnAfterMiss() // a re-pin closes the old iterator and ends the run
	if s.last != nil && bytes.Compare(key, s.last) >= 0 {
		if s.stepTo(key) {
			s.last = append(s.last[:0], key...)
			return s.itemAt(key), nil
		}
		s.last = nil
		if s.probeSkip == 0 {
			s.probeSkip = lendProbeMin
		}
		s.probeBelow = left - s.probeSkip
		s.probeSkip *= 2
	}
	if runAhead || (left >= lendProbeMin && (s.probeSkip == 0 || left <= s.probeBelow)) {
		if s.it == nil {
			iopts := badgerv4.DefaultIteratorOptions
			iopts.PrefetchValues = false // see seekAfterMiss
			s.it = txn.NewIterator(iopts)
			noteScanRead(scanReadIter)
		}
		s.it.Seek(key)
		noteScanRead(scanReadSeek)
		s.last = append(s.last[:0], key...)
		return s.itemAt(key), nil
	}
	noteScanRead(scanReadGet)
	item, err := txn.Get(key)
	if errors.Is(err, badgerv4.ErrKeyNotFound) {
		return nil, nil // absent: deleted / orphaned index entry
	}
	return item, err
}

// lendProbeMin is the fewest keys left in a lend batch for which
// lendAfterMiss probes for a run without an ID proving one.
const lendProbeMin = 32

// lendRunAhead reports whether the ID after ids[i] lies above it by at most
// seekSteps: at most seekSteps-1 rows of the keyspace lie between theirs, so
// a step from ids[i]'s row reaches the next (lendAfterMiss).
func lendRunAhead[ID ~int64](ids []ID, i int) bool {
	if i+1 >= len(ids) {
		return false
	}
	d := ids[i+1] - ids[i]
	return d > 0 && d <= seekSteps
}

// stepTo moves a positioned iterator forward to the first key at or after key
// with at most seekSteps Next calls, reporting whether it got there; false
// leaves the Seek to the caller. Ascending targets that lie close together
// (the rows of consecutive IDs) then cost a Next each instead of a Seek
// through every level of the tree.
func (s *scanSnapshot) stepTo(key []byte) bool {
	for range seekSteps {
		if !s.it.Valid() {
			return false
		}
		if bytes.Compare(s.it.Item().Key(), key) >= 0 {
			return true
		}
		s.it.Next()
		noteScanRead(scanReadNext)
	}
	return s.it.Valid() && bytes.Compare(s.it.Item().Key(), key) >= 0
}

// scanReadHook, nil outside tests, sees each badger operation a scanSnapshot
// makes for a row the cache missed: the seam of the lend cost test (LENDCOST).
var scanReadHook func(scanRead)

// scanRead names one badger operation of a scanSnapshot read.
type scanRead uint8

const (
	scanReadIter scanRead = iota // an iterator created
	scanReadSeek                 // an iterator Seek
	scanReadNext                 // an iterator Next
	scanReadGet                  // a point Txn.Get
)

func noteScanRead(r scanRead) {
	if scanReadHook != nil {
		scanReadHook(r)
	}
}

// seekSteps bounds stepTo: past it a Seek is cheaper than more Next calls.
const seekSteps = 4

// close releases the iterator and transaction. Safe to call repeatedly.
// After the store closed the transaction is dropped without Discard, as the
// read snapshot's (DISCARDCLOSE: badger's Discard after db.Close sends to a
// 100-slot channel nobody drains, so the 101st blocks forever).
func (s *scanSnapshot) close() {
	if s.it != nil {
		s.it.Close()
		s.it = nil
	}
	s.last = nil
	if s.txn != nil {
		s.bs.endReadTxn(s.txn, s.pin, !s.bs.isClosingOrClosed())
		s.txn = nil
	}
}
