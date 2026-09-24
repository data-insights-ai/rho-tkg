package badger

import (
	"bytes"

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
	db     *badgerv4.DB
	epoch  func() uint64 // the flush epoch of the cache the reader consults
	pinned uint64        // epoch read immediately before txn opened
	txn    *badgerv4.Txn
	it     *badgerv4.Iterator
}

func newScanSnapshot(db *badgerv4.DB, epoch func() uint64) *scanSnapshot {
	return &scanSnapshot{db: db, epoch: epoch}
}

// txnAfterMiss returns a read transaction that holds the latest committed
// version of any row the cache has released. Call it only AFTER the cache
// missed for the row about to be read: the epoch must be read after the miss.
func (s *scanSnapshot) txnAfterMiss() *badgerv4.Txn {
	e := s.epoch()
	if s.txn == nil || e != s.pinned {
		s.close()
		s.pinned = e
		s.txn = s.db.NewTransaction(false)
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
		s.txn = s.db.NewTransaction(false)
	}
	return s.txn
}

// seekAfterMiss returns the item stored exactly at key, or nil when the key is
// absent. Same precondition as txnAfterMiss. Keys must be sought in ascending
// order between re-pins; the iterator is re-created with each new snapshot.
//
// PrefetchValues=false is LOAD-BEARING: the iterator does a Seek per target ID
// (not a linear Next-scan), and badger's value prefetch re-fills a value window
// on every Seek, almost all of it discarded. prefetch=true is much SLOWER than
// the per-row Txn.Get path it replaced. Do not flip this.
func (s *scanSnapshot) seekAfterMiss(key []byte) *badgerv4.Item {
	txn := s.txnAfterMiss() // a re-pin closes the old iterator
	if s.it == nil {
		iopts := badgerv4.DefaultIteratorOptions
		iopts.PrefetchValues = false
		s.it = txn.NewIterator(iopts)
	}
	s.it.Seek(key)
	if !s.it.Valid() {
		return nil // past end of keyspace — absent
	}
	item := s.it.Item()
	if !bytes.Equal(item.Key(), key) {
		return nil // key not present — deleted / orphaned index entry
	}
	return item
}

// close releases the iterator and transaction. Safe to call repeatedly.
func (s *scanSnapshot) close() {
	if s.it != nil {
		s.it.Close()
		s.it = nil
	}
	if s.txn != nil {
		s.txn.Discard()
		s.txn = nil
	}
}
