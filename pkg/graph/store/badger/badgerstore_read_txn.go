package badger

import (
	"sync/atomic"

	badgerv4 "github.com/dgraph-io/badger/v4"
)

// readTxnsTotal counts the badger read transactions the store's read paths
// open (every bs.view and every scan or read snapshot), across every store of
// the process: the seam of the transactions-per-scan tests (backlog 55).
var readTxnsTotal atomic.Int64

// view is db.View, counted.
func (bs *Store) view(fn func(txn *badgerv4.Txn) error) error {
	readTxnsTotal.Add(1)
	return bs.db.View(fn)
}

// newReadTxn opens a read transaction, counted; the caller discards it.
func newReadTxn(db *badgerv4.DB) *badgerv4.Txn {
	readTxnsTotal.Add(1)
	return db.NewTransaction(false)
}
