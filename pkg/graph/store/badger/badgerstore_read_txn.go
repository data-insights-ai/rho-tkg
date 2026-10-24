package badger

import (
	"fmt"
	"sync/atomic"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// readTxnsTotal counts the badger read transactions the store's read paths
// open (every bs.view and every scan or read snapshot), across every store of
// the process: the seam of the transactions-per-scan tests (backlog 55).
// openReadTxns counts the snapshot transactions open right now (a View's
// lives only inside the call): the seam that no snapshot outlives the reads
// it serves.
var readTxnsTotal, openReadTxns atomic.Int64

// view is db.View, counted.
func (bs *Store) view(fn func(txn *badgerv4.Txn) error) (err error) {
	readTxnsTotal.Add(1)
	defer recoverBadgerPanic(&err)
	return bs.db.View(fn)
}

// recoverBadgerPanic turns a panic out of badger into ErrCorruptWire. badger
// parses a table block's trailer before it verifies the block's checksum
// (table/table.go:575-604), so a damaged trailer panics the reader even under
// ChecksumVerificationMode OnBlockRead (VERIFY; badgerstore_verify_test.go).
// The read fails; the process lives.
func recoverBadgerPanic(err *error) {
	if p := recover(); p != nil {
		*err = fmt.Errorf("%w: badger panicked reading the store (a damaged table): %v", storecontract.ErrCorruptWire, p)
	}
}

// newReadTxn opens a read transaction, counted; the caller ends it with
// endReadTxn.
func newReadTxn(db *badgerv4.DB) *badgerv4.Txn {
	readTxnsTotal.Add(1)
	openReadTxns.Add(1)
	return db.NewTransaction(false)
}

// endReadTxn ends a transaction newReadTxn opened; discard is false once the
// database closed (its read mark died with it).
func endReadTxn(txn *badgerv4.Txn, discard bool) {
	openReadTxns.Add(-1)
	if discard {
		txn.Discard()
	}
}
