package badger

import (
	"errors"
	"testing"

	badgerv4 "github.com/dgraph-io/badger/v4"
)

// BACKLOG 18o: commitPropertyIndexOnDiskBackfill / commitTemporalIndexOnDiskBackfill
// use bs.db.NewWriteBatch()+wb.Flush(), the exact API class that hangs forever
// (WaitForMark on a stopped oracle) when Flush is called after db.Close() —
// unlike bs.db.Update, which self-guards via IsClosed() (see the flush()
// guard and its own test, TestFlushWithClosedDB). These two backfill commits
// lacked the matching dbClosed guard flush() has. Currently unreachable (the
// backfill runs during New()'s own construction, before any caller could
// hold a Store to Close()), but a landmine for a future refactor — pins the
// guard the same way flush()'s own dbClosed test does.

func TestCommitPropertyIndexOnDiskBackfill_DbClosedGuard(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	bs.dbClosed.Store(true)
	err := bs.commitPropertyIndexOnDiskBackfill([]writeOp{{opType: writeOpSet, key: []byte("k"), value: []byte("v")}})
	bs.dbClosed.Store(false) // allow t.Cleanup Close() to proceed

	if !errors.Is(err, badgerv4.ErrDBClosed) {
		t.Fatalf("commitPropertyIndexOnDiskBackfill err = %v, want ErrDBClosed", err)
	}
}

func TestCommitTemporalIndexOnDiskBackfill_DbClosedGuard(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	bs.dbClosed.Store(true)
	err := bs.commitTemporalIndexOnDiskBackfill([]writeOp{{opType: writeOpSet, key: []byte("k"), value: []byte("v")}})
	bs.dbClosed.Store(false) // allow t.Cleanup Close() to proceed

	if !errors.Is(err, badgerv4.ErrDBClosed) {
		t.Fatalf("commitTemporalIndexOnDiskBackfill err = %v, want ErrDBClosed", err)
	}
}
