package badger

import "sync/atomic"

// indexVisitCounter (tests only) counts the iterator steps every on-disk index
// walk of the process makes: the property index equality lookup and the member
// entry walks. It is nil in production, so a step pays one atomic load.
var indexVisitCounter atomic.Pointer[atomic.Int64]

// noteIndexEntryVisited counts one iterator step of an on-disk index walk.
func noteIndexEntryVisited() {
	if c := indexVisitCounter.Load(); c != nil {
		c.Add(1)
	}
}

// CountIndexEntriesVisitedForTest starts counting the iterator steps of every
// on-disk index walk in this process (the property index equality lookup, the
// member entry walks, the paged member walk, the on-disk adjacency walk of a
// node's snapshot) and returns the count so far and
// the function that stops counting. Not concurrency-safe across tests: call it
// from a test that runs alone. Not for production use.
func CountIndexEntriesVisitedForTest() (visited func() int64, stop func()) {
	c := new(atomic.Int64)
	indexVisitCounter.Store(c)
	return c.Load, func() { indexVisitCounter.Store(nil) }
}

// txMemberCopyCounter (tests only) counts the ever-members every sidecar copy
// of the process makes for a label or type gather (emitTxMemberCopy): the
// native count of the work a gather's hold bounds, which no read meter keeps.
var txMemberCopyCounter atomic.Pointer[atomic.Int64]

func noteTxMembersCopied(n int) {
	if c := txMemberCopyCounter.Load(); c != nil {
		c.Add(int64(n))
	}
}

// CountTxMembersCopiedForTest starts counting the ever-members the sidecar
// copies of this process make; stop ends it. Tests that use it must not run in
// parallel with other gathers.
func CountTxMembersCopiedForTest() (copied func() int64, stop func()) {
	c := new(atomic.Int64)
	txMemberCopyCounter.Store(c)
	return c.Load, func() { txMemberCopyCounter.Store(nil) }
}
