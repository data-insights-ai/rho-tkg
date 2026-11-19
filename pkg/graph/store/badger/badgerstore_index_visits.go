package badger

import "sync/atomic"

// indexVisitCounter (tests only) counts the iterator steps every on-disk index
// walk of the process makes: the property index equality lookup and the member
// entry walks. It is nil in production, so a step pays one atomic load.
var indexVisitCounter atomic.Pointer[atomic.Int64]

var pendingVisitCounter atomic.Pointer[atomic.Int64]

var membershipDecodeCounter atomic.Pointer[atomic.Int64]

func noteMembershipDecoded() {
	if c := membershipDecodeCounter.Load(); c != nil {
		c.Add(1)
	}
}

// CountMembershipDecodesForTest counts actual membership wire decodes. Tests
// using the process-wide counter run alone.
func CountMembershipDecodesForTest() (decoded func() int64, stop func()) {
	c := new(atomic.Int64)
	membershipDecodeCounter.Store(c)
	return c.Load, func() { membershipDecodeCounter.Store(nil) }
}

func notePendingKeyVisited() {
	if c := pendingVisitCounter.Load(); c != nil {
		c.Add(1)
	}
}

// CountPendingKeysVisitedForTest counts entries actually visited by pending
// buffer walks, independently of their read meter. Tests using it run alone.
func CountPendingKeysVisitedForTest() (visited func() int64, stop func()) {
	c := new(atomic.Int64)
	pendingVisitCounter.Store(c)
	return c.Load, func() { pendingVisitCounter.Store(nil) }
}

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

// labelTxMemberRecordCounter (tests only) counts the former members the label
// membership sidecars of the process take in (a node new under a label): the
// native count of what a sidecar keeps in RAM, which no read meter keeps.
var labelTxMemberRecordCounter atomic.Pointer[atomic.Int64]

func noteLabelTxMemberRecorded() {
	if c := labelTxMemberRecordCounter.Load(); c != nil {
		c.Add(1)
	}
}

// CountLabelTxMembersRecordedForTest starts counting the former members the
// label membership sidecars of this process take in; stop ends it. Tests that
// use it must not run in parallel with other writers or pinned scans.
func CountLabelTxMembersRecordedForTest() (recorded func() int64, stop func()) {
	c := new(atomic.Int64)
	labelTxMemberRecordCounter.Store(c)
	return c.Load, func() { labelTxMemberRecordCounter.Store(nil) }
}
