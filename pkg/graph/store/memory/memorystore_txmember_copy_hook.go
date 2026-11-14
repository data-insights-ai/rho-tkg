package memory

import "sync/atomic"

// txMemberCopyCounter (tests only) counts the ever-members every sidecar copy
// of the process makes for a label or type gather (emitMemoryTxMemberCopy):
// the native count of the copy, which no read meter keeps, called by the copy
// itself. nil in production, so a copy pays one atomic load.
var txMemberCopyCounter atomic.Pointer[atomic.Int64]

var txMemberBuildCounter atomic.Pointer[atomic.Int64]

func noteTxMemberBuildRow() {
	if c := txMemberBuildCounter.Load(); c != nil {
		c.Add(1)
	}
}

// CountTxMemberBuildRowsForTest counts rows actually fed to a membership
// build, independently of its meter. Tests using it run alone.
func CountTxMemberBuildRowsForTest() (visited func() int64, stop func()) {
	c := new(atomic.Int64)
	txMemberBuildCounter.Store(c)
	return c.Load, func() { txMemberBuildCounter.Store(nil) }
}

func noteTxMembersCopied(n int) {
	if c := txMemberCopyCounter.Load(); c != nil {
		c.Add(int64(n))
	}
}

// CountTxMembersCopiedForTest starts counting the ever-members the memory
// store's sidecar copies of this process make; stop ends it. Tests that use it
// must not run in parallel with other gathers. Not for production use.
func CountTxMembersCopiedForTest() (copied func() int64, stop func()) {
	c := new(atomic.Int64)
	txMemberCopyCounter.Store(c)
	return c.Load, func() { txMemberCopyCounter.Store(nil) }
}
