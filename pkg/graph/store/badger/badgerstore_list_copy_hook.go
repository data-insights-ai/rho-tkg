package badger

import "sync/atomic"

var currentMemberCopyCounter atomic.Pointer[atomic.Int64]
var exAdjCopyCounter atomic.Pointer[atomic.Int64]
var validEndMemberCopyCounter atomic.Pointer[atomic.Int64]

func noteCurrentMemberCopied() {
	if c := currentMemberCopyCounter.Load(); c != nil {
		c.Add(1)
	}
}
func noteExAdjacentCopied() {
	if c := exAdjCopyCounter.Load(); c != nil {
		c.Add(1)
	}
}

// CountCurrentMembersCopiedForTest observes native current-ID copies independently
// of the meter. Tests using this process-wide observer run alone.
func CountCurrentMembersCopiedForTest() (copied func() int64, stop func()) {
	c := new(atomic.Int64)
	currentMemberCopyCounter.Store(c)
	return c.Load, func() { currentMemberCopyCounter.Store(nil) }
}

// CountExAdjacentCopiedForTest observes native ex-adjacency ID copies. Tests
// using this process-wide observer run alone.
func CountExAdjacentCopiedForTest() (copied func() int64, stop func()) {
	c := new(atomic.Int64)
	exAdjCopyCounter.Store(c)
	return c.Load, func() { exAdjCopyCounter.Store(nil) }
}

// CountValidEndMembersCopiedForTest observes IDs copied into the on-disk
// label's temporary membership set, independently of the meter. Tests run alone.
func CountValidEndMembersCopiedForTest() (copied func() int64, stop func()) {
	c := new(atomic.Int64)
	validEndMemberCopyCounter.Store(c)
	return c.Load, func() { validEndMemberCopyCounter.Store(nil) }
}

func noteValidEndMemberCopied() {
	if c := validEndMemberCopyCounter.Load(); c != nil {
		c.Add(1)
	}
}
