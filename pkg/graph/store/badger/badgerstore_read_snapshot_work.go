package badger

import "sync/atomic"

var historyBytesCopiedCounter atomic.Pointer[atomic.Int64]

func noteHistoryBytesCopied(n int) {
	if c := historyBytesCopiedCounter.Load(); c != nil {
		c.Add(int64(n))
	}
}

// CountHistoryBytesCopiedForTest counts retained raw history bytes copied by
// overlays, snapshots and as-of reads, independently of a meter. Tests run alone.
func CountHistoryBytesCopiedForTest() (copied func() int64, stop func()) {
	c := new(atomic.Int64)
	historyBytesCopiedCounter.Store(c)
	return c.Load, func() { historyBytesCopiedCounter.Store(nil) }
}

// A separate observer isolates actual anchor copies from overlay/run copies.
var historyAnchorBytesCopiedCounter atomic.Pointer[atomic.Int64]

func noteHistoryAnchorBytesCopied(n int) {
	if c := historyAnchorBytesCopiedCounter.Load(); c != nil {
		c.Add(int64(n))
	}
}

// CountHistoryAnchorBytesCopiedForTest counts native history anchor bytes
// copied, independently of admission counters. Tests using it run alone.
func CountHistoryAnchorBytesCopiedForTest() (copied func() int64, stop func()) {
	c := new(atomic.Int64)
	historyAnchorBytesCopiedCounter.Store(c)
	return c.Load, func() { historyAnchorBytesCopiedCounter.Store(nil) }
}
