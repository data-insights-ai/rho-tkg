package badger

import "sync/atomic"

var asOfIndexDecodeCounter, asOfIndexProbeCounter, asOfIndexCopyCounter, asOfIndexSeekCounter atomic.Pointer[atomic.Int64]

// Tests can commit a replacement anchor after selection but before decode.
// Like the native observers below, the test using this seam runs alone.
var asOfIndexedBeforeDecodeTestHook func()

func noteAsOfIndexProbe() {
	if c := asOfIndexProbeCounter.Load(); c != nil {
		c.Add(1)
	}
}

func noteAsOfIndexCopy(n int) {
	if c := asOfIndexCopyCounter.Load(); c != nil {
		c.Add(int64(n))
	}
}

func noteAsOfIndexDecode() {
	if c := asOfIndexDecodeCounter.Load(); c != nil {
		c.Add(1)
	}
}

func noteAsOfIndexSeekEntry() {
	if c := asOfIndexSeekCounter.Load(); c != nil {
		c.Add(1)
	}
}

// CountAsOfIndexSeekEntriesForTest observes the as-of seek keys an indexed
// read visits: one per answer the index gives. Unlike
// CountIndexEntriesVisitedForTest it counts no other index's walk, so a read
// that probes the proof and then takes the scan visits none. Tests using it
// run alone.
func CountAsOfIndexSeekEntriesForTest() (visited func() int64, stop func()) {
	c := new(atomic.Int64)
	asOfIndexSeekCounter.Store(c)
	return c.Load, func() { asOfIndexSeekCounter.Store(nil) }
}

// CountAsOfIndexDecodesForTest observes actual proof, generation and seek-key
// scalar decodes independently of read admission. Tests using it run alone.
func CountAsOfIndexDecodesForTest() (decoded func() int64, stop func()) {
	c := new(atomic.Int64)
	asOfIndexDecodeCounter.Store(c)
	return c.Load, func() { asOfIndexDecodeCounter.Store(nil) }
}

// CountAsOfIndexProbesForTest observes native proof/generation point probes,
// including misses. Tests using it run alone.
func CountAsOfIndexProbesForTest() (probes func() int64, stop func()) {
	c := new(atomic.Int64)
	asOfIndexProbeCounter.Store(c)
	return c.Load, func() { asOfIndexProbeCounter.Store(nil) }
}

// CountAsOfIndexBytesCopiedForTest observes retained proof/generation bytes.
// Tests using it run alone.
func CountAsOfIndexBytesCopiedForTest() (copied func() int64, stop func()) {
	c := new(atomic.Int64)
	asOfIndexCopyCounter.Store(c)
	return c.Load, func() { asOfIndexCopyCounter.Store(nil) }
}

// asOfHookWork is the as-of index's native Badger work outside the reads'
// own probes: the write hook's point reads that leave both write buffers, its
// first-row iterator seeks, and the committed views opened (by a flush, an
// idle tick, Clear, open or update's renewal; neither the hook nor an indexed
// read opens a transaction) and ended (snaps - ended views are alive: the
// installed one and those a read still holds). A nil counter (no test
// observing) costs one atomic load per event.
type asOfHookWork struct{ gets, seeks, snaps, ended atomic.Int64 }

var asOfHookCounter atomic.Pointer[asOfHookWork]

func noteAsOfHookGet() {
	if c := asOfHookCounter.Load(); c != nil {
		c.gets.Add(1)
	}
}

func noteAsOfHookSeek() {
	if c := asOfHookCounter.Load(); c != nil {
		c.seeks.Add(1)
	}
}

func noteAsOfViewOpened() {
	if c := asOfHookCounter.Load(); c != nil {
		c.snaps.Add(1)
	}
}

func noteAsOfViewEnded() {
	if c := asOfHookCounter.Load(); c != nil {
		c.ended.Add(1)
	}
}

// countAsOfHookWork observes that work process-wide until stop. Tests using
// it run alone.
func countAsOfHookWork() (w *asOfHookWork, stop func()) {
	w = new(asOfHookWork)
	asOfHookCounter.Store(w)
	return w, func() { asOfHookCounter.Store(nil) }
}
