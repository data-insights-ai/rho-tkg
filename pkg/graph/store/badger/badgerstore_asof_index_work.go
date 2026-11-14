package badger

import "sync/atomic"

var asOfIndexDecodeCounter, asOfIndexProbeCounter, asOfIndexCopyCounter atomic.Pointer[atomic.Int64]

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
