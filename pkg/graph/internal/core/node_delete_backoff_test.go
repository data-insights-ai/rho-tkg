package core

import (
	"testing"
	"time"
)

// TestNodeDeleteRetryBackoff_BoundedAndCapped guards BACKLOG 9r: the TOCTOU
// retry backoff must actually sleep a bounded, randomized duration (not the
// unconditional runtime.Gosched() it replaced) and must not grow unbounded
// as the attempt number increases — nodeDeleteRetryBackoffCap caps the
// exponent so even a pathological attempt count keeps the max sleep small.
// Asserts the COMPUTED duration, not observed wall clock. It used to time an
// actual sleep with 50ms of slack and still failed on CI at 61.8ms: a shared
// runner can park a goroutine far past any slack a correctness test should carry,
// so that assertion was measuring the scheduler rather than the code. The bound
// and the cap are what BACKLOG 9r guards, and both are properties of the returned
// duration.
func TestNodeDeleteRetryBackoff_BoundedAndCapped(t *testing.T) {
	t.Parallel()

	maxAtCap := nodeDeleteRetryBackoffBase << nodeDeleteRetryBackoffCap

	for _, attempt := range []int{0, 1, nodeDeleteRetryBackoffCap, nodeDeleteRetryBackoffCap + 1, 100} {
		for i := 0; i < 1000; i++ { // many more draws than the timed version could afford
			d := nodeDeleteRetryBackoffDuration(attempt)
			if d < 0 {
				t.Fatalf("attempt %d: negative backoff %v", attempt, d)
			}
			if d >= maxAtCap {
				t.Fatalf("attempt %d: backoff %v, want < %v — the exponent cap did not hold",
					attempt, d, maxAtCap)
			}
		}
	}
}

// TestNodeDeleteRetryBackoff_GrowsWithAttempt is a statistical (not exact)
// check that the backoff genuinely scales with the attempt number rather
// than being a fixed sleep — sampling many draws at a low attempt and a high
// (pre-cap) attempt, the high-attempt mean must be clearly larger.
func TestNodeDeleteRetryBackoff_GrowsWithAttempt(t *testing.T) {
	t.Parallel()

	sample := func(attempt int, n int) time.Duration {
		var total time.Duration
		for i := 0; i < n; i++ {
			start := time.Now()
			nodeDeleteRetryBackoff(attempt)
			total += time.Since(start)
		}
		return total / time.Duration(n)
	}

	const n = 200
	low := sample(0, n)
	high := sample(nodeDeleteRetryBackoffCap, n)
	if high <= low {
		t.Fatalf("mean backoff did not grow with attempt: attempt=0 mean=%v, attempt=%d mean=%v", low, nodeDeleteRetryBackoffCap, high)
	}
}
