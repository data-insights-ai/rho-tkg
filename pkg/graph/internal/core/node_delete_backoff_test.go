package core

import (
	"testing"
	"time"
)

// TestNodeDeleteRetryBackoff_BoundedAndCapped guards the TOCTOU retry
// backoff's computed duration: non-negative, below base<<attempt, and capped —
// nodeDeleteRetryBackoffCap caps the exponent so even a pathological attempt
// count keeps the max wait small. It asserts the duration
// nodeDeleteRetryBackoffDuration returns, not that nodeDeleteRetryBackoff
// sleeps it (the sleep has no injection seam). Timing a real sleep measured the
// scheduler: CI once overshot 50 ms of slack at 61.8 ms.
func TestNodeDeleteRetryBackoff_BoundedAndCapped(t *testing.T) {
	t.Parallel()

	maxAtCap := nodeDeleteRetryBackoffBase << nodeDeleteRetryBackoffCap

	for _, attempt := range []int{0, 1, nodeDeleteRetryBackoffCap, nodeDeleteRetryBackoffCap + 1, 100} {
		bound := nodeDeleteRetryBackoffBase << min(attempt, nodeDeleteRetryBackoffCap)
		for i := 0; i < 1000; i++ { // many more draws than the timed version could afford
			d := nodeDeleteRetryBackoffDuration(attempt)
			if d < 0 {
				t.Fatalf("attempt %d: negative backoff %v", attempt, d)
			}
			if d >= bound {
				t.Fatalf("attempt %d: backoff %v, want < %v (base << attempt)", attempt, d, bound)
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

	// Samples the COMPUTED duration, not an observed sleep. Timing real sleeps made
	// this a scheduler measurement: CI reported attempt=0 mean=2.002993ms when the
	// whole range at attempt 0 is 50µs, i.e. the wake-up latency dwarfed the value
	// under test and buried the signal. The distribution is the property; the sleep
	// is not.
	sample := func(attempt int, n int) time.Duration {
		var total time.Duration
		for i := 0; i < n; i++ {
			total += nodeDeleteRetryBackoffDuration(attempt)
		}
		return total / time.Duration(n)
	}

	const n = 20_000 // free now that no sleeping happens; tightens the statistic
	low := sample(0, n)
	high := sample(nodeDeleteRetryBackoffCap, n)
	if high <= low {
		t.Fatalf("mean backoff did not grow with attempt: attempt=0 mean=%v, attempt=%d mean=%v", low, nodeDeleteRetryBackoffCap, high)
	}
}
