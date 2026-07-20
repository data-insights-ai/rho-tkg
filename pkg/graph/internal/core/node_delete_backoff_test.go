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
func TestNodeDeleteRetryBackoff_BoundedAndCapped(t *testing.T) {
	t.Parallel()

	maxAtCap := nodeDeleteRetryBackoffBase << nodeDeleteRetryBackoffCap

	for _, attempt := range []int{0, 1, nodeDeleteRetryBackoffCap, nodeDeleteRetryBackoffCap + 1, 100} {
		for i := 0; i < 20; i++ {
			start := time.Now()
			nodeDeleteRetryBackoff(attempt)
			elapsed := time.Since(start)
			if elapsed < 0 {
				t.Fatalf("attempt %d: negative elapsed duration %v", attempt, elapsed)
			}
			// Generous upper bound: the randomized sleep itself is capped at
			// maxAtCap, plus scheduling slack.
			if elapsed > maxAtCap+50*time.Millisecond {
				t.Fatalf("attempt %d: slept %v, want <= ~%v (capped)", attempt, elapsed, maxAtCap)
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
