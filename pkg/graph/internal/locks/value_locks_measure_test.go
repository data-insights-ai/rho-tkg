//go:build measure

// Measurement level (René 4 Nov, `scripts/measure.sh`): testing.AllocsPerRun reads
// the process-wide malloc count, and this package runs parallel tests, so the
// bound runs by hand, not in a gate.

package locks

import "testing"

// TestValueStripeZeroAllocs pins the BACKLOG 15g perf fix: ValueStripe must
// not allocate. The prior fnv.New64a()-based implementation allocated its
// concrete *sum64a to the heap on every call (hash.Hash64 is an interface),
// once per constrained property per node write — a hot path.
func TestValueStripeZeroAllocs(t *testing.T) {
	value := []byte("s:alice@example.com")
	allocs := testing.AllocsPerRun(100, func() {
		_ = ValueStripe(3, 9, value)
	})
	if allocs != 0 {
		t.Fatalf("ValueStripe allocated %.1f times per call, want 0", allocs)
	}
}
