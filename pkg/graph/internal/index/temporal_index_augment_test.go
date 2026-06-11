package index

import (
	"math/rand"
	"sort"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// brute-force reference for QueryAt: from <= t AND (to == 0 OR to > t).
func refQueryAt(entries []IntervalEntry, t types.Instant) []snowflake.ID {
	var ids []snowflake.ID
	for _, e := range entries {
		if e.From <= t && (e.To == 0 || e.To > t) {
			ids = append(ids, e.ID)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// brute-force reference for QueryOverlap: from < end AND (to == 0 OR to > start).
func refQueryOverlap(entries []IntervalEntry, start, end types.Instant) []snowflake.ID {
	if start >= end {
		return nil
	}
	var ids []snowflake.ID
	for _, e := range entries {
		if e.From < end && (e.To == 0 || e.To > start) {
			ids = append(ids, e.ID)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func eqIDs(a, b []snowflake.ID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestTemporalIndex_AugmentEquivalence drives the maxTo-augmented stabbing search
// against a brute-force reference across many randomized interval sets and probes.
// Any pruning-logic error (a subtree wrongly skipped, a right-branch cut too early,
// an open-ended interval mis-handled) shows up as a mismatch. This is the primary
// correctness gate for the O(log n + k) augmentation introduced in X2.
func TestTemporalIndex_AugmentEquivalence(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(0xA11CE))

	for trial := 0; trial < 200; trial++ {
		n := rng.Intn(60) // 0..59 intervals — includes empty + odd/even sizes
		ti := NewTemporalIndex()
		ref := make([]IntervalEntry, 0, n)
		for i := 0; i < n; i++ {
			id := snowflake.ID(i + 1)
			from := types.Instant(rng.Intn(50)) // dense range forces collisions/expiries
			var to types.Instant
			switch rng.Intn(3) {
			case 0:
				to = 0 // open-ended
			default:
				// finite: span 0..30 beyond from; may equal from (degenerate empty interval)
				to = from + types.Instant(rng.Intn(30))
			}
			ti.Add(id, from, to)
			ref = append(ref, IntervalEntry{From: from, To: to, ID: id})
		}

		for probe := 0; probe < 40; probe++ {
			tp := types.Instant(rng.Intn(60) - 5) // probe slightly outside the value range too
			if got, want := ti.QueryAt(tp), refQueryAt(ref, tp); !eqIDs(got, want) {
				t.Fatalf("trial %d QueryAt(%d) = %v, want %v", trial, tp, got, want)
			}
			a := types.Instant(rng.Intn(60) - 5)
			b := types.Instant(rng.Intn(60) - 5)
			if got, want := ti.QueryOverlap(a, b), refQueryOverlap(ref, a, b); !eqIDs(got, want) {
				t.Fatalf("trial %d QueryOverlap(%d,%d) = %v, want %v", trial, a, b, got, want)
			}
		}
	}
}

// BenchmarkTemporalIndex_QueryAt_ManyExpired exercises the case the old O(n) scan
// handled worst: a large index where most intervals started before the probe but
// already expired, plus a handful still live. The augmentation prunes the expired
// subtrees, so cost tracks the result size, not the index size.
func BenchmarkTemporalIndex_QueryAt_ManyExpired(b *testing.B) {
	ti := NewTemporalIndex()
	const n = 100_000
	for i := 0; i < n; i++ {
		// from in [0, n): all start before the probe at 2n.
		from := types.Instant(i)
		// All but the last 16 expire well before the probe.
		to := from + 10
		if i >= n-16 {
			to = 0 // open-ended — these are the only live ones at the probe
		}
		ti.AddKnownAbsent(snowflake.ID(i+1), from, to)
	}
	probe := types.Instant(2 * n)
	ti.QueryAt(probe) // warm the sort + augmentation
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ti.QueryAt(probe)
	}
}

// TestTemporalIndex_RemoveRebuildsAugmentation removes an entry (which leaves the
// slice sorted but shrinks it) and then queries. Without Remove marking the index
// dirty, the maxTo array would span the pre-removal length and either read a stale
// bound or index out of range. Pins that Remove forces a rebuild.
func TestTemporalIndex_RemoveRebuildsAugmentation(t *testing.T) {
	t.Parallel()
	ti := NewTemporalIndex()
	ti.Add(snowflake.ID(1), 10, 100)
	ti.Add(snowflake.ID(2), 20, 0) // open-ended — its effTo is the subtree max
	ti.Add(snowflake.ID(3), 30, 40)

	// Force a build of the augmentation at the full length.
	if got := ti.QueryAt(25); len(got) != 2 { // ids 1 and 2 valid at 25
		t.Fatalf("pre-remove QueryAt(25) = %v, want 2 ids", got)
	}

	// Remove the open-ended entry whose effTo dominated subMax.
	ti.Remove(snowflake.ID(2))

	// QueryAt must reflect the removal with a correctly rebuilt augmentation.
	if got := ti.QueryAt(25); len(got) != 1 || got[0] != snowflake.ID(1) {
		t.Fatalf("post-remove QueryAt(25) = %v, want [1]", got)
	}
	if got := ti.QueryAt(35); len(got) != 2 { // ids 1 and 3
		t.Fatalf("post-remove QueryAt(35) = %v, want 2 ids", got)
	}
	if got := ti.QueryOverlap(0, 1000); len(got) != 2 {
		t.Fatalf("post-remove QueryOverlap = %v, want ids 1 and 3", got)
	}
}
