package index

import (
	"math/rand"
	"sort"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// Probes for the chunked ordered key set that replaced the flat sorted
// slice (the O(D) insert memmove forced a 100k distinct-key cap;
// the chunk split is the new failure surface).

// TestOrderedKeys_ModelEquivalence drives random insert/remove churn
// against a reference model (Go map + re-sorted slice), comparing the FULL
// in-order iteration after every batch. Catches split aliasing (left/right
// chunks sharing a backing array — appends to one clobber the other),
// directory mis-ordering, and lost/duplicated keys around chunk
// boundaries.
func TestOrderedKeys_ModelEquivalence(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // deterministic test
	var o sortedChunks[float64]
	model := map[float64]struct{}{}

	check := func(step int) {
		want := make([]float64, 0, len(model))
		for k := range model {
			want = append(want, k)
		}
		sort.Float64s(want)
		var got []float64
		o.forEachFrom(-1e308, func(k float64) bool {
			got = append(got, k)
			return true
		})
		if len(got) != len(want) || o.n != len(want) {
			t.Fatalf("step %d: %d keys iterated, n=%d, model has %d", step, len(got), o.n, len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("step %d: position %d = %v, want %v", step, i, got[i], want[i])
			}
		}
	}

	for step := 0; step < 200; step++ {
		// Batches large enough to force many splits (chunk cap 1024).
		for i := 0; i < 100; i++ {
			k := float64(rng.Intn(40_000))
			if rng.Intn(3) == 0 {
				if _, ok := model[k]; ok {
					delete(model, k)
					o.remove(k)
				}
			} else {
				if _, ok := model[k]; !ok {
					model[k] = struct{}{}
					o.insert(k)
				}
			}
		}
		check(step)
	}
}

// assertNoMissedChunkMerge fails if two adjacent chunks are both below half
// occupancy AND would fit within the split cap if merged — i.e. a merge
// opportunity mergeIfUndersized should have taken was left on the table.
// BACKLOG 16l.
func assertNoMissedChunkMerge(t *testing.T, o *sortedChunks[float64]) {
	t.Helper()
	for i := 0; i+1 < len(o.chunks); i++ {
		a, b := o.chunks[i], o.chunks[i+1]
		if len(a) < orderedKeyChunk/2 && len(b) < orderedKeyChunk/2 && len(a)+len(b) <= 2*orderedKeyChunk {
			t.Fatalf("adjacent chunks %d (%d keys) and %d (%d keys) are both undersized and mergeable but weren't merged",
				i, len(a), i+1, len(b))
		}
	}
}

// TestOrderedKeys_RemoveMergesUndersizedAdjacentChunks (BACKLOG 16l) drives
// heavy insert/remove churn concentrated so that many chunks shrink well
// below half occupancy without ever fully draining to empty (the only case
// the pre-fix remove() shrank the directory for), and asserts after every
// remove that no adjacent pair of undersized, mergeable chunks was left
// unmerged. A long-lived high-churn property index that never happens to
// drain a chunk to exactly zero would otherwise carry an ever-growing chunk
// directory relative to its live key count, degrading chunkIdx's binary
// search and full-range iteration.
func TestOrderedKeys_RemoveMergesUndersizedAdjacentChunks(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(7)) //nolint:gosec // deterministic test
	var o sortedChunks[float64]
	live := map[float64]struct{}{}

	const universe = 6_000 // enough distinct keys to force several splits
	for i := 0; i < universe; i++ {
		k := float64(i)
		o.insert(k)
		live[k] = struct{}{}
	}
	if len(o.chunks) < 3 {
		t.Fatalf("setup: only %d chunks after inserting %d keys, want several", len(o.chunks), universe)
	}

	// Remove ~97% of keys in random order, checking the no-missed-merge
	// invariant after every single removal — this is exactly the churn
	// pattern (heavy deletion, never draining every chunk to zero at once)
	// that leaves a long tail of tiny fragments without merge-on-shrink.
	order := make([]float64, 0, universe)
	for k := range live {
		order = append(order, k)
	}
	rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })

	removeCount := universe * 97 / 100
	for i := 0; i < removeCount; i++ {
		k := order[i]
		o.remove(k)
		delete(live, k)
		assertNoMissedChunkMerge(t, &o)
	}

	if o.n != len(live) {
		t.Fatalf("o.n = %d, want %d", o.n, len(live))
	}
	// With ~3% of keys surviving (well under one chunk's worth), the whole
	// remaining set should have collapsed toward very few chunks instead of
	// still carrying the original several-chunk directory from setup.
	if got := len(o.chunks); got > 2 {
		t.Fatalf("after draining to %d live keys, chunk directory still has %d chunks — undersized chunks were not merged",
			len(live), got)
	}
}

// TestOrderedKeys_ForEachFromMidRange pins the iteration lower bound across
// chunk boundaries: starting exactly at, just below, and just above a key
// that sits at a chunk edge after forced splits.
func TestOrderedKeys_ForEachFromMidRange(t *testing.T) {
	t.Parallel()
	var o sortedChunks[float64]
	const n = 5_000 // ~5 chunks after splits
	for i := 0; i < n; i++ {
		o.insert(float64(i) * 2) // evens 0..9998
	}
	for _, lo := range []float64{0, 1, 2, 4097, 4098, 4099, 9998, 9999} {
		var got []float64
		o.forEachFrom(lo, func(k float64) bool {
			got = append(got, k)
			return len(got) < 3
		})
		// First emitted key must be the smallest even >= lo.
		want := lo
		if w := int(lo); float64(w) != lo || w%2 != 0 {
			want = float64((int(lo)/2 + 1) * 2)
		}
		if lo > 9998 {
			if len(got) != 0 {
				t.Fatalf("lo=%v: emitted %v, want none", lo, got)
			}
			continue
		}
		if len(got) == 0 || got[0] != want {
			t.Fatalf("lo=%v: first emitted %v, want %v", lo, got, want)
		}
	}
}

// TestRangeNodeIDs_Past100kDistinctKeys is the cap-lift regression: the
// flat-slice implementation DISABLED the ordered view past 100k distinct
// keys (rangeDisabled — supported=false, callers fell back to label
// scans). The chunked set must stay supported and correct.
func TestRangeNodeIDs_Past100kDistinctKeys(t *testing.T) {
	t.Parallel()
	pi := NewPropertyIndex()
	const n = 150_000
	for i := 0; i < n; i++ {
		pi.Add(snowflake.ID(i+1), int64(i))
	}

	ids, supported := pi.RangeNodeIDs(120_000, 120_004, true, true)
	if !supported {
		t.Fatal("ordered view unsupported past 100k distinct keys — cap not lifted")
	}
	if len(ids) != 5 {
		t.Fatalf("range returned %d candidates, want 5", len(ids))
	}
	seen := map[int64]bool{}
	for _, id := range ids {
		seen[int64(id)] = true
	}
	for v := 120_000; v <= 120_004; v++ {
		if !seen[int64(v+1)] {
			t.Fatalf("missing id %d in range result", v+1)
		}
	}

	// Removal keeps the view consistent at scale.
	pi.Remove(snowflake.ID(120_003), int64(120_002))
	ids, supported = pi.RangeNodeIDs(120_000, 120_004, true, true)
	if !supported || len(ids) != 4 {
		t.Fatalf("after remove: supported=%v len=%d, want true/4", supported, len(ids))
	}
}
