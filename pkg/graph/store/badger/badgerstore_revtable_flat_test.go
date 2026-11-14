package badger

import (
	"math/rand/v2"
	"testing"
)

// TestFlatRevTableMatchesTheMadeTable: a flat table (the 128 KB of stripes not
// made, every stripe still 1) answers get, and moves through bump and bumpAll,
// as a made table does, so a store that keeps the flat one until its first
// write (a transient read-only open) reads and compares revisions as before.
// A random run of operations on both, compared after every step on sampled
// IDs. Catches a flat get that answers 0 (a live entity then reads as absent),
// a bump from flat that restarts the counter and a bumpAll that misses the
// stripes the flat table never held.
func TestFlatRevTableMatchesTheMadeTable(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(7, 8))
	for run := 0; run < 20; run++ {
		made, flat := newRevTable(), newFlatRevTable()
		sample := make([]int64, 40)
		for i := range sample {
			sample[i] = 1 + rng.Int64N(1<<40)
		}
		check := func(when string) {
			t.Helper()
			for _, id := range sample {
				if got, want := flat.get(id), made.get(id); got != want || got == 0 {
					t.Fatalf("run %d %s: flat get(%d) = %d, made %d", run, when, id, got, want)
				}
			}
			if flat.next != made.next {
				t.Fatalf("run %d %s: flat next %d, made %d", run, when, flat.next, made.next)
			}
		}
		check("untouched")
		for step := 0; step < 30; step++ {
			switch rng.IntN(6) {
			case 0:
				flat.bumpAll()
				made.bumpAll()
			default:
				id := sample[rng.IntN(len(sample))]
				flat.bump(id)
				made.bump(id)
			}
			check("after a write")
		}
	}
}
